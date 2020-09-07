package file

import (
	"encoding/binary"
	"io"
)

// An StringEncoderFunc is the signature for a function that can be used to encode a string into its binary
// format.
type StringEncoderFunc func(string) (int, error)

// A BoolEncoderFunc is the signature for a focution that can be used to mark a record as tombstoned.
type BoolEncoderFunc func(bool) (int, error)

// An Encoder encodes DBFileEntry objects.
type Encoder struct {
	enc StringEncoderFunc
	tmb BoolEncoderFunc
}

// NewEncoder creates a new Encoder that will write entries to a writer.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		enc: BuildStringEncoderFunc(w),
		tmb: BuildBoolEncoderFunc(w),
	}
}

// Encode encodes a DBFileEntry to a binary format and writes it to the Encoder's underlying writer.
func (e *Encoder) Encode(entry DBFileEntry) (n int, err error) {
	var (
		nT, nK, nV int
	)

	nT, err = e.tmb(entry.deleted)
	if err != nil {
		return 0, err
	}

	nK, err = e.enc(entry.key)
	if err != nil {
		return 0, err
	}

	// If the record has been deleted, then we don't save the value since that would be a waste of space.
	if !entry.deleted {
		nV, err = e.enc(entry.value)
		if err != nil {
			return 0, err
		}
	}

	return nT + nK + nV, nil
}

// BuildBoolEncoderFunc creates a TombstonerFunc that will write to a specified io.Writer.
func BuildBoolEncoderFunc(w io.Writer) BoolEncoderFunc {
	var err error
	return func(b bool) (int, error) {
		err = binary.Write(w, binary.BigEndian, b)
		if err != nil {
			return 0, err
		}
		return binary.Size(b), nil
	}
}

// BuildStringEncoderFunc builds an EncoderFunc that will write to an io.Writer.
func BuildStringEncoderFunc(w io.Writer) StringEncoderFunc {
	var err error
	return func(s string) (int, error) {
		b := []byte(s)
		n := int16(binary.Size(b))

		err = binary.Write(w, binary.BigEndian, n)
		if err != nil {
			return 0, err
		}

		err = binary.Write(w, binary.BigEndian, b)
		if err != nil {
			return 0, err
		}

		return binary.Size(n) + int(n), nil
	}
}

// EncodeTo is a utility function that will encode a DBFileEntry and write it to an io.Writer.
func EncodeTo(w io.Writer, d DBFileEntry) (int, error) {
	return NewEncoder(w).Encode(d)
}

// A StringDecoderFunc is the signature of a function that can read the binary format of a string into a
// string.
type StringDecoderFunc func(s *string) (int, error)

// A BoolDecoderFunc is the signature of a function that can read the binary format of a bool into a bool.
type BoolDecoderFunc func(b *bool) (int, error)

// A Decoder can decode DBFileEntry objects from a reader.
type Decoder struct {
	dec StringDecoderFunc
	tmb BoolDecoderFunc
}

// NewDecoder creates a new Decoder that will read from an io.Reader.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		dec: BuildStringDecoderFunc(r),
		tmb: BuildBoolDecoderFunc(r),
	}
}

// Decode reads binary data from its io.Reader into a DBFileEntry.
func (d *Decoder) Decode(entry *DBFileEntry) (int, error) {
	var (
		nT, nK, nV int
		err        error
	)

	nT, err = d.tmb(&entry.deleted)
	if err != nil {
		return 0, err
	}

	nK, err = d.dec(&entry.key)
	if err != nil {
		return 0, err
	}

	// Tombstoned records have only a key and a deleted bit.
	if !entry.deleted {
		nV, err = d.dec(&entry.value)
		if err != nil {
			return 0, err
		}
	}

	return nT + nK + nV, nil
}

// BuildBoolDecoderFunc creates a new BoolDecoderFunc that will read from the specified io.Reader.
func BuildBoolDecoderFunc(r io.Reader) BoolDecoderFunc {
	var err error

	return func(b *bool) (int, error) {
		err = binary.Read(r, binary.BigEndian, b)
		if err != nil {
			return 0, err
		}

		return binary.Size(true), nil
	}
}

// BuildStringDecoderFunc builds a DecoderFunc that will read from the specified io.Reader.
func BuildStringDecoderFunc(r io.Reader) StringDecoderFunc {
	var err error
	var n int16
	var nSize = binary.Size(n)

	return func(s *string) (int, error) {
		err = binary.Read(r, binary.BigEndian, &n)
		if err != nil {
			return 0, err
		}

		v := make([]byte, n)
		err = binary.Read(r, binary.BigEndian, v)
		if err != nil {
			return 0, err
		}

		*s = string(v)
		return nSize + int(n), nil
	}
}

// DecodeFrom will read a single DBFileEntry from an io.Reader.
func DecodeFrom(r io.Reader, d *DBFileEntry) (int, error) {
	return NewDecoder(r).Decode(d)
}
