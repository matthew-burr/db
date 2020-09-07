package file

import (
	"encoding/binary"
	"io"
)

// An EncoderFunc is the signature for a function that can be used to encode a string into its binary
// format.
type EncoderFunc func(string) (int, error)

// An Encoder encodes DBFileEntry objects.
type Encoder struct {
	enc EncoderFunc
}

// NewEncoder creates a new Encoder that will write entries to a writer.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		enc: BuildEncoderFunc(w),
	}
}

// Encode encodes a DBFileEntry to a binary format and writes it to the Encoder's underlying writer.
func (e *Encoder) Encode(entry DBFileEntry) (n int, err error) {
	var (
		nK, nV int
	)

	nK, err = e.enc(entry.Key())
	if err != nil {
		return 0, err
	}

	nV, err = e.enc(entry.Value())
	if err != nil {
		return 0, err
	}

	return nK + nV, nil
}

// BuildEncoderFunc builds an EncoderFunc that will write to an io.Writer.
func BuildEncoderFunc(w io.Writer) EncoderFunc {
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
	enc := BuildEncoderFunc(w)
	var (
		nK, nV int
		err    error
	)

	nK, err = enc(d.Key())
	if err != nil {
		return 0, err
	}

	nV, err = enc(d.Value())
	if err != nil {
		return 0, err
	}

	return nK + nV, nil
}

// A DecoderFunc is the signature of a function that can read the binary format of a string into a
// string.
type DecoderFunc func(s *string) (int, error)

// A Decoder can decode DBFileEntry objects from a reader.
type Decoder struct {
	dec DecoderFunc
}

// NewDecoder creates a new Decoder that will read from an io.Reader.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		dec: BuildDecoderFunc(r),
	}
}

// Decode reads binary data from its io.Reader into a DBFileEntry.
func (d *Decoder) Decode(entry *DBFileEntry) (int, error) {
	var (
		nK, nV int
		err    error
	)
	nK, err = d.dec(&entry.key)
	if err != nil {
		return 0, err
	}

	nV, err = d.dec(&entry.value)
	if err != nil {
		return 0, err
	}
	return nK + nV, nil
}

// BuildDecoderFunc builds a DecoderFunc that will read from the specified io.Reader.
func BuildDecoderFunc(r io.Reader) DecoderFunc {
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
	dec := BuildDecoderFunc(r)
	var (
		nK, nV int
		err    error
	)
	nK, err = dec(&d.key)
	if err != nil {
		return 0, err
	}

	nV, err = dec(&d.value)
	if err != nil {
		return 0, err
	}
	return nK + nV, nil
}
