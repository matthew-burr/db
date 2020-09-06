package file

import (
	"bytes"
	"encoding/binary"
	"io"
)

func BuildEncoder(w io.Writer) func(string) (int, error) {
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

func Encode(d DBFileEntry) []byte {
	buf := new(bytes.Buffer)
	EncodeTo(buf, d)
	return buf.Bytes()
}

func EncodeTo(w io.Writer, d DBFileEntry) (int, error) {
	enc := BuildEncoder(w)
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

func BuildDecoder(r io.Reader) func(s *string) (int, error) {
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

func Decode(b []byte) DBFileEntry {
	d := &DBFileEntry{}
	DecodeFrom(bytes.NewBuffer(b), d)
	return *d
}

func DecodeFrom(r io.Reader, d *DBFileEntry) (int, error) {
	dec := BuildDecoder(r)
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
