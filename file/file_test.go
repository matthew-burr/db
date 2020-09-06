package file_test

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncoding(t *testing.T) {
	k, v := "my_key", "my_value"

	enc1 := method1(k, v)
	enc2 := method2(k, v)

	p := func(name string, enc []byte) {
		fmt.Printf("%s\nLength: %d\nRepr: % x\n\n", name, len(enc), enc)
	}

	p("enc1", enc1)
	p("enc2", enc2)

	assert.LessOrEqual(t, len(enc1), len(enc2))
}

func TestDecoding(t *testing.T) {
	k, v := "my_key", "my_value"

	tt := []struct {
		name string
		enc  func(string, string) []byte
		dec  func([]byte) (string, string)
	}{
		// {"method1", method1, method1Dec},
		{"method2", method2, method2Dec},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			gK, gV := tc.dec(tc.enc(k, v))
			assert.Equal(t, k, gK)
			assert.Equal(t, v, gV)
		})
	}
}

func BenchmarkEncodingSpeed(b *testing.B) {
	bt := []struct {
		name string
		f    func(string, string) []byte
	}{
		{"enc1", method1},
		{"enc2", method2},
	}

	for _, bc := range bt {
		b.Run(bc.name, func(b *testing.B) {
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				bc.f("my_key", "my_value")
			}
		})
	}
}

func BenchmarkDecodingSpeed(b *testing.B) {
	bt := []struct {
		name string
		enc  func(string, string) []byte
		dec  func([]byte) (string, string)
	}{
		{"dec1", method1, method1Dec},
		{"dec2", method2, method2Dec},
	}

	for _, bc := range bt {
		b.Run(bc.name, func(b *testing.B) {
			data := bc.enc("my_key", "my_value")
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				bc.dec(data)
			}
		})
	}
}

func method1(key, value string) []byte {
	// Using a custom binary encoding
	bK, bV := []byte(key), []byte(value)
	nK, nV := int16(binary.Size(bK)), int16(binary.Size(bV))
	fields := []interface{}{nK, bK, nV, bV}
	buf := new(bytes.Buffer)
	for _, f := range fields {
		if err := binary.Write(buf, binary.BigEndian, f); err != nil {
			panic(err)
		}
	}
	return buf.Bytes()
}

func method1Dec(b []byte) (key, value string) {
	// Decoding custom encoding
	var (
		bK, bV []byte
		nK, nV int16
	)
	buf := bytes.NewBuffer(b)
	if err := binary.Read(buf, binary.BigEndian, &nK); err != nil {
		panic(err)
	}
	bK = make([]byte, nK)
	if err := binary.Read(buf, binary.BigEndian, bK); err != nil {
		panic(err)
	}

	if err := binary.Read(buf, binary.BigEndian, &nV); err != nil {
		panic(err)
	}
	bV = make([]byte, nV)
	if err := binary.Read(buf, binary.BigEndian, bV); err != nil {
		panic(err)
	}
	return string(bK), string(bV)
}

func method2(key, value string) []byte {
	// Using gob
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	fields := []interface{}{key, value}
	for _, f := range fields {
		if err := enc.Encode(f); err != nil {
			panic(err)
		}
	}
	return buf.Bytes()
}

func method2Dec(b []byte) (key, value string) {
	// Decode using gob
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&key); err != nil {
		panic(err)
	}
	if err := dec.Decode(&value); err != nil {
		panic(err)
	}
	return key, value
}
