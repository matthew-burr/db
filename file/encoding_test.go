package file_test

import (
	"bytes"
	"testing"

	. "github.com/matthew-burr/db/file"
	"github.com/stretchr/testify/assert"
)

func TestEncodeTo(t *testing.T) {
	buf := new(bytes.Buffer)
	entry := NewEntry("my_key", "my_value")
	n, _ := EncodeTo(buf, entry)
	assert.Equal(t, len(buf.Bytes()), n)
}

func TestDecodeFrom(t *testing.T) {
	buf := new(bytes.Buffer)
	wantE := NewEntry("my_key", "my_value")
	wantN, _ := EncodeTo(buf, wantE)

	buf = bytes.NewBuffer(buf.Bytes())
	gotE := DBFileEntry{}
	gotN, _ := DecodeFrom(buf, &gotE)

	assert.Equal(t, wantN, gotN)
	assert.Equal(t, wantE.Key(), gotE.Key())
	assert.Equal(t, wantE.Value(), gotE.Value())
}
