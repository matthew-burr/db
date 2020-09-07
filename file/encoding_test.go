package file_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	. "github.com/matthew-burr/db/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeTo(t *testing.T) {
	buf := new(bytes.Buffer)
	entry := NewEntry("my_key", "my_value")
	n, _ := EncodeTo(buf, entry)
	assert.Equal(t, len(buf.Bytes()), n)
}

func TestEncode_SetsDeletedBit(t *testing.T) {
	buf := new(bytes.Buffer)
	e := NewEntry("my_key", "my_value").Delete()
	EncodeTo(buf, e)

	buf = bytes.NewBuffer(buf.Bytes())
	var deleted bool
	err := binary.Read(buf, binary.BigEndian, &deleted)
	require.NoError(t, err)
	assert.True(t, deleted)
}

func TestEncode_OnlyAddsKey(t *testing.T) {
	buf := new(bytes.Buffer)
	EncodeTo(buf, NewEntry("my_key", "my_value").Delete())

	buf = bytes.NewBuffer(buf.Bytes())
	var (
		deleted    bool
		nKey, nVal int16
		key        []byte
	)
	err := binary.Read(buf, binary.BigEndian, &deleted)
	require.NoError(t, err)
	err = binary.Read(buf, binary.BigEndian, &nKey)
	require.NoError(t, err)
	key = make([]byte, nKey)
	err = binary.Read(buf, binary.BigEndian, key)
	require.NoError(t, err)

	err = binary.Read(buf, binary.BigEndian, &nVal)
	assert.Equal(t, err, io.EOF)
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
