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
	entry := NewEntry("my_key", Value("my_value"))
	n, _ := EncodeTo(buf, entry)
	assert.Equal(t, len(buf.Bytes()), n)
}

func TestEncode_SetsDeletedBit(t *testing.T) {
	buf := new(bytes.Buffer)
	e := NewEntry("my_key", Value("my_value"), Deleted)
	EncodeTo(buf, e)

	buf = bytes.NewBuffer(buf.Bytes())
	var deleted bool
	err := binary.Read(buf, binary.BigEndian, &deleted)
	require.NoError(t, err)
	assert.True(t, deleted)
}

func TestEncode_OnlyAddsKey(t *testing.T) {
	buf := new(bytes.Buffer)
	EncodeTo(buf, NewEntry("my_key", Value("my_value"), Deleted))

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
	wantE := NewEntry("my_key", Value("my_value"))
	wantN, _ := EncodeTo(buf, wantE)

	buf = bytes.NewBuffer(buf.Bytes())
	gotE := DBFileEntry{}
	gotN, _ := DecodeFrom(buf, &gotE)

	assert.Equal(t, wantN, gotN)
	assert.Equal(t, wantE.Key(), gotE.Key())
	assert.Equal(t, wantE.Value(), gotE.Value())
}

func TestDecode_ReadsDeletedBit(t *testing.T) {
	buf := new(bytes.Buffer)
	sEnc, bEnc := BuildStringEncoderFunc(buf), BuildBoolEncoderFunc(buf)
	bEnc(true)
	sEnc("my_key")

	buf = bytes.NewBuffer(buf.Bytes())
	entry := DBFileEntry{}
	_, err := DecodeFrom(buf, &entry)
	require.NoError(t, err)

	assert.True(t, entry.Deleted())
}

func TestDecode_SetsKeyButNotValue(t *testing.T) {

	tt := []struct {
		name    string
		deleted bool
		want    DBFileEntry
	}{
		{"Value filled if not deleted", false, NewEntry("my_key", Value("my_value"))},
		{"Value empty if deleted", true, NewEntry("my_key", Deleted)},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			sEnc, bEnc := BuildStringEncoderFunc(buf), BuildBoolEncoderFunc(buf)

			bEnc(tc.deleted)
			sEnc("my_key")
			// In reality, we wouldn't set the value for a deleted record, but for
			// purposes of this test, we want to ensure that it is skipped over for
			// deleted records.
			sEnc("my_value")

			buf = bytes.NewBuffer(buf.Bytes())
			var got DBFileEntry
			_, err := DecodeFrom(buf, &got)
			require.NoError(t, err)

			assert.Equal(t, tc.want.Deleted(), got.Deleted())
			assert.Equal(t, tc.want.Key(), got.Key())
			assert.Equal(t, tc.want.Value(), got.Value())
		})
	}
}
