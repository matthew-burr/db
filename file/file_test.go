package file_test

import (
	"os"
	"testing"

	"github.com/matthew-burr/db/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func SetupFileTestDat() (*file.DBFile, func()) {
	filepath := "file_test.dat"
	d := file.Open(filepath)
	return d, func() { d.File.Close(); os.Remove(filepath) }
}

func TestDeleteEntry_RemoveEntryFromIndex(t *testing.T) {
	d, cleanup := SetupFileTestDat()
	defer cleanup()

	key := "test"
	entry := file.NewEntry(key, "record")
	d.Index.Update(entry, 0)
	require.Contains(t, d.Index, key)

	d.DeleteEntry(key)
	require.NotContains(t, d.Index, key)
}

func TestDeleteEntry_ReturnsDeletedDBFileEntry(t *testing.T) {
	d, cleanup := SetupFileTestDat()
	defer cleanup()

	key := "test"
	got := d.DeleteEntry(key)
	assert.True(t, got.Deleted())
	assert.Equal(t, key, got.Key())
}

func TestDeleteEntry_WritesTombstoneToFile(t *testing.T) {
	d, cleanup := SetupFileTestDat()
	defer cleanup()

	key := "test"
	d.DeleteEntry(key)

	rdr, err := os.Open(d.File.Name())
	require.NoError(t, err)

	var got file.DBFileEntry
	_, err = file.DecodeFrom(rdr, &got)
	require.NoError(t, err)

	assert.True(t, got.Deleted())
	assert.Equal(t, key, got.Key())
	assert.Equal(t, "", got.Value())
}

func TestWriteEntry_AddsEntryToIndex(t *testing.T) {
	d, cleanup := SetupFileTestDat()
	defer cleanup()

	key := "test"
	d.WriteEntry(file.NewEntry(key, "entry"))
	assert.Contains(t, d.Index, key)
	assert.Equal(t, d.Index[key], int64(0))
}
