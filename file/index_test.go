package file_test

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/matthew-burr/db/file"
	"github.com/stretchr/testify/assert"
)

func MakeBufReaderFunc(filepath string, size int) func() io.Reader {
	return func() io.Reader {
		f, _ := os.Open(filepath)
		return bufio.NewReaderSize(f, size)
	}
}

func BenchmarkReindex(b *testing.B) {
	BuildBigFile(64, 1024, "test.dat")

	bt := []struct {
		name string
		rdr  func() io.Reader
	}{
		{"Direct read", func() io.Reader { f, _ := os.Open("test.dat"); return f }},
		{"4K Buffer", MakeBufReaderFunc("test.dat", 4096)},
		{"8KB Buffer", MakeBufReaderFunc("test.dat", 8192)},
		{"16KB Buffer", MakeBufReaderFunc("test.dat", 16*1024)},
		{"64KB Buffer", MakeBufReaderFunc("test.dat", 64*1024)},
	}

	for _, bc := range bt {
		b.Run(bc.name, func(b *testing.B) {
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				file.BuildIndex(bc.rdr())
			}
		})
	}

	b.StopTimer()
	os.Remove("test.dat")
}

func BuildBigFile(size, count int, filepath string) *file.DBFile {
	entry := file.NewEntry(
		"test",
		strings.Repeat("x", size),
	)

	d := file.Open(filepath)
	for i := 0; i < count; i++ {
		d.WriteEntry(entry)
	}

	return d
}

func TestReindex_ExcludesDeletedRecords(t *testing.T) {
	buf := new(bytes.Buffer)
	file.EncodeTo(buf, file.NewEntry("deleted", "record").Delete())
	file.EncodeTo(buf, file.NewEntry("not", "deleted"))

	buf = bytes.NewBuffer(buf.Bytes())
	got := file.BuildIndex(buf)
	assert.NotContains(t, got, "deleted")
	assert.Contains(t, got, "not")
}

func TestReindex_RemovesDeletedRecords(t *testing.T) {
	buf := new(bytes.Buffer)
	file.EncodeTo(buf, file.NewEntry("delete", "me"))
	file.EncodeTo(buf, file.NewEntry("delete", "me").Delete())

	buf = bytes.NewBuffer(buf.Bytes())
	got := file.BuildIndex(buf)
	assert.NotContains(t, got, "delete")
}

func TestRemove_RemovesAnItem(t *testing.T) {
	idx := make(file.DBIndex)
	idx["test"] = 0
	idx.Remove("test")
	assert.NotContains(t, idx, "test")
}

func TestUpdate_AddsAKey(t *testing.T) {
	idx := make(file.DBIndex)
	entry := file.NewEntry("test", "entry")
	idx.Update(entry, int64(0))
	assert.Contains(t, idx, "test")
}

func TestUpdate_UpdatesAnOffset(t *testing.T) {
	idx := make(file.DBIndex)
	idx.Update(file.NewEntry("test", "me"), int64(0))
	idx.Update(file.NewEntry("test", "this"), int64(1))
	assert.Equal(t, int64(1), idx["test"])
}

func TestUpdate_RemovesDeletedItem(t *testing.T) {
	idx := make(file.DBIndex)
	idx["test"] = int64(0)
	idx.Update(file.NewEntry("test", "delete").Delete(), int64(1))
	assert.NotContains(t, idx, "test")
}
