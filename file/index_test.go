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
	"github.com/stretchr/testify/require"
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
		file.Value(strings.Repeat("x", size)),
	)

	d := file.Open(filepath)
	for i := 0; i < count; i++ {
		d.WriteEntry(entry)
	}

	return d
}

func TestReindex_ExcludesDeletedRecords(t *testing.T) {
	buf := new(bytes.Buffer)
	file.EncodeTo(buf, file.NewEntry("deleted", file.Value("record"), file.Deleted))
	file.EncodeTo(buf, file.NewEntry("not", file.Value("deleted")))

	buf = bytes.NewBuffer(buf.Bytes())
	got := file.BuildIndex(buf)
	assert.NotContains(t, got, "deleted")
	assert.Contains(t, got, "not")
}

func TestReindex_RemovesDeletedRecords(t *testing.T) {
	buf := new(bytes.Buffer)
	file.EncodeTo(buf, file.NewEntry("delete", file.Value("me")))
	file.EncodeTo(buf, file.NewEntry("delete", file.Value("me"), file.Deleted))

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
	entry := file.NewEntry("test", file.Value("entry"))
	idx.Update(entry, int64(0))
	assert.Contains(t, idx, "test")
}

func TestUpdate_UpdatesAnOffset(t *testing.T) {
	idx := make(file.DBIndex)
	idx.Update(file.NewEntry("test", file.Value("me")), int64(0))
	idx.Update(file.NewEntry("test", file.Value("this")), int64(1))
	assert.Equal(t, int64(1), idx["test"])
}

func TestUpdate_RemovesDeletedItem(t *testing.T) {
	idx := make(file.DBIndex)
	idx["test"] = int64(0)
	idx.Update(file.NewEntry("test", file.Value("delete"), file.Deleted), int64(1))
	assert.NotContains(t, idx, "test")
}

func SetupForCompressTests(entry ...file.DBFileEntry) (w *bytes.Buffer, r *bytes.Reader, idx file.DBIndex) {
	buf := new(bytes.Buffer)
	enc := file.NewEncoder(buf)

	for _, e := range entry {
		enc.Encode(e)
	}

	return new(bytes.Buffer), bytes.NewReader(buf.Bytes()), file.BuildIndex(bytes.NewBuffer(buf.Bytes()))
}

func CountEntry(r io.Reader, key string) (count int, lastEntry file.DBFileEntry) {
	dec := file.NewDecoder(r)
	var entry file.DBFileEntry
	for _, err := dec.Decode(&entry); err != io.EOF; _, err = dec.Decode(&entry) {
		if err != nil {
			panic(err)
		}
		if entry.Key() == key {
			count++
			lastEntry = entry
		}
	}
	return
}

func TestCompress_KeepsOnlyTheLastEntry(t *testing.T) {
	w, r, idx := SetupForCompressTests(
		file.NewEntry("test", file.Value("1")),
		file.NewEntry("test", file.Value("2")),
		file.NewEntry("test", file.Value("3")),
	)

	idx = idx.Compress(w, r)
	gotCount, gotEntry := CountEntry(bytes.NewBuffer(w.Bytes()), "test")
	wantCount, wantEntry := 1, file.NewEntry("test", file.Value("3"))
	assert.Equal(t, wantCount, gotCount)
	assert.True(t, gotEntry.Equals(wantEntry))
}

func TestCompress_KeepsAllEntriesFromIndex(t *testing.T) {
	w, r, idx := SetupForCompressTests(
		file.NewEntry("test", file.Value("1")),
		file.NewEntry("other", file.Value("2")),
	)

	idx = idx.Compress(w, r)

	for _, key := range []string{"test", "other"} {
		got, _ := CountEntry(bytes.NewBuffer(w.Bytes()), key)
		want := 1
		assert.Equal(t, want, got)
	}
}

func TestCompress_DoesNotAddDeletedItems(t *testing.T) {
	w, r, idx := SetupForCompressTests(
		file.NewEntry("test"),
		file.NewEntry("test", file.Deleted),
	)

	idx = idx.Compress(w, r)

	got, _ := CountEntry(bytes.NewBuffer(w.Bytes()), "test")
	want := 0
	assert.Equal(t, want, got)
}

func TestCompress_ReturnsIndex(t *testing.T) {
	w, r, idx := SetupForCompressTests(
		file.NewEntry("test"),
		file.NewEntry("test"),
	)
	require.Greater(t, idx["test"], int64(0))

	idx = idx.Compress(w, r)
	got := idx["test"]
	want := int64(0)
	assert.Equal(t, want, got)
}
