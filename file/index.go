package file

import (
	"bufio"
	"fmt"
	"io"
)

const (
	// BufferSize is the size of a buffer for a buffered reader. Benchmarking suggests 8KB is preferrable.
	BufferSize int = 8192
)

// A DBIndex is a map of keys to their offset in the DBFile.
type DBIndex map[string]int64

// BuildIndex builds a new index of a DBFile.
func BuildIndex(rdr io.Reader) DBIndex {
	index := make(DBIndex)

	// Benchmarking shows that using a buffered reader is much faster,
	// and 8KB seems to be the optimal size.
	dec := NewDecoder(bufio.NewReaderSize(rdr, BufferSize))
	offset := int64(0)
	entry := DBFileEntry{}
	for n, err := dec.Decode(&entry); err == nil; n, err = dec.Decode(&entry) {
		index.Update(entry, offset)
		offset += int64(n)
	}

	return index
}

// Update updates the index with a DBFileEntry by adding or setting the key to the offset, or by removing
// the key, if the entry has been deleted.
func (d DBIndex) Update(entry DBFileEntry, offset int64) {
	if entry.deleted {
		d.Remove(entry.key)
		return
	}
	d[entry.key] = offset
}

// Remove removes a key from the index.
func (d DBIndex) Remove(key string) {
	delete(d, key)
}

// Debug prints information about the DBIndex and a particular entry in it to the provided writer.
func (d DBIndex) Debug(w io.Writer, key string) {
	offset, found := d[key]
	if !found {
		offset = -1
	}

	fmt.Fprintf(w, `
DBIndex Info
------------
Key Found: %v
Key Offset: %d,
Total Entry Count: %d,
`, found, offset, len(d))
}
