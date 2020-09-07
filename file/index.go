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
		index[entry.key] = offset
		offset += int64(n)
	}

	return index
}

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
