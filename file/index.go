package file

import "io"

// A DBIndex is a map of keys to their offset in the DBFile.
type DBIndex map[string]int64

// BuildIndex builds a new index of a DBFile.
func BuildIndex(f io.Reader) DBIndex {
	index := make(DBIndex)

	for buf := Iterator(f); !buf.Done(); buf.MoveNext() {
		k, _ := buf.ReadEntry().Tuple()
		index[k] = buf.Offset()
	}

	return index
}
