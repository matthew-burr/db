package database

import "github.com/matthew-burr/db/file"

type DBIndexRecord interface {
	Key() string
	Offset() int64
}

// A DBIndex is a map of keys to their offset in the DBFile.
type DBIndex map[string]int64

// BuildIndex builds a new index of a DBFile.
func BuildIndex(f file.Cloner) DBIndex {
	index := make(DBIndex)

	for buf := file.Iterator(f); !buf.Done(); buf.MoveNext() {
		k, _ := buf.ReadEntry().Tuple()
		index[k] = buf.Offset()
	}

	return index
}

func (d DBIndex) Update(record DBIndexRecord) {
	d[record.Key()] = record.Offset()
}
