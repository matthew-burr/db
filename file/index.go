package file

import "io"

// A DBIndex is a map of keys to their offset in the DBFile.
type DBIndex map[string]int64

// BuildIndex builds a new index of a DBFile.
func BuildIndex(f io.Reader) DBIndex {
	index := make(DBIndex)

	dec := NewDecoder(f)
	offset := int64(0)
	entry := DBFileEntry{}
	for n, err := dec.Decode(&entry); err == nil; n, err = dec.Decode(&entry) {
		index[entry.key] = offset
		offset += int64(n)
	}

	return index
}
