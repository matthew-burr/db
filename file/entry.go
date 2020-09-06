package file

import (
	"fmt"
	"io"
	"strings"
)

// A DBFileEntry is a single entry in a DBFile.
type DBFileEntry struct {
	offset     int64
	key, value string
}

// NewEntry creates a new DBFileEntry with the given key and value.
func NewEntry(key, value string) DBFileEntry {
	return DBFileEntry{
		key:    key,
		value:  value,
		offset: -1,
	}
}

// ParseEntry returns a new DBFileEntry from a string of the format key:value.
func ParseEntry(entry string) DBFileEntry {
	parts := strings.SplitN(entry, ":", 2)
	if len(parts) != 2 {
		panic(fmt.Errorf("corrupt file"))
	}
	return NewEntry(parts[0], parts[1])
}

// At sets the offset for a DBFileEntry.
func (d DBFileEntry) At(offset int64) DBFileEntry {
	d.offset = offset
	return d
}

// Key returns the DBFileEntry's key.
func (d DBFileEntry) Key() string {
	return d.key
}

// Value returns the DBFileEntry's value.
func (d DBFileEntry) Value() string {
	return d.value
}

// Offset returns the DBFileEntry's offset.
// Note that unless At has been called to set the DBFileEntry's offset, it will be invalid and set to -1.
func (d DBFileEntry) Offset() int64 {
	return d.offset
}

// Tuple returns the DBFileEntry's key and value as a key/value pair.
func (d DBFileEntry) Tuple() (key, value string) {
	return d.key, d.value
}

// WriteTo writes the DBFileEntry in a key:value format to a writer.
func (d DBFileEntry) WriteTo(w io.Writer) (int64, error) {
	n, err := fmt.Fprintf(w, "%s", d)
	return int64(n), err
}

// String presents the DBFileEntry as a string
func (d DBFileEntry) String() string {
	return fmt.Sprintf("%s:%s", d.key, d.value)
}
