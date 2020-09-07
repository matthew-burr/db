package file

import (
	"fmt"
	"io"
	"strings"
)

// A DBFileEntry is a single entry in a DBFile.
type DBFileEntry struct {
	deleted    bool
	key, value string
}

// NewEntry creates a new DBFileEntry with the given key and value.
func NewEntry(key, value string) DBFileEntry {
	return DBFileEntry{
		key:   key,
		value: value,
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

// Key returns the DBFileEntry's key.
func (d DBFileEntry) Key() string {
	return d.key
}

// Value returns the DBFileEntry's value.
func (d DBFileEntry) Value() string {
	return d.value
}

// Tuple returns the DBFileEntry's key and value as a key/value pair.
func (d DBFileEntry) Tuple() (key, value string) {
	return d.key, d.value
}

// Delete marks an entry as deleted.
func (d DBFileEntry) Delete() DBFileEntry {
	d.deleted = true
	return d
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
