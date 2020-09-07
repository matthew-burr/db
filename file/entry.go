package file

import (
	"fmt"
	"io"
	"strings"
)

// An EntryOption is an optional setting you may provide to a DBFileEntry.
type EntryOption func(*DBFileEntry)

// Value is an EntryOption that sets the value of the entry to a given value.
func Value(s string) EntryOption {
	return func(d *DBFileEntry) {
		d.value = s
	}
}

// Deleted is an EntryOption that marks the entry as deleted.
func Deleted(d *DBFileEntry) {
	d.deleted = true
}

// A DBFileEntry is a single entry in a DBFile.
type DBFileEntry struct {
	deleted    bool
	key, value string
}

// NewEntry creates a new DBFileEntry with the given key and value.
func NewEntry(key string, option ...EntryOption) DBFileEntry {
	d := DBFileEntry{
		key: key,
	}
	for _, o := range option {
		o(&d)
	}
	return d
}

// ParseEntry returns a new DBFileEntry from a string of the format key:value.
func ParseEntry(entry string) DBFileEntry {
	parts := strings.SplitN(entry, ":", 2)
	if len(parts) != 2 {
		panic(fmt.Errorf("corrupt file"))
	}
	return NewEntry(parts[0], Value(parts[1]))
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

// Deleted returns a bool indicating whether or not the record has been deleted.
func (d DBFileEntry) Deleted() bool {
	return d.deleted
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

// Equals compares this DBFileEntry to another and returns true if they have the same content.
func (d DBFileEntry) Equals(other DBFileEntry) bool {
	return d.deleted == other.deleted && d.key == other.key && d.value == other.value
}
