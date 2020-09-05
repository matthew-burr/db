package file

import (
	"fmt"
	"io"
	"strings"
)

type DBFileEntry struct {
	offset     int64
	key, value string
}

func NewEntry(key, value string) DBFileEntry {
	return DBFileEntry{
		key:   key,
		value: value,
	}
}

func ParseEntry(entry string) DBFileEntry {
	parts := strings.SplitN(entry, ":", 2)
	if len(parts) != 2 {
		panic(fmt.Errorf("corrupt file"))
	}
	return NewEntry(parts[0], parts[1])
}

func (d DBFileEntry) At(offset int64) DBFileEntry {
	d.offset = offset
	return d
}

func (d DBFileEntry) Key() string {
	return d.key
}

func (d DBFileEntry) Value() string {
	return d.value
}

func (d DBFileEntry) Offset() int64 {
	return d.offset
}

func (d DBFileEntry) Tuple() (key, value string) {
	return d.key, d.value
}

func (d DBFileEntry) WriteTo(w io.Writer) (int64, error) {
	n, err := fmt.Fprintf(w, "%s:%s", d.key, d.value)
	return int64(n), err
}
