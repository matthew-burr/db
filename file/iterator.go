package file

import (
	"io"
)

// A DBFileIterator helps to iterate over a DBFile one entry at a time.
type DBFileIterator struct {
	rdr                io.Reader
	offset, nextOffset int64
	entry              DBFileEntry
}

// A Cloner is an object that provides a method to produce an io.Reader as a Clone of an existing io.Reader.
type Cloner interface {
	Clone() io.Reader
}

// Iterator creates a new DBFileIterator.
func Iterator(file Cloner) *DBFileIterator {
	d := &DBFileIterator{
		rdr: file.Clone(),
	}
	d.MoveNext()
	return d
}

// MoveNext moves the iterator to the next entry in the DBFile.
func (d *DBFileIterator) MoveNext() {
	d.offset = d.nextOffset

	entry := DBFileEntry{}
	n, err := DecodeFrom(d.rdr, &entry)
	if err != nil {
		if err == io.EOF {
			d.offset = -1
			d.nextOffset = -1
			return
		}
		panic(err)
	}

	d.entry = entry
	d.nextOffset += int64(n)
}

// ReadEntry returns the current entry.
func (d *DBFileIterator) ReadEntry() DBFileEntry {
	return d.entry
}

// Offset is the offset of the most recently read entry.
func (d *DBFileIterator) Offset() int64 {
	return d.offset
}

// Done returns true when the iterator has reached its end.
func (d *DBFileIterator) Done() bool {
	return d.offset == -1
}
