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

// Iterator creates a new DBFileIterator.
func Iterator(rdr io.Reader) *DBFileIterator {
	d := &DBFileIterator{
		rdr: rdr,
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
		// if err == io.EOF {
		d.offset = -1
		d.nextOffset = -1
		return
		// }
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
