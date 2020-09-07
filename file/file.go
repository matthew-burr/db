package file

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

// A DBFile encapsulates the interaction between the database and the filesystem.
// It provides key information to help the DB keep track of locations in the file.
type DBFile struct {
	File   *os.File
	Index  DBIndex
	Offset int64 // The current offset in the file.
}

// Open opens a file for use as a DBFile.
func Open(filepath string) *DBFile {
	file := openFile(filepath)

	d := &DBFile{
		File:  file,
		Index: BuildIndex(file),
	}

	d.moveToEnd()
	return d
}

func openFile(filepath string) *os.File {
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_SYNC|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	return file
}

// moveToEnd moves the DBFile's offset to the end of the file.
func (d *DBFile) moveToEnd() {
	offset, err := d.File.Seek(0, io.SeekEnd)
	if err != nil {
		panic(err)
	}
	d.Offset = offset
}

// moveToOffset moves the DBFile's offset to a specific position relative to the start of the file.
func (d *DBFile) moveToOffset(offset int64) {
	_, err := d.File.Seek(offset, io.SeekStart)
	if err != nil {
		panic(err)
	}
	d.Offset = offset
}

// CurrentOffset returns the DBFile's current position in the file.
func (d *DBFile) CurrentOffset() int64 {
	return d.Offset
}

// WriteEntry writes a new key value pair to the DBFile.
// It returns the entry updated with the entry's offset
func (d *DBFile) WriteEntry(entry DBFileEntry) DBFileEntry {
	n, err := EncodeTo(d.File, entry)
	if err != nil {
		panic(err)
	}
	d.Index.Update(entry, d.CurrentOffset())
	d.Offset += int64(n)
	return entry
}

// DeleteEntry deletes the entry with the given key from the file.
// It returns a DBFileEntry object with the deleted entry.
func (d *DBFile) DeleteEntry(key string) DBFileEntry {
	return d.WriteEntry(NewEntry(key, Deleted))
}

// ReadEntry retrieves the DBFileEntry at the given offset.
func (d *DBFile) ReadEntry(key string) DBFileEntry {
	offset, found := d.Index[key]
	if !found {
		return NewEntry(key, Value("<not found>"))
	}

	d.moveToOffset(offset)
	defer d.moveToEnd()

	entry := DBFileEntry{}
	_, err := DecodeFrom(d.File, &entry)
	if err != nil {
		panic(err)
	}

	return entry
}

// Close closes the file.
func (d *DBFile) Close() {
	d.File.Close()
}

// Reindex rebuilds the index for the DBFile.
func (d *DBFile) Reindex() {
	file := openFile(d.File.Name())
	d.Index = BuildIndex(file)
}

// Debug provides some information about the DBFile.
func (d *DBFile) Debug(w io.Writer, key string) {
	rdr := bufio.NewReaderSize(
		openFile(d.File.Name()),
		BufferSize,
	)
	dec := NewDecoder(rdr)
	totalCount, entryCount := 0, 0
	entry := &DBFileEntry{}
	for _, err := dec.Decode(entry); err == nil; _, err = dec.Decode(entry) {
		totalCount++
		if entry.key == key {
			entryCount++
		}
	}

	fmt.Fprintf(os.Stdout, `
DBFile Info
-----------
Current Offset: %d
Key Occurrences: %d
Total Entry Count: %d
`, d.CurrentOffset(), entryCount, totalCount)
	d.Index.Debug(w, key)
}
