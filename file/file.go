package file

import (
	"bufio"
	"io"
	"os"
)

// A DBFile encapsulates the interaction between the database and the filesystem.
// It provides key information to help the DB keep track of locations in the file.
type DBFile struct {
	File   *os.File
	Offset int64 // The current offset in the file.
}

// Open opens a file for use as a DBFile.
func Open(filepath string) *DBFile {
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_SYNC|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	return &DBFile{
		File:   file,
		Offset: int64(0),
	}
}

// Clone creates a copy of the DBFile with a new file pointer.
// The cloned copy is positioned at the beginning of the file.
func (d *DBFile) Clone() *DBFile {
	return Open(d.File.Name())
}

// MoveToStart moves the DBFile's offset to the start of the file.
func (d *DBFile) MoveToStart() {
	offset, err := d.File.Seek(0, io.SeekStart)
	if err != nil {
		panic(err)
	}
	d.Offset = offset
}

// MoveToEnd moves the DBFile's offset to the end of the file.
func (d *DBFile) MoveToEnd() {
	offset, err := d.File.Seek(0, io.SeekEnd)
	if err != nil {
		panic(err)
	}
	d.Offset = offset
}

// MoveToOffset moves the DBFile's offset to a specific position relative to the start of the file.
func (d *DBFile) MoveToOffset(offset int64) {
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
func (d *DBFile) WriteEntry(entry DBFileEntry) int64 {
	offset := d.CurrentOffset()

	count, err := entry.WriteTo(d.File)
	if err != nil {
		panic(err)
	}
	d.Offset += count

	return offset
}

// ReadRawEntry reads the raw data from a specified offset in the file.
func (d *DBFile) ReadRawEntry(offset int64) string {
	d.MoveToOffset(offset)
	defer d.MoveToEnd()

	scn := bufio.NewScanner(d)
	if scn.Scan() {
		return scn.Text()
	}

	return ""
}

// ReadEntry retrieves the DBFileEntry at the given offset.
func (d *DBFile) ReadEntry(offset int64) DBFileEntry {
	return ParseEntry(d.ReadRawEntry(offset)).At(offset)
}

// Read implements the io.Reader interface for reading the file.
func (d *DBFile) Read(b []byte) (n int, err error) {
	return d.File.Read(b)
}

// Write implements the io.Writer interface for writing to the file.
func (d *DBFile) Write(b []byte) (n int, err error) {
	return d.File.Write(b)
}

// Close closes the file.
func (d *DBFile) Close() {
	d.File.Close()
}
