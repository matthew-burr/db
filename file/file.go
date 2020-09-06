package file

import (
	"encoding/binary"
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
func (d *DBFile) Clone() io.Reader {
	return Open(d.File.Name())
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
	offset := d.CurrentOffset()

	bits := []byte(entry.String())

	var n int64 = int64(binary.Size(bits))
	if err := binary.Write(d.File, binary.LittleEndian, n); err != nil {
		panic(err)
	}

	n += int64(binary.Size(n))
	if err := binary.Write(d.File, binary.LittleEndian, bits); err != nil {
		panic(err)
	}

	d.Offset += int64(n)
	return entry.At(offset)
}

// ReadRawEntry reads the raw data from a specified offset in the file.
func (d *DBFile) ReadRawEntry(offset int64) string {
	d.moveToOffset(offset)
	defer d.moveToEnd()

	var n int64
	if err := binary.Read(d.File, binary.LittleEndian, &n); err != nil {
		return ""
	}

	bits := make([]byte, n)
	if err := binary.Read(d.File, binary.LittleEndian, bits); err != nil {
		panic(err)
	}

	return string(bits)
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
