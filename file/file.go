package file

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

// A DBFile encapsulates the interaction between the database and the filesystem.
// It provides key information to help the DB keep track of locations in the file.
type DBFile struct {
	File   *os.File
	Offset int64 // The current offset in the file.
}

// OpenDBFile opens a file for use as a DBFile.
func OpenDBFile(filepath string) *DBFile {
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
	return OpenDBFile(d.File.Name())
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
func (d *DBFile) writeEntry(entry string) int64 {
	offset := d.CurrentOffset()

	count, err := fmt.Fprintln(d, entry)
	if err != nil {
		panic(err)
	}
	d.Offset += int64(count)

	return offset
}

// WriteParseEntry parses a key value pair into a valid entry and writes it to the file.
func (d *DBFile) WriteParseEntry(key, value string) int64 {
	return d.writeEntry(fmt.Sprintf("%s:%s", key, value))
}

// ReadEntry reads the raw data from a specified offset in the file.
func (d *DBFile) ReadEntry(offset int64) string {
	d.MoveToOffset(offset)
	defer d.MoveToEnd()

	scn := bufio.NewScanner(d)
	if scn.Scan() {
		return scn.Text()
	}

	return ""
}

// ReadParseEntry reads an entry from the file, parsing it into its key, value pair.
func (d *DBFile) ReadParseEntry(offset int64) (key, value string) {
	return d.ParseEntry(d.ReadEntry(offset))
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

// ParseEntry parses a file entry into a key, value pair.
func (*DBFile) ParseEntry(entry string) (key, value string) {
	parts := strings.SplitN(entry, ":", 2)
	if len(parts) != 2 {
		panic(fmt.Errorf("corrupt file"))
	}
	return parts[0], parts[1]
}

func parseEntry(entry string) (key, value string) {
	parts := strings.SplitN(entry, ":", 2)
	if len(parts) != 2 {
		panic(fmt.Errorf("corrupt file"))
	}
	return parts[0], parts[1]
}

// A DBFileIterator helps to iterate over a DBFile one entry at a time.
type DBFileIterator struct {
	rdr                *bufio.Reader
	offset, nextOffset int64
	ln                 string
}

// NewDBFileIterator creates a new DBFileIterator.
func NewDBFileIterator(file *DBFile) *DBFileIterator {
	d := &DBFileIterator{
		rdr: bufio.NewReaderSize(file.Clone(), 4096),
	}
	d.MoveNext()
	return d
}

func (d *DBFileIterator) MoveNext() {
	d.offset = d.nextOffset

	ln, err := d.rdr.ReadString('\n')
	if err != nil {
		d.offset = -1
		d.nextOffset = -1
		return
	}

	d.ln = ln
	d.nextOffset += int64(len(ln))
}

func (d *DBFileIterator) ReadParseEntry() (key, value string) {
	return parseEntry(d.ln)
}

// Offset is the offset of the most recently read entry.
func (d *DBFileIterator) Offset() int64 {
	return d.offset
}

func (d *DBFileIterator) Done() bool {
	return d.offset == -1
}
