package main

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
	File *os.File
}

func OpenDBFile(filepath string) *DBFile {
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_SYNC|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	return &DBFile{
		File: file,
	}
}

func (d *DBFile) MoveToStart() {
	_, err := d.File.Seek(0, io.SeekStart)
	if err != nil {
		panic(err)
	}
}

func (d *DBFile) MoveToEnd() {
	_, err := d.File.Seek(0, io.SeekEnd)
	if err != nil {
		panic(err)
	}
}

func (d *DBFile) MoveToOffset(offset int64) {
	_, err := d.File.Seek(offset, io.SeekStart)
	if err != nil {
		panic(err)
	}
}

func (d *DBFile) CurrentOffset() int64 {
	offset, err := d.File.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}
	return offset
}

func (d *DBFile) WriteEntry(key, value string) int64 {
	offset := d.CurrentOffset()

	_, err := fmt.Fprintf(d, "%s:%s\n", key, value)
	if err != nil {
		panic(err)
	}

	return offset
}

func (d *DBFile) ReadEntry(offset int64) string {
	d.MoveToOffset(offset)
	defer d.MoveToEnd()

	scn := bufio.NewScanner(d)
	if scn.Scan() {
		return scn.Text()
	}

	return ""
}

func (d *DBFile) ReadParseEntry(offset int64) (key, value string) {
	return d.ParseEntry(d.ReadEntry(offset))
}

func (d *DBFile) Read(b []byte) (n int, err error) {
	return d.File.Read(b)
}

func (d *DBFile) Write(b []byte) (n int, err error) {
	return d.File.Write(b)
}

func (d *DBFile) Close() {
	d.File.Close()
}

func (*DBFile) ParseEntry(entry string) (key, value string) {
	parts := strings.SplitN(entry, ":", 2)
	if len(parts) != 2 {
		panic(fmt.Errorf("corrupt file"))
	}
	return parts[0], parts[1]
}
