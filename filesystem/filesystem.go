package filesystem

import (
	"io"

	"github.com/matthew-burr/db/file"
)

// A DBFileSystem is the interface between the DB and underlying DBFile's.
type DBFileSystem struct {
	File *file.DBFile
}

func Init(dbName string) *DBFileSystem {
	return &DBFileSystem{
		File: file.Open(dbName + ".dat"),
	}
}

func (d *DBFileSystem) WriteEntry(entry file.DBFileEntry) file.DBFileEntry {
	return d.File.WriteEntry(entry)
}

func (d *DBFileSystem) ReadEntry(key string) file.DBFileEntry {
	return d.File.ReadEntry(key)
}

func (d *DBFileSystem) DeleteEntry(key string) file.DBFileEntry {
	return d.File.DeleteEntry(key)
}

func (d *DBFileSystem) Close() {
	d.File.Close()
}

func (d *DBFileSystem) Debug(w io.Writer, key string) {
	d.File.Debug(w, key)
}
