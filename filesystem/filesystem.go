package filesystem

import (
	"github.com/matthew-burr/db/file"
)

// A DBFileSystem is the interface between the DB and underlying DBFile's.
type DBFileSystem struct {
	File *file.DBFile
}

func Init(dbName string) *DBFileSystem {
	return &DBFileSystem{
		File: file.Open("test.dat"),
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

func (d *DBFileSystem) Shutdown() {
	d.File.Close()
}
