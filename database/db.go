package database

import (
	"os"

	"github.com/matthew-burr/db/file"
	"github.com/matthew-burr/db/filesystem"
)

// A DB is a simple key, value database.
type DB struct {
	DBFile *filesystem.DBFileSystem
}

// Init initializes the database from a file. Once initialized, you can start querying the database.
func Init(dbName string) *DB {
	fs := filesystem.Init(dbName)
	return &DB{
		DBFile: fs,
	}
}

// Write adds or updates a database entry by writing the value to the key.
func (d *DB) Write(key, value string) file.DBFileEntry {
	return d.DBFile.WriteEntry(file.NewEntry(key, file.Value(value)))
}

// Read reads a key's value into a string.
// To facilitate a pattern of repeated reads, Read accepts a pointer to a string where it will
// write the value, and then returns the DB.
func (d *DB) Read(key string) file.DBFileEntry {
	return d.DBFile.ReadEntry(key)
}

// Delete removes an entry from the database.
func (d *DB) Delete(key string) file.DBFileEntry {
	return d.DBFile.DeleteEntry(key)
}

// Shutdown closes the database and should always be executed before quitting the program.
func (d *DB) Shutdown() {
	d.DBFile.Close()
}

// Debug provides some basic ability to check the validity of the database structure. Given a key, it will
// determine the offset for that key, insure it's a valid offset, and return what data it finds at that offset.
func (d *DB) Debug(key string) {
	d.DBFile.File.Debug(os.Stdout, key)
}
