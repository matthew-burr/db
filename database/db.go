package database

import (
	"os"

	"github.com/matthew-burr/db/file"
)

// A DB is a simple key, value database.
type DB struct {
	*file.DBFile
}

// Init initializes the database from a file. Once initialized, you can start querying the database.
func Init(filepath string) DB {
	d := DB{
		DBFile: file.Open(filepath),
	}
	return d
}

// Write adds or updates a database entry by writing the value to the key.
func (d DB) Write(key, value string) DB {
	d.DBFile.WriteEntry(file.NewEntry(key, value))
	return d
}

// Read reads a key's value into a string.
// To facilitate a pattern of repeated reads, Read accepts a pointer to a string where it will
// write the value, and then returns the DB.
func (d DB) Read(key string, value *string) DB {
	entry := d.DBFile.ReadEntry(key)
	*value = entry.Value()
	return d
}

// Shutdown closes the database and should always be executed before quitting the program.
func (d DB) Shutdown() {
	d.DBFile.Close()
}

// Debug provides some basic ability to check the validity of the database structure. Given a key, it will
// determine the offset for that key, insure it's a valid offset, and return what data it finds at that offset.
func (d DB) Debug(key string) DB {
	d.DBFile.Debug(os.Stdout, key)
	return d
}
