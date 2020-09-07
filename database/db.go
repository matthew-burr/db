package database

import (
	"fmt"

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
	d.WriteTuple(key, value)
	return d
}

// Debug provides some basic ability to check the validity of the database structure. Given a key, it will
// determine the offset for that key, insure it's a valid offset, and return what data it finds at that offset.
func (d DB) Debug(key string) DB {
	if offset, found := d.Index[key]; found {
		fmt.Printf("key: %s: offset = %d\n", key, offset)
		if fileSize := d.CurrentOffset(); offset > fileSize {
			fmt.Printf("offset exceeds file size of %d\n", fileSize)
			return d
		}
		// fmt.Printf("entry at %d: %s\n", offset, d.ReadRawEntry(offset))
	} else {
		fmt.Printf("key: %s: not found in index\n", key)
	}
	return d
}

// Read reads a key's value into a string.
// To facilitate a pattern of repeated reads, Read accepts a pointer to a string where it will
// write the value, and then returns the DB.
func (d DB) Read(key string, value *string) DB {
	d.ReadKey(key, value)
	return d
}

// Shutdown closes the database and should always be executed before quitting the program.
func (d DB) Shutdown() {
	d.DBFile.Close()
}
