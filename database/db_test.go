package database_test

import (
	"os"
	"testing"

	"github.com/matthew-burr/db/database"
	"github.com/stretchr/testify/assert"
)

func SetupDBForTests() (db *database.DB, cleanup func()) {
	db = database.Init("db_test.dat")
	cleanup = func() {
		db.Shutdown()
		os.Remove("db_test.dat")
	}
	return
}

func TestDelete_RemovesEntry(t *testing.T) {
	db, cleanup := SetupDBForTests()
	defer cleanup()

	db.Write("hello", "world")
	var value string
	db.Read("hello", &value)
	assert.Equal(t, "world", value)

	db.Delete("hello")
	db.Read("hello", &value)
	assert.Equal(t, "<not found>", value)
}

func TestWrite_AddsEntryToFile(t *testing.T) {
	db, c := SetupDBForTests()
	defer c()

	db.Write("hello", "there")
	entry := db.DBFile.ReadEntry("hello")
	assert.Equal(t, "hello", entry.Key())
	assert.Equal(t, "there", entry.Value())
}

func TestWrite_ReturnsDBEntry(t *testing.T) {
	db, c := SetupDBForTests()
	defer c()

	entry := db.Write("hello", "again")
	assert.Equal(t, "hello", entry.Key())
	assert.Equal(t, "again", entry.Value())

}
