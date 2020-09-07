package database_test

import (
	"os"
	"testing"

	"github.com/matthew-burr/db/database"
	"github.com/matthew-burr/db/file"
	"github.com/stretchr/testify/assert"
)

func SetupDBForTests() (db *database.DB, cleanup func()) {
	db = database.Init("db_test")
	cleanup = func() {
		db.Shutdown()
		os.Remove("db_test.dat")
	}
	return
}

func AssertEqualEntry(t *testing.T, want, got file.DBFileEntry) {
	assert.Equal(t, want.Deleted(), got.Deleted())
	assert.Equal(t, want.Key(), got.Key())
	assert.Equal(t, want.Value(), got.Value())
}

func TestDelete_RemovesEntry(t *testing.T) {
	db, cleanup := SetupDBForTests()
	defer cleanup()

	db.Write("hello", "world")
	value := db.Read("hello").Value()
	assert.Equal(t, "world", value)

	db.Delete("hello")
	value = db.Read("hello").Value()
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

func TestRead_ReturnsEntry(t *testing.T) {
	db, c := SetupDBForTests()
	defer c()

	db.Write("test", "me")
	entry := db.Read("test")
	AssertEqualEntry(t, file.NewEntry("test", file.Value("me")), entry)
}

func TestDelete_ReturnsDeletedEntry(t *testing.T) {
	db, c := SetupDBForTests()
	defer c()

	db.Write("delete", "test")
	got := db.Delete("delete")
	want := file.NewEntry("delete", file.Deleted)
	AssertEqualEntry(t, want, got)
}
