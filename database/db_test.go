package database_test

import (
	"os"
	"testing"

	"github.com/matthew-burr/db/database"
	"github.com/stretchr/testify/assert"
)

func TestDelete_RemovesEntry(t *testing.T) {
	db := database.Init("db_test.dat")
	defer os.Remove("db_test.dat")
	defer db.Shutdown()

	db.Write("hello", "world")
	var value string
	db.Read("hello", &value)
	assert.Equal(t, "world", value)

	db.Delete("hello")
	db.Read("hello", &value)
	assert.Equal(t, "<not found>", value)
}
