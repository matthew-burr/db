package file_test

import (
	"testing"

	"github.com/matthew-burr/db/file"
	"github.com/stretchr/testify/assert"
)

func TestValueOption(t *testing.T) {
	entry := file.NewEntry("test", file.Value("value"))
	file.Value("other value")(&entry)
	assert.Equal(t, "other value", entry.Value())
}

func TestDeleteOption(t *testing.T) {
	entry := file.NewEntry("not", file.Value("deleted"))
	file.Deleted(&entry)
	assert.True(t, entry.Deleted())
}

func TestNewEntryhOptions_SetsKey(t *testing.T) {
	entry := file.NewEntry("test")
	assert.Equal(t, "test", entry.Key())
}

func TestNewEntryhOptions_SetsValue(t *testing.T) {
	entry := file.NewEntry("test", file.Value("value"))
	assert.Equal(t, "value", entry.Value())
}

func TestNewEntryhOptions_SetsDeleted(t *testing.T) {
	entry := file.NewEntry("test", file.Deleted)
	assert.True(t, entry.Deleted())
}

func TestNewEntryhOptions_OldStyle(t *testing.T) {
	entry := file.NewEntry("test", file.Value("value"))
	assert.Equal(t, "test", entry.Key())
	assert.Equal(t, "value", entry.Value())
}

func TestParseEntry_SetsKey(t *testing.T) {
	entry := file.ParseEntry("key:value")
	assert.Equal(t, "key", entry.Key())
}

func TestParseEntry_SetsValue(t *testing.T) {
	entry := file.ParseEntry("key:value")
	assert.Equal(t, "value", entry.Value())
}
