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

func TestEquals(t *testing.T) {
	tt := []struct {
		name string
		arg  file.DBFileEntry
		want bool
	}{
		{"Identical", file.NewEntry("test", file.Value("entry"), file.Deleted), true},
		{"Different deleted", file.NewEntry("test", file.Value("entry")), false},
		{"Different key", file.NewEntry("other", file.Value("entry"), file.Deleted), false},
		{"Different value", file.NewEntry("test", file.Value("foo"), file.Deleted), false},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			got := file.NewEntry("test", file.Value("entry"), file.Deleted).Equals(tc.arg)
			assert.Equal(t, tc.want, got)

		})
	}
}
