package filesystem_test

import (
	"errors"
	"os"
	"testing"

	"github.com/matthew-burr/db/file"
	"github.com/matthew-burr/db/filesystem"
	"github.com/stretchr/testify/assert"
)

func SetupTestFileSystem() (fs *filesystem.DBFileSystem, cleanup func()) {
	fs = filesystem.Init("test")
	cleanup = func() {
		fs.Shutdown()
		os.Remove("test.dat")
	}
	return
}

func TestInit_OpensFile(t *testing.T) {
	fs := filesystem.Init("test")

	assert.Equal(t, "test.dat", fs.File.File.Name())
}

func TestShutdown_ClosesFile(t *testing.T) {
	fs := filesystem.Init("test")
	fs.Shutdown()

	err := fs.File.File.Close()
	assert.True(t, errors.Is(err, os.ErrClosed))
}

func TestWriteEntry_WritesToFile(t *testing.T) {
	fs, c := SetupTestFileSystem()
	defer c()

	want := file.NewEntry("test", file.Value("value"))
	fs.WriteEntry(want)

	got := fs.File.ReadEntry("test")
	assert.True(t, want.Equals(got))
}

func TestWriteEntry_ReturnsWrittenEntry(t *testing.T) {
	fs, c := SetupTestFileSystem()
	defer c()

	want := file.NewEntry("test", file.Value("foo"))
	got := fs.WriteEntry(want)
	assert.True(t, want.Equals(got))
}

func TestReadEntry_ReadsEntry(t *testing.T) {
	fs, c := SetupTestFileSystem()
	defer c()
	want := file.NewEntry("test", file.Value("read"))
	fs.File.WriteEntry(want)

	got := fs.ReadEntry("test")
	assert.True(t, want.Equals(got))
}

func TestDeleteEntry_DeletesEntry(t *testing.T) {
	fs, c := SetupTestFileSystem()
	defer c()

	fs.File.WriteEntry(file.NewEntry("test", file.Value("delete")))

	fs.DeleteEntry("test")
	assert.NotContains(t, fs.File.Index, "test")
}
