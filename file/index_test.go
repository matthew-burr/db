package file_test

import (
	"bufio"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/matthew-burr/db/file"
)

func MakeBufReaderFunc(filepath string, size int) func() io.Reader {
	return func() io.Reader {
		f, _ := os.Open(filepath)
		return bufio.NewReaderSize(f, size)
	}
}

func BenchmarkReindex(b *testing.B) {
	BuildBigFile(64, 1024, "test.dat")

	bt := []struct {
		name string
		rdr  func() io.Reader
	}{
		{"Direct read", func() io.Reader { f, _ := os.Open("test.dat"); return f }},
		{"4K Buffer", MakeBufReaderFunc("test.dat", 4096)},
		{"8KB Buffer", MakeBufReaderFunc("test.dat", 8192)},
		{"16KB Buffer", MakeBufReaderFunc("test.dat", 16*1024)},
		{"64KB Buffer", MakeBufReaderFunc("test.dat", 64*1024)},
	}

	for _, bc := range bt {
		b.Run(bc.name, func(b *testing.B) {
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				file.BuildIndex(bc.rdr())
			}
		})
	}

	b.StopTimer()
	os.Remove("test.dat")
}

func BuildBigFile(size, count int, filepath string) *file.DBFile {
	entry := file.NewEntry(
		"test",
		strings.Repeat("x", size),
	)

	d := file.Open(filepath)
	for i := 0; i < count; i++ {
		d.WriteEntry(entry)
	}

	return d
}
