package file_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncoding(t *testing.T) {
	buf := new(bytes.Buffer)

	b := []byte("key:value")
	var n int64 = int64(binary.Size(b))
	t.Log(n)

	err := binary.Write(buf, binary.LittleEndian, n)
	require.NoError(t, err)
	err = binary.Write(buf, binary.LittleEndian, b)
	require.NoError(t, err)
	fmt.Printf("% x\n", buf.Bytes())

	buf = bytes.NewBuffer(buf.Bytes())
	var x int64
	err = binary.Read(buf, binary.LittleEndian, &x)
	require.NoError(t, err)

	entry := make([]byte, x)
	err = binary.Read(buf, binary.LittleEndian, entry)
	require.NoError(t, err)
	fmt.Println(string(entry))
}
