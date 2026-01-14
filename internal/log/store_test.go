package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

const (
	loop_count = 3
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < loop_count+1; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, n, width)
		require.Equal(t, pos+n, i*width)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	pos := uint64(0)
	for range uint64(loop_count) {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, read, write)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := 0, int64(0); i < loop_count; i++ {
		h := make([]byte, lenWidth)
		n, err := s.ReadAt(h, off)
		require.NoError(t, err)
		require.Equal(t, n, lenWidth)

		off += lenWidth
		size := endian.Uint64(h)
		read := make([]byte, size)
		n, err = s.ReadAt(read, off)
		require.NoError(t, err)
		require.Equal(t, write, read)
		require.Equal(t, n, int(size))
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	f, afterSize, err := openFile(f.Name())
	require.NoError(t, err)

	require.True(t, afterSize >= beforeSize)
}

func openFile(name string) (*os.File, int64, error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
