package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp("", "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	idx, err := newIndex(f)
	require.NoError(t, err)
	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		seq uint32
		pos uint64
	}{
		{seq: 0, pos: 0},
		{seq: 1, pos: 10},
	}

	for _, ent := range entries {
		err = idx.Write(ent.seq, ent.pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(ent.seq))
		require.NoError(t, err)
		require.Equal(t, pos, ent.pos)
	}

	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, err, io.EOF)
	_ = idx.Close()

	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f)
	require.NoError(t, err)

	seq, pos, err := idx.Read(-1)
	require.Equal(t, uint32(1), seq)
	require.Equal(t, pos, entries[1].pos)
}
