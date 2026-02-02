package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
	"github.com/winter-loo/proglog/internal/errs"
)

type store struct {
	mu sync.Mutex
	// In Go, embedding means method promotion.
	// Any exported methods of *os.File are promoted to store.
	*os.File
	buf  *bufio.Writer
	size uint64
}

var (
	endian = binary.BigEndian
)

const (
	lenWidth = 8
	maxSize  = 1024 * 1024 * 1024
)

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) IsFull() bool {
	return s.size >= maxSize
}

// return the number of bytes written, written start position
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size

	if err := binary.Write(s.buf, endian, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	_, err := s.File.ReadAt(size, int64(pos))
	if err != nil {
		return nil, err
	}

	pos += lenWidth
	pl := endian.Uint64(size)
	payload := make([]byte, pl)
	_, err = s.File.ReadAt(payload, int64(pos))
	if err != nil {
		return nil, err
	}

	return payload, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return errs.Wrap(err)
	}

	return errs.Wrap(s.File.Close())
}
