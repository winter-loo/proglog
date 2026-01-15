package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// **index** stores indexing entries for log record in **store** file.
// Each index entry has exactly two fields: a seq number for log record
// and the log record offset in the **store** file.

const (
	seqWidth    uint64 = 4
	posWidth    uint64 = 8
	entWidth           = seqWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, config *Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if idx.size >= config.Segment.MaxIndexBytes-entWidth {
		return nil, io.EOF
	}

	err = f.Truncate(int64(config.Segment.MaxIndexBytes))
	if err != nil {
		return nil, err
	}

	idx.mmap, err = gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

func (self *index) LastLsn() uint32 {
	if self.size == 0 {
		return 0
	}
	return uint32(self.size/entWidth) - 1
}

func (self *index) Read(seq int64) (out uint32, pos uint64, err error) {
	if self.size == 0 {
		return 0, 0, io.EOF
	}
	if seq == -1 {
		out = uint32(self.size/entWidth) - 1
	} else {
		out = uint32(seq)
	}
	pos = uint64(out) * entWidth
	if pos >= self.size {
		return 0, 0, io.EOF
	}
	out = endian.Uint32(self.mmap[pos : pos+seqWidth])
	pos = endian.Uint64(self.mmap[pos+seqWidth : pos+entWidth])
	return out, pos, nil
}

func (self *index) Write(seq uint32, pos uint64) error {
	if len(self.mmap) < int(self.size+entWidth) {
		return io.EOF
	}
	endian.PutUint32(self.mmap[self.size:self.size+seqWidth], seq)
	endian.PutUint64(self.mmap[self.size+seqWidth:self.size+entWidth], pos)
	self.size += entWidth
	return nil
}

func (self *index) Close() error {
	if err := self.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := self.file.Sync(); err != nil {
		return err
	}
	if err := self.file.Truncate(int64(self.size)); err != nil {
		return err
	}
	return self.file.Close()
}

func (self *index) Name() string {
	return self.file.Name()
}
