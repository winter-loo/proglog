package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"

	api "github.com/winter-loo/proglog/api/v1"
)

const (
	maxStoreBytes = 1024 * 1024 * 1024
	maxIndexBytes = entWidth * 1024 * 1024 * 50
)

type Log struct {
	segments      []*segment
	activeSegment *segment

	Dir    string
	Config Config

	mu sync.RWMutex
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = maxStoreBytes
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = maxIndexBytes
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

func (self *Log) setup() error {
	files, err := os.ReadDir(self.Dir)
	if err != nil {
		return err
	}

	var baseLsnList []uint64
	for _, f := range files {
		baseLsnStr := strings.TrimSuffix(f.Name(), path.Ext(f.Name()))
		baseLsn, _ := strconv.ParseUint(baseLsnStr, 10, 0)
		baseLsnList = append(baseLsnList, baseLsn)
	}

	slices.Sort(baseLsnList)

	for i := 0; i < len(baseLsnList); i++ {
		lsn := baseLsnList[i]
		if err = self.newSegment(lsn); err != nil {
			return err
		}

		// baseLsnList contains dup for index and store so we skip
		// the dup
		i++
	}

	if self.segments == nil {
		if err = self.newSegment(self.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

func (self *Log) newSegment(baseLsn uint64) error {
	s, err := newSegment(self.Dir, baseLsn, self.Config)
	if err != nil {
		return err
	}
	self.segments = append(self.segments, s)
	self.activeSegment = s
	return nil
}

func (self *Log) Append(record *api.Record) (lsn uint64, err error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	lsn, err = self.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if self.activeSegment.IsFull() {
		self.newSegment(lsn + 1)
	}
	return lsn, nil
}

func (self *Log) Read(lsn uint64) (record *api.Record, err error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	i, _ := slices.BinarySearchFunc(self.segments, lsn, func(seg *segment, lsn uint64) int {
		return int(seg.baseLsn - lsn)
	})

	if i >= len(self.segments) {
		return nil, fmt.Errorf("lsn out of range: %d", lsn)
	}

	return self.segments[i].Read(lsn)
}

func (self *Log) Close() error {
	self.mu.Lock()
	defer self.mu.Unlock()

	for _, segment := range self.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (self *Log) Remove() error {
	if err := self.Close(); err != nil {
		return err
	}
	return os.RemoveAll(self.Dir)
}

func (self *Log) Reset() error {
	if err := self.Remove(); err != nil {
		return err
	}
	return self.setup()
}

func (self *Log) LowestOffset() (uint64, error) {
	self.mu.RLock()
	defer self.mu.RUnlock()
	return self.segments[0].baseLsn, nil
}

func (self *Log) HighestOffset() (uint64, error) {
	self.mu.RLock()
	defer self.mu.RUnlock()
	off := self.segments[len(self.segments)-1].nextLsn
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

func (self *Log) Truncate(lowest uint64) error {
	self.mu.Lock()
	defer self.mu.Unlock()
	var segments []*segment
	for _, s := range self.segments {
		if s.nextLsn <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	self.segments = segments
	return nil
}

func (self *Log) Reader() io.Reader {
	self.mu.RLock()
	defer self.mu.RUnlock()

	readers := make([]io.Reader, len(self.segments))
	for i, segment := range self.segments {
	readers[i] = &originReader {segment.store, 0}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off int64
}

func (self *originReader)	Read(p []byte) (n int, err error) {
	n, err = self.ReadAt(p, self.off)
	self.off += int64(n)
	return n, err
}
