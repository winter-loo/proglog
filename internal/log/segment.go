package log

import (
	"fmt"
	"os"

	api "github.com/winter-loo/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// segment is an abstraction interface used to manage **store** and **index**.
//
// Each segment begins with an absolute logic offset from the log start point
// and it uses internally a relative offset for log record.
//
// Log Sequence Number(lsn) will be referred as the logic offset.

type segment struct {
	store *store
	index *index
	// two absolute lsn
	baseLsn uint64
	nextLsn uint64
	config  Config
}

func newSegment(dir string, baseLsn uint64, c Config) (*segment, error) {
	seg := &segment{
		baseLsn: baseLsn,
		config:  c,
	}

	store, err := os.OpenFile(fmt.Sprintf("%s/%d.store", dir, baseLsn), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	if err != nil {
		return nil, err
	}
	seg.store, err = newStore(store)
	if err != nil {
		return nil, err
	}

	idx, err := os.OpenFile(fmt.Sprintf("%s/%d.index", dir, baseLsn), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	if err != nil {
		return nil, err
	}
	seg.index, err = newIndex(idx, &seg.config)
	if err != nil {
		return nil, err
	}
	if seg.index.size == 0 {
		seg.nextLsn = seg.baseLsn
	} else {
		seg.nextLsn = uint64(seg.index.LastLsn()) + seg.baseLsn + 1
	}

	return seg, nil
}

func (self *segment) Append(record *api.Record) (lsn uint64, err error) {
	bytes, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, fileOffset, err := self.store.Append(bytes)
	if err != nil {
		return 0, err
	}

	currLsn := self.nextLsn
	// index uses relative lsn
	err = self.index.Write(uint32(currLsn-self.baseLsn), fileOffset)
	if err != nil {
		return 0, err
	}
	self.nextLsn++
	return currLsn, nil
}

func (self *segment) Read(lsn uint64) (*api.Record, error) {
	_, fileOffset, err := self.index.Read(int64(lsn - self.baseLsn))
	if err != nil {
		return nil, err
	}
	payload, err := self.store.Read(fileOffset)
	if err != nil {
		return nil, err
	}
	r := &api.Record{}
	err = proto.Unmarshal(payload, r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (self *segment) IsFull() bool {
	return self.index.size >= self.config.Segment.MaxIndexBytes ||
		self.store.size >= self.config.Segment.MaxStoreBytes
}

func (self *segment) Remove() error {
	err := self.Close()
	if err != nil {
		return err
	}

	err = os.Remove(self.index.Name())
	if err != nil {
		return err
	}

	err = os.Remove(self.store.Name())
	if err != nil {
		return err
	}

	return nil
}

func (self *segment) Close() error {
	err := self.store.Close()
	if err != nil {
		return err
	}
	err = self.index.Close()
	if err != nil {
		return err
	}
	return nil
}

func nearestMultiple(j, k uint64) uint64 {
	return ((j - k + 1) / k) * k
}
