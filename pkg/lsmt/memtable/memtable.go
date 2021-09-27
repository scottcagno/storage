package memtable

import (
	"bytes"
	"github.com/scottcagno/storage/pkg/lsmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/rbtree"
	"github.com/scottcagno/storage/pkg/lsmt/wal"
)

type Memtable struct {
	data     *rbtree.RBTree
	flushing bool
	walg     *wal.WAL
}

func Open(walg *wal.WAL) (*Memtable, error) {
	// error check
	if walg == nil {
		return nil, lsmt.ErrFileClosed
	}
	// create new memtable
	memt := &Memtable{
		data:     rbtree.NewRBTree(),
		flushing: false,
		walg:     walg,
	}
	// load mem-table from commit log
	err := memt.loadEntries(walg)
	if err != nil {
		return nil, err
	}
	return memt, nil
}

// loadEntries loads any entries from the supplied segmented file back into the memtable
func (mt *Memtable) loadEntries(walg *wal.WAL) error {
	return walg.Scan(func(e *binary.Entry) bool {
		mt.data.Put(string(e.Key), e.Value)
		return true
	})
}

func (mt *Memtable) Put(k string, e *binary.Entry) error {
	mt.data.Put(string(e.Key), e.Value)
	if mt.data.Size() > lsmt.FlushThreshold {
		return lsmt.ErrFlushThreshold
	}
	return nil
}

func (mt *Memtable) Get(k string) (*binary.Entry, error) {
	v, ok := mt.data.Get(k)
	if !ok {
		return nil, lsmt.ErrKeyNotFound
	}
	if v == nil || bytes.Equal(v, lsmt.Tombstone) {
		return nil, lsmt.ErrFoundTombstone
	}
	return &binary.Entry{[]byte(k), v}, nil
}
