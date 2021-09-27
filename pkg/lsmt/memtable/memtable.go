package memtable

import (
	"github.com/scottcagno/storage/pkg/lsmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/rbtree"
	"github.com/scottcagno/storage/pkg/lsmt/sfile"
)

type Memtable struct {
	data     *rbtree.RBTree
	flushing bool
	walg     *sfile.SegmentedFile
}

func Open(walg *sfile.SegmentedFile) (*Memtable, error) {
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
func (mt *Memtable) loadEntries(walg *sfile.SegmentedFile) error {
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

func (mt *Memtable) Get()
