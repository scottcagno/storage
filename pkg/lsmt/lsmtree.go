package lsmt

import (
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	memtable "github.com/scottcagno/storage/pkg/lsmt/memtable"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"os"
	"path/filepath"
	"sync"
)

const (
	walPath        = "log"
	sstPath        = "data"
	FlushThreshold = 256 << 10 // 256KB
)

type LSMTree struct {
	base    string              // base is the base filepath for the database
	walbase string              // walbase is the write-ahead commit log base filepath
	sstbase string              // sstbase is the sstable and index base filepath where data resides
	lock    sync.RWMutex        // lock is a mutex that synchronizes access to the data
	memt    *memtable.Memtable  // memt is the main memtable instance
	sstm    *sstable.SSTManager // sstm is the sorted-strings table manager
}

func Open(base string) (*LSMTree, error) {
	// make sure we are working with absolute paths
	base, err := filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	// sanitize any path separators
	base = filepath.ToSlash(base)
	// create log base directory
	walbase := filepath.Join(base, walPath)
	err = os.MkdirAll(walbase, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// create data base directory
	sstbase := filepath.Join(base, sstPath)
	err = os.MkdirAll(sstbase, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// open mem-table
	memt, err := memtable.OpenMemtable(walbase, FlushThreshold)
	if err != nil {
		return nil, err
	}
	// open sst-manager
	sstm, err := sstable.OpenSSTManager(sstbase)
	if err != nil {
		return nil, err
	}
	// create lsm-tree instance and return
	lsmt := &LSMTree{
		base:    base,
		walbase: walbase,
		sstbase: sstbase,
		memt:    memt,
		sstm:    sstm,
	}
	// return lsm-tree
	return lsmt, nil
}

func (lsm *LSMTree) Put(k string, v []byte) error {
	// create binary entry
	e := &binary.Entry{Key: []byte(k), Value: v}
	// write entry to mem-table
	err := lsm.memt.Put(e)
	if err != nil {
		return err
	}
	// return an error that is unknown
	if err != nil {
		// make sure it's the mem-table doesn't need flushing
		if err != memtable.ErrFlushThreshold {
			return err
		}
		// looks like it needs a flush
		err = lsm.sstm.FlushMemtable(lsm.memt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (lsm *LSMTree) Get(k string) ([]byte, error) {
	// search memtable
	e, err := lsm.memt.Get(k)
	if err == nil {
		// we found it!
		return e.Value, nil
	}
	// check sparse index, and ss-tables, young to old
	index, err := lsm.sstm.SearchSparseIndex(k)
	if err != nil {
		return nil, err
	}
	// open ss-table for reading
	sst, err := sstable.OpenSSTable(lsm.sstbase, index)
	if err != nil {
		return nil, err
	}
	// search the sstable
	de, err := sst.Read(k)
	if err != nil {
		return nil, err
	}
	// close ss-table
	err = sst.Close()
	if err != nil {
		return nil, err
	}
	// may have found it
	return de.Value, nil
}

func (lsm *LSMTree) Del(k string) error {
	// write entry to memtable
	err := lsm.memt.Del(k)
	if err != nil {
		return err
	}
	// return an error that is unknown
	if err != nil {
		// make sure it's the mem-table doesn't need flushing
		if err != memtable.ErrFlushThreshold {
			return err
		}
		// looks like it needs a flush
		err = lsm.sstm.FlushMemtable(lsm.memt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (lsm *LSMTree) Close() error {
	// close memtable
	err := lsm.memt.Close()
	if err != nil {
		return err
	}
	// close sst-manager
	err = lsm.sstm.Close()
	if err != nil {
		return err
	}
	return nil
}
