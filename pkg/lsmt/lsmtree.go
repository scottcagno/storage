package lsmt

import (
	"github.com/scottcagno/storage/pkg/lsmt/memtable"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"github.com/scottcagno/storage/pkg/lsmt/wal"
	"os"
	"path/filepath"
	"sync"
)

const (
	walPath        = "log"
	sstPath        = "data"
	FlushThreshold = 256 << 10 // 256KB
)

var Tombstone = []byte(nil)

type LSMTree struct {
	base    string // base is the base filepath for the database
	walbase string
	sstbase string
	lock    sync.RWMutex // lock is a mutex that synchronizes access to the data
	walg    *wal.WAL
	memt    *memtable.Memtable
	sstm    *sstable.SSTManager
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
	// open write-ahead logger
	walg, err := wal.Open(walbase)
	if err != nil {
		return nil, err
	}
	// open mem-table
	memt, err := memtable.Open(walg)
	if err != nil {
		return nil, err
	}
	// open sst-manager
	sstm, err := sstable.Open(sstbase)
	if err != nil {
		return nil, err
	}
	// create lsm-tree instance and return
	lsmt := &LSMTree{
		base:    base,
		walbase: walbase,
		sstbase: sstbase,
		walg:    walg,
		memt:    memt,
		sstm:    sstm,
	}
	return lsmt, nil
}

func (lsm *LSMTree) Put(k string, v []byte) error {
	// write entry to write-ahead commit log
	_, err := lsm.walg.Write(k, v)
	if err != nil {
		return err
	}
	// write entry to mem-table
	err = lsm.memt.Put(k, v)
	if err != nil && err != memtable.ErrFlushThreshold {
		return err
	}
	// otherwise, maybe it's time to flush?
	if err == memtable.ErrFlushThreshold {
		// flush to sstable
		err = lsm.memt.FlushToSSTable(lsm.sstm)
		if err != nil {
			return err
		}
	}
	return nil
}

func (lsm *LSMTree) Get(k string) ([]byte, error) {
	// search memtable
	v, err := lsm.memt.Get(k)
	if err == nil {
		// found it
		return v, nil
	}
	// check sparse index, and sstables young to old
	path, offset := lsm.sstm.Get(k)
	if offset > -1 {
		// found it
		return v, nil
	}
	// TODO: complete...
	_ = path
	// could not find it
	return nil, ErrKeyNotFound
}

func (lsm *LSMTree) Del(k string) error {
	// write entry to write-ahead commit log
	_, err := lsm.walg.Write(k, Tombstone)
	if err != nil {
		return err
	}
	// write entry to mem-table
	err = lsm.memt.Put(k, Tombstone)
	if err != nil && err != memtable.ErrFlushThreshold {
		return err
	}
	// otherwise, maybe it's time to flush?
	if err == memtable.ErrFlushThreshold {
		// flush to sstable
		err = lsm.memt.FlushToSSTable(lsm.sstm)
		if err != nil {
			return err
		}
	}
	return ErrKeyNotFound
}

func (lsm *LSMTree) Close() error {
	// close wal
	err := lsm.walg.Close()
	if err != nil {
		return err
	}
	// close mem-table
	err = lsm.memt.Close()
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
