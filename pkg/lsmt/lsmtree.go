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
	base        string // base is the base filepath for the database
	fullWALPath string
	fullSSTPath string
	lock        sync.RWMutex // lock is a mutex that synchronizes access to the data
	walg        *wal.WAL
	memt        *memtable.Memtable
	sstm        *sstable.SSTManager
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
	fullWALPath := filepath.Join(base, walPath)
	err = os.MkdirAll(fullWALPath, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// create data base directory
	fullSSTPath := filepath.Join(base, sstPath)
	err = os.MkdirAll(fullSSTPath, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// open write-ahead logger
	walg, err := wal.Open(fullWALPath)
	if err != nil {
		return nil, err
	}
	// open mem-table
	memt, err := memtable.Open(walg)
	if err != nil {
		return nil, err
	}
	// open sst-manager
	sstm, err := sstable.Open(fullSSTPath)
	if err != nil {
		return nil, err
	}
	// create lsm-tree instance and return
	lsmt := &LSMTree{
		base:        base,
		fullWALPath: fullWALPath,
		fullSSTPath: fullSSTPath,
		walg:        walg,
		memt:        memt,
		sstm:        sstm,
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
	// [1] check mem-table first (mutable, then immutable)
	// [2] check ss-manager sparse index
	// [3] check ss-tables young to old
	return nil, ErrKeyNotFound
}

func (lsm *LSMTree) Del(k string) error {
	return ErrKeyNotFound
}

func (lsm *LSMTree) Close() error {
	return nil
}
