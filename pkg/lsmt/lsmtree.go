package lsmt

import (
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/memtable"
	"github.com/scottcagno/storage/pkg/lsmt/sfile"
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
	base        string // base is the base filepath for the database
	fullWALPath string
	fullSSTPath string
	lock        sync.RWMutex // lock is a mutex that synchronizes access to the data
	// note: should wal implementation go here, or in the memtable??
	walg *sfile.SegmentedFile
	memt *memtable.Memtable
	sstm *sstable.SSTManager
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
	walg, err := sfile.Open(fullWALPath)
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
	// create a new entry
	e := binary.Entry{Key: k, Value: v}
	// write entry to write-ahead commit log
	_, err := lsm.walg.WriteEntry(e)
	if err != nil {
		return err
	}
	// write entry to mem-table
	_, err = lsm.memt.Put(e)
	if err != nil {
		if err == ErrMaxSizeReached
	}
	return nil, nil
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
