package lsmt

import (
	"github.com/scottcagno/storage/pkg/lsmt/binary"
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
	base    string       // base is the base filepath for the database
	walbase string       // walbase is the write-ahead commit log base filepath
	sstbase string       // sstbase is the sstable and index base filepath where data resides
	lock    sync.RWMutex // lock is a mutex that synchronizes access to the data
	walg    *wal.WAL     // walg is a write-ahead commit log
	memt    *memtable.Memtable
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
	// open write-ahead logger
	walg, err := wal.Open(walbase)
	if err != nil {
		return nil, err
	}
	//// open mem-table
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
	// create binary entry
	e := &binary.Entry{Key: []byte(k), Value: v}
	// write entry to write-ahead commit log
	_, err := lsm.walg.Write(e)
	if err != nil {
		return err
	}
	// write entry to mem-table
	err = lsm.memt.Put(e)
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
	e, err := lsm.memt.Get(k)
	if err == nil {
		// found it
		return e.Value, nil
	}
	// check sparse index, and ss-tables, young to old
	e, err = lsm.sstm.Get(k)
	if err == nil {
		// found it
		return e.Value, nil
	}
	// could not find it
	return nil, ErrKeyNotFound
}

func (lsm *LSMTree) Del(k string) error {
	// create binary entry
	e := &binary.Entry{Key: []byte(k), Value: Tombstone}
	// write entry to write-ahead commit log
	_, err := lsm.walg.Write(e)
	if err != nil {
		return err
	}
	// write entry to mem-table
	err = lsm.memt.Put(e)
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
