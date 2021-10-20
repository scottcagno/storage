package lsmt

import (
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/memtable"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"github.com/scottcagno/storage/pkg/util"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	walPath               = "log"
	sstPath               = "data"
	defaultBasePath       = "lsm-db"
	defaultFlushThreshold = 1 << 20 // 1 MB
	defaultSyncOnWrite    = false
)

var defaultLSMConfig = &LSMConfig{
	BasePath:       defaultBasePath,
	FlushThreshold: defaultFlushThreshold,
	SyncOnWrite:    defaultSyncOnWrite,
}

type LSMConfig struct {
	BasePath       string // base storage path
	FlushThreshold int64  // memtable flush threshold in KB
	SyncOnWrite    bool   // perform sync every time an entry is write
}

func checkLSMConfig(conf *LSMConfig) *LSMConfig {
	if conf == nil {
		return defaultLSMConfig
	}
	if conf.BasePath == *new(string) {
		conf.BasePath = defaultBasePath
	}
	if conf.FlushThreshold < 1 {
		conf.FlushThreshold = defaultFlushThreshold
	}
	return conf
}

type LSMTree struct {
	conf    *LSMConfig
	walbase string              // walbase is the write-ahead commit log base filepath
	sstbase string              // sstbase is the sstable and index base filepath where data resides
	lock    sync.RWMutex        // lock is a mutex that synchronizes access to the data
	memt    *memtable.Memtable  // memt is the main memtable instance
	sstm    *sstable.SSTManager // sstm is the sorted-strings table manager
}

func OpenLSMTree(c *LSMConfig) (*LSMTree, error) {
	// check lsm config
	conf := checkLSMConfig(c)
	// make sure we are working with absolute paths
	base, err := filepath.Abs(conf.BasePath)
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
	memt, err := memtable.OpenMemtable(&memtable.MemtableConfig{
		BasePath:       walbase,
		FlushThreshold: conf.FlushThreshold,
		SyncOnWrite:    conf.SyncOnWrite,
	})
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
		conf:    conf,
		walbase: walbase,
		sstbase: sstbase,
		memt:    memt,
		sstm:    sstm,
	}
	// return lsm-tree
	return lsmt, nil
}

func (lsm *LSMTree) checkCompact() {
	// lock
	lsm.lock.Lock()
	defer lsm.lock.Unlock()
	// do compact
	if err := lsm.sstm.CompactAllSSTables(); err != nil {
		log.Printf("lsmt.checkCompact error: (%T) %v\n", err, err)
	}
	time.AfterFunc(5*time.Minute, func() { lsm.checkCompact() })
}

func (lsm *LSMTree) Put(k string, v []byte) error {
	// lock
	lsm.lock.Lock()
	defer lsm.lock.Unlock()
	// create binary entry
	e := &binary.Entry{Key: []byte(k), Value: v}
	// write entry to mem-table
	err := lsm.memt.Put(e)
	// check err properly
	if err != nil {
		// make sure it's the mem-table doesn't need flushing
		if err != memtable.ErrFlushThreshold {
			return err
		}
		// looks like it needs a flush
		err = lsm.sstm.FlushMemtableToSSTable(lsm.memt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (lsm *LSMTree) PutBatch(batch *binary.Batch) error {
	// lock
	lsm.lock.Lock()
	defer lsm.lock.Unlock()
	// write batch to mem-table
	err := lsm.memt.PutBatch(batch)
	// check err properly
	if err != nil {
		// make sure it's the mem-table doesn't need flushing
		if err != memtable.ErrFlushThreshold {
			return err
		}
		// looks like it needs a flush
		err = lsm.sstm.FlushMemtableToSSTable(lsm.memt)
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
	// did not find it in the mem-table
	// need to check error for tombstone
	if e == nil && err == memtable.ErrFoundTombstone {
		// found tombstone entry (means this entry was
		// deleted) so we can end our search here; just
		// MAKE SURE you check for tombstone errors!!!
		return nil, ErrNotFound
	}
	doBruteForceSearch := true
	// check sparse index, and ss-tables, young to old
	de, err := lsm.sstm.Search(k)
	if err != nil {
		if err == binary.ErrBadEntry {
			if doBruteForceSearch {
				util.DEBUG("LSMT=> Performing brute force search for key=%q\n", k)
				// do linear semi-binary-ish search
				de = lsm.sstm.LinearSearch(k)
				if de == nil || de.Value == nil {
					return nil, ErrNotFound
				}
				// otherwise, we found it homey!
				return de.Value, nil
			}
			// we can assume (at this point at least)
			// that the entry does not exist
			return nil, ErrNotFound
		}
		util.DEBUG(">>>>>>>>>>>>>> RIGHT HERE IS THE CULPRIT <<<<<<<<<<<<<<<<<")
		return nil, err
	}
	// check to make sure entry is not a tombstone
	if de == nil || de.Value == nil {
		return nil, ErrNotFound
	}
	// may have found it
	return de.Value, nil
}

type Iterator struct {
	entry *binary.Entry
}

func (lsm *LSMTree) GetIteratorAt(k string) (*Iterator, error) {
	// TODO: finish this...
	return nil, nil
}

func (lsm *LSMTree) Del(k string) error {
	// write entry to memtable
	err := lsm.memt.Del(k)
	// check err properly
	if err != nil {
		// make sure it's the mem-table doesn't need flushing
		if err != memtable.ErrFlushThreshold {
			return err
		}
		// looks like it needs a flush
		err = lsm.sstm.FlushMemtableToSSTable(lsm.memt)
		if err != nil {
			return err
		}
	}
	// update sparse index
	lsm.sstm.CheckDeleteInSparseIndex(k)
	return nil
}

func (lsm *LSMTree) Sync() error {
	// sync memtable
	err := lsm.memt.Sync()
	if err != nil {
		return err
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
