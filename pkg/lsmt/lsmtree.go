package lsmt

import (
	"github.com/scottcagno/storage/pkg/bloom"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/memtable"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"github.com/scottcagno/storage/pkg/lsmt/trees/rbtree"
	"os"
	"path/filepath"
	"sync"
)

const (
	walPath                      = "log"
	sstPath                      = "data"
	defaultBasePath              = "lsm-db"
	defaultFlushThreshold        = 1 << 20 // 1 MB
	defaultSyncOnWrite           = false
	defaultCompactAndMergeOnOpen = false
	defaultBloomFilterSize       = 1 << 12 // 4 KB
)

var defaultLSMConfig = &LSMConfig{
	BasePath:              defaultBasePath,
	FlushThreshold:        defaultFlushThreshold,
	SyncOnWrite:           defaultSyncOnWrite,
	CompactAndMergeOnOpen: defaultCompactAndMergeOnOpen,
	BloomFilterSize:       defaultBloomFilterSize,
}

type LSMConfig struct {
	BasePath              string // base storage path
	FlushThreshold        int64  // memtable flush threshold in KB
	SyncOnWrite           bool   // perform sync every time an entry is write
	CompactAndMergeOnOpen bool
	BloomFilterSize       uint // specify the bloom filter size
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
	if conf.BloomFilterSize < 1 {
		conf.BloomFilterSize = defaultBloomFilterSize
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
	bloom   *bloom.BloomFilter  // bloom is a bloom filter
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
		bloom:   bloom.NewBloomFilter(conf.BloomFilterSize),
	}
	if conf.CompactAndMergeOnOpen {
		// attempt to compact and merge
		err = lsmt.checkCompactAndMerge()
		if err != nil {
			return nil, err
		}
	}
	// populate bloom filter
	err = lsmt.populateBloomFilter()
	if err != nil {
		return nil, err
	}
	// return lsm-tree
	return lsmt, nil
}

func (lsm *LSMTree) populateBloomFilter() error {
	// lock
	lsm.lock.Lock()
	defer lsm.lock.Unlock()
	// add entries from mem-table
	lsm.memt.Scan(func(me rbtree.RBEntry) bool {
		// isolate binary entry
		e := me.(memtable.MemtableEntry).Entry
		// make sure entry is not a tombstone
		if e != nil && e.Value != nil {
			// add entry to bloom filter
			lsm.bloom.Set(e.Key)
		}
		return true
	})
	// add entries from linear ss-table scan
	err := lsm.sstm.Scan(sstable.ScanNewToOld, func(e *binary.Entry) bool {
		// make sure entry is not a tombstone
		if e != nil && e.Value != nil {
			// add entry to bloom filter
			lsm.bloom.Set(e.Key)
		}
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

func (lsm *LSMTree) checkCompactAndMerge() error {
	// lock
	lsm.lock.Lock()
	defer lsm.lock.Unlock()
	// do compact
	err := lsm.sstm.CompactAllSSTables()
	if err != nil {
		return err
	}
	// set merge threshold
	mergeThreshold := 16
	// do merge
	err = lsm.sstm.MergeAllSSTables(mergeThreshold)
	if err != nil {
		return err
	}
	return nil
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
	// add to bloom filter
	lsm.bloom.Set([]byte(k))
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
	// add to bloom filter
	for _, e := range batch.Entries {
		lsm.bloom.Set(e.Key)
	}
	return nil
}

func (lsm *LSMTree) Has(k string) bool {
	// check bloom filter
	if ok := lsm.bloom.MayHave([]byte(k)); !ok {
		// definitely not in the lsm tree
		return false
	}
	// low probability of false positive,
	// but let's check the mem-table
	if ok := lsm.memt.Has(k); ok {
		return true
	}
	// so I suppose at this point it's really
	// unlikely to be found, but let's search
	// anyway, because well... why not?
	de := lsm.sstm.LinearSearch(k)
	if de == nil || de.Value == nil {
		return false
	}
	// otherwise, we found it homey!
	return true
}

func (lsm *LSMTree) Get(k string) ([]byte, error) {
	// check bloom filter
	if ok := lsm.bloom.MayHave([]byte(k)); !ok {
		// definitely not in the lsm tree
		return nil, ErrNotFound
	}
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
	// check sparse index, and ss-tables, young to old
	de, err := lsm.sstm.Search(k)
	if err != nil {
		// if we get a bad entry, it most likely means
		// that our sparse index couldn't find it, but
		// there is still a chance it may be on disk
		if err == binary.ErrBadEntry {
			// do linear semi-binary-ish search
			de = lsm.sstm.LinearSearch(k)
			if de == nil || de.Value == nil {
				return nil, ErrNotFound
			}
			// otherwise, we found it homey!
			return de.Value, nil
		}
		// -> IF YOU ARE HERE...
		// Then the value may not be here (or you didn't check
		// all the potential errors that can be returned), dummy
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
