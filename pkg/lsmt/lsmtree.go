package lsmt

import (
	"bytes"
	"github.com/scottcagno/storage/pkg/bloom"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/memtable"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"github.com/scottcagno/storage/pkg/lsmt/trees/rbtree"
	"os"
	"path/filepath"
	"sync"
)

// LSMTree is an LSMTree
type LSMTree struct {
	conf    *LSMConfig
	walbase string              // walbase is the write-ahead commit log base filepath
	sstbase string              // sstbase is the sstable and index base filepath where data resides
	lock    sync.RWMutex        // lock is a mutex that synchronizes access to the data
	memt    *memtable.Memtable  // memt is the main memtable instance
	sstm    *sstable.SSTManager // sstm is the sorted-strings table manager
	bloom   *bloom.BloomFilter  // bloom is a bloom filter

	mtable *rbtree.RBTree
}

// OpenLSMTree opens or creates an LSMTree instance.
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
	// open ss-table-manager
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

		mtable: rbtree.NewRBTree(),
	}
	// populate bloom filter
	err = lsmt.populateBloomFilter()
	if err != nil {
		return nil, err
	}
	// return lsm-tree
	return lsmt, nil
}

// populateBloomFilter attempts to read through the keys in the
// mem-table, and then the ss-table(s) and fill out the bloom
// filter as thoroughly as possible.
func (lsm *LSMTree) populateBloomFilter() error {
	// lock
	lsm.lock.Lock()
	defer lsm.lock.Unlock()
	var count int
	// add entries from mem-table
	lsm.memt.Scan(func(me rbtree.RBEntry) bool {
		// isolate binary entry
		e := me.(memtable.MemtableEntry).Entry
		// make sure entry is not a tombstone
		if e != nil && e.Value != nil && !bytes.Equal(e.Value, memtable.Tombstone) {
			// add entry to bloom filter
			lsm.bloom.Set(e.Key)
			count++
		}
		return true
	})
	// add entries from linear ss-table scan
	err := lsm.sstm.Scan(sstable.ScanNewToOld, func(e *binary.Entry) bool {
		// make sure entry is not a tombstone
		if e != nil && e.Value != nil && !bytes.Equal(e.Value, sstable.Tombstone) {
			// add entry to bloom filter
			lsm.bloom.Set(e.Key)
			count++
		}
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

// Put takes a key and a value and adds them to the LSMTree. If
// the entry already exists, it should overwrite the old entry.
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

// PutBatch takes a batch of entries and adds all of them at
// one time. It acts a bit like a transaction. If you have a
// configuration option of SyncOnWrite: true it will be disabled
// temporarily and the batch will sync at the end of all the
// writes. This is to give a slight performance advantage. It
// should be worth noting that very large batches may have an
// impact on performance and may also cause frequent ss-table
// flushes which may result in fragmentation.
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

// Has returns a boolean signaling weather or not the key
// is in the LSMTree. It should be noted that in some cases
// this may return a false positive, but it should never
// return a false negative.
func (lsm *LSMTree) Has(k string) bool {
	// check bloom filter
	if ok := lsm.bloom.MayHave([]byte(k)); !ok {
		// definitely not in the bloom filter
		return false
	}
	// low probability of false positive,
	// but let's check the mem-table
	if ok := lsm.memt.Has(k); ok {
		// definitely in the mem-table, return true.
		// it should be noted that we cannot return
		// false from here, because if we do we are
		// saying that it is not in the mem-table, but
		// it still could be found on disk....
		return true
	}
	// so I suppose at this point it's really
	// unlikely to be found, but let's search
	// anyway, because well... why not?
	de := lsm.sstm.LinearSearch(k)
	if de == nil || de.Value == nil {
		// definitely not in the ss-table
		return false
	}
	// otherwise, we MAY have found it
	return true
}

// Get takes a key and attempts to find a match in the LSMTree. If
// a match cannot be found Get returns a nil value and ErrNotFound.
// Get first checks the bloom filter, then the mem-table. If it is
// still not found it attempts to do a binary search on the for the
// key in the ss-index and if that yields no result it will try to
// find the entry by doing a linear search of the ss-table itself.
func (lsm *LSMTree) Get(k string) ([]byte, error) {
	// check bloom filter
	if ok := lsm.bloom.MayHave([]byte(k)); !ok {
		// definitely not in the lsm tree
		return nil, ErrNotFound
	}
	// according to the bloom filter, it "may" be in
	// tree, so lets start by searching the mem-table
	e, err := lsm.memt.Get(k)
	if err == nil {
		// we found it!
		return e.Value, nil
	}
	// we did not find it in the mem-table
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

// GetBatch attempts to find entries matching the keys provided. If a matching
// entry is found, it is added to the batch that is returned. If a matching
// entry cannot be found it is simply skipped and not added to the batch. GetBatch
// will return a nil error if all the matching entries were found. If it found
// some but not all, GetBatch will return ErrIncompleteSet along with the batch
// of entries that it could find. If it could not find any matches at all, the
// batch will be nil and GetBatch will return an ErrNotFound
func (lsm *LSMTree) GetBatch(keys ...string) (*binary.Batch, error) {
	// create batch to return
	batch := binary.NewBatch()
	// iterate over keys
	for _, key := range keys {
		// check bloom filter
		if ok := lsm.bloom.MayHave([]byte(key)); !ok {
			// definitely not in the lsm tree
			continue // skip and look for next key
		}
		// according to the bloom filter, it "may" be in
		// tree, so lets start by searching the mem-table
		e, err := lsm.memt.Get(key)
		if err == nil {
			// we found a match! add match to batch, and...
			batch.WriteEntry(e)
			continue // skip and lok for next key
		}
		// we did not find it in the mem-table
		// need to check error for tombstone
		if e == nil && err == memtable.ErrFoundTombstone {
			// found tombstone entry (means this entry was
			// deleted) so we can end our search here
			continue // skip and look for the next key
		}
		// boom filter says maybe, checked the mem-table with
		// no luck apparently, so now let us check the sparse
		// index and see what we come up with
		de, err := lsm.sstm.Search(key)
		if err != nil {
			// if we get a bad entry, it most likely means
			// that our sparse index couldn't find it, but
			// there is still a chance it may be on disk
			if err == binary.ErrBadEntry {
				// do linear semi-binary-ish search
				de = lsm.sstm.LinearSearch(key)
				if de == nil || de.Value == nil {
					continue // skip and look for the next key
				}
				// otherwise, we found it homey! add match to batch, and...
				batch.WriteEntry(de)
				continue // skip and lok for next key
			}
			// -> IF YOU ARE HERE...
			// Then the value may not be here (or you didn't check
			// all the potential errors that can be returned), dummy
			continue // skip and lok for next key
		}
		// check to make sure entry is not a tombstone
		if de == nil || de.Value == nil {
			continue // skip and lok for next key
		}
		// may have found it; add match to batch, and...
		batch.WriteEntry(de)
		continue // skip and lok for next key
	}
	// check the batch
	if batch.Len() == 0 {
		// nothing at all was found
		return nil, ErrNotFound
	}
	if batch.Len() == len(keys) {
		// we found all the potential matches!
		return batch, nil
	}
	return batch, ErrIncompleteSet
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
	// remove from bloom filter
	lsm.bloom.Unset([]byte(k))
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
