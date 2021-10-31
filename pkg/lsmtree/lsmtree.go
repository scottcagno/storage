package lsmtree

import (
	"sync"
)

type LSMTree struct {
	lock   sync.RWMutex
	opt    *Options
	logDir string
	sstDir string
	wacl   *commitLog
	memt   *memTable
	sstm   *ssTableManager
}

// OpenLSMTree opens or creates an LSMTree instance
func OpenLSMTree(options *Options) (*LSMTree, error) {
	return nil, nil
}

// Has returns a boolean signaling weather or not the key
// is in the LSMTree. It should be noted that in some cases
// this may return a false positive, but it should never
// return a false negative.
func (lsm *LSMTree) Has(k []byte) (bool, error) {
	// read lock
	lsm.lock.RLock()
	defer lsm.lock.RUnlock()
	// make entry and check it
	e := &Entry{Key: k}
	err := checkKey(e, lsm.opt.MaxKeySize)
	if err != nil {
		return false, err
	}
	return false, nil
}

// Get takes a key and attempts to find a match in the LSMTree. If
// a match cannot be found Get returns a nil value and ErrNotFound.
// Get first checks the bloom filter, then the mem-table. If it is
// still not found it attempts to do a binary search on the for the
// key in the ss-index and if that yields no result it will try to
// find the entry by doing a linear search of the ss-table itself.
func (lsm *LSMTree) Get(k []byte) ([]byte, error) {
	// read lock
	lsm.lock.RLock()
	defer lsm.lock.RUnlock()
	// make entry and check it
	e := &Entry{Key: k}
	err := checkKey(e, lsm.opt.MaxKeySize)
	if err != nil {
		return nil, err
	}
	// call internal getEntry
	ent, err := lsm.getEntry(e)
	if err != nil {
		return nil, err
	}
	// otherwise, we got it!
	return ent.Value, nil
}

// Put takes a key and a value and adds them to the LSMTree. If
// the entry already exists, it should overwrite the old entry.
func (lsm *LSMTree) Put(k, v []byte) error {
	// write lock
	lsm.lock.Lock()
	defer lsm.lock.Unlock()
	// make entry
	e := &Entry{
		Key:   k,
		Value: v,
		CRC:   checksum(append(k, v...)),
	}
	// check entry
	err := checkEntry(e, lsm.opt.MaxKeySize, lsm.opt.MaxValueSize)
	if err != nil {
		return err
	}
	// call internal putEntry
	err = lsm.putEntry(e)
	if err != nil {
		return err
	}
	return nil
}

// Del takes a key and overwrites the record with a Tombstone or
// a 'deleted' or nil entry. It leaves the key in the LSMTree
// so that future table versions can properly merge.
func (lsm *LSMTree) Del(k []byte) error {
	// write lock
	lsm.lock.Lock()
	defer lsm.lock.Unlock()
	// make entry
	e := &Entry{
		Key:   k,
		Value: makeTombstone(),
		CRC:   checksum(append(k, Tombstone...)),
	}
	// check entry
	err := checkEntry(e, lsm.opt.MaxKeySize, lsm.opt.MaxValueSize)
	if err != nil {
		return err
	}
	// call internal delEntry
	err = lsm.delEntry(e)
	if err != nil {
		return err
	}
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
func (lsm *LSMTree) PutBatch(b *Batch) error {
	return nil
}

// GetBatch attempts to find entries matching the keys provided. If a matching
// entry is found, it is added to the batch that is returned. If a matching
// entry cannot be found it is simply skipped and not added to the batch. GetBatch
// will return a nil error if all the matching entries were found. If it found
// some but not all, GetBatch will return ErrIncompleteSet along with the batch
// of entries that it could find. If it could not find any matches at all, the
// batch will be nil and GetBatch will return an ErrNotFound
func (lsm *LSMTree) GetBatch(keys ...[]byte) (*Batch, error) {
	return nil, nil
}

// Sync forces a sync on all underlying structures no matter what the configuration
func (lsm *LSMTree) Sync() error {
	return nil
}

// Close syncs and closes the LSMTree
func (lsm *LSMTree) Close() error {
	return nil
}

// getEntry is the internal "get" implementation
func (lsm *LSMTree) getEntry(e *Entry) (*Entry, error) {
	// look in mem-table
	ent, err := lsm.memt.get(e)
	if err == nil {
		// found it
		return ent, nil
	}
	// look in ss-tables
	ent, err = lsm.sstm.get(e)
	if err == nil {
		// found it
		return ent, nil
	}
	return nil, ErrNotFound
}

// putEntry is the internal "get" implementation
func (lsm *LSMTree) putEntry(e *Entry) error {
	// write entry to the commit log
	err := lsm.wacl.put(e)
	if err != nil {
		return err
	}
	// write entry to the mem-table
	err = lsm.memt.put(e)
	// check if we should do a flush
	if err != nil && err == ErrFlushThreshold {
		// attempt to flush
		err = lsm.flushToSSTable(lsm.memt)
		if err != nil {
			return err
		}
		// cycle the commit log
		err = lsm.cycleCommitLog()
		if err != nil {
			return err
		}
	}
	return nil
}

// delEntry is the internal "get" implementation
func (lsm *LSMTree) delEntry(e *Entry) error {
	// write entry to the commit log
	err := lsm.wacl.put(e)
	if err != nil {
		return err
	}
	// write entry to the mem-table
	err = lsm.memt.put(e)
	// check if we should do a flush
	if err != nil && err == ErrFlushThreshold {
		// attempt to flush
		err = lsm.flushToSSTable(lsm.memt)
		if err != nil {
			return err
		}
		// cycle the commit log
		err = lsm.cycleCommitLog()
		if err != nil {
			return err
		}
	}
	return nil
}

// loadDataFromCommitLog looks for any commit logs on disk
// and reads the contents of the commit log in order to
// re-populate the MemTable on a restart
func (lsm *LSMTree) loadDataFromCommitLog() error {
	// write lock
	lsm.lock.Lock()
	defer lsm.lock.Unlock()

	return nil
}

// cycleCommitLog closes the current commit log and removes
// the log files on disk and opens a fresh one. This is used
// after a mem-table is flushed out to disk as a ss-table.
func (lsm *LSMTree) cycleCommitLog() error {
	return nil
}

// flushToSSTable takes a mem-table instance and flushes it
// to disk as a ss-table. After flushing the mem-table is
// reset (cleared out) and the commit log is cycled.
func (lsm *LSMTree) flushToSSTable(memt *memTable) error {
	return nil
}
