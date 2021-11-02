package lsmtree

import (
	"os"
	"path/filepath"
	"sync"
)

const ()

type LSMTree struct {
	lock   sync.RWMutex
	opt    *Options
	logDir string
	sstDir string
	wacl   *commitLog
	memt   *memTable
	sstm   *ssTableManager
	logger *Logger
}

// OpenLSMTree opens or creates an LSMTree instance
func OpenLSMTree(options *Options) (*LSMTree, error) {
	// check lsm config
	opt := checkOptions(options)
	// initialize base path
	base, err := initBasePath(opt.BaseDir)
	if err != nil {
		return nil, err
	}
	// create commit log base directory
	logdir := filepath.Join(base, defaultWalDir)
	err = os.MkdirAll(logdir, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// open commit log
	wacl, err := openCommitLog(logdir, opt.SyncOnWrite)
	if err != nil {
		return nil, err
	}
	// create ss-table data base directory
	sstdir := filepath.Join(base, defaultSstDir)
	err = os.MkdirAll(sstdir, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// open ss-table-manager
	sstm, err := openSSTableManager(sstdir)
	if err != nil {
		return nil, err
	}
	// open memtable
	memt, err := openMemTable(opt.flushThreshold)
	if err != nil {
		return nil, err
	}
	// create lsm-tree instance
	lsmt := &LSMTree{
		opt:    opt,
		logDir: logdir,
		sstDir: sstdir,
		wacl:   wacl,
		memt:   memt,
		sstm:   sstm,
		logger: newLogger(opt.LoggingLevel),
	}
	// load mem-table with commit log data
	err = lsmt.loadDataFromCommitLog()
	if err != nil {
		return nil, err
	}
	// return lsm-tree
	return lsmt, nil
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
	err := checkKey(e)
	if err != nil {
		return false, err
	}
	// call internal get method
	e, err = lsm.getEntry(e)
	if err != nil {
		return false, err
	}
	// otherwise, we got it
	return e != nil, err
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
	err := checkKey(e)
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
	err := checkEntry(e)
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
	err := checkEntry(e)
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
	// lock
	lsm.lock.Lock()
	defer lsm.lock.Unlock()
	// iterate batch entries
	for _, e := range b.Entries {
		// call internal putEntry
		err := lsm.putEntry(e)
		if err != nil {
			return err
		}
	}
	// we're done
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
	// read lock
	lsm.lock.RLock()
	defer lsm.lock.RUnlock()
	// create new batch to return
	batch := NewBatch()
	// iterate over keys
	for _, k := range keys {
		// make entry and check it
		e := &Entry{Key: k}
		err := checkKey(e)
		if err != nil {
			return nil, err
		}
		// call internal getEntry
		ent, err := lsm.getEntry(e)
		if err != nil {
			if err == ErrFoundTombstone {
				// found tombstone entry (means this entry was
				// deleted) so we can try to find the next one
				continue
			}
			// if not tombstone, then it may be a bad entry, or a
			// bad checksum, or not found! either way, return err
			return nil, err
		}
		// otherwise, we got it! add to batch
		_ = batch.writeEntry(ent)
		continue
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
	// otherwise, we found some but not all
	return batch, ErrIncompleteSet
}

// Sync forces a sync on all underlying structures no matter what the configuration
func (lsm *LSMTree) Sync() error {
	// write lock
	lsm.lock.Lock()
	defer lsm.lock.Unlock()
	// sync commit log
	err := lsm.wacl.sync()
	if err != nil {
		return err
	}
	return nil
}

// Close syncs and closes the LSMTree
func (lsm *LSMTree) Close() error {
	// close commit log
	err := lsm.wacl.close()
	if err != nil {
		return err
	}
	return nil
}

// getEntry is the internal "get" implementation
func (lsm *LSMTree) getEntry(e *Entry) (*Entry, error) {
	// check to make sure there is data
	if lsm.memt.table.size < 1 {
		return nil, ErrNoDataFound
	}
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
	_, err := lsm.wacl.put(e)
	if err != nil {
		return err
	}
	// write entry to the mem-table
	err = lsm.memt.put(e)
	// check if we should do a flush
	if err != nil && err == ErrFlushThreshold {
		// attempt to flush
		err = lsm.sstm.flushToSSTable(lsm.memt)
		if err != nil {
			return err
		}
		// cycle the commit log
		err = lsm.wacl.cycle()
		if err != nil {
			return err
		}
	}
	return nil
}

// delEntry is the internal "get" implementation
func (lsm *LSMTree) delEntry(e *Entry) error {
	// write entry to the commit log
	_, err := lsm.wacl.put(e)
	if err != nil {
		return err
	}
	// write entry to the mem-table
	err = lsm.memt.put(e)
	// check if we should do a flush
	if err != nil && err == ErrFlushThreshold {
		// attempt to flush
		err = lsm.sstm.flushToSSTable(lsm.memt)
		if err != nil {
			return err
		}
		// cycle the commit log
		err = lsm.wacl.cycle()
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
	// iterate through the commit log...
	err := lsm.wacl.scan(func(e *Entry) bool {
		// ...and insert entries back into mem-table
		err := lsm.memt.put(e)
		return err == nil
	})
	if err != nil {
		return err
	}
	return nil
}
