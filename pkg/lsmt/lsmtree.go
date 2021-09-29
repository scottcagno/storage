package lsmt

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/rbtree/augmented"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"github.com/scottcagno/storage/pkg/lsmt/wal"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	walPath        = "log"
	sstPath        = "data"
	FlushThreshold = 256 << 10 // 256KB
)

type LSMTree struct {
	base    string       // base is the base filepath for the database
	walbase string       // walbase is the write-ahead commit log base filepath
	sstbase string       // sstbase is the sstable and index base filepath where data resides
	lock    sync.RWMutex // lock is a mutex that synchronizes access to the data
	walg    *wal.WAL     // walg is a write-ahead commit log
	//memt    *memtable.Memtable

	memt *augmented.RBTree
	sstm *sstable.SSTManager // sstm is the sorted-strings table manager
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
		walg:    walg,
		memt:    augmented.NewRBTree(),
		sstm:    sstm,
	}
	// load any commit log data
	err = lsmt.loadDataFromCommitLog()
	if err != nil {
		return nil, err
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
	lsm.memt.Put(memtableEntry{Key: string(e.Key), Entry: e})
	// check to see if mem-table size has hit the threshold
	if lsm.memt.Size() > FlushThreshold {
		// flush to sstable
		err = lsm.flushMemtableToSSTable()
		if err != nil {
			return err
		}
	}
	return nil
}

func (lsm *LSMTree) Get(k string) ([]byte, error) {
	// search memtable
	e, found := lsm.memt.Get(memtableEntry{Key: k})
	if found {
		return e.(memtableEntry).Entry.Value, nil
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
	// create binary entry
	e := &binary.Entry{Key: []byte(k), Value: sstable.Tombstone}
	// write entry to write-ahead commit log
	_, err := lsm.walg.Write(e)
	if err != nil {
		return err
	}
	// write entry to mem-table
	lsm.memt.Put(memtableEntry{Key: string(e.Key), Entry: e})
	// check to see if mem-table size has hit the threshold
	if lsm.memt.Size() > FlushThreshold {
		// flush to sstable
		err = lsm.flushMemtableToSSTable()
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
	lsm.memt.Close()
	// close sst-manager
	err = lsm.sstm.Close()
	if err != nil {
		return err
	}
	return nil
}

type memtableEntry struct {
	Key   string
	Entry *binary.Entry
}

func (me memtableEntry) Compare(that augmented.RBEntry) int {
	return strings.Compare(me.Key, that.(memtableEntry).Key)
}

func (me memtableEntry) Size() int {
	return len(me.Key) + len(me.Entry.Key) + len(me.Entry.Value)
}

func (me memtableEntry) String() string {
	return fmt.Sprintf("entry.key=%q", me.Key)
}

// loadDataFromCommitLog loads any entries from the supplied segmented file back into the memtable
func (lsm *LSMTree) loadDataFromCommitLog() error {
	err := lsm.walg.Scan(func(e *binary.Entry) bool {
		lsm.memt.Put(memtableEntry{Key: string(e.Key), Entry: e})
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

func (lsm *LSMTree) flushMemtableToSSTable() error {
	// make new ss-table batch
	batch := lsm.sstm.NewBatch()
	// scan the whole tree and write each entry to the batch
	lsm.memt.Scan(func(e augmented.RBEntry) bool {
		batch.WriteEntry(e.(memtableEntry).Entry)
		return true
	})
	// pass batch to sst-manager
	err := lsm.sstm.WriteBatch(batch)
	if err != nil {
		return err
	}
	// "free" batch
	batch = nil
	// reset tree
	lsm.memt.Reset()
	return nil
}
