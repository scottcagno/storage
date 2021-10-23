package v2

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/trees/rbtree"
	"github.com/scottcagno/storage/pkg/lsmt/wal"
	"path/filepath"
	"strings"
	"sync"
)

type memtableEntry struct {
	Key   string
	Entry *binary.Entry
}

func (me memtableEntry) Compare(that rbtree.RBEntry) int {
	return strings.Compare(me.Key, that.(memtableEntry).Key)
}

func (me memtableEntry) Size() int {
	return len(me.Key) + len(me.Entry.Key) + len(me.Entry.Value)
}

func (me memtableEntry) String() string {
	return fmt.Sprintf("entry.key=%q", me.Key)
}

const (
	defaultBasePath       = "log"
	defaultFlushThreshold = 1 << 20 // 1 MB
	defaultSyncOnWrite    = false
)

var defaultMemtableConfig = &MemtableConfig{
	BasePath:       defaultBasePath,
	FlushThreshold: defaultFlushThreshold,
	SyncOnWrite:    defaultSyncOnWrite,
}

type MemtableConfig struct {
	BasePath       string // base storage path
	FlushThreshold int64  // memtable flush threshold in KB
	SyncOnWrite    bool   // perform sync every time an entry is write
}

func checkMemtableConfig(conf *MemtableConfig) *MemtableConfig {
	if conf == nil {
		return defaultMemtableConfig
	}
	if conf.BasePath == *new(string) {
		conf.BasePath = defaultBasePath
	}
	if conf.FlushThreshold < 1 {
		conf.FlushThreshold = defaultFlushThreshold
	}
	return conf
}

// Memtable represents a mem-table
type Memtable struct {
	lock     sync.RWMutex
	conf     *MemtableConfig
	wacl     *wal.WAL
	wacls    []*wal.WAL
	data     *rbtree.RBTree
	overflow *binary.Batch
}

func OpenMemtable(c *MemtableConfig) (*Memtable, error) {
	// check memtable config
	conf := checkMemtableConfig(c)
	// open write-ahead commit log
	wacl, err := wal.OpenWAL(&wal.WALConfig{
		BasePath:    conf.BasePath,
		MaxFileSize: -1, // use wal defaultMaxFileSize
		SyncOnWrite: conf.SyncOnWrite,
	})
	if err != nil {
		return nil, err
	}
	// open write-ahead commit log (0)
	wacl0, err := wal.OpenWAL(&wal.WALConfig{
		BasePath:    filepath.Join(conf.BasePath, fmt.Sprintf("%06d", 0)),
		MaxFileSize: -1, // use wal defaultMaxFileSize
		SyncOnWrite: conf.SyncOnWrite,
	})
	if err != nil {
		return nil, err
	}
	// open write-ahead commit log (1)
	wacl1, err := wal.OpenWAL(&wal.WALConfig{
		BasePath:    filepath.Join(conf.BasePath, fmt.Sprintf("%06d", 1)),
		MaxFileSize: -1, // use wal defaultMaxFileSize
		SyncOnWrite: conf.SyncOnWrite,
	})
	if err != nil {
		return nil, err
	}
	wacls := []*wal.WAL{
		wacl0,
		wacl1,
	}
	// create new memtable
	memt := &Memtable{
		conf:     conf,
		wacl:     wacl,
		wacls:    wacls,
		data:     rbtree.NewRBTree(),
		overflow: binary.NewBatch(),
	}
	// load mem-table entries from commit log
	err = memt.loadDataFromCommitLog()
	if err != nil {
		return nil, err
	}
	return memt, nil
}

// loadEntries loads any entries from the supplied segmented file back into the memtable
func (mt *Memtable) loadDataFromCommitLog() error {
	// lock
	mt.lock.Lock()
	defer mt.lock.Unlock()
	// scan write-ahead commit log
	err := mt.wacl.Scan(func(e *binary.Entry) bool {
		mt.data.Put(memtableEntry{Key: string(e.Key), Entry: e})
		return true
	})
	if err != nil {
		return err
	}
	// scan write-ahead commit log (0)
	err = mt.wacls[0].Scan(func(e *binary.Entry) bool {
		mt.data.Put(memtableEntry{Key: string(e.Key), Entry: e})
		return true
	})
	if err != nil {
		return err
	}
	// scan write-ahead commit log (1)
	err = mt.wacls[1].Scan(func(e *binary.Entry) bool {
		mt.data.Put(memtableEntry{Key: string(e.Key), Entry: e})
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

func (mt *Memtable) Put(e *binary.Entry) error {
	// check to see if mem-table is "full"
	if mt.data.Size() > mt.conf.FlushThreshold {
		// it's full, so proceed along "corrected course"
		return mt.insertFull(e)
	}
	// not full, so just proceed normally
	return mt.insertNonFull(e)
}

func (mt *Memtable) insertFull(e *binary.Entry) error {
	// lock
	mt.lock.Lock()
	defer mt.lock.Unlock()
	// write entry to the (overflow) write-ahead commit log first
	_, err := mt.wacls[1].Write(e)
	if err != nil {
		return err
	}
	// then insert entry into the mem-table's overflow batch
	mt.overflow.WriteEntry(e)
	// return that the mem-table has reached the
	// provided flush threshold and is full
	return ErrFlushThreshold
}

func (mt *Memtable) insertNonFull(e *binary.Entry) error {
	// lock
	mt.lock.Lock()
	defer mt.lock.Unlock()
	// write entry to the write-ahead commit log first
	_, err := mt.wacls[0].Write(e)
	if err != nil {
		return err
	}
	// then insert entry into the mem-table's red-black tree
	mt.data.Put(memtableEntry{Key: string(e.Key), Entry: e})
	return nil
}

func (mt *Memtable) Reset() error {
	// lock
	mt.lock.Lock()
	defer mt.lock.Unlock()
	//
	return nil
}
