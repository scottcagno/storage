package memtable

import (
	"bytes"
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/trees/rbtree"
	"github.com/scottcagno/storage/pkg/lsmt/wal"
	"os"
	"strings"
	"sync"
)

var Tombstone = []byte(nil)

type MemtableEntry = memtableEntry

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

type Memtable struct {
	lock sync.RWMutex
	conf *MemtableConfig
	data *rbtree.RBTree
	wacl *wal.WAL
}

func (mt *Memtable) Lock() {
	mt.lock.Lock()
}

func (mt *Memtable) Unlock() {
	mt.lock.Unlock()
}

func (mt *Memtable) RLock() {
	mt.lock.RLock()
}

func (mt *Memtable) RUnlock() {
	mt.lock.RUnlock()
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
	// create new memtable
	memt := &Memtable{
		conf: conf,
		data: rbtree.NewRBTree(),
		wacl: wacl,
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
	return mt.wacl.Scan(func(e *binary.Entry) bool {
		mt.data.Put(memtableEntry{Key: string(e.Key), Entry: e})
		return true
	})
}

func (mt *Memtable) Reset() error {
	// grab current configuration
	walConf := mt.wacl.GetConfig()
	// close write-ahead commit log
	err := mt.wacl.Close()
	if err != nil {
		return err
	}
	// wipe write-ahead commit log
	err = os.RemoveAll(mt.conf.BasePath)
	if err != nil {
		return err
	}
	// open fresh write-ahead commit log
	mt.wacl, err = wal.OpenWAL(walConf)
	if err != nil {
		return err
	}
	// reset tree data
	mt.data.Reset()
	return nil
}

func (mt *Memtable) insert(e *binary.Entry) error {
	mt.data.Put(memtableEntry{Key: string(e.Key), Entry: e})
	if mt.data.Size() > mt.conf.FlushThreshold {
		return ErrFlushThreshold
	}
	return nil
}

func (mt *Memtable) Size() int64 {
	return mt.data.Size()
}

func (mt *Memtable) ShouldFlush() bool {
	return mt.data.Size() > mt.conf.FlushThreshold
}

func (mt *Memtable) Put(e *binary.Entry) error {
	// write entry to the write-ahead commit log
	_, err := mt.wacl.Write(e)
	if err != nil {
		return err
	}
	// write entry to the mem-table
	err = mt.insert(e)
	if err != nil {
		return err
	}
	return nil
}

func (mt *Memtable) PutBatch(batch *binary.Batch) error {
	// write batch to the write-ahead commit log
	err := mt.wacl.WriteBatch(batch)
	if err != nil {
		return err
	}
	// write batch entries to the mem-table
	for i := range batch.Entries {
		e := batch.Entries[i]
		mt.data.Put(memtableEntry{Key: string(e.Key), Entry: e})
	}
	// after batch writing is finished, check
	// and return to flush or not to flush
	if mt.data.Size() > mt.conf.FlushThreshold {
		return ErrFlushThreshold
	}
	return nil
}

func (mt *Memtable) Has(k string) bool {
	return mt.data.Has(memtableEntry{Key: k})
}

func (mt *Memtable) Get(k string) (*binary.Entry, error) {
	v, ok := mt.data.Get(memtableEntry{Key: k})
	if !ok {
		return nil, ErrKeyNotFound
	}
	if v.(memtableEntry).Entry == nil || bytes.Equal(v.(memtableEntry).Entry.Value, Tombstone) {
		return nil, ErrFoundTombstone
	}
	return v.(memtableEntry).Entry, nil
}

func (mt *Memtable) Del(k string) error {
	// create delete entry
	e := &binary.Entry{Key: []byte(k), Value: Tombstone}
	// write entry to the write-ahead commit log
	_, err := mt.wacl.Write(e)
	if err != nil {
		return err
	}
	// write entry to the mem-table
	err = mt.insert(e)
	if err != nil {
		return err
	}
	return nil
}

func (mt *Memtable) Scan(iter func(me rbtree.RBEntry) bool) {
	if mt.data.Len() < 1 {
		return
	}
	mt.data.Scan(iter)
}

func (mt *Memtable) Len() int {
	return mt.data.Len()
}

func (mt *Memtable) GetConfig() *MemtableConfig {
	return mt.conf
}

func (mt *Memtable) Sync() error {
	return mt.wacl.Sync()
}

func (mt *Memtable) Close() error {
	mt.data.Close()
	err := mt.wacl.Close()
	if err != nil {
		return err
	}
	return nil
}
