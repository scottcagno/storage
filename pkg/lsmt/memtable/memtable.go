package memtable

import (
	"bytes"
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"github.com/scottcagno/storage/pkg/lsmt/trees/rbtree"
	"github.com/scottcagno/storage/pkg/lsmt/wal"
	"os"
	"strings"
)

const defaultFlushThreshold = 256 << 10 // 256KB

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

type Memtable struct {
	base  string
	flush int64
	data  *rbtree.RBTree
	wacl  *wal.WAL
}

func OpenMemtable(base string, flush int64) (*Memtable, error) {
	if flush < 1 {
		flush = defaultFlushThreshold
	}
	// open write-ahead commit log
	wacl, err := wal.OpenWAL(base)
	if err != nil {
		return nil, err
	}
	// create new memtable
	memt := &Memtable{
		base:  base,
		flush: flush << 10, // flush x KB
		data:  rbtree.NewRBTree(),
		wacl:  wacl,
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
	// close write-ahead commit log
	err := mt.wacl.Close()
	if err != nil {
		return err
	}
	// wipe write-ahead commit log
	err = os.RemoveAll(mt.base)
	if err != nil {
		return err
	}
	// open fresh write-ahead commit log
	mt.wacl, err = wal.OpenWAL(mt.base)
	if err != nil {
		return err
	}
	// reset tree data
	mt.data.Reset()
	return nil
}

func (mt *Memtable) FlushToSSTable(sstm *sstable.SSTManager) error {
	// error check
	if sstm == nil {
		return binary.ErrFileClosed
	}
	// lock tree
	mt.data.Lock()
	defer mt.data.Unlock()
	// make new ss-table batch
	batch := sstm.NewBatch()
	// scan the whole tree and write each entry to the batch
	mt.data.Scan(func(e rbtree.RBEntry) bool {
		batch.WriteEntry(e.(memtableEntry).Entry)
		return true
	})
	// pass batch to sst-manager
	err := sstm.WriteBatch(batch)
	if err != nil {
		return err
	}
	// reset tree
	mt.data.Reset()
	return nil
}

func (mt *Memtable) insert(e *binary.Entry) error {
	mt.data.Put(memtableEntry{Key: string(e.Key), Entry: e})
	if mt.data.Size() > mt.flush {
		return ErrFlushThreshold
	}
	return nil
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

func (mt *Memtable) Get(k string) (*binary.Entry, error) {
	v, ok := mt.data.Get(memtableEntry{Key: k})
	if !ok {
		return nil, ErrKeyNotFound
	}
	if v.(memtableEntry).Entry == nil || bytes.Equal(v.(memtableEntry).Entry.Value, sstable.Tombstone) {
		return nil, ErrFoundTombstone
	}
	return v.(memtableEntry).Entry, nil
}

func (mt *Memtable) Del(k string) error {
	// create delete entry
	e := &binary.Entry{Key: []byte(k), Value: sstable.Tombstone}
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

func (mt *Memtable) Close() error {
	mt.data.Close()
	err := mt.wacl.Close()
	if err != nil {
		return err
	}
	return nil
}
