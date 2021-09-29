package memtable

import (
	"bytes"
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/rbtree/augmented"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"github.com/scottcagno/storage/pkg/lsmt/wal"
	"strings"
)

type rbTreeEntry struct {
	Key   string
	Entry *binary.Entry
}

func (r rbTreeEntry) Compare(that augmented.RBEntry) int {
	return strings.Compare(r.Key, that.(rbTreeEntry).Key)
}

func (r rbTreeEntry) Size() int {
	return len(r.Key) + len(r.Entry.Key) + len(r.Entry.Value)
}

func (r rbTreeEntry) String() string {
	return fmt.Sprintf("entry.key=%q", r.Key)
}

type Memtable struct {
	data *augmented.RBTree
}

func Open(walg *wal.WAL) (*Memtable, error) {
	// error check
	if walg == nil {
		return nil, binary.ErrFileClosed
	}
	// create new memtable
	memt := &Memtable{
		data: augmented.NewRBTree(),
	}
	// load mem-table from commit log
	err := memt.loadEntries(walg)
	if err != nil {
		return nil, err
	}
	return memt, nil
}

// loadEntries loads any entries from the supplied segmented file back into the memtable
func (mt *Memtable) loadEntries(walg *wal.WAL) error {
	return walg.Scan(func(e *binary.Entry) bool {
		mt.data.Put(rbTreeEntry{Key: string(e.Key), Entry: e})
		return true
	})
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
	mt.data.Scan(func(e augmented.RBEntry) bool {
		batch.WriteEntry(e.(rbTreeEntry).Entry)
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

func (mt *Memtable) Put(e *binary.Entry) error {
	return mt.insert(e)
}

func (mt *Memtable) insert(e *binary.Entry) error {
	mt.data.Put(rbTreeEntry{Key: string(e.Key), Entry: e})
	if mt.data.Size() > lsmt.FlushThreshold {
		return ErrFlushThreshold
	}
	return nil
}

func (mt *Memtable) Get(k string) (*binary.Entry, error) {
	v, ok := mt.data.Get(rbTreeEntry{Key: k})
	if !ok {
		return nil, lsmt.ErrKeyNotFound
	}
	if v.(rbTreeEntry).Entry == nil || bytes.Equal(v.(rbTreeEntry).Entry.Value, lsmt.Tombstone) {
		return nil, lsmt.ErrFoundTombstone
	}
	return v.(rbTreeEntry).Entry, nil
}

func (mt *Memtable) Del(k string) error {
	return mt.insert(&binary.Entry{Key: []byte(k), Value: lsmt.Tombstone})
}

func (mt *Memtable) Close() error {
	mt.data.Close()
	return nil
}
