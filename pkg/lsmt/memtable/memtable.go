package memtable

import (
	"errors"
	"github.com/scottcagno/storage/pkg/index/rbtree"
	"github.com/scottcagno/storage/pkg/lsmt"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"github.com/scottcagno/storage/pkg/wal"
	"runtime"
	"sync"
	"time"
)

const (
	walPath      = "data/memtable"
	sstPath      = "data/sstable"
	regularEntry = 0x1A
	removedEntry = 0x1B
)

var (
	ErrNotFound = errors.New("error: not found")
)

type Memtable struct {
	mu   sync.RWMutex
	mem  *rbtree.RBTree
	wal  *wal.WriteAheadLog
	size int64
}

func OpenMemtable() (*Memtable, error) {
	l, err := wal.Open(walPath)
	if err != nil {
		return nil, err
	}
	m := &Memtable{
		mem: rbtree.NewRBTree(),
		wal: l,
	}
	err = m.load()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Memtable) load() error {
	// read lock
	m.mu.RLock()
	defer m.mu.RUnlock()
	// check to see if there are entries in the
	// write-ahead log we must load back into
	// the Memtable
	if m.wal.Count() > 0 {
		err := m.wal.Scan(func(index uint64, data []byte) bool {
			if data == nil {
				return false
			}
			var ent *lsmt.Entry
			err := ent.UnmarshalBinary(data)
			if err != nil {
				return false
			}
			err = m.Put(ent.Key, ent.Value)
			if err != nil {
				return false
			}
			return true
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Memtable) Put(key string, value []byte) error {
	// lock
	m.mu.Lock()
	defer m.mu.Unlock()
	// encode key-value pair into a new record
	ent := &lsmt.Entry{
		Type:      regularEntry,
		Timestamp: time.Now(),
		Key:       key,
		Value:     value,
	}
	data, err := ent.MarshalBinary()
	if err != nil {
		return err
	}
	// write put entry to the write-ahead logger
	_, err = m.wal.Write(data)
	if err != nil {
		return err
	}
	// write put entry to the memtable
	_, ok := m.mem.Put(key, value)
	// update size in memtable
	if ok {
		m.size += int64(len(data))
	}
	return nil
}

func (m *Memtable) Get(key string) ([]byte, error) {
	// read lock
	m.mu.RLock()
	defer m.mu.RUnlock()
	// see if it's in the memtable
	value, ok := m.mem.Get(key)
	if !ok {
		return nil, ErrNotFound
	}
	return value, nil
}

func (m *Memtable) Del(key string) error {
	// lock
	m.mu.Lock()
	defer m.mu.Unlock()
	// encode key-value pair into a tombstone record
	ent := &lsmt.Entry{
		Type:      removedEntry,
		Timestamp: time.Now(),
		Key:       key,
		Value:     nil,
	}
	data, err := ent.MarshalBinary()
	if err != nil {
		return err
	}
	// write put entry to the write-ahead logger
	_, err = m.wal.Write(data)
	if err != nil {
		return err
	}
	// write put entry to the memtable
	_, ok := m.mem.Put(key, nil)
	// update size in memtable
	if ok {
		m.size += int64(len(data))
	}
	return nil
}

func (m *Memtable) FlushToSSTable() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// create new sstable file
	sst, err := sstable.Create(sstPath)
	if err != nil {
		return err
	}
	// iterate all of the entries in the memtable in sorted
	// order and write each entry to the sstable file
	m.mem.ScanFront(func(e rbtree.Entry) bool {
		ent := &lsmt.Entry{
			Type:      regularEntry,
			Timestamp: time.Now(),
			Key:       e.Key,
			Value:     e.Value,
		}
		if ent.Key != "" && ent.Value == nil {
			ent.Type = removedEntry
		}
		data, err := ent.MarshalBinary()
		if err != nil {
			return false
		}
		err = sst.Write(data)
		if err != nil {
			return false
		}
		return true
	})
	// make sure sstable file is flushed to disk
	err = sst.Sync()
	if err != nil {
		return err
	}
	// reset the memtable data
	m.mem = m.mem.Reset()
	// reset the write-ahead log file
	m.wal, err = m.wal.Reset()
	if err != nil {
		return err
	}
	return nil
}

// Close closes down the memtable
func (m *Memtable) Close() error {
	// lock
	m.mu.Lock()
	defer m.mu.Unlock()
	// close stuff down
	m.mem.Close()
	err := m.wal.Close()
	if err != nil {
		return err
	}
	m.size = 0
	runtime.GC()
	return nil
}
