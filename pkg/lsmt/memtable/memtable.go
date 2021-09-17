package memtable

import (
	"errors"
	"github.com/scottcagno/storage/pkg/index/rbtree"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"github.com/scottcagno/storage/pkg/wal"
	"runtime"
	"sync"
)

const (
	walPath = "data/memtable"
)

var (
	ErrNotFound = errors.New("error: not found")
)

type Memtable struct {
	mu       sync.RWMutex
	mem      *rbtree.RBTree
	wal      *wal.WriteAheadLog
	sstables []*sstable.SSTable
	size     int64
}

// Open opens and returns a Memtable instance
func Open() (*Memtable, error) {
	l, err := wal.Open(walPath)
	if err != nil {
		return nil, err
	}
	m := &Memtable{
		mem: rbtree.NewRBTree(),
		wal: l,
	}
	err = m.loadFromLog()
	if err != nil {
		return nil, err
	}
	return m, nil
}

// loadFromLog checks and loads any entries that
// were saved to the commit log.
func (m *Memtable) loadFromLog() error {
	// read lock
	m.mu.RLock()
	defer m.mu.RUnlock()
	// check to see if there are entries in the
	// write-ahead log we must load back into
	// the Memtable
	if m.wal.Count() > 0 {
		err := m.wal.Scan(func(i uint64, k, v []byte) bool {
			if k == nil {
				return false
			}
			err := m.Put(string(k), v)
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

// Put adds a key and value pair to the Memtable
func (m *Memtable) Put(key string, value []byte) error {
	// lock
	m.mu.Lock()
	defer m.mu.Unlock()
	// write put entry to the write-ahead logger
	_, err := m.wal.Write([]byte(key), value)
	if err != nil {
		return err
	}
	// write put entry to the memtable
	_, ok := m.mem.Put(key, value)
	// update size in memtable
	if ok {
		m.size += int64(len(key) + len(value))
	}
	return nil
}

// Get attempts to find a key-value pair in the Memtable
func (m *Memtable) Get(key string) ([]byte, error) {
	// read lock
	m.mu.RLock()
	defer m.mu.RUnlock()
	// see if it's in the memtable
	value, ok := m.mem.Get(key)
	if !ok {
		return nil, ErrNotFound
	}
	// return the value if it is found
	return value, nil
}

// Del writes a tombstone to the Memtable
func (m *Memtable) Del(key string) error {
	// lock
	m.mu.Lock()
	defer m.mu.Unlock()
	// write del entry to the write-ahead logger
	_, err := m.wal.Write([]byte(key), nil)
	if err != nil {
		return err
	}
	// write put entry to the memtable
	_, ok := m.mem.Put(key, nil)
	// update size in memtable
	if ok {
		m.size += int64(len(key))
	}
	return nil
}

// Size returns current active size of memtable
func (m *Memtable) Size() int64 {
	// lock
	m.mu.Lock()
	defer m.mu.Unlock()
	// return size
	return m.size
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
