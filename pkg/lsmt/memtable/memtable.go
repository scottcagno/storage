package memtable

import (
	"github.com/scottcagno/storage/pkg/index/rbtree"
	"github.com/scottcagno/storage/pkg/lsmt"
	"github.com/scottcagno/storage/pkg/wal"
	"sync"
)

const walPath = "memdata"

type Memtable struct {
	mu   sync.RWMutex
	mem  *rbtree.RBTree
	wal  *wal.Log
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
			r, err := lsmt.DecodeRecord(data)
			if err != nil {
				return false
			}
			err = m.Put(string(r.Key), r.Value)
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
	data, err := lsmt.EncodeRecord([]byte(key), value)
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
	return nil, nil
}

func (m *Memtable) Del() error {
	// lock
	m.mu.Lock()
	defer m.mu.Unlock()
	return nil
}

func (m *Memtable) Close() error {
	// lock
	m.mu.Lock()
	defer m.mu.Unlock()
	return nil
}
