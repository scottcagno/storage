package memtable

import (
	"github.com/scottcagno/storage/pkg/index/rbtree"
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
	return nil
}

func (m *Memtable) Put() error {
	// lock
	m.mu.Lock()
	defer m.mu.Unlock()
	return nil
}

func (m *Memtable) Get() ([]byte, error) {
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
