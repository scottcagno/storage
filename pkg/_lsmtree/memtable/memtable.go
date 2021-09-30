package memtable

import (
	"errors"
	"github.com/scottcagno/storage/pkg/_lsmtree/container/rbtree"
	"github.com/scottcagno/storage/pkg/_lsmtree/sstable"
	"github.com/scottcagno/storage/pkg/x/file"
	"os"
	"runtime"
)

var ErrNotFound = errors.New("error: value not found")

type Memtable struct {
	base string // base is the base path of the db
	rbt  *rbtree.RBTree
	//wal  *wal.WAL
	wal *file.SegmentManager
}

func Open(base string) (*Memtable, error) {
	wall, err := file.Open(base)
	if err != nil {
		return nil, err
	}
	mem := &Memtable{
		base: base,
		rbt:  rbtree.NewRBTree(),
		wal:  wall,
	}
	return mem, nil
}

func (m *Memtable) Size() int64 {
	return m.rbt.Size()
}

func (m *Memtable) Put(key string, value []byte) (int64, error) {
	// write entry to the wal
	_, err := m.wal.Write(key, value)
	if err != nil {
		return -1, err
	}
	// write entry to the memtable
	_, _ = m.rbt.Put(key, value)
	// success, return size
	return m.rbt.Size(), nil
}

func (m *Memtable) Get(key string) ([]byte, error) {
	// check memtable for value
	value, ok := m.rbt.Get(key)
	if !ok {
		return nil, ErrNotFound
	}
	// return value
	return value, nil
}

func (m *Memtable) Del(key string) (int64, error) {
	// write del entry to the wal
	_, err := m.wal.Write(key, nil)
	if err != nil {
		return -1, err
	}
	// write del entry to the memtable
	_, _ = m.rbt.Put(key, nil)
	// return size
	return m.rbt.Size(), nil
}

func (m *Memtable) FlushToSSTableBatch(batch *sstable.Batch) {
	m.rbt.Scan(func(key string, value []byte) bool {
		batch.Write(key, value)
		return true
	})
}

func (m *Memtable) Scan(iter func(key string, value []byte) bool) {
	m.rbt.Scan(iter)
}

func (m *Memtable) Reset() error {
	walPath := m.wal.Path()
	err := m.wal.Close()
	if err != nil {
		return err
	}
	err = os.RemoveAll(walPath)
	if err != nil {
		return err
	}
	m.wal, err = file.Open(m.base)
	if err != nil {
		return err
	}
	m.rbt.Close()
	runtime.GC()
	m.rbt = rbtree.NewRBTree()
	return nil
}

func (m *Memtable) Close() error {
	err := m.wal.Close()
	if err != nil {
		return err
	}
	return nil
}
