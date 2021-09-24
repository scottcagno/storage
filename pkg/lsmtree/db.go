package lsmtree

import (
	"github.com/scottcagno/storage/pkg/lsmtree/memtable"
	"github.com/scottcagno/storage/pkg/lsmtree/sstable"
	"path/filepath"
	"sync"
)

const (
	MaxMemtableSize      = 2 << 20 // 2MB
	defaultCommitLogPath = "wal"
	defaultSSTablePath   = "data"
)

type DB struct {
	lock  sync.RWMutex
	base  string // base is the base path of the db
	mem   *memtable.Memtable
	ssm   *sstable.SSManager
	index int64 // auto increment ID
}

func Open(base string) (*DB, error) {
	mem, err := memtable.Open(filepath.Join(base, defaultCommitLogPath))
	if err != nil {
		return nil, err
	}
	ssm, err := sstable.OpenSSManager(filepath.Join(base, defaultSSTablePath))
	if err != nil {
		return nil, err
	}
	db := &DB{
		base:  base,
		mem:   mem,
		ssm:   ssm,
		index: ssm.GetLatestIndex(),
	}
	return db, nil
}

func (db *DB) Put(key string, value []byte) error {
	// lock
	db.lock.Lock()
	defer db.lock.Unlock()
	// pass key and value to internal upsert
	return db.upsert(key, value)
}

func (db *DB) upsert(key string, value []byte) error {
	// insert into the memtable
	size, err := db.mem.Put(key, value)
	if err != nil {
		return err
	}
	// check size
	if size >= MaxMemtableSize {
		// create new sstable batch to dump to
		data := sstable.NewBatch()
		// scan "inactive" memtable, add entries to batch
		db.mem.Scan(func(key string, value []byte) bool {
			data.Write(key, value)
			return true
		})
		// create new sstable
		sst, err := sstable.CreateSSTable(db.base, db.ssm.GetLatestIndex()+1)
		if err != nil {
			return err
		}
		// write batch to sstable
		err = sst.WriteBatch(data)
		if err != nil {
			return err
		}
		// sync and close sstable
		err = sst.Close()
		if err != nil {
			return err
		}
		// increment new gidx
	}
	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	// lock
	db.lock.Lock()
	defer db.lock.Unlock()
	// search memtable
	value, err := db.mem.Get(key)
	if err == nil {
		// we found it!
		return value, nil
	}
	// search sstable(s)
	value, err = db.ssm.Get(key)
	if err == nil {
		// we found it
		return value, nil
	}
	// not fount
	return nil, memtable.ErrNotFound
}

func (db *DB) Del(key string) error {
	// lock
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.upsert(key, nil)
}

func (db *DB) Close() error {
	err := db.mem.Close()
	if err != nil {
		return err
	}
	err = db.ssm.Close()
	if err != nil {
		return err
	}
	return nil
}
