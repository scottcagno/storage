package lsmtree

import (
	"github.com/scottcagno/storage/pkg/lsmtree/memtable"
	"github.com/scottcagno/storage/pkg/lsmtree/sstable"
)

type DB struct {
	base string // base is the base path of the db
	mem  *memtable.Memtable
	ssm  *sstable.SSManager
}

func Open(base string) (*DB, error) {
	mem, err := memtable.Open(base)
	if err != nil {
		return nil, err
	}
	ssm, err := sstable.OpenSSManager(base)
	if err != nil {
		return nil, err
	}
	db := &DB{
		base: base,
		mem:  mem,
		ssm:  ssm,
	}
	return db, nil
}

func (db *DB) Put(key string, value []byte) error {
	// TODO: implement...
	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	// TODO: implement...
	return nil, nil
}

func (db *DB) Del(key string) error {
	// TODO: implement...
	return nil
}

func (db *DB) Scan(iter func(key string) bool) error {
	// TODO: implement...
	return nil
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
