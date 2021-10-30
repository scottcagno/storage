package lsmtree

import (
	"github.com/scottcagno/storage/pkg/_junk/_lsmtree/memtable"
	"github.com/scottcagno/storage/pkg/_junk/_lsmtree/sstable"
	"log"
	"path/filepath"
	"sync"
	"time"
)

const (
	MaxMemtableSize      = 64 << 10 // 64 KB
	defaultCommitLogPath = "wal"
	defaultSSTablePath   = "data"

	VERBOSE = true
)

type DB struct {
	lock     sync.RWMutex
	base     string // base is the base path of the db
	mem      *memtable.Memtable
	ssm      *sstable.SSManager
	sstindex int64 // sst index
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
		base:     base,
		mem:      mem,
		ssm:      ssm,
		sstindex: ssm.GetLatestIndex(),
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

func (db *DB) writeMemtableToBatch() (*sstable.Batch, error) {
	// create new sstable batch to dump to
	data := sstable.NewBatch()
	// scan "inactive" memtable, add entries to batch
	db.mem.FlushToSSTableBatch(data)
	// clear memtable
	err := db.mem.Reset()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (db *DB) upsert(key string, value []byte) error {
	// insert into the memtable
	size, err := db.mem.Put(key, value)
	if err != nil {
		return err
	}
	// check size
	if size >= MaxMemtableSize {

		log.Printf("Max Memtable size has been reached (%d KB)\nFlushing to SSTable... ", MaxMemtableSize>>10)
		ts := time.Now()

		// create new sstable batch
		batch, err := db.writeMemtableToBatch()
		if err != nil {
			return err
		}
		// create new sstable
		sst, err := sstable.CreateSSTable(filepath.Join(db.base, defaultSSTablePath), db.sstindex+1)
		if err != nil {
			return err
		}
		// write batch to sstable
		err = sst.WriteBatch(batch)
		if err != nil {
			return err
		}
		// sync and close sstable
		err = sst.Close()
		if err != nil {
			return err
		}
		// increment db sst index
		db.sstindex++

		log.Printf("[took %dms]\n", time.Since(ts).Milliseconds())
	}
	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	// lock
	db.lock.Lock()
	defer db.lock.Unlock()
	// search memtable
	value, err := db.mem.Get(key)
	log.Println(">>>1", err)
	if err == nil {
		log.Printf(">>> DEBUG-1\n")
		// we found it!
		log.Printf("Found value for (%s) in Memtable...\n", key)
		return value, nil
	}
	// search sstable(s)
	value, err = db.ssm.Get(key)
	log.Println(">>>2", err)
	if err == nil {
		log.Printf(">>> DEBUG-2\n")
		// we found it
		log.Printf("Found value for (%s) in SSTable...\n", key)
		return value, nil
	}
	log.Printf(">>> DEBUG-3\n")
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
