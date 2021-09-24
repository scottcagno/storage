package lsmtree

import (
	"github.com/scottcagno/storage/pkg/lsmtree/memtable"
	"github.com/scottcagno/storage/pkg/lsmtree/sstable"
	"sync"
)

const (
	MaxMemtableSize = 2 << 20 // 2MB
)

var ai autoInc // global instance

type autoInc struct {
	sync.Mutex // ensures autoInc is goroutine-safe
	index      int64
}

func (a *autoInc) Index() (index int64) {
	a.Lock()
	defer a.Unlock()
	index = a.index
	a.index++
	return
}

type DB struct {
	lock  sync.RWMutex
	base  string // base is the base path of the db
	mem   []*memtable.Memtable
	am    int // active memtable
	ssm   *sstable.SSManager
	index int64
}

func Open(base string) (*DB, error) {
	m1, err := memtable.Open(base)
	if err != nil {
		return nil, err
	}
	m2, err := memtable.Open(base)
	if err != nil {
		return nil, err
	}
	ssm, err := sstable.OpenSSManager(base)
	if err != nil {
		return nil, err
	}
	db := &DB{
		base:  base,
		mem:   []*memtable.Memtable{m1, m2},
		am:    0,
		ssm:   ssm,
		index: ai.Index(),
	}
	return db, nil
}

// return "active" memtable
func (db *DB) mtAct() *memtable.Memtable {
	return db.mem[db.am]
}

// return "inactive" memtable
func (db *DB) mtInA() *memtable.Memtable {
	return db.mem[db.am]
}

func (db *DB) mtSwap() {
	db.lock.Lock()
	defer db.lock.Unlock()
	if db.am == 0 {
		db.am = 1
		return
	}
	if db.am == 1 {
		db.am = 0
		return
	}
}

func (db *DB) Put(key string, value []byte) error {
	// lock
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.upsert(key, value)
}

func (db *DB) upsert(key string, value []byte) error {
	// insert into the memtable
	size, err := db.mtAct().Put(key, value)
	if err != nil {
		return err
	}
	// check size
	if size >= MaxMemtableSize {
		// switch memtables
		db.mtSwap()
		// create new sstable batch to dump to
		data := sstable.NewBatch()
		// scan "inactive" memtable, add entries to batch
		db.mtInA().Scan(func(key string, value []byte) bool {
			data.Write(key, value)
			return true
		})
		// clear memtable
		err = db.mtInA().Close()
		if err != nil {
			return err
		}
		// create new sstable
		sst, err := sstable.CreateSSTable(db.base, ai.Index())
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
	}
	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	// lock
	db.lock.Lock()
	defer db.lock.Unlock()
	// search memtable
	value, err := db.mtAct().Get(key)
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
	err := db.mem[0].Close()
	if err != nil {
		return err
	}
	err = db.mem[1].Close()
	if err != nil {
		return err
	}
	err = db.ssm.Close()
	if err != nil {
		return err
	}
	return nil
}
