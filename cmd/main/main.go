package main

import (
	"fmt"
	"github.com/scottcagno/storage"
	"github.com/scottcagno/storage/pkg/lsmt/memtable"
	"log"
)

func main() {
	mydb, err := OpenMyDB()
	errCheck(err)

	err = mydb.Put("foo-1", []byte("bar-1"))
	errCheck(err)

	err = mydb.Put("foo-2", []byte("bar-2"))
	errCheck(err)

	val, err := mydb.Get("foo-2")
	errCheck(err)
	fmt.Printf("got: %q -> %s\n", "foo-2", val)

	err = mydb.Del("foo-1")
	errCheck(err)

	_, err = mydb.Get("foo-1")
	fmt.Printf("got: %q -> %s\n", "foo-1", err)

	err = mydb.Close()
	errCheck(err)
}

func errCheck(err error) {
	if err != nil {
		log.Panicf("got error: %v\n", err)
	}
}

type MyDB struct {
	db storage.Storage
}

func OpenMyDB() (*MyDB, error) {
	// THIS LINE HERE WOULD BE THE ONLY ONE
	// YOU WOULD NEED TO CHANGE AS ONGOING
	// WORK TO THE PACKAGE WOULD CHANGE
	db, err := memtable.Open("cmd/main/mydata")
	if err != nil {
		return nil, err
	}
	return &MyDB{
		db: db,
	}, nil
}

func (m *MyDB) Put(key string, value []byte) error {
	return m.db.Put(key, value)
}

func (m *MyDB) Get(key string) ([]byte, error) {
	return m.db.Get(key)
}

func (m *MyDB) Del(key string) error {
	return m.db.Del(key)
}

func (m *MyDB) Close() error {
	err := m.db.Close()
	if err != nil {
		return err
	}
	return nil
}
