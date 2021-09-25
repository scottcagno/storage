package lsmtree

import (
	"fmt"
	"testing"
)

func TestDB(t *testing.T) {

	// open db
	db, err := Open("test_db/ks-test")
	if err != nil {
		t.Errorf("db.open: %v\n", err)
	}

	// close db
	err = db.Close()
	if err != nil {
		t.Errorf("db.close: %v\n", err)
	}
}

func TestDB_Write(t *testing.T) {

	// open db
	db, err := Open("test_db/ks-test")
	if err != nil {
		t.Errorf("db.open: %v\n", err)
	}

	count := 5000
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%06d", i)
		val := []byte(fmt.Sprintf("value-%012d", i))
		err := db.Put(key, val)
		if err != nil {
			t.Errorf("db.put: %v\n", err)
		}
	}

	// close db
	err = db.Close()
	if err != nil {
		t.Errorf("db.close: %v\n", err)
	}
}

func TestDB_Read(t *testing.T) {

	// open db
	db, err := Open("test_db/ks-test")
	if err != nil {
		t.Errorf("db.open: %v\n", err)
	}

	count := 5000
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%06d", i)
		val, err := db.Get(key)
		if err != nil {
			t.Errorf("db.get: %v\n", err)
		}
		fmt.Printf("key=%q, value=%q\n", key, val)
	}

	// close db
	err = db.Close()
	if err != nil {
		t.Errorf("db.close: %v\n", err)
	}
}
