package lsmtree

import (
	"fmt"
	"testing"
)

func TestDB(t *testing.T) {

	// open db
	db, err := Open("db/ks-test")
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
	db, err := Open("db/ks-test")
	if err != nil {
		t.Errorf("db.open: %v\n", err)
	}

	count := 50000
	for i := 0; i < count; i++ {
		err := db.Put(fmt.Sprintf("key-%06d", i), []byte(fmt.Sprintf("value-%012d", i)))
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
