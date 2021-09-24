package lsmtree

import "testing"

func TestDB(t *testing.T) {

	// open db
	db, err := Open("db")
	if err != nil {
		t.Errorf("db.open: %v\n", err)
	}

	// close db
	err = db.Close()
	if err != nil {
		t.Errorf("db.close: %v\n", err)
	}
}
