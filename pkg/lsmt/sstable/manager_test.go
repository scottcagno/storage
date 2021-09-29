package sstable

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"os"
	"testing"
)

func TestSSTManager_Open(t *testing.T) {

	var count int

	// add some data
	for i := 1; i <= 5; i++ {
		// open sstable
		sst, err := OpenSSTable("testing", int64(i))
		if err != nil {
			t.Errorf("opening sstable (%d): %v\n", i, err)
		}
		// write data
		stop := count + 59
		for k := count; k < stop; k++ {
			data := fmt.Sprintf("data-%04d", k)
			err = sst.Write(&binary.Entry{Key: []byte(data), Value: []byte(data)})
			if err != nil {
				t.Errorf("writing entry: %v\n", err)
			}
			count++
		}
		// close sstable
		err = sst.Close()
		if err != nil {
			t.Errorf("closing sstable (%d): %v\n", i, err)
		}
	}

	// open ss-table-manager
	sstm, err := OpenSSTManager("testing")
	if err != nil {
		t.Errorf("opening sstablemanager: %v\n", err)
	}

	// list ss-tables
	fmt.Printf("listing ss-tables....\n")
	sstables := sstm.ListSSTables()
	for _, sst := range sstables {
		fmt.Printf("ss-table: %s\n", sst)
	}

	// list ss-table-indexes
	fmt.Printf("\nlisting ss-table indexes....\n")
	sstidxs := sstm.ListSSTIndexes()
	for _, ssi := range sstidxs {
		fmt.Printf("ss-table-index: %s\n", ssi)
	}

	// view sparse index
	fmt.Printf("\nviewing sparse index....\n")
	kps := sstm.GetSparseIndex()
	for _, kp := range kps {
		fmt.Printf("%s\n", kp)
	}

	// search sparse index
	fmt.Printf("\nsearching sparse indexes....\n")
	i, err := sstm.SearchSparseIndex("data-0025")
	if err != nil {
		t.Errorf("searching sparse index (%d): %v\n", 25, err)
	}
	fmt.Printf("searching sparse index for %d, got found in index: %d\n", 25, i)

	i, err = sstm.SearchSparseIndex("data-0150")
	if err != nil {
		t.Errorf("searching sparse index (%d): %v\n", 150, err)
	}
	fmt.Printf("searching sparse index for %d, got found in index: %d\n", 150, i)

	i, err = sstm.SearchSparseIndex("data-0250")
	if err != nil {
		t.Errorf("searching sparse index (%d): %v\n", 250, err)
	}
	fmt.Printf("searching sparse index for %d, got found in index: %d\n", 250, i)

	i, err = sstm.SearchSparseIndex("data-0500")
	if err == nil {
		t.Errorf("searching sparse index (%d): %v\n", 500, err)
	}
	fmt.Printf("searching sparse index for %d, got found in index: %d\n", 500, i)

	// close ss-table-manager
	err = sstm.Close()
	if err != nil {
		t.Errorf("closing ss-table-manager: %v\n", err)
	}

	// remove ss-tables
	for _, sst := range sstables {
		err = os.Remove("testing/" + sst)
		if err != nil {
			t.Errorf("removing table %q: %v\n", sst, err)
		}
	}

	// remove ss-table-indexes
	for _, ssi := range sstidxs {
		err = os.Remove("testing/" + ssi)
		if err != nil {
			t.Errorf("removing table index %q: %v\n", ssi, err)
		}
	}
}
