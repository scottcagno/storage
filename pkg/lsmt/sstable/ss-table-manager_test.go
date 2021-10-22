package sstable

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/util"
	"os"
	"testing"
	"time"
)

func TestSSTManager(t *testing.T) {

	var count int
	base := "sst-manager-testing"

	// open ss-table-manager
	sstm, err := OpenSSTManager(base)
	if err != nil {
		t.Errorf("opening ss-table-manager: %v\n", err)
	}

	ts1 := time.Now()
	// add some data
	for i := 1; i <= 5; i++ {
		// get fresh batch to write to
		batch := binary.NewBatch()
		// write data to batch
		stop := count + 59
		for k := count; k < stop; k++ {
			data := fmt.Sprintf("data-%04d", k)
			batch.WriteEntry(&binary.Entry{Key: []byte(data), Value: []byte(data)})
			count++
		}
		// write batch to ss-table
		err = sstm.FlushBatchToSSTable(batch)
		if err != nil {
			t.Errorf("flushing batch to ss-table: %v\n", err)
		}
	}
	ts2 := time.Now()
	fmt.Println(util.FormatTime("writing Entries", ts1, ts2))

	// list ss-tables
	//fmt.Printf("listing ss-tables....\n")
	//sstables := sstm.ListSSTables()
	//for _, sst := range sstables {
	//	fmt.Printf("ss-table: %s\n", sst)
	//}

	// list ss-table-indexes
	//fmt.Printf("\nlisting ss-table indexes....\n")
	//sstidxs := sstm.ListSSTIndexes()
	//for _, ssi := range sstidxs {
	//	fmt.Printf("ss-table-gindex: %s\n", ssi)
	//}

	/*
		// view sparse gindex
		fmt.Printf("\nviewing sparse gindex....\n")
		kps := sstm.GetSparseIndex()
		for _, kp := range kps {
			fmt.Printf("%s\n", kp)
		}
	*/

	// search sparse gindex
	fmt.Printf("\nsearching sparse indexes....\n")
	i, err := sstm.SearchSparseIndex("data-0000")
	if err != nil {
		t.Errorf("searching sparse gindex (%d): %v\n", 0, err)
	}
	fmt.Printf("searching sparse gindex for %d, got found in gindex: %d\n", 0, i)

	i, err = sstm.SearchSparseIndex("data-0025")
	if err != nil {
		t.Errorf("searching sparse gindex (%d): %v\n", 25, err)
	}
	fmt.Printf("searching sparse gindex for %d, got found in gindex: %d\n", 25, i)

	i, err = sstm.SearchSparseIndex("data-0150")
	if err != nil {
		t.Errorf("searching sparse gindex (%d): %v\n", 150, err)
	}
	fmt.Printf("searching sparse gindex for %d, got found in gindex: %d\n", 150, i)

	i, err = sstm.SearchSparseIndex("data-0250")
	if err != nil {
		t.Errorf("searching sparse gindex (%d): %v\n", 250, err)
	}
	fmt.Printf("searching sparse gindex for %d, got found in gindex: %d\n", 250, i)

	i, err = sstm.SearchSparseIndex("data-0500")
	if err == nil {
		t.Errorf("searching sparse gindex (%d): %v\n", 500, err)
	}
	fmt.Printf("searching sparse gindex for %d, got found in gindex: %d\n", 500, i)

	// close ss-table-manager
	err = sstm.Close()
	if err != nil {
		t.Errorf("closing ss-table-manager: %v\n", err)
	}

	doClean := false
	if doClean {
		err = os.RemoveAll(base)
		if err != nil {
			t.Errorf("removing all: %v\n", err)
		}
		/*
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
					t.Errorf("removing table gindex %q: %v\n", ssi, err)
				}
			}
		*/
	}
}
