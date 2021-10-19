package lsmt

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"github.com/scottcagno/storage/pkg/util"
	"log"
	"strconv"
	"testing"
	"time"
)

func _makeKey(i int) string {
	return fmt.Sprintf("key-%06d", i)
}

func _makeVal(i int) []byte {
	return []byte(fmt.Sprintf("value-%08d", i))
}

func logger(s string) {
	log.SetPrefix("[INFO] ")
	log.Printf("%s\n", s)
}

var conf = &LSMConfig{
	BasePath:       "lsm-testing",
	FlushThreshold: -1,
	SyncOnWrite:    false,
}

func TestOpenAndCloseNoWrite(t *testing.T) {

	db, err := OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	err = db.Close()
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	db, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	err = db.Close()
	if err != nil {
		t.Errorf("open: %v\n", err)
	}
}

func TestLSMTree(t *testing.T) {

	strt := 0
	stop := strt + 50000

	// open lsm tree
	logger("opening lsm tree")
	lsm, err := OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// get last key and update counter
	k, err := lsm.GetLastKey()
	if err != nil && err != sstable.ErrSSTIndexNotFound {
		t.Errorf("get last key: %v\n", err)
	}
	if k != "" {
		keyn, err := strconv.Atoi(k[4:])
		if err != nil {
			t.Errorf("get last key and set to count: %v\n", err)
		}
		if keyn == stop-1 {
			strt = stop
			stop = strt + 50000
		}
	}
	log.Printf("start: %d, stop: %d\n", strt, stop)
	time.Sleep(3 * time.Second)

	// write Entries
	logger("writing data")
	ts1 := time.Now()
	for i := strt; i < stop; i++ {
		err := lsm.Put(makeKey(i), makeVal(i))
		if err != nil {
			t.Errorf("put: %v\n", err)
		}
	}
	ts2 := time.Now()
	fmt.Println(util.FormatTime("writing Entries", ts1, ts2))

	err = lsm.Sync()
	if err != nil {
		t.Errorf(">>> syncing: %v\n", err)
	}

	// close
	logger("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open
	logger("opening lsm tree")
	lsm, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// read Entries
	logger("reading data")
	ts1 = time.Now()
	for i := strt; i < stop; i++ {
		v, err := lsm.Get(makeKey(i))
		if err != nil {
			t.Errorf("get: %v\n", err)
		}
		if i%1000 == 0 {
			fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
		}
	}
	ts2 = time.Now()
	fmt.Println(util.FormatTime("reading Entries", ts1, ts2))

	// remove Entries
	logger("removing data (only odds)")
	ts1 = time.Now()
	for i := strt; i < stop; i++ {
		if i%2 != 0 {
			err = lsm.Del(makeKey(i))
			if err != nil {
				t.Errorf("del: %v\n", err)
			}
		}
	}
	ts2 = time.Now()
	fmt.Println(util.FormatTime("removing Entries", ts1, ts2))

	// close
	logger("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open
	logger("opening lsm tree")
	lsm, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// read Entries
	logger("reading data")
	ts1 = time.Now()
	for i := strt; i < stop; i++ {
		v, err := lsm.Get(makeKey(i))
		log.Printf("%T, %v\n", err, err)
		if err != nil {
			if err == sstable.ErrSSTIndexNotFound {
				continue
			}
			t.Errorf("get: %v\n", err)
		}
		if i%1000 == 0 {
			fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
		}
	}
	ts2 = time.Now()
	fmt.Println(util.FormatTime("reading data", ts1, ts2))

	// close
	logger("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

}

func TestLSMTree_Put(t *testing.T) {

}

func TestLSMTree_Get(t *testing.T) {

}

func TestLSMTree_Del(t *testing.T) {

}

func TestLSMTree_Close(t *testing.T) {

}
