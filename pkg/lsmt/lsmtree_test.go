package lsmt

import (
	"encoding/binary"
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
	"log"
	"os"
	"path/filepath"
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

func TestLSMTreeReadEmptyDir(t *testing.T) {
	sanitize := func(base string) (string, error) {
		base, err := filepath.Abs(base)
		if err != nil {
			return "", err
		}
		base = filepath.ToSlash(base)
		return base, nil
	}
	base, err := sanitize("testing-empty-dir")
	if err != nil {
		t.Errorf("sanitize: %v\n", err)
	}
	_, err = os.ReadDir(base)
	if err != nil {
		t.Errorf("read dir: %T %v\n", err, err)
	}
}

func TestLSMTree(t *testing.T) {

	count := 100
	strt := 0
	stop := strt + count

	n, err := ReadLastSequenceNumber(conf.BasePath)
	if n > 0 && err == nil {
		strt = int(n)
		stop = strt + count
	}
	util.DEBUG("start: %d, stop: %d, count: %d\n", strt, stop, count)

	// open lsm tree
	logger("opening lsm tree")
	lsm, err := OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

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

	doPrintAllReads := true
	doPrintSomeReads := false

	// read Entries
	logger("reading data")
	ts1 = time.Now()
	for i := strt; i < stop; i++ {
		v, err := lsm.Get(makeKey(i))
		if err != nil {
			t.Errorf("get: %v\n", err)
		}
		if doPrintAllReads {
			fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
		} else if doPrintSomeReads {
			if i%1000 == 0 {
				fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
			}
		}
	}
	ts2 = time.Now()
	fmt.Println(util.FormatTime("reading Entries", ts1, ts2))

	// remove Entries
	logger("removing data (only odds)")
	ts1 = time.Now()
	for i := strt; i < stop; i++ {
		if i%2 != 0 {
			key := makeKey(i)
			util.DEBUG("DELETING KEY %q\n", key)
			err = lsm.Del(key)
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

	util.DEBUG("LSMTree memtable count: %d\n", lsm.memt.Len())

	// read Entries
	logger("reading data")
	ts1 = time.Now()
	for i := strt; i < stop; i++ {
		v, err := lsm.Get(makeKey(i))
		log.Printf("%T, %v\n", err, err)
		if err != nil {
			if err == ErrFoundTombstone {
				util.DEBUG("FOUND TOMBSTONE ENTRY!\n")
				continue
			}
			t.Errorf("get: %v\n", err)
		}
		if doPrintAllReads {
			fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
		} else if doPrintSomeReads {
			if i%1000 == 0 {
				fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
			}
		}
	}
	ts2 = time.Now()
	fmt.Println(util.FormatTime("reading data", ts1, ts2))

	_ = WriteLastSequenceNumber(int64(stop-1), conf.BasePath)

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

func WriteLastSequenceNumber(n int64, base string) error {
	dat := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(dat, n)
	file := filepath.Join(base, "last-seq.dat")
	err := os.WriteFile(file, dat, 0666)
	if err != nil {
		return err
	}
	return nil
}

func ReadLastSequenceNumber(base string) (int64, error) {
	file := filepath.Join(base, "last-seq.dat")
	dat, err := os.ReadFile(file)
	if err != nil {
		return -1, err
	}
	err = os.RemoveAll(file)
	n, _ := binary.Varint(dat)
	return n, nil
}
