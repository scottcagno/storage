package lsmt

import (
	"encoding/binary"
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func makeKey(i int) string {
	//return fmt.Sprintf("key-%06d", i)
	hexa := strconv.FormatInt(int64(i), 16)
	return fmt.Sprintf("%s%06s", "key-", hexa)
}

func makeVal(i int) []byte {
	return []byte(fmt.Sprintf("value-%08d", i))
}

func logger(s string) {
	log.SetPrefix("[INFO] ")
	log.Printf("%s\n", s)
}

var conf = &LSMConfig{
	BasePath:              "lsm-testing",
	FlushThreshold:        -1,
	SyncOnWrite:           false,
	CompactAndMergeOnOpen: false,
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
	max := 100000
	for i := 10; i <= max; i *= 10 {
		log.Printf("running tests with count: %d\n", i)
		testingLSMTreeN(i, t)
		runtime.GC()
		time.Sleep(3)
	}
	doClean := false
	if doClean {
		err := os.RemoveAll(conf.BasePath)
		if err != nil {
			t.Errorf("remove: %s, err: %v\n", conf.BasePath, err)
		}
	}
}

func testingLSMTreeN(count int, t *testing.T) {

	strt := 0
	stop := strt + count

	origPath := conf.BasePath
	tempPath := filepath.Join(conf.BasePath, strconv.Itoa(count))
	conf.BasePath = tempPath

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
	fmt.Println(util.FormatTime("writing entries", ts1, ts2))

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

	doPrintAllReads := false
	doPrintSomeReads := true

	// read Entries
	logger("reading data")
	ts1 = time.Now()
	var step int
	if count >= 100 {
		step = count / 100
	} else {
		step = 1
	}
	for i := strt; i < stop; i += step {
		v, err := lsm.Get(makeKey(i))
		if err != nil && err == ErrNotFound {
			// skip, we don't care if it's not found
			continue
		}
		if err != nil {
			t.Errorf("get: %v\n", err)
		}
		if doPrintAllReads {
			fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
		} else if doPrintSomeReads {
			if i%step == 0 {
				fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
			}
		}
	}
	ts2 = time.Now()
	fmt.Println(util.FormatTime("reading Entries", ts1, ts2))

	doDelete := true
	if doDelete {
		// remove Entries
		logger("removing data (only odds)")
		ts1 = time.Now()
		for i := strt; i < stop; i++ {
			if i%2 != 0 {
				key := makeKey(i)
				err = lsm.Del(key)
				if err != nil {
					t.Errorf("del: %v\n", err)
				}
			}
		}
		ts2 = time.Now()
		fmt.Println(util.FormatTime("removing Entries", ts1, ts2))
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
	for i := strt; i < stop; i += step {
		v, err := lsm.Get(makeKey(i))
		if err != nil && err == ErrNotFound {
			// skip, we don't care if it's not found
			continue
		}
		if err != nil {
			t.Errorf("get: %v\n", err)
		}
		if doPrintAllReads {
			fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
		} else if doPrintSomeReads {
			if i%step == 0 {
				fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
			}
		}
	}
	ts2 = time.Now()
	fmt.Println(util.FormatTime("reading data", ts1, ts2))

	_ = WriteLastSequenceNumber(int64(stop-1), conf.BasePath)

	//
	err = lsm.sstm.CompactAllSSTables()
	if err != nil {
		t.Errorf("lsm.compact error: %s\n", err)
	}

	// close
	logger("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	conf.BasePath = origPath

}

func TestLSMTree_Put(t *testing.T) {
	fnv.New32()
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
