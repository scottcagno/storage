package lsmt

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"log"
	"testing"
)

func makeKey(i int) string {
	return fmt.Sprintf("key-%06d", i)
}

func makeVal(i int) []byte {
	return []byte(fmt.Sprintf("value-%08d", i))
}

func logger(s string) {
	log.SetPrefix("[INFO] ")
	log.Printf("%s\n", s)
}

func TestLSMTree(t *testing.T) {

	count := 10000

	// open lsm tree
	logger("opening lsm tree")
	lsm, err := OpenLSMTree("lsm-testing")
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// write data
	logger("writing data")
	for i := 0; i < count; i++ {
		err := lsm.Put(makeKey(i), makeVal(i))
		if err != nil {
			t.Errorf("put: %v\n", err)
		}
	}

	// close
	logger("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open
	logger("opening lsm tree")
	lsm, err = OpenLSMTree("lsm-testing")
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// read data
	logger("reading data")
	for i := 0; i < count; i++ {
		v, err := lsm.Get(makeKey(i))
		if err != nil {
			t.Errorf("get: %v\n", err)
		}
		if i%1000 == 0 {
			fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
		}
	}

	// remove data
	logger("removing data (only odds)")
	for i := 0; i < count; i++ {
		if i%2 != 0 {
			err = lsm.Del(makeKey(i))
			if err != nil {
				t.Errorf("del: %v\n", err)
			}
		}
	}

	// close
	logger("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open
	logger("opening lsm tree")
	lsm, err = OpenLSMTree("lsm-testing")
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// read data
	logger("reading data")
	for i := 0; i < count; i++ {
		v, err := lsm.Get(makeKey(i))
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
