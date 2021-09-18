package sstable

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/binary"
	"github.com/scottcagno/storage/pkg/index/bptree"
	"testing"
)

func Test_SSTableWrite(t *testing.T) {

	// create
	sst, err := Create("testdata")
	if err != nil {
		t.Fatalf("create err: %v\n", err)
	}

	// write
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("val-%08d", i)
		err = sst.Write(key, []byte(val))
		if err != nil {
			t.Fatalf("write err: %v\n", err)
		}
	}

	// close
	err = sst.Close()
	if err != nil {
		t.Fatalf("close err: %v\n", err)
	}
}

func Test_SSTableRead(t *testing.T) {

	// create
	sst, err := Open("testdata/dat-1631940819494146.sst")
	if err != nil {
		t.Fatalf("open err: %v\n", err)
	}

	// read
	err = sst.Scan(func(e *binary.Entry) bool {
		fmt.Printf("%s\n", e)
		return true
	})

	// close
	err = sst.Close()
	if err != nil {
		t.Fatalf("close err: %v\n", err)
	}
}

func Test_SSTableIndex(t *testing.T) {

	// create
	index, err := OpenSSTableIndex(32, "testdata/dat-1631940819494146.sst")
	if err != nil {
		t.Fatalf("open err: %v\n", err)
	}

	doRange := false
	if doRange {
		index.index.Range(func(k string, v []byte) bool {
			fmt.Printf("entry.key=%q, entry.value=%d\n", k, bptree.ValToInt(v))
			return true
		})
	}

	// search
	key := "key-0094"
	offset, ok := index.Search(key)
	fmt.Printf("searching: %q, got offset=%d, ok=%v\n", key, offset, ok)

	fmt.Printf("index.len=%d\n", index.index.Len())

}
