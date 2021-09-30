package memtable

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/trees/rbtree"
	"os"
	"testing"
)

func TestMemtable_All(t *testing.T) {

	path := "testing"

	// open mem-table
	memt, err := OpenMemtable(path, -1)
	HandleErr(t, "opening", err)

	// if there is data, read it and exit
	if memt.Len() > 0 {
		memt.Scan(func(me rbtree.RBEntry) bool {
			fmt.Printf("%s\n", me.(memtableEntry))
			return true
		})
		goto close
	}

	// otherwise, write some data
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i+1)
		val := fmt.Sprintf("my-value-%06d", i+1)
		err = memt.Put(&binary.Entry{Key: []byte(key), Value: []byte(val)})
		HandleErr(t, "writing", err)
	}

close:
	// close mem-table
	err = memt.Close()
	HandleErr(t, "closing", err)

	// clean up (maybe)
	doClean := true
	if doClean {
		err = os.RemoveAll(path)
		HandleErr(t, "remove all", err)
	}
}

func HandleErr(t *testing.T, str string, err error) {
	if err != nil {
		t.Fatalf("%s: %v\n", str, err)
	}
}
