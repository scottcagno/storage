package memtable

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/trees/rbtree"
	"os"
	"testing"
)

var conf = &MemtableConfig{
	BasePath:       "memtable-testing",
	FlushThreshold: -1,
	SyncOnWrite:    false,
}

func TestMemtable_Reset(t *testing.T) {

	path := conf.BasePath

	// open mem-table
	memt, err := OpenMemtable(conf)
	HandleErr(t, "opening", err)

	// create new batch
	batch := binary.NewBatch()

	// write some data to batch
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i+1)
		val := fmt.Sprintf("my-value-%06d", i+1)
		batch.WriteEntry(&binary.Entry{Key: []byte(key), Value: []byte(val)})
		HandleErr(t, "writing", err)
	}

	// write batch data to memt
	for i := range batch.Entries {
		e := batch.Entries[i]
		err = memt.Put(e)
		HandleErr(t, "writing", err)
	}

	// attempt reset
	err = memt.Reset()
	HandleErr(t, "reset", err)

	// write some more data
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key-%04d", i+1)
		val := fmt.Sprintf("my-value-%06d", i+1)
		err = memt.Put(&binary.Entry{Key: []byte(key), Value: []byte(val)})
		HandleErr(t, "writing", err)
	}

	// close mem-table
	err = memt.Close()
	HandleErr(t, "closing", err)

	// clean up (maybe)
	doClean := false
	if doClean {
		err = os.RemoveAll(path)
		HandleErr(t, "remove all", err)
	}
}

func TestMemtable_All(t *testing.T) {

	path := conf.BasePath

	// open mem-table
	memt, err := OpenMemtable(conf)
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
	doClean := false
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
