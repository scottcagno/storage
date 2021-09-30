package sstable

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestSparseIndex(t *testing.T) {
	// create new sstable
	sst, err := CreateSSTable("data", 1)
	if err != nil {
		t.Fatalf("creating sst: %v\n", err)
	}

	// create and test batch
	batch := NewBatch()
	for i := 0; i < 500; i++ {
		// odd numbers
		k, v := fmt.Sprintf("key-%04d", i), fmt.Sprintf("value-%06d", i)
		batch.Write(k, []byte(v))
	}
	err = sst.WriteBatch(batch)
	if err != nil {
		t.Fatalf("writing (batch) to sst: %v\n", err)
	}

	// close sst
	err = sst.Close()
	if err != nil {
		t.Fatalf("closing sst: %v\n", err)
	}

	// opening sparse index
	ssm, err := OpenSSManager("data")
	if err != nil {
		t.Fatalf("opening ssm: %v\n", err)
	}

	for i := range ssm.sparse {
		fmt.Printf("%s\n", ssm.sparse[i])
	}

	key := "key-0037"
	p, o := ssm.SearchSparseIndex(key)
	fmt.Printf("searching(%q): path=%q, offset=%d\n", key, filepath.Base(p), o)

	key = "key-0002"
	p, o = ssm.SearchSparseIndex(key)
	fmt.Printf("searching(%q): path=%q, offset=%d\n", key, filepath.Base(p), o)

	key = "key-0321"
	p, o = ssm.SearchSparseIndex(key)
	fmt.Printf("searching(%q): path=%q, offset=%d\n", key, filepath.Base(p), o)

	key = "key-0905"
	p, o = ssm.SearchSparseIndex(key)
	fmt.Printf("searching(%q): path=%q, offset=%d\n", key, filepath.Base(p), o)

	key = "key-3754"
	p, o = ssm.SearchSparseIndex(key)
	fmt.Printf("searching(%q): path=%q, offset=%d\n", key, filepath.Base(p), o)

	err = ssm.Close()
	if err != nil {
		t.Fatalf("closing ssm: %v\n", err)
	}
}

func TestCompactSSTables(t *testing.T) {
	ssm, err := OpenSSManager("data")
	if err != nil {
		t.Fatalf("compacting: %v\n", err)
	}
	err = ssm.CompactSSTables(3)
	if err != nil {
		t.Fatalf("compacting: %v\n", err)
	}
}

func TestMergeSSTable(t *testing.T) {

	// create new sstable
	sst, err := CreateSSTable("data", 1)
	if err != nil {
		t.Fatalf("creating sst: %v\n", err)
	}

	// create and test batch
	batch := NewBatch()
	for i := 0; i < 5000; i++ {
		if i%2 == 1 {
			// odd numbers
			k, v := fmt.Sprintf("key-%04d", i), fmt.Sprintf("value-%06d", i)
			batch.Write(k, []byte(v))
		}
	}
	err = sst.WriteBatch(batch)
	if err != nil {
		t.Fatalf("writing (batch) to sst: %v\n", err)
	}

	// close sst
	err = sst.Close()
	if err != nil {
		t.Fatalf("closing sst: %v\n", err)
	}

	// create new sstable
	sst, err = CreateSSTable("data", 2)
	if err != nil {
		t.Fatalf("creating sst: %v\n", err)
	}

	// create and test batch
	batch = NewBatch()
	for i := 0; i < 5000; i++ {
		if i%2 == 0 {
			// even numbers
			k, v := fmt.Sprintf("key-%04d", i), fmt.Sprintf("value-%06d", i)
			batch.Write(k, []byte(v))
		} else {
			// odd numbers on 2nd table, write tombstones
			batch.Write(fmt.Sprintf("key-%04d", i), TombstoneEntry)
		}
	}
	err = sst.WriteBatch(batch)
	if err != nil {
		t.Fatalf("writing (batch) to sst: %v\n", err)
	}

	// close sst
	err = sst.Close()
	if err != nil {
		t.Fatalf("closing sst: %v\n", err)
	}

	ssm, err := OpenSSManager("data")
	if err != nil {
		t.Fatalf("ssm: %v\n", err)
	}

	ts1 := time.Now()
	err = ssm.MergeSSTables(1, 2)
	if err != nil {
		t.Fatalf("closing sst: %v\n", err)
	}
	ts2 := time.Since(ts1)
	fmt.Printf("MERGE TOO: %v microseconds\n", ts2.Microseconds())
}

func TestCreateSSTable(t *testing.T) {

	// create new sstable
	sst, err := CreateSSTable("data", 1)
	if err != nil {
		t.Fatalf("creating sst: %v\n", err)
	}

	// create and test batch
	batch := NewBatch()
	batch.Write("key-01", []byte("value-01"))
	batch.Write("key-02", []byte("value-02"))
	batch.Write("key-03", []byte("value-03"))
	err = sst.WriteBatch(batch)
	if err != nil {
		t.Fatalf("writing (batch) to sst: %v\n", err)
	}

	// write some entries
	err = sst.WriteEntry(&sstDataEntry{
		key:   "abc",
		value: []byte("ABC"),
	})
	if err != nil {
		t.Fatalf("writing to sst: %v\n", err)
	}
	err = sst.WriteEntry(&sstDataEntry{
		key:   "def",
		value: []byte("DEF"),
	})
	if err != nil {
		t.Fatalf("writing to sst: %v\n", err)
	}
	err = sst.WriteEntry(&sstDataEntry{
		key:   "ghi",
		value: []byte("GHI"),
	})
	if err != nil {
		t.Fatalf("writing to sst: %v\n", err)
	}

	// close sst
	err = sst.Close()
	if err != nil {
		t.Fatalf("closing sst: %v\n", err)
	}
}

func TestOpenSSTable(t *testing.T) {

	// open sstable
	sst, err := OpenSSTable("data", 1)
	if err != nil {
		t.Fatalf("opening: %v\n", err)
	}

	fmt.Printf("printing sst index...\n")
	for _, idx := range sst.index.data {
		fmt.Printf("%s\n", idx)
	}

	key := "def"
	off, err := sst.index.GetEntryOffset(key)
	if err != nil {
		t.Fatalf("finding entry: %v\n", err)
	}
	fmt.Printf("got entry offset for %q, offset=%d\n", key, off)

	fmt.Printf("size of entry index: %d\n", len(sst.index.data))
	sst.index.data = nil
	fmt.Printf("size of entry index: %d\n", len(sst.index.data))
	err = RebuildSSTableIndex("data", 1)
	if err != nil {
		t.Fatalf("re-building index: %v\n", err)
	}
	fmt.Printf("size of entry index: %d\n", len(sst.index.data))

	// close sstable
	err = sst.Close()
	if err != nil {
		t.Fatalf("closing: %v\n", err)
	}
}
