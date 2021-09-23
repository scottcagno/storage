package sstable

import (
	"fmt"
	"testing"
)

func TestMergeSSTable(t *testing.T) {

	// create new sstable
	sst, err := CreateSSTable("data", 1)
	if err != nil {
		t.Fatalf("creating sst: %v\n", err)
	}

	// create and test batch
	batch := NewBatch()
	batch.Write("key-01", []byte("value-01"))
	batch.Write("key-03", []byte("value-03"))
	batch.Write("key-05", []byte("value-05"))
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
	batch.Write("key-02", []byte("value-02"))
	batch.Write("key-04", []byte("value-04"))
	batch.Write("key-05", []byte("new-value-05"))
	batch.Write("key-06", []byte("value-06"))
	err = sst.WriteBatch(batch)
	if err != nil {
		t.Fatalf("writing (batch) to sst: %v\n", err)
	}

	// close sst
	err = sst.Close()
	if err != nil {
		t.Fatalf("closing sst: %v\n", err)
	}

	err = MergeSSTables("data", 1, 2)
	if err != nil {
		t.Fatalf("closing sst: %v\n", err)
	}
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
