package sstable

import (
	"fmt"
	"testing"
)

func TestCreateSSTable(t *testing.T) {

	// create new sstable
	sst, err := CreateSSTable("data", 1)
	if err != nil {
		t.Fatalf("creating sst: %v\n", err)
	}

	// write some entries
	err = sst.WriteEntry(&sstEntry{
		key:   "abc",
		value: []byte("ABC"),
	})
	if err != nil {
		t.Fatalf("writing to sst: %v\n", err)
	}
	err = sst.WriteEntry(&sstEntry{
		key:   "def",
		value: []byte("DEF"),
	})
	if err != nil {
		t.Fatalf("writing to sst: %v\n", err)
	}
	err = sst.WriteEntry(&sstEntry{
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
	for _, idx := range sst.GetIndex() {
		fmt.Printf("%s\n", idx)
	}

	key := "def"
	off, err := sst.GetEntryOffset(key)
	if err != nil {
		t.Fatalf("finding entry: %v\n", err)
	}
	fmt.Printf("got entry offset for %q, offset=%d\n", key, off)

	// close sstable
	err = sst.Close()
	if err != nil {
		t.Fatalf("closing: %v\n", err)
	}
}
