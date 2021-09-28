package sstable

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"testing"
)

func TestSSTableAndSSTIndex(t *testing.T) {

	// create new sstable
	sst, err := OpenSSTable("data", 1)
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
	err = sst.Write(&binary.Entry{
		Key:   []byte("abc"),
		Value: []byte("ABC"),
	})
	if err != nil {
		t.Fatalf("writing to sst: %v\n", err)
	}
	err = sst.Write(&binary.Entry{
		Key:   []byte("def"),
		Value: []byte("DEF"),
	})
	if err != nil {
		t.Fatalf("writing to sst: %v\n", err)
	}
	err = sst.Write(&binary.Entry{
		Key:   []byte("ghi"),
		Value: []byte("GHI"),
	})
	if err != nil {
		t.Fatalf("writing to sst: %v\n", err)
	}
	// close sst
	err = sst.Close()
	if err != nil {
		t.Fatalf("closing sst: %v\n", err)
	}

	// open index
	ssi, err := OpenSSTIndex("data", 1)
	if err != nil {
		t.Fatalf("opening ssi: %v\n", err)
	}

	key := "def"
	i, err := ssi.Find(key)
	if err != nil {
		t.Fatalf("finding key: %v\n", err)
	}
	fmt.Printf("ssi.Find(%q)=%s\n", key, i)
}
