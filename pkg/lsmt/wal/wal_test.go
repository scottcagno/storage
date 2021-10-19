package wal

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"os"
	"testing"
)

var conf = &WALConfig{
	BasePath:    "wal-testing",
	MaxFileSize: 2 << 10, // 2KB,
	SyncOnWrite: false,
}

func TestOpenAndCloseNoWrite(t *testing.T) {
	// open
	wal, err := OpenWAL(conf)
	if err != nil {
		t.Fatalf("opening: %v\n", err)
	}
	// close
	err = wal.Close()
	if err != nil {
		t.Fatalf("closing: %v\n", err)
	}
	// open
	wal, err = OpenWAL(conf)
	if err != nil {
		t.Fatalf("opening: %v\n", err)
	}
	// close
	err = wal.Close()
	if err != nil {
		t.Fatalf("closing: %v\n", err)
	}
}

func TestWAL(t *testing.T) {
	//
	// open log
	wal, err := OpenWAL(conf)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// get path for cleanup
	path := wal.GetConfig().BasePath
	//
	// do some writing
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i+1)
		val := fmt.Sprintf("my-value-%06d", i+1)
		_, err := wal.Write(&binary.Entry{Key: []byte(key), Value: []byte(val)})
		if err != nil {
			t.Fatalf("error writing: %v\n", err)
		}
	}
	//
	// do some reading
	err = wal.Scan(func(e *binary.Entry) bool {
		fmt.Printf("%s\n", e)
		return true
	})
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// clean up
	doClean := false
	if doClean {
		err = os.RemoveAll(path)
		if err != nil {
			t.Fatalf("got error: %v\n", err)
		}
	}
}

func TestLog_TruncateFront(t *testing.T) {

	//
	// open log
	wal, err := OpenWAL(conf)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// get path for cleanup
	path := wal.GetConfig().BasePath
	//
	// do some writing
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i+1)
		val := fmt.Sprintf("my-value-%06d", i+1)
		_, err := wal.Write(&binary.Entry{Key: []byte(key), Value: []byte(val)})
		if err != nil {
			t.Fatalf("error writing: %v\n", err)
		}
	}
	//
	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// open log
	wal, err = OpenWAL(conf)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// print segment info
	fmt.Printf("--- PRINTING SEGMENT INFO ---\n")
	for _, s := range wal.segments {
		fmt.Printf("%s\n", s)
	}
	//
	// print dir structure
	files, err := os.ReadDir(path)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	for _, file := range files {
		fmt.Printf("segment: %s\n", file.Name())
	}
	//
	// test truncate front
	err = wal.TruncateFront(256)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	fmt.Printf("--- PRINTING SEGMENT INFO ---\n")
	for _, s := range wal.segments {
		fmt.Printf("%s\n", s)
	}
	//
	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// clean up
	doClean := false
	if doClean {
		err = os.RemoveAll(path)
		if err != nil {
			t.Fatalf("got error: %v\n", err)
		}
	}
}
