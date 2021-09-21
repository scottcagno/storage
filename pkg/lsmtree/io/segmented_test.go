package wal

import (
	"fmt"
	"os"
	"testing"
)

func TestSegmentedFile(t *testing.T) {
	//
	// set max file size
	maxFileSize = 2 << 10
	//
	// open log
	wal, err := Open("data")
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// get path for cleanup
	path := wal.Path()
	//
	// do some writing
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i+1)
		val := []byte(fmt.Sprintf("my-value-%06d", i+1))
		_, err := wal.Write2(key, val)
		if err != nil {
			t.Fatalf("error writing: %v\n", err)
		}
	}
	//
	// do some reading
	wal.Scan(func(i int64, k string, v []byte) bool {
		fmt.Printf("index=%d, key=%q, value=%q\n", i, k, v)
		return true
	})
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

func TestSegmentedFile_TruncateFront(t *testing.T) {
	//
	// set max file size
	maxFileSize = 2 << 10
	//
	// open log
	wal, err := Open("data")
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// get path for cleanup
	path := wal.Path()
	//
	// do some writing
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i+1)
		val := []byte(fmt.Sprintf("my-value-%06d", i+1))
		_, err := wal.Write(key, val)
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
	wal, err = Open("data")
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
