package v2

import (
	"fmt"
	"os"
	"testing"
)

func TestLog_TruncateFront(t *testing.T) {
	//
	// set max file size
	maxFileSize = 1 << 10
	//
	// open log
	wal, err := Open("logs")
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// get path for cleanup
	path := wal.Path()
	//
	// do some writing
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("my-value-%06d", i))
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
	wal, err = Open("logs")
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
