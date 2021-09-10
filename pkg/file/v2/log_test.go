package v2

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {

	// initial test
	var wal *Log
	if wal != nil {
		t.Fatalf("should be nil, got: %v\n", wal)
	}
	// open log
	wal, err := Open("logs")
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// get path for cleanup later
	path := wal.Path()

	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// check for nil
	if wal == nil {
		t.Fatalf("wal should not be nil, got: %v\n", wal)
	}

	// cleanup
	err = os.RemoveAll(path)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
}

func TestLog_Read(t *testing.T) {

	// open log
	wal, err := Open("logs")
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// get path for cleanup later
	path := wal.Path()

	// do stuff
	// ...

	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// clean up
	err = os.RemoveAll(path)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
}

func TestLog_Write(t *testing.T) {

	// open log
	wal, err := Open("logs")
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// get path for cleanup later
	path := wal.Path()

	// init idx for testing later
	var idx uint64

	// do some writing
	for i := 0; i < 5000; i++ {
		data := []byte(fmt.Sprintf("#%d -- this is entry number %d for the record!", i, i))
		if i < 4999 {
			_, err := wal.Write(data)
			if err != nil {
				t.Fatalf("error writing: %v\n", err)
			}
		} else {
			idx, err = wal.Write(data)
			if err != nil {
				t.Fatalf("error writing: %v\n", err)
			}
		}
	}

	// read record
	ent, err := wal.Read(idx)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	log.Printf("read entry at index %d, got %q\n", idx, ent)

	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// clean up
	err = os.RemoveAll(path)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
}

func TestLog_Sync(t *testing.T) {

	// open log
	wal, err := Open("logs")
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// turn off syncing
	wal.noSync = true

	// get path for cleanup later
	path := wal.Path()

	// do some writing
	for i := 0; i < 5000; i++ {
		data := []byte(fmt.Sprintf("#%d -- this is entry number %d for the record!", i, i))
		_, err := wal.Write(data)
		if err != nil {
			t.Fatalf("error writing: %v\n", err)
		}
	}
	log.Println("wrote 500 entries...")
	dent, err := os.ReadDir(path)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	log.Printf("dir contains %d items\n", len(dent))
	time.Sleep(5 * time.Second)

	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// clean up
	err = os.RemoveAll(path)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

}

func TestLog_Close(t *testing.T) {

	// open log
	wal, err := Open("logs")
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// get path for cleanup later
	path := wal.Path()

	// do stuff
	// ...

	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// clean up
	err = os.RemoveAll(path)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

}
