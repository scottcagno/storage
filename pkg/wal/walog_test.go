package wal

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
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

	// keep indexes for later
	indexes := make([]uint64, 0)

	// do some writing
	for i := 0; i < 5000; i++ {
		data := []byte(fmt.Sprintf("#%d -- this is entry number %d for the record!", i, i))
		idx, err := wal.Write(data)
		if err != nil {
			t.Fatalf("error writing: %v\n", err)
		}
		indexes = append(indexes, idx)
	}

	log.Printf(">>> wrote %d entries, closing file\n\n", len(indexes))

	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	log.Printf(">>> opening log file...\n")

	// reopen log
	wal, err = Open("logs")
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// take a look at what we wrote
	files, err := os.ReadDir(path)
	if err != nil {
		t.Fatalf("error reading dir\n")
	}
	log.Printf("wrote %d entries, in %d files...\n", len(indexes), len(files))
	for _, file := range files {
		log.Printf("%s\n", file.Name())
	}

	// print log information
	log.Printf("%s\n", wal)

	index := uint64(25)
	log.Printf("lets read entry at index %d...\n", index)
	data, err := wal.Read(index)
	if err != nil {
		t.Fatalf("error reading at index %d\n", index)
	}
	log.Printf("data: %q\n", data)

	// do some reading
	for i := 0; i < len(indexes); i++ {
		idx := indexes[i]
		data, err := wal.Read(idx)
		if err != nil {
			t.Fatalf("error reading at index %d\n", idx)
		}
		if len(data) == 0 {
			t.Fatalf("expected data length to be greater than 0\n")
		}
		if i < 50 {
			fmt.Printf("looking up entry at index: %d -> %s\n", idx, data)
		}
	}

	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// clean up
	doClean := false
	if doClean {
		err = os.RemoveAll(path)
		if err != nil {
			t.Fatalf("got error: %v\n", err)
		}
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

	// do some writing
	for i := 0; i < 1000; i++ {
		data := []byte(fmt.Sprintf("#%d -- this is entry number %d for the record!", i, i))
		_, err := wal.Write(data)
		if err != nil {
			t.Fatalf("error writing: %v\n", err)
		}
	}

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

func TestLog_ReadWriteOne(t *testing.T) {

	// open log
	wal, err := Open("logs")
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// get path for cleanup later
	path := wal.Path()

	// write entry
	idx, err := wal.Write([]byte("this is a valid entry"))
	if err != nil {
		t.Fatalf("error writing: %v\n", err)
	}

	// read entry
	data, err := wal.Read(idx)
	if err != nil {
		t.Fatalf("error reading: %v\n", err)
	}
	log.Printf("data: %q\n", data)

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

func TestLog_TruncateFront(t *testing.T) {

	// open log
	wal, err := OpenWithOptions(Options{BasePath: "logs", MaxFileSize: 2 << 10})
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// get path for cleanup
	path := wal.Path()

	// do some writing
	for i := 0; i < 500; i++ {
		data := []byte(fmt.Sprintf("#%d -- this is entry number %d for the record!", i, i))
		_, err := wal.Write(data)
		if err != nil {
			t.Fatalf("error writing: %v\n", err)
		}
	}

	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// open log
	wal, err = OpenWithOptions(Options{BasePath: "logs", MaxFileSize: 2 << 10})
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// test truncate
	err = wal.truncateFront(256)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	doClean := false

	// clean up
	if doClean {
		err = os.RemoveAll(path)
		if err != nil {
			t.Fatalf("got error: %v\n", err)
		}
	}
}

func TestLog_TruncateBack(t *testing.T) {

	// open log
	wal, err := OpenWithOptions(Options{BasePath: "logs", MaxFileSize: 2 << 10})
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// get path for cleanup
	path := wal.Path()

	// do some writing
	for i := 0; i < 500; i++ {
		data := []byte(fmt.Sprintf("#%d -- this is entry number %d for the record!", i, i))
		_, err := wal.Write(data)
		if err != nil {
			t.Fatalf("error writing: %v\n", err)
		}
	}

	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// open log
	wal, err = OpenWithOptions(Options{BasePath: "logs", MaxFileSize: 2 << 10})
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// test truncate
	err = wal.truncateBack(256)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	doClean := true

	// clean up
	if doClean {
		err = os.RemoveAll(path)
		if err != nil {
			t.Fatalf("got error: %v\n", err)
		}
	}
}

func TestLog_Scan(t *testing.T) {

	// open log
	wal, err := Open("logs")
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// get path for cleanup
	path := wal.Path()

	// do some writing
	for i := 0; i < 25; i++ {
		data := []byte(fmt.Sprintf("#%d -- this is entry number %d for the record!", i, i))
		_, err := wal.Write(data)
		if err != nil {
			t.Fatalf("error writing: %v\n", err)
		}
	}

	// finally, test out the sacn
	err = wal.Scan(func(index uint64, data []byte) bool {
		fmt.Printf("index: %d, data: %q\n", index, data)
		return true
	})

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

	// get path for cleanup
	path := wal.Path()

	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	empty := &Log{}
	if !reflect.DeepEqual(wal, empty) {
		t.Fatalf("expected empty, got: %+v\n", wal)
	}

	// clean up
	err = os.RemoveAll(path)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

}

func Test_IndexLocation(t *testing.T) {

	// open log
	wal, err := Open("logs")
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	// get path for cleanup
	path := wal.Path()

	// do some writing
	for i := 0; i < 500; i++ {
		data := []byte(fmt.Sprintf("#%d -- this is entry number %d for the record!", i, i))
		_, err := wal.Write(data)
		if err != nil {
			t.Fatalf("error writing: %v\n", err)
		}
	}

	// list all the segments
	for _, s := range wal.segments {
		fmt.Printf("%s\n", s)
	}

	index := uint64(2)
	locateEntry(wal, index)
	_, err = wal.Read(index)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	index = 245
	locateEntry(wal, index)
	_, err = wal.Read(index)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	index = 45
	locateEntry(wal, index)
	_, err = wal.Read(index)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	index = 392
	locateEntry(wal, index)
	_, err = wal.Read(index)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	index = 472
	locateEntry(wal, index)
	_, err = wal.Read(index)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

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

func locateSegment(wal *Log, index uint64) {
	s := wal.segments[wal.findSegmentIndex(int64(index))]
	fmt.Printf("index %d is located in the following segment\n%s\n", index, s)
}

func locateEntry(wal *Log, index uint64) {
	s := wal.segments[wal.findSegmentIndex(int64(index))]
	fmt.Printf("index %d is located in the following segment\n%s", index, s)
	e := s.findEntry(index)
	fmt.Printf("\tin entry number %d\n\t%s\n", e, s.entries[e])
}
