package lsmtree

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func makeData(prefix string, i int) []byte {
	return []byte(fmt.Sprintf("%s-%06d", prefix, i))
}

func TestReadWriteEntries(t *testing.T) {

	// offsets for read at later on
	var offsets []int64

	// create temp directory
	dir := t.TempDir()

	// get temp file
	fd, err := os.CreateTemp(dir, "readwrite-test-data-*")
	if err != nil {
		t.Errorf("create and open temp file: %v\n", err)
	}
	defer fd.Close()

	fmt.Println("writing entries...")
	// write entries
	for i := 0; i < 500; i++ {
		// make entry
		e := &Entry{
			Key:   makeData("key", i),
			Value: makeData("value", i),
		}
		// add checksum
		e.CRC = checksum(append(e.Key, e.Value...))

		// get current offset for later
		offset, err := fd.Seek(0, io.SeekCurrent)
		if err != nil {
			t.Errorf("seek current offset: %v\n", err)
		}
		offsets = append(offsets, offset)

		// write entry
		_, err = writeEntry(fd, e)
		if err != nil {
			t.Errorf("writing entry: %v\n", err)
		}
	}

	// rewind
	_, err = fd.Seek(0, io.SeekStart)
	if err != nil {
		t.Errorf("rewind: %v\n", err)
	}

	fmt.Println("reading entries...")
	// read entries
	i := 0
	for {
		// read entry
		e, err := readEntry(fd)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			t.Errorf("reading entry: %v\n", err)
		}
		if i%25 == 0 {
			fmt.Printf("%s\n", e)
		}
		i++
	}

	fmt.Println("reading entries at...")
	// read entries at
	for i := 500 - 1; i > 0; i -= 25 {
		e, err := readEntryAt(fd, offsets[i])
		if err != nil {
			t.Errorf("reading entry at (%d): %v\n", err, offsets[i])
		}
		fmt.Printf("offset: %d, %s\n", offsets[i], e)
	}
}
