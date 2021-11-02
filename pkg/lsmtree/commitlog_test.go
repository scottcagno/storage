package lsmtree

import (
	"fmt"
	"testing"
)

const baseDir = "commit-log-testing"
const count = 500

func TestCommitLogWriteAndReadAt(t *testing.T) {

	// offsets for later
	var offsets []int64

	fmt.Println("opening commit log...")
	// open commit log
	c, err := openCommitLog(baseDir, false)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	fmt.Println("writing entries...")
	// write entries
	for i := 0; i < count; i++ {
		// make entry
		e := &Entry{
			Key:   makeData("key", i),
			Value: makeData("value", i),
		}
		// add checksum
		e.CRC = checksum(append(e.Key, e.Value...))

		// write entry
		offset, err := c.put(e)
		if err != nil {
			t.Errorf("put: %v\n", err)
		}

		// add offset to set
		offsets = append(offsets, offset)
	}

	fmt.Println("closing commit log...")
	// close commit log
	err = c.close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	fmt.Println("opening commit log, again...")
	// open commit log
	c, err = openCommitLog(baseDir, false)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	fmt.Println("reading entries at...")
	// read entries at
	for i := range offsets {
		e, err := c.get(offsets[i])
		if err != nil {
			t.Errorf("reading entry at (%d): %v\n", err, offsets[i])
		}
		fmt.Printf("offset: %d, %s\n", offsets[i], e)
	}

	fmt.Println("closing commit log, again...")
	// close commit log
	err = c.close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}
}

func TestCommitLogAppend(t *testing.T) {

	// offsets for later
	var offsets []int64

	fmt.Println("opening commit log...")
	// open commit log
	c, err := openCommitLog(baseDir, false)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	fmt.Println("writing entries...")
	// write entries
	for i := count; i < count*2; i++ {
		// make entry
		e := &Entry{
			Key:   makeData("key", i),
			Value: makeData("value", i),
		}
		// add checksum
		e.CRC = checksum(append(e.Key, e.Value...))

		// write entry
		offset, err := c.put(e)
		if err != nil {
			t.Errorf("put: %v\n", err)
		}

		// add offset to set
		offsets = append(offsets, offset)
	}

	fmt.Println("closing commit log...")
	// close commit log
	err = c.close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	fmt.Println("opening commit log, again...")
	// open commit log
	c, err = openCommitLog(baseDir, false)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	fmt.Println("reading entries at...")
	// read entries at
	for i := range offsets {
		e, err := c.get(offsets[i])
		if err != nil {
			t.Errorf("reading entry at (%d): %v\n", err, offsets[i])
		}
		fmt.Printf("offset: %d, %s\n", offsets[i], e)
	}

	fmt.Println("closing commit log, again...")
	// close commit log
	err = c.close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}
}

func TestCommitLogReset(t *testing.T) {

	// offsets for later
	var offsets []int64

	fmt.Println("opening commit log...")
	// open commit log
	c, err := openCommitLog(baseDir, false)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	fmt.Println("cycling...")
	// cycling
	err = c.cycle()
	if err != nil {
		t.Errorf("cycling: %v\n", err)
	}

	fmt.Println("writing entries...")
	// write entries
	for i := count / 2; i < count; i++ {
		// make entry
		e := &Entry{
			Key:   makeData("key", i),
			Value: makeData("value", i),
		}
		// add checksum
		e.CRC = checksum(append(e.Key, e.Value...))

		// write entry
		offset, err := c.put(e)
		if err != nil {
			t.Errorf("put: %v\n", err)
		}

		// add offset to set
		offsets = append(offsets, offset)
	}

	fmt.Println("closing commit log...")
	// close commit log
	err = c.close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	fmt.Println("opening commit log, again...")
	// open commit log
	c, err = openCommitLog(baseDir, false)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	fmt.Println("reading entries at...")
	// read entries at
	for i := range offsets {
		e, err := c.get(offsets[i])
		if err != nil {
			t.Errorf("reading entry at (%d): %v\n", err, offsets[i])
		}
		fmt.Printf("offset: %d, %s\n", offsets[i], e)
	}

	fmt.Println("cycling again...")
	// cycling
	err = c.cycle()
	if err != nil {
		t.Errorf("cycling: %v\n", err)
	}

	fmt.Println("closing commit log, again...")
	// close commit log
	err = c.close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}
}

func TestCommitLogTestLargeLoad(t *testing.T) {

	// offsets for later
	var offsets []int64

	fmt.Println("opening commit log...")
	// open commit log
	c, err := openCommitLog(baseDir, false)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	fmt.Println("cycling...")
	// cycling
	err = c.cycle()
	if err != nil {
		t.Errorf("cycling: %v\n", err)
	}

	fmt.Println("syncing...")
	// syncing
	err = c.sync()
	if err != nil {
		t.Errorf("syncing: %v\n", err)
	}

	fmt.Println("writing entries...")
	// write entries
	for i := 0; i < count*count; i++ {
		// make entry
		e := &Entry{
			Key:   makeData("key", i),
			Value: makeData("value", i),
		}
		// add checksum
		e.CRC = checksum(append(e.Key, e.Value...))

		// write entry
		offset, err := c.put(e)
		if err != nil {
			t.Errorf("put: %v\n", err)
		}

		// add offset to set
		offsets = append(offsets, offset)
	}

	fmt.Println("closing commit log...")
	// close commit log
	err = c.close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	fmt.Println("opening commit log, again...")
	// open commit log
	c, err = openCommitLog(baseDir, false)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	fmt.Println("reading entries at...")
	// read entries at
	j := 0
	for i := range offsets {
		e, err := c.get(offsets[i])
		if err != nil {
			t.Errorf("reading entry at (%d): %v\n", err, offsets[i])
		}
		if i%500 == 0 {
			fmt.Printf("offset: %d, %s\n", offsets[i], e)
		}
		j++
	}

	fmt.Printf(">>> read %d entries\n", j)

	fmt.Println("closing commit log, again...")
	// close commit log
	err = c.close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}
}

func TestCommitLogScanNext(t *testing.T) {

	fmt.Println("opening commit log...")
	// open commit log
	c, err := openCommitLog(baseDir, false)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	fmt.Println("writing entries...")
	// write entries
	for i := count / 2; i < count; i++ {
		// make entry
		e := &Entry{
			Key:   makeData("key", i),
			Value: makeData("value", i),
		}
		// add checksum
		e.CRC = checksum(append(e.Key, e.Value...))

		// write entry
		_, err := c.put(e)
		if err != nil {
			t.Errorf("put: %v\n", err)
		}
	}

	fmt.Println("closing commit log...")
	// close commit log
	err = c.close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	fmt.Println("opening commit log, again...")
	// open commit log
	c, err = openCommitLog(baseDir, false)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	fmt.Println("reading entries using scan next...")
	err = c.scan(func(e *Entry) bool {
		fmt.Printf("%s\n", e)
		return true
	})
	if err != nil {
		t.Errorf("next: %v\n", err)
	}

	fmt.Println("cycling...")
	// cycling
	err = c.cycle()
	if err != nil {
		t.Errorf("cycling: %v\n", err)
	}

	fmt.Println("closing commit log")
	// close commit log
	err = c.close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}
}
