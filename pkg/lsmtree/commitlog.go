package lsmtree

import (
	"io"
	"os"
	"path/filepath"
)

const defaultCommitLogFileName = "wal-backup.log"

type commitLog struct {
	baseDir     string
	syncOnWrite bool
	fd          *os.File
	offsets     []int64
}

func openCommitLog(base string, syncOnWrite bool) (*commitLog, error) {
	// initialize base path
	base, err := initBasePath(base)
	if err != nil {
		return nil, err
	}
	// full file path
	file := filepath.Join(base, defaultCommitLogFileName)
	// open file
	fd, err := os.OpenFile(file, os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	// create commit log instance
	c := &commitLog{
		baseDir:     base,
		syncOnWrite: syncOnWrite,
		fd:          fd,
	}
	// load entry index
	err = c.loadIndex()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *commitLog) loadIndex() error {
	for {
		// get offset of entry
		offset, err := c.fd.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		// read entry
		_, err = readEntry(c.fd)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// read entry successful, add to index
		c.offsets = append(c.offsets, offset)
	}
	return nil
}

func (c *commitLog) get(offset int64) (*Entry, error) {
	// read entry at provided offset
	e, err := readEntryAt(c.fd, offset)
	if err != nil {
		return nil, err
	}
	// found it
	return e, nil
}

func (c *commitLog) put(e *Entry) (int64, error) {
	// write provided entry
	offset, err := writeEntry(c.fd, e)
	if err != nil {
		return -1, err
	}
	// return offset
	return offset, nil
}

func (c *commitLog) scan(iter func(e *Entry) bool) error {
	for i := range c.offsets {
		// read entry
		e, err := readEntryAt(c.fd, c.offsets[i])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// check entry against iterator boolean function
		if !iter(e) {
			// if it returns false, then process next segEntry
			continue
		}
	}
	return nil
}

func (c *commitLog) cycle() error {
	// seek to start
	_, err := c.fd.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	// truncate file
	err = c.fd.Truncate(0)
	if err != nil {
		return err
	}
	return nil
}

func (c *commitLog) sync() error {
	// flush data
	err := c.fd.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (c *commitLog) close() error {
	// flush data
	err := c.fd.Sync()
	if err != nil {
		return err
	}
	// close that thing
	err = c.fd.Close()
	if err != nil {
		return err
	}
	return nil
}
