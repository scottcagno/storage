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
	// seek to end of file
	_, err = fd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	// create commit log instance
	c := &commitLog{
		baseDir:     base,
		syncOnWrite: syncOnWrite,
		fd:          fd,
	}
	return c, nil
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

func (c *commitLog) reset() error {
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
