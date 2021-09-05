package v3

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type WAL struct {
	mu         sync.RWMutex
	path       string
	closed     bool
	firstIndex uint64
	lastIndex  uint64
	segfile    *os.File
	segments   []*segment
	segcache   *segment
}

func Open(path string) (*WAL, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	wal := &WAL{
		path:   path,
		closed: true,
	}
	err = wal.load()
	if err != nil {
		return nil, err
	}
	return wal, nil
}

func (wal *WAL) firstSegment() *segment {
	return wal.segments[0]
}

func (wal *WAL) lastSegment() *segment {
	return wal.segments[len(wal.segments)-1]
}

func (wal *WAL) load() error {
	// check to see if directory exists
	_, err := os.Stat(wal.path)
	if os.IsNotExist(err) {
		// create it if it does not exist
		err = os.MkdirAll(wal.path, os.ModeDir)
		if err != nil {
			return err
		}
	}
	// simply start walking the directory
	files, err := os.ReadDir(wal.path)
	if err != nil {
		return err
	}
	// read files in dir and load segments
	for _, file := range files {
		// skip files that are not log segments
		if file.IsDir() || !strings.HasSuffix(file.Name(), "seg.log") {
			continue
		}
		// read header portion of each log segment to get the checksum and index
		header, err := readLogSegmentHeader(file.Name())
		if err != nil {
			return err
		}
		if !header.hasValidChecksum() {
			// TODO: handle this somehow??
			continue
		}
		// add to log segment cache
		wal.segments = append(wal.segments, &segment{
			index: header.getIndex(),
			path:  filepath.Join(wal.path, file.Name()),
		})
		fmt.Println(file.Name())
	}
	// if no segments, create a new log segment and return
	if len(wal.segments) == 0 {
		wal.segments = append(wal.segments, &segment{
			index: 1,
			path:  filepath.Join(wal.path, segmentName(1)),
		})
		wal.segfile, err = os.Create(wal.segments[0].path)
		return err
	}
	// open last log segment for appending

	wal.segfile, err = os.OpenFile(wal.lastSegment().path, os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	_, err = wal.segfile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	return nil
}

func (wal *WAL) Read(index uint64) ([]byte, error) {
	// TODO: all the stuff...
	return nil, nil
}

func (wal *WAL) Write(index uint64, data []byte) error {
	// TODO: all the stuff...
	return nil
}

func (wal *WAL) Sync() error {
	// TODO: all the stuff...
	return nil
}

func (wal *WAL) Close() error {
	// TODO: all the stuff...
	return nil
}
