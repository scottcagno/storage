package v4

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type segment struct {
	mu      sync.RWMutex
	path    string
	file    *os.File
	index   uint64
	seqnum  uint64
	entries []uint64
}

func openSegment(path string, index uint64) (*segment, error) {
	// check to see if directory exists
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		// create it if it does not exist
		err = os.MkdirAll(path, os.ModeDir)
		if err != nil {
			return nil, err
		}
	}
	s := &segment{
		path: filepath.Join(path, segmentName(index)),
	}
	return s, nil
}

func (s *segment) FileInfo() (string, string) {
	return filepath.Split(s.path)
}

func segmentName(index uint64) string {
	return fmt.Sprintf("wal-%020d.seg", index)
}

func (s *segment) Read() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// read entry length
	var buf [8]byte
	_, err := io.ReadFull(s.file, buf[:])
	if err != nil {
		return nil, err
	}
	// decode entry length
	elen := binary.LittleEndian.Uint64(buf[:])
	// make byte slice of entry length size
	entry := make([]byte, elen)
	// read entry from reader into slice
	_, err = io.ReadFull(s.file, entry)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (s *segment) Write(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// get current offset
	offset, err := s.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	// encode entry length
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(len(data)))
	// write entry length
	_, err = s.file.Write(buf[:])
	if err != nil {
		return err
	}
	// write entry data
	_, err = s.file.Write(data)
	if err != nil {
		return err
	}
	// ensure durability
	err = s.file.Sync()
	if err != nil {
		return err
	}
	// add new entry index
	s.entries = append(s.entries, uint64(offset))
	return nil
}

func (s *segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.file.Sync()
	if err != nil {
		return err
	}
	err = s.file.Close()
	if err != nil {
		return err
	}
	return nil
}
