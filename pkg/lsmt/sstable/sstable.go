package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	ErrFileClosed = errors.New("error: file is closed")
)

// SSTable is a sorted strings table
type SSTable struct {
	mu   sync.RWMutex
	fd   *os.File
	open bool
}

// Create creates and returns a new sstable
func Create(base string) (*SSTable, error) {
	// sanitize base path
	base, err := filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	base = filepath.ToSlash(base)
	// create dirs if they don't exist
	err = os.MkdirAll(base, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// create new sstable file
	path := filepath.Join(base, fmt.Sprintf("dat-%d.sst", time.Now().Unix()))
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	// return new sstable
	return &SSTable{
		fd:   fd,
		open: true,
	}, nil
}

// Write writes data to the sstable file
func (s *SSTable) Write(data []byte) error {
	// lock
	s.mu.Lock()
	defer s.mu.Unlock()
	// check to make sure file is open
	if !s.open {
		return ErrFileClosed
	}
	// write entry header
	var hdr [8]byte
	binary.LittleEndian.PutUint64(hdr[:], uint64(len(data)))
	_, err := s.fd.Write(hdr[:])
	if err != nil {
		return err
	}
	// write entry data
	_, err = s.fd.Write(data)
	if err != nil {
		return err
	}
	return nil
}

// Sync flushes any buffered data to the disk
func (s *SSTable) Sync() error {
	// lock
	s.mu.Lock()
	defer s.mu.Unlock()
	// call sync
	err := s.fd.Sync()
	if err != nil {
		return err
	}
	return nil
}

// Close closes the sstable and files (it makes sure to sync first)
func (s *SSTable) Close() error {
	// lock
	s.mu.Lock()
	defer s.mu.Unlock()
	// call sync
	err := s.fd.Sync()
	if err != nil {
		return err
	}
	// call close
	err = s.fd.Close()
	if err != nil {
		return err
	}
	return nil
}
