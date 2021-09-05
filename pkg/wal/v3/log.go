package v3

import (
	"fmt"
	"os"
	"path/filepath"
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

func (wal *WAL) load() error {
	files, err := os.ReadDir(wal.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		fmt.Println(file.Name())
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
