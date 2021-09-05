package v3

import (
	"os"
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

func (w *WAL) load() error {
	// TODO: all the stuff...
	return nil
}

func (w *WAL) Open(path string) (*WAL, error) {
	// TODO: all the stuff...
	return nil, nil
}

func (w *WAL) Read(index uint64) ([]byte, error) {
	// TODO: all the stuff...
	return nil, nil
}

func (w *WAL) Write(index uint64, data []byte) error {
	// TODO: all the stuff...
	return nil
}

func (w *WAL) Sync() error {
	// TODO: all the stuff...
	return nil
}

func (w *WAL) Close() error {
	// TODO: all the stuff...
	return nil
}
