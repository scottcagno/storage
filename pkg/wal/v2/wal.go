package v2

import (
	"github.com/scottcagno/storage/pkg/binary"
	"os"
	"path/filepath"
	"sync"
)

// WAL is a write-ahead log structure
type WAL struct {
	lock       sync.RWMutex   // lock is a mutual exclusion lock
	base       string         // base is the base filepath
	r          *binary.Reader // r is a binary reader
	w          *binary.Writer // w is a binary writer
	firstIndex uint64         // firstIndex is the index of the first entry
	lastIndex  uint64         // lastIndex is the index of the last entry
	segments   []*segment     // segments is an index of the current file segments
	active     *segment       // active is the current active segment
}

// OpenWAL opens and returns a new write-ahead log structure
func OpenWAL(path string) (*WAL, error) {
	// make sure we are working with absolute paths
	base, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	// sanitize any path separators
	base = filepath.ToSlash(base)
	// create any directories if they are not there
	err = os.MkdirAll(base, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// create a new write-ahead log instance
	l := &WAL{
		base:       base,
		firstIndex: 1,
		segments:   make([]*segment, 0),
	}
	// attempt to load segments
	err = l.loadSegments()
	if err != nil {
		return nil, err
	}
	// return write-ahead log
	return l, nil
}

// loadSegments initializes the segment index. It looks for segment
// files in the base directory and attempts to index the segment as
// well as any of the entries within the segment. If this is a new
// instance, it will create a new segment that is ready for writing.
func (l *WAL) loadSegments() error {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	return nil
}

// Read reads an entry from the write-ahead log at the specified index
func (l *WAL) Read() error {
	// read lock
	l.lock.RLock()
	defer l.lock.RUnlock()
	return nil
}

// Write writes an entry to the write-ahead log in an append-only fashion
func (l *WAL) Write() error {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	return nil
}

// Scan provides an iterator method for the write-ahead log
func (l *WAL) Scan() error {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	return nil
}

// Close closes the write-ahead log
func (l *WAL) Close() error {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	return nil
}

// String is the stringer method for the write-ahead log
func (l *WAL) String() error {
	return nil
}
