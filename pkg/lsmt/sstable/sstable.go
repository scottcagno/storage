package sstable

import (
	"errors"
	"fmt"
	"github.com/scottcagno/storage/pkg/binary"
	"github.com/scottcagno/storage/pkg/index/bptree"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	SSTablePrefix = "dat-"
	SSTableSuffix = ".sst"
)

var (
	ErrFileClosed = errors.New("error: file is closed")
)

// Batch is a batch of entries
type Batch struct {
	entries []*binary.Entry
}

// NewBatch returns a pointer to a new Batch
func NewBatch() *Batch {
	return &Batch{
		entries: make([]*binary.Entry, 0),
	}
}

// Write writes an entry to the batch
func (b *Batch) Write(key string, value []byte) {
	b.entries = append(b.entries, &binary.Entry{
		Id:    0,
		Key:   []byte(key),
		Value: value,
	})
}

// Close closes (frees) the batch
func (b *Batch) Close() {
	b.entries = nil
	return
}

// SSTableIndex is a sparse index for the SSTable
type SSTableIndex struct {
	path  string         // path is the path of the sstable file that is being indexed
	index *bptree.BPTree // index is a sparse index for a given table
}

// OpenSSTableIndex opens and returns a new *SSTableIndex
// for the SSTable file located at the path provided
func OpenSSTableIndex(rate int, path string) (*SSTableIndex, error) {
	// check to make sure sstable
	// exists at provided path
	_, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	// open sstable
	sst, err := Open(path)
	if err != nil {
		return nil, err
	}
	// dont forget to close
	defer sst.Close()
	// create new sstable index structure
	// to load data into
	si := &SSTableIndex{
		path:  path,
		index: bptree.NewBPTree(),
	}
	// scan through the sstable entries
	// and fill index at rate specified
	// scan the table sequentially
	for count := 0; ; count++ {
		e, err := sst.Read()
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		// if count entries not at rate, just continue to next entry
		if count%rate != 0 {
			continue
		}
		// otherwise, index entry
		si.index.Put(string(e.Key), bptree.IntToVal(e.Id))
	}
	return si, nil
}

// Search searches the sparse index for a key, and returns
// the offset of found key and true if it is an exact offset
// or false if it is an approximate offset. It will return -1
// along with false if there is any kind of error.
func (si *SSTableIndex) Search(key string) (int64, bool) {
	k, v := si.index.GetClosest(key)
	if v == nil {
		return -1, false
	}
	return bptree.ValToInt(v), k == key
}

// Close closes up the index
func (si *SSTableIndex) Close() {
	si.index.Close()
}

// SSTable is a sorted strings table
type SSTable struct {
	lock sync.RWMutex
	path string         // path is the filepath of this sstable
	r    *binary.Reader // r is a binary file reader for this table
	w    *binary.Writer // r is a binary file writer for this table
}

// makeFileName returns a file name using the provided timestamp.
// If t is nil, it will create a new name using time.Now()
func makeFileName() string {
	t := time.Now()
	//tf := t.Format("2006-01-03_15:04:05:000000")
	//return fmt.Sprintf("%s%s%s", LogPrefix, time.RFC3339Nano, LogSuffix)
	return fmt.Sprintf("%s%d%s", SSTablePrefix, t.UnixMicro(), SSTableSuffix)
}

// Create creates and returns a new sstable
func Create(base string) (*SSTable, error) {
	// sanitize base path
	base, err := cleanPath(base)
	if err != nil {
		return nil, err
	}
	// create dirs if they don't exist
	err = os.MkdirAll(base, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// create new sstable file
	path := filepath.Join(base, makeFileName())
	fd, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	err = fd.Close()
	if err != nil {
		return nil, err
	}
	// open file reader
	breader, err := binary.OpenReader(path)
	if err != nil {
		return nil, err
	}
	// open file writer
	bwriter, err := binary.OpenWriter(path)
	if err != nil {
		return nil, err
	}
	// return new sstable
	return &SSTable{
		path: path,
		r:    breader,
		w:    bwriter,
	}, nil
}

// Open returns a new sstable if it exists
func Open(path string) (*SSTable, error) {
	_, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	// open file reader
	breader, err := binary.OpenReader(path)
	if err != nil {
		return nil, err
	}
	// return sstable
	return &SSTable{
		path: path,
		r:    breader,
		w:    nil,
	}, nil
}

// Read reads the next single entry from the sstable file sequentially
func (s *SSTable) Read() (*binary.Entry, error) {
	// lock
	s.lock.Lock()
	defer s.lock.Unlock()
	// read next entry
	e, err := s.r.ReadEntry()
	if err != nil {
		return nil, err
	}
	return e, nil
}

// ReadAt reads a single entry from the sstable file at the provided offset
func (s *SSTable) ReadAt(offset int64) (*binary.Entry, error) {
	// lock
	s.lock.Lock()
	defer s.lock.Unlock()
	// read entry
	e, err := s.r.ReadEntryAt(offset)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// Write writes a single entry to the sstable file (sequentially)
func (s *SSTable) Write(key string, value []byte) error {
	// lock
	s.lock.Lock()
	defer s.lock.Unlock()
	// get offset to add to entry
	offset, err := s.w.Offset()
	if err != nil {
		return err
	}
	// create entry
	e := &binary.Entry{
		Id:    offset,
		Key:   []byte(key),
		Value: value,
	}
	// write entry
	_, err = s.w.WriteEntry(e)
	if err != nil {
		return err
	}
	return nil
}

// WriteBatch writes a batch of entries to the sstable file (sequentially)
func (s *SSTable) WriteBatch(batch *Batch) error {
	// lock
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, e := range batch.entries {
		// get offset to add to entry
		offset, err := s.w.Offset()
		if err != nil {
			return err
		}
		// update entry
		e.Id = offset
		// write entry
		_, err = s.w.WriteEntry(e)
		if err != nil {
			return err
		}
	}
	batch.Close()
	return nil
}

// Search performs a binary search on the sstable and returns the offset if found
func (s *SSTable) Search(key string) (int64, error) {
	/*
		// declare for later
		i, j := 0, s.index // j should equal number of entries
		// otherwise, perform binary search
		for i < j {
			h := i + (j-i)/2
			if index >= s.entries[h].index {
				i = h + 1
			} else {
				j = h
			}
		}
		return i - 1
	*/
	return 0, nil
}

// Scan provides an iterator for the sstable
func (s *SSTable) Scan(iter func(e *binary.Entry) bool) error {
	// lock
	s.lock.Lock()
	defer s.lock.Unlock()
	// seek to beginning of file
	_, err := s.r.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	// scan the table sequentially
	for {
		e, err := s.r.ReadEntry()
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		if !iter(e) {
			continue
		}
	}
	return nil
}

// Close closes the sstable and files (it makes sure to sync first)
func (s *SSTable) Close() error {
	// lock
	s.lock.Lock()
	defer s.lock.Unlock()
	// call sync
	err := s.r.Close()
	if err != nil {
		return err
	}
	if s.w != nil {
		// call close
		err = s.w.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// cleanPath sanitizes path provided
func cleanPath(path string) (string, error) {
	// sanitize base path
	base, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.ToSlash(base), nil
}
