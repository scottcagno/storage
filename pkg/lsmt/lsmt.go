package lsmt

import (
	"errors"
	"github.com/scottcagno/storage/pkg/lsmt/memtable"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"os"
	"path/filepath"
	"strings"
)

const (
	memtablePath    = "data/memtable"
	sstablePath     = "data/sstable"
	sparseFactor    = 64
	maxMemtableSize = 2 << 20 // 2 MB
)

var (
	ErrNotFound = errors.New("error: not found")
)

// LSMTree is a "Log-Structured Merge Tree"
type LSMTree struct {
	basePath string // base path
	memPath  string
	sstPath  string
	mem      *memtable.Memtable
	idx      []*sstable.SSTableIndex // sparse indexes for SSTables
}

// Open initializes, loads and returns an *LSMTree instance
func Open(path string) (*LSMTree, error) {
	// open a new memtable instance
	memPath := filepath.Join(path, memtablePath)
	mem, err := memtable.Open(memPath)
	if err != nil {
		return nil, err
	}
	// initialize path for sstables if it doesn't exist
	sstPath := filepath.Join(path, sstablePath)
	err = os.MkdirAll(sstPath, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// create LSMTree instance
	l := &LSMTree{
		basePath: path,
		memPath:  memPath,
		sstPath:  sstPath,
		mem:      mem,
		idx:      make([]*sstable.SSTableIndex, 0),
	}
	// populate sparse index
	err = l.loadSparseIndex()
	if err != nil {
		return nil, err
	}
	// return LSMTree
	return l, nil
}

// loadSparseIndex populates the LSMTree's sparse index for the SSTables
func (l *LSMTree) loadSparseIndex() error {
	// check out base sstable path
	files, err := os.ReadDir(l.sstPath)
	if err != nil {
		return err
	}
	// if there are no files, simply return
	if len(files) == 0 {
		return nil
	}
	// attempt to load, if they are proper files
	for _, file := range files {
		// skip files that are not sstables
		if !strings.HasSuffix(file.Name(), sstable.SSTableSuffix) {
			continue
		}
		// open a sparse index for sstable file
		si, err := sstable.OpenSSTableIndex(sparseFactor, file.Name())
		if err != nil {
			return err
		}
		// append sparse index to tree
		l.idx = append(l.idx, si)
	}
	return nil
}

// Put adds or updates an entry in the LSMTree
func (l *LSMTree) Put(key string, value []byte) error {
	// insert entry into the memtable
	err := l.mem.Put(key, value)
	if err != nil {
		return err
	}
	// check the size of the memtable to see
	// if we need to dump out to an sstable
	if l.mem.Size() >= maxMemtableSize-4<<10 {
		// make a new sstable
		sst, err := sstable.Create(l.sstPath)
		if err != nil {
			return err
		}
		// write memtable data to sstable
		err = l.mem.Scan(func(k string, v []byte) bool {
			// write entry to sstable
			err = sst.Write(k, v)
			if err != nil {
				return false
			}
			return true
		})
		// grab the path of the sstable
		path := sst.Path()
		// dont forget to close sstable
		err = sst.Close()
		if err != nil {
			return err
		}
		// make sure we create an index
		si, err := sstable.OpenSSTableIndex(sparseFactor, path)
		if err != nil {
			return err
		}
		// add to our sparse index set
		l.idx = append(l.idx, si)
		// next, we need to reset the memtable
		// get base path
		path = l.mem.Path()
		// close the memtable
		err = l.mem.Close()
		if err != nil {
			return err
		}
		// clean up commit log file
		err = os.RemoveAll(path)
		if err != nil {
			return err
		}
		// re-open a "fresh" memtable
		l.mem, err = memtable.Open(l.memPath)
		if err != nil {
			return err
		}
	}
	// otherwise, the memtable is not full, and we can simply return
	return nil
}

// Get finds an entry in the LSMTree
func (l *LSMTree) Get(key string) ([]byte, error) {
	// first, lets check the memtable
	v, err := l.mem.Get(key)
	if err == nil && v != nil {
		// we found it in the memtable!
		return v, nil
	}
	// it is not in the memtable, so lets check the sparse index...
	//
	// attempt to find using the sparse index.
	// if the key you are searching is greater
	// than the last index of current table index,
	// then we might want to check another table
	var active *sstable.SSTableIndex
	for i := range l.idx {
		if strings.Compare(key, l.idx[i].LastIndex) > 0 {
			continue
		}
		// located the table which *should* contain the key
		active = l.idx[i]
		break
	}
	// search the active table where the key should reside
	offset, ok := active.Search(key)
	if offset == -1 && !ok {
		return nil, ErrNotFound
	}
	// TODO: there has got to be a better way of doing this
	// TODO: opening and closing sstables all the time is gonna suck
	// open sstable for reading data
	sst, err := sstable.Open(active.Path())
	if err != nil {
		return nil, err
	}
	defer sst.Close()
	// we have located something, lets check to see if the
	// found offset is exact, or approximate
	if ok {
		// offset is exact, read from table
		e, err := sst.ReadAt(offset)
		if err != nil {
			return nil, err
		}
		// return found value
		return e.Value, nil
	}
	// offset is approximate, lets start scanning
	// TODO: make something happen here...
	return nil, nil
}

// Del "removes" an entry in the LSMTree
func (l *LSMTree) Del(key string) error {
	// TODO: implement...
	return nil
}

// Close makes sure everything is synchronized and gracefully closes
func (l *LSMTree) Close() error {
	// TODO: implement...
	return nil
}
