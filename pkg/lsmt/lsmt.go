package lsmt

import (
	"github.com/scottcagno/storage/pkg/binary"
	"github.com/scottcagno/storage/pkg/index/bptree"
	"github.com/scottcagno/storage/pkg/lsmt/memtable"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"os"
	"path/filepath"
	"strings"
)

const (
	memtablePath = "data/memtable"
	sstablePath  = "data/sstable"
	sparseFactor = 64
)

// LSMTree is a "Log-Structured Merge Tree"
type LSMTree struct {
	basePath string // base path
	memPath  string
	sstPath  string
	mem      *memtable.Memtable
	idx      *bptree.BPTree // sparse index for SSTables
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
		idx:      bptree.NewBPTree(),
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
		// otherwise, read sstable and build sparse index
		sst, err := sstable.Open(file.Name())
		if err != nil {
			return err
		}
		// defer sstable close
		defer sst.Close()
		// lets scan the table
		count := 0 // used for sparse index
		err = sst.Scan(func(e *binary.Entry) bool {
			if count/sparseFactor == 0 {
				// add entry and offset to sparse index
				// TODO: implement...
			}
			return false // otherwise, skip
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Put adds or updates an entry in the LSMTree
func (l *LSMTree) Put(key string, value []byte) error {
	// TODO: implement...
	return nil
}

// Get finds an entry in the LSMTree
func (l *LSMTree) Get(key string) ([]byte, error) {
	// TODO: implement...
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
