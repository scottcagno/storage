package sstable

import (
	"bytes"
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/memtable"
	"github.com/scottcagno/storage/pkg/lsmt/trees/rbtree"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	filePrefix      = "sst-"
	dataFileSuffix  = ".dat"
	indexFileSuffix = ".idx"
)

var Tombstone = []byte(nil)

type KeyRange struct {
	index int64
	first string
	last  string
}

func (kr *KeyRange) InKeyRange(k string) bool {
	return kr.first <= k && k <= kr.last
}

func (kr *KeyRange) String() string {
	return fmt.Sprintf("kr.gindex=%d, kr.first=%q, kr.last=%q", kr.index, kr.first, kr.last)
}

type SSTManager struct {
	lock   sync.RWMutex
	base   string
	sparse []*KeyRange
	gindex int64
}

// OpenSSTManager opens and returns a SSTManager, which allows you to
// perform operations across all the ss-table and ss-table-indexes,
// hopefully without too much hassle
func OpenSSTManager(base string) (*SSTManager, error) {
	// make sure we are working with absolute paths
	base, err := filepath.Abs(base)
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
	// create ss-table-manager instance
	sstm := &SSTManager{
		base:   base,
		sparse: make([]*KeyRange, 0),
	}
	// read the ss-table directory
	files, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	// lock
	sstm.lock.RLock()
	defer sstm.lock.RUnlock()
	// go over all the files
	for _, file := range files {
		// skip all non ss-tables
		if file.IsDir() || !strings.HasSuffix(file.Name(), dataFileSuffix) {
			continue
		}
		// get ss-table id from file name
		index, err := IndexFromDataFileName(file.Name())
		if err != nil {
			return nil, err
		}
		// open the ss-table-gindex
		ssi, err := OpenSSTIndex(sstm.base, index)
		if err != nil {
			return nil, err
		}
		// create a new key-range "gindex"
		kr := &KeyRange{
			index: index,     // gindex of the ss-table
			first: ssi.first, // first key in the ss-table
			last:  ssi.last,  // last key in the ss-table
		}
		// add it to our "sparse" gindex
		sstm.sparse = append(sstm.sparse, kr)
		// close gindex
		err = ssi.Close()
		if err != nil {
			return nil, err
		}
	}
	// update the last global gindex
	sstm.gindex = sstm.getLastGIndex()
	return sstm, nil
}

func (sstm *SSTManager) getLastGIndex() int64 {
	if len(sstm.sparse) == 0 {
		return 0
	}
	return sstm.sparse[len(sstm.sparse)-1].index
}

func (sstm *SSTManager) addKeyRange(first, last string) {
	kr := &KeyRange{index: sstm.gindex, first: first, last: last}
	sstm.sparse = append(sstm.sparse, kr)
}

// FlushMemtableToSSTable takes a pointer to a memtable and writes it to disk as an ss-table
func (sstm *SSTManager) FlushMemtableToSSTable(memt *memtable.Memtable) error {
	// lock
	sstm.lock.Lock()
	defer sstm.lock.Unlock()
	// make new batch
	batch := sstm.NewBatch()
	// iterate mem-table entries
	memt.Scan(func(me rbtree.RBEntry) bool {
		// and write each entry to the batch
		batch.WriteEntry(me.(memtable.MemtableEntry).Entry)
		return true
	})
	// reset memtable asap
	err := memt.Reset()
	if err != nil {
		return err
	}
	// open new ss-table
	sst, err := OpenSSTable(sstm.base, sstm.gindex+1)
	if err != nil {
		return err
	}
	// write batch to ss-table
	err = sst.WriteBatch(batch)
	if err != nil {
		return err
	}
	// save for later
	first, last := sst.index.first, sst.index.last
	// flush and close ss-table
	err = sst.Close()
	if err != nil {
		return err
	}
	// in the clear, increment gindex
	sstm.gindex++
	// add new entry to sparse index
	sstm.addKeyRange(first, last)
	// return
	return nil
}

func (sstm *SSTManager) NewBatch() *Batch {
	return new(Batch)
}

func (sstm *SSTManager) FlushBatchToSSTable(batch *Batch) error {
	// lock
	sstm.lock.Lock()
	defer sstm.lock.Unlock()
	// open new ss-table
	sst, err := OpenSSTable(sstm.base, sstm.gindex+1)
	if err != nil {
		return err
	}
	// write batch to ss-table
	err = sst.WriteBatch(batch)
	if err != nil {
		return err
	}
	// save for later
	first, last := sst.index.first, sst.index.last
	// flush and close ss-table
	err = sst.Close()
	if err != nil {
		return err
	}
	// in the clear, increment gindex
	sstm.gindex++
	// add new entry to sparse index
	sstm.addKeyRange(first, last)
	return nil
}

func (sstm *SSTManager) SearchSparseIndex(k string) (int64, error) {
	// read lock
	sstm.lock.RLock()
	defer sstm.lock.RUnlock()
	for _, kr := range sstm.sparse {
		if !kr.InKeyRange(k) {
			continue
		}
		return kr.index, nil
	}
	return -1, ErrSSTIndexNotFound
}

func (sstm *SSTManager) GetSparseIndex() []*KeyRange {
	return sstm.sparse
}

func (sstm *SSTManager) ListSSTables() []string {
	// read lock
	sstm.lock.RLock()
	defer sstm.lock.RUnlock()
	files, err := os.ReadDir(sstm.base)
	if err != nil {
		return nil
	}
	var ssts []string
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), dataFileSuffix) {
			continue
		}
		ssts = append(ssts, file.Name())
	}

	return ssts
}

func (sstm *SSTManager) ListSSTIndexes() []string {
	// read lock
	sstm.lock.RLock()
	defer sstm.lock.RUnlock()
	files, err := os.ReadDir(sstm.base)
	if err != nil {
		return nil
	}
	var ssti []string
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), indexFileSuffix) {
			continue
		}
		ssti = append(ssti, file.Name())
	}
	return ssti
}

func (sstm *SSTManager) CompactSSTables(index int64) error {
	// lock
	sstm.lock.Lock()
	defer sstm.lock.Unlock()
	// load sstable
	sst, err := OpenSSTable(sstm.base, index)
	if err != nil {
		return err
	}
	// make batch
	batch := NewBatch()
	// iterate
	err = sst.Scan(func(e *binary.Entry) bool {
		// add any data entries that are not tombstones to batch
		if e.Value != nil && !bytes.Equal(e.Value, Tombstone) {
			batch.WriteEntry(e)
		}
		return true
	})
	if err != nil {
		return err
	}
	// get path
	tpath, ipath := sst.path, sst.index.path
	// close sstable
	err = sst.Close()
	if err != nil {
		return err
	}
	// remove old table
	err = os.Remove(tpath)
	if err != nil {
		return err
	}
	// remove old gindex
	err = os.Remove(ipath)
	if err != nil {
		return err
	}
	// open new sstable to write to
	sst, err = OpenSSTable(sstm.base, index)
	if err != nil {
		return err
	}
	// write batch to table
	err = sst.WriteBatch(batch)
	// flush and close sstable
	err = sst.Close()
	if err != nil {
		return err
	}
	return nil
}

func (sstm *SSTManager) MergeSSTables(iA, iB int64) error {
	// lock
	sstm.lock.Lock()
	defer sstm.lock.Unlock()
	// load sstable A
	sstA, err := OpenSSTable(sstm.base, iA)
	if err != nil {
		return err
	}
	// and sstable B
	sstB, err := OpenSSTable(sstm.base, iB)
	if err != nil {
		return err
	}
	// make batch to write data to
	batch := NewBatch()
	// pass tables to the merge writer
	err = mergeWriter(sstA, sstB, batch)
	if err != nil {
		return err
	}
	// close table A
	err = sstA.Close()
	if err != nil {
		return err
	}
	// close table B
	err = sstB.Close()
	if err != nil {
		return err
	}
	// open new sstable to write to
	sstC, err := OpenSSTable(sstm.base, iB+1)
	if err != nil {
		return err
	}
	// write batch to table
	err = sstC.WriteBatch(batch)
	// flush and close sstable
	err = sstC.Close()
	if err != nil {
		return err
	}
	return nil
}

func (sstm *SSTManager) Close() error {
	// TODO: implement...
	return nil
}

func mergeWriter(sstA, sstB *SSTable, batch *Batch) error {

	i, j := 0, 0
	n1, n2 := sstA.index.Len(), sstB.index.Len()

	var err error
	var de *binary.Entry
	for i < n1 && j < n2 {
		if bytes.Compare(sstA.index.data[i].Key, sstB.index.data[j].Key) == 0 {
			// read entry from sstB
			de, err = sstB.ReadAt(sstB.index.data[j].Offset)
			if err != nil {
				return err
			}
			// write entry to batch
			batch.WriteEntry(de)
			i++
			j++
			continue
		}
		if bytes.Compare(sstA.index.data[i].Key, sstB.index.data[j].Key) == -1 {
			// read entry from sstA
			de, err = sstA.ReadAt(sstA.index.data[i].Offset)
			if err != nil {
				return err
			}
			// write entry to batch
			batch.WriteEntry(de)
			i++
			continue
		}
		if bytes.Compare(sstB.index.data[j].Key, sstA.index.data[i].Key) == -1 {
			// read entry from sstB
			de, err = sstB.ReadAt(sstB.index.data[j].Offset)
			if err != nil {
				return err
			}
			// write entry to batch
			batch.WriteEntry(de)
			j++
			continue
		}
	}

	// print remaining
	for i < n1 {
		// read entry from sstA
		de, err = sstA.ReadAt(sstA.index.data[i].Offset)
		if err != nil {
			return err
		}
		// write entry to batch
		batch.WriteEntry(de)
		i++
	}

	// print remaining
	for j < n2 {
		// read entry from sstB
		de, err = sstB.ReadAt(sstB.index.data[j].Offset)
		if err != nil {
			return err
		}
		// write entry to batch
		batch.WriteEntry(de)
		j++
	}

	// return error free
	return nil
}