package sstable

import (
	"bytes"
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/memtable"
	"github.com/scottcagno/storage/pkg/lsmt/trees/rbtree"
	"log"
	"os"
	"path/filepath"
	"sort"
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

type KeyRangeSlice []*KeyRange

func (krs KeyRangeSlice) Len() int {
	return len(krs)
}

func (krs KeyRangeSlice) Less(i, j int) bool {
	return krs[i].first < krs[j].first
}

func (krs KeyRangeSlice) Swap(i, j int) {
	krs[i], krs[j] = krs[j], krs[i]
}

type SSTManager struct {
	lock    sync.RWMutex
	base    string
	inrange []*KeyRange
	//sparse    map[int64]*SparseIndex
	gindex    int64
	cachedSST *SSTable
}

// https://play.golang.org/p/m_cJtw4wWMc

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
		base:    base,
		inrange: make([]*KeyRange, 0),
		//sparse:  make(map[int64]*SparseIndex, 0),
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
		// add it to our key in-range index
		sstm.inrange = append(sstm.inrange, kr)
		// populate sparse index
		//sstm.sparse[index] = makeNewSparseIndex(index, ssi)
		// close gindex
		err = ssi.Close()
		if err != nil {
			return nil, err
		}
	}
	// update the last global gindex
	sstm.gindex = sstm.getLastGIndex()

	//log.Println(sstm.inrange, len(sstm.inrange), sstm.gindex)

	return sstm, nil
}

func (sstm *SSTManager) getLastGIndex() int64 {
	if len(sstm.inrange) == 0 {
		return 0
	}
	return sstm.inrange[len(sstm.inrange)-1].index
}

func (sstm *SSTManager) addKeyRange(first, last string) {
	kr := &KeyRange{index: sstm.gindex, first: first, last: last}
	sstm.inrange = append(sstm.inrange, kr)
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

func (sstm *SSTManager) NewBatch() *binary.Batch {
	return new(binary.Batch)
}

func (sstm *SSTManager) FlushBatchToSSTable(batch *binary.Batch) error {
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
	// add new sparse index
	//sstm.sparse[sstm.gindex+1] = makeNewSparseIndex(sstm.gindex+1, sst.index)
	// flush and close ss-table
	err = sst.Close()
	if err != nil {
		return err
	}
	// in the clear, increment gindex
	sstm.gindex++
	// add new entry to key in-range index
	sstm.addKeyRange(first, last)
	return nil
}

func (sstm *SSTManager) isInRange(k string) (int64, error) { //(*SparseIndex, error) {
	if len(sstm.inrange) == 1 {
		return sstm.getLastGIndex(), nil
	}
	keys := KeyRangeSlice(sstm.inrange)
	sort.Sort(keys)
	n := sort.Search(keys.Len(),
		func(i int) bool {
			return sstm.inrange[i].first <= k && k <= sstm.inrange[i].last
		})
	log.Println("DEBUG >> N=", n, len(sstm.inrange))
	if n < 0 {
		return -1, ErrSSTIndexNotFound
	}

	//	if i < len(data) && data[i] == x {
	//		// x is present at data[i]
	//	} else {
	//		// x is not present in data,
	//		// but i is the index where it would be inserted.
	//	}

	//for _, kr := range sstm.inrange {
	//	if !kr.InKeyRange(k) {
	//		continue
	//	}
	//	return kr.index, nil
	//spi, ok := sstm.sparse[kr.index]
	//if !ok {
	//	continue
	//}
	//return spi, nil
	//}
	return int64(n), nil
}

func (sstm *SSTManager) Get(k string) (*binary.Entry, error) {
	// read lock
	sstm.lock.RLock()
	defer sstm.lock.RUnlock()
	// search sparse index
	index, err := sstm.isInRange(k)
	if err != nil {
		return nil, err
	}
	if index == -1 {
		return nil, ErrSSTIndexNotFound
	}
	// get table path index, and relative offset
	//index, offset := spi.Search(k)
	// open ss-table for reading
	sst, err := OpenSSTable(sstm.base, index)
	if err != nil {
		return nil, err
	}
	// scan starting at location until we find match
	//var de *binary.Entry
	//err = sst.ScanAt(offset, func(e *binary.Entry) bool {
	//	if string(e.Key) == k {
	//		de = e
	//		// got match, lets break
	//		return false
	//	}
	//	return true
	//})
	de, err := sst.Read(k)
	if err != nil {
		return nil, err
	}
	// close ss-table
	err = sst.Close()
	if err != nil {
		return nil, err
	}
	// return entry
	return de, nil
}

func (sstm *SSTManager) GetEntryIndex(k string) (*binary.Index, error) {
	// read lock
	sstm.lock.RLock()
	defer sstm.lock.RUnlock()
	// search sparse index
	index, err := sstm.isInRange(k)
	if err != nil {
		return nil, err
	}
	if index == -1 {
		return nil, ErrSSTIndexNotFound
	}
	// open ss-table-index for reading
	sst, err := OpenSSTable(sstm.base, index)
	if err != nil {
		return nil, err
	}
	// read index data
	di, err := sst.ReadIndex(k)
	if err != nil {
		return nil, err
	}
	// close ss-table
	err = sst.Close()
	if err != nil {
		return nil, err
	}
	// return entry
	return di, nil
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
	batch := binary.NewBatch()
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
	batch := binary.NewBatch()
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

	return nil
}

func mergeWriter(sstA, sstB *SSTable, batch *binary.Batch) error {

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
