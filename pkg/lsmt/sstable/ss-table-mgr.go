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

type spiEntry struct {
	Key        string
	SSTIndex   int64
	IndexEntry *binary.Index
}

func (r spiEntry) Compare(that rbtree.RBEntry) int {
	return strings.Compare(r.Key, that.(spiEntry).Key)
}

func (r spiEntry) Size() int {
	return len(r.Key) + 16
}

func (r spiEntry) String() string {
	return fmt.Sprintf("entry.LastKey=%q", r.Key)
}

type Int64Slice []int64

func (x Int64Slice) Len() int           { return len(x) }
func (x Int64Slice) Less(i, j int) bool { return x[i] < x[j] }
func (x Int64Slice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

type SSTManager struct {
	lock        sync.RWMutex
	base        string
	sequence    int64
	sparseIndex *rbtree.RBTree
	fileIndexes []int64
}

func OpenSSTManager(base string) (*SSTManager, error) {
	// make sure we are working with absolute paths
	base, err := filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	// sanitize any path separators
	base = filepath.ToSlash(base)
	// create base directory
	err = os.MkdirAll(base, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// create ss-table-manager instance
	sstm := &SSTManager{
		base:        base,
		sequence:    0,
		sparseIndex: rbtree.NewRBTree(),
	}
	// read the ss-table directory
	files, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	// lock
	sstm.lock.RLock()
	defer sstm.lock.RUnlock()
	// iterate over all the ss-index files
	for _, file := range files {
		// skip all non ss-index files
		if file.IsDir() || !strings.HasSuffix(file.Name(), indexFileSuffix) {
			continue
		}
		// get ss-index index number from file name
		index, err := IndexFromDataFileName(file.Name())
		if err != nil {
			return nil, err
		}
		// open the ss-index file
		ssi, err := OpenSSTIndex(sstm.base, index)
		if err != nil {
			return nil, err
		}
		// generate and populate sparse index
		err = sstm.AddSparseIndex(ssi)
		if err != nil {
			return nil, err
		}
		// close ss-index
		err = ssi.Close()
		if err != nil {
			return nil, err
		}
		// update the last sequence number
		if index > sstm.sequence {
			sstm.sequence = index
		}
		// add to list of file indexes
		sstm.fileIndexes = append(sstm.fileIndexes, index)
	}
	return sstm, nil
}

func (sstm *SSTManager) FlushMemtableToSSTable(mt *memtable.Memtable) error {
	// lock
	sstm.lock.Lock()
	defer sstm.lock.Unlock()
	// open new ss-table
	sst, err := OpenSSTable(sstm.base, sstm.sequence+1)
	if err != nil {
		return err
	}
	// iterate mem-table entries
	mt.Scan(func(me rbtree.RBEntry) bool {
		// and write each entry to the ss-table
		err = sst.Write(me.(memtable.MemtableEntry).Entry)
		if err != nil {
			log.Println(err)
			return false
		}
		return true
	})
	//// sync ss-table writes
	//err = sst.Sync()
	//if err != nil {
	//	return err
	//}
	// reset mem-table asap
	err = mt.Reset()
	if err != nil {
		return err
	}
	// add new entries to sparse index
	err = sstm.AddSparseIndex(sst.index)
	if err != nil {
		return err
	}
	// flush and close ss-table
	err = sst.Close()
	if err != nil {
		return err
	}
	// in the clear, increment sequence number
	sstm.sequence++
	// return
	return nil
}

func (sstm *SSTManager) FlushMemtableToSSTableOLD(mt *memtable.Memtable) error {
	// lock
	sstm.lock.Lock()
	defer sstm.lock.Unlock()
	// make new batch
	batch := binary.NewBatch()
	// iterate mem-table entries
	mt.Scan(func(me rbtree.RBEntry) bool {
		// and write each entry to the batch
		batch.WriteEntry(me.(memtable.MemtableEntry).Entry)
		return true
	})
	// open new ss-table
	sst, err := OpenSSTable(sstm.base, sstm.sequence+1)
	if err != nil {
		return err
	}
	// write batch to ss-table
	err = sst.WriteBatch(batch)
	if err != nil {
		return err
	}
	// add new entries to sparse index
	err = sstm.AddSparseIndex(sst.index)
	if err != nil {
		return err
	}
	// flush and close ss-table
	err = sst.Close()
	if err != nil {
		return err
	}
	// reset mem-table
	//err = mt.Reset()
	//if err != nil {
	//	return err
	//}
	// in the clear, increment sequence number
	sstm.sequence++
	// return
	return nil
}

func (sstm *SSTManager) FlushBatchToSSTable(batch *binary.Batch) error {
	// lock
	sstm.lock.Lock()
	defer sstm.lock.Unlock()
	// open new ss-table
	sst, err := OpenSSTable(sstm.base, sstm.sequence+1)
	if err != nil {
		return err
	}
	// write batch to ss-table
	err = sst.WriteBatch(batch)
	if err != nil {
		return err
	}
	// add new entries to sparse index
	err = sstm.AddSparseIndex(sst.index)
	if err != nil {
		return err
	}
	// flush and close ss-table
	err = sst.Close()
	if err != nil {
		return err
	}
	// in the clear, increment sequence
	sstm.sequence++
	// return, dummy
	return nil
}

// TODO: depricate this method, i think....
func (sstm *SSTManager) AddSparseIndex1(ssi *SSTIndex) error {
	// generate and return sparse index set from the ss-index
	sparseSet, err := ssi.GenerateAndGetSparseIndex()
	if err != nil {
		return err
	}
	// get index from ssi filename
	index, err := ssi.GetIndexNumber()
	if err != nil {
		return err
	}
	// iterate the sparse set
	for i := range sparseSet {
		// create a new sparse index entry
		ie := spiEntry{
			Key:        string(sparseSet[i].Key),
			SSTIndex:   index,
			IndexEntry: sparseSet[i],
		}
		// add each entry to the sparse index
		sstm.sparseIndex.Put(ie)
	}
	return nil
}

func (sstm *SSTManager) AddSparseIndex(ssi *SSTIndex) error {
	// generate sparse index and fill out/add to the supplied sparseIndex
	err := ssi.GenerateAndPutSparseIndex(sstm.sparseIndex)
	if err != nil {
		return err
	}
	return nil
}

func (sstm *SSTManager) SearchSparseIndex(k string) (int64, error) {
	e, err := sstm.searchSparseIndex(k)
	if err != nil {
		return e.SSTIndex, err
	}
	return e.SSTIndex, nil
}

func (sstm *SSTManager) searchSparseIndex(k string) (spiEntry, error) {
	// search "sparse index"
	e, nearMin, nearMax, exact := sstm.sparseIndex.GetApproxPrevNext(spiEntry{Key: k})
	if exact {
		// found exact entry
		return e.(spiEntry), nil
	}
	// check to see if key is greater than near max
	if nearMax == nil || k > nearMax.(spiEntry).Key {
		// note: nearMax should be nil if the key is out of range
		// key is greater than the near max, which
		// means it is not located in this table
		return spiEntry{SSTIndex: -1}, binary.ErrBadEntry
	}
	// if we get here, key is less than near max
	if nearMin != nil && k >= nearMin.(spiEntry).Key {
		// and key is greater than the near min which
		// means that it is most likely in this table
		//util.DEBUG("[nearMin] searchSparseIndex(%q) returning: entry found in near min\n", k)
		return nearMin.(spiEntry), nil
	}
	//util.DEBUG("[weird end stage] searchSparseIndex(%q) returning: error bad entry\n", k)
	// if we get here something bad happened?? this uaully
	// means a key was searched for that is less than the near
	// min or something that just doesn't compute well
	return spiEntry{SSTIndex: -1}, binary.ErrBadEntry
}

type scanDirection int

const (
	ScanOldToNew = 0
	ScanNewToOld = 1
)

func (sstm *SSTManager) Scan(direction scanDirection, iter func(e *binary.Entry) bool) error {
	if direction != ScanOldToNew && direction != ScanNewToOld {
		return ErrInvalidScanDirection
	}
	if direction == ScanNewToOld {
		// sort the ss-index files so the most recent ones are first
		sort.Sort(sort.Reverse(Int64Slice(sstm.fileIndexes)))
	}
	if direction == ScanOldToNew {
		// sort the ss-index files so the least recent ones are first
		sort.Sort(Int64Slice(sstm.fileIndexes))
	}
	// start iterating
	for _, index := range sstm.fileIndexes {
		// open the ss-table
		sst, err := OpenSSTable(sstm.base, index)
		if err != nil {
			return err
		}
		// scan the ss-table
		err = sst.Scan(iter)
		if err != nil {
			return err
		}
		// close the ss-table
		err = sst.Close()
		if err != nil {
			return nil
		}
	}
	return nil
}

func (sstm *SSTManager) LinearSearch(k string) *binary.Entry {
	// read lock
	sstm.lock.RLock()
	defer sstm.lock.RUnlock()
	// sort the ss-index files so the most recent ones are first
	sort.Sort(sort.Reverse(Int64Slice(sstm.fileIndexes)))
	// iterate the ss-index files (backward)
	for _, index := range sstm.fileIndexes {
		// open the ss-table
		sst, err := OpenSSTable(sstm.base, index)
		if err != nil {
			return nil
		}
		// perform binary search, attempt to
		// locate a matching entry
		de, err := sst.Read(k)
		if err != nil {
			return nil
		}
		// do not forget to close the ss-table
		err = sst.Close()
		if err != nil {
			return nil
		}
		// double check entry
		if de == nil {
			continue
		}
		// otherwise, return
		return de
	}
	return nil
}

func (sstm *SSTManager) CheckDeleteInSparseIndex(k string) {
	// lock
	sstm.lock.Lock()
	defer sstm.lock.Unlock()
	// make sparse index entry
	sie := spiEntry{Key: k}
	// search for exact key in sparse index
	if sstm.sparseIndex.Has(sie) {
		// remove key from sparse index
		sstm.sparseIndex.Del(sie)
	}
}

func (sstm *SSTManager) Search(k string) (*binary.Entry, error) {
	// read lock
	sstm.lock.RLock()
	defer sstm.lock.RUnlock()
	// search "sparse index"
	sie, err := sstm.searchSparseIndex(k)
	if err != nil {
		return nil, err
	}
	// open ss-table
	sst, err := OpenSSTable(sstm.base, sie.SSTIndex)
	if err != nil {
		return nil, err
	}
	// create an entry to return if we find a match
	matchedEntry := new(binary.Entry)
	// for key match at offset in spiEntry
	err = sst.ScanAt(sie.IndexEntry.Offset, func(e *binary.Entry) bool {
		if string(e.Key) == k {
			// we found our match, write data into matchedEntry
			matchedEntry = e
			return false // to stop scanning
		}
		return true // to keep scanning
	})
	// make sure to error check the scanner
	if err != nil {
		return nil, err
	}
	// close ss-table
	err = sst.Close()
	if err != nil {
		return nil, err
	}
	// double check matched entry
	if matchedEntry == nil {
		return nil, binary.ErrBadEntry
	}
	// entry might be tombstone?? maybe we should return anyway
	if matchedEntry.Value == nil {
		return nil, binary.ErrBadEntry
	}
	return matchedEntry, nil
}

func (sstm *SSTManager) CompactAllSSTables() error {
	for _, index := range sstm.fileIndexes {
		err := sstm.CompactSSTable(index)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sstm *SSTManager) CompactSSTable(index int64) error {
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
	// remove old ss-table
	err = os.Remove(tpath)
	if err != nil {
		return err
	}
	// remove old ss-index
	err = os.Remove(ipath)
	if err != nil {
		return err
	}
	// open new ss-table to write to
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

func (sstm *SSTManager) MergeAllSSTables(mergeThreshold int) error {
	// ensure there is an even number of tables
	// before attempting to merge--if not, just
	// silently return a nil error....
	if len(sstm.fileIndexes)%2 != 0 {
		// odd number of tables, difficult to merge
		return nil
	}
	// otherwise, we have an even number of tables
	// and could merge in theory--check threshold
	if len(sstm.fileIndexes) < mergeThreshold {
		// haven't reached the merge threshold
		// so, silently return a nil error
		return nil
	}
	// otherwise, start by sorting...
	sort.Sort(Int64Slice(sstm.fileIndexes))
	// then iterate and attempt to merge...
	for i := int(sstm.fileIndexes[0]); i < len(sstm.fileIndexes); i += 2 {
		// merging pair
		err := sstm.MergeSSTables(int64(i), int64(i+1))
		if err != nil {
			return err
		}
	}
	// everything merge successfully
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
	err = mergeTablesAndWriteToBatch(sstA, sstB, batch)
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
	// TODO: implement me
	return nil
}

func mergeTablesAndWriteToBatch(sstA, sstB *SSTable, batch *binary.Batch) error {

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
