package sstable

import (
	"bytes"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/memtable"
	"github.com/scottcagno/storage/pkg/lsmt/trees/rbtree"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type SSTManager2 struct {
	lock        sync.RWMutex
	base        string
	sequence    int64
	sparseIndex *rbtree.RBTree
}

// go over all the files

func OpenSSTManager2(base string) (*SSTManager2, error) {
	// make sure we are working with absolute paths
	base, err := filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	// sanitize any path separators
	base = filepath.ToSlash(base)
	// create ss-table-manager instance
	sstm := &SSTManager2{
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
		// generate sparse index set from the ss-index
		sparseSet, err := ssi.GenerateSparseIndexSet()
		if err != nil {
			return nil, err
		}
		// iterate the sparse set
		for i := range sparseSet {
			// create a new sparse index entry
			ie := sparseIndexEntry{
				SSTIndex:   index,
				IndexEntry: sparseSet[i],
			}
			// add each entry to the sparse index
			sstm.sparseIndex.Put(ie)
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
	}
	return sstm, nil
}

/*
func (spi *SparseIndex) Search(k string) (int64, int64) {
	v, _ := spi.rbt.GetNearMin(sparseIndexEntry{Key: k})
	return v.(sparseIndexEntry).Path, v.(sparseIndexEntry).Index.Offset
}
*/

func (sstm *SSTManager2) FlushMemtableToSSTable(mt *memtable.Memtable) error {
	// TODO: implement me
	return nil
}

func (sstm *SSTManager2) Search(k string) (*binary.Entry, error) {
	// TODO: implement me
	return nil, nil
}

func (sstm *SSTManager2) CompactSSTables(index int64) error {
	// TODO: implement me
	return nil
}

func (sstm *SSTManager2) MergeSSTables(iA, iB int64) error {
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
	return nil
}

func (sstm *SSTManager2) Close() error {
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
