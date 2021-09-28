package sstable

import (
	"bytes"
	"github.com/scottcagno/storage/pkg/lsmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"os"
	"strings"
)

const (
	filePrefix      = "sst-"
	dataFileSuffix  = ".dat"
	indexFileSuffix = ".idx"
)

type SSTManager struct {
	base   string
	sparse []*SparseIndex
}

func Open(base string) (*SSTManager, error) {
	sstm := &SSTManager{
		base:   base,
		sparse: make([]*SparseIndex, 0),
	}
	files, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), dataFileSuffix) {
			continue
		}
		index, err := IndexFromDataFileName(file.Name())
		if err != nil {
			return nil, err
		}
		spi, err := OpenSparseIndex(sstm.base, index)
		if err != nil {
			return nil, err
		}
		sstm.sparse = append(sstm.sparse, spi)
	}
	return sstm, nil
}

func (sstm *SSTManager) Get(k string) (string, int64) {
	for _, index := range sstm.sparse {
		if index.HasKey(k) {
			return index.Search(k)
		}
	}
	return "", -1
}

func (sstm *SSTManager) Close() error {
	// TODO: implement
	return nil
}

func (sstm *SSTManager) CompactSSTables(index int64) error {
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
		if e.Value != nil && !bytes.Equal(e.Value, lsmt.Tombstone) {
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
	// remove old index
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
