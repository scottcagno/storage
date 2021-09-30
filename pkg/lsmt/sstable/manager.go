package sstable

import (
	"bytes"
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/memtable"
	"github.com/scottcagno/storage/pkg/lsmt/rbtree"
	"os"
	"strings"
)

const (
	filePrefix      = "sst-"
	dataFileSuffix  = ".dat"
	indexFileSuffix = ".idx"
)

var Tombstone = []byte(nil)

type KeyPair struct {
	index int64
	first string
	last  string
}

func (kp *KeyPair) KeyBetween(k string) bool {
	return kp.first <= k && k <= kp.last
}

func (kp *KeyPair) String() string {
	return fmt.Sprintf("kp.index=%d, kp.first=%q, kp.last=%q", kp.index, kp.first, kp.last)
}

type SSTManager struct {
	base string
	//sparse []*SparseIndex
	sparse []*KeyPair
	index  int64
}

func OpenSSTManager(base string) (*SSTManager, error) {
	sstm := &SSTManager{
		base:   base,
		sparse: make([]*KeyPair, 0),
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
		// open index
		ssi, err := OpenSSTIndex(sstm.base, index)
		if err != nil {
			return nil, err
		}
		kp := &KeyPair{
			index: index,
			first: ssi.first,
			last:  ssi.last,
		}
		sstm.sparse = append(sstm.sparse, kp)
		// close index
		err = ssi.Close()
		if err != nil {
			return nil, err
		}
	}
	sstm.index = sstm.sparse[len(sstm.sparse)-1].index
	return sstm, nil
}

func (sstm *SSTManager) FlushMemtable(memt *memtable.Memtable) error {
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
	sst, err := OpenSSTable(sstm.base, sstm.index+1)
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
	// in the clear, increment index
	sstm.index++
	// add new entry to sparse index
	sstm.sparse = append(sstm.sparse, &KeyPair{index: sstm.index, first: first, last: last})
	// return
	return nil
}

/*
func _openSSTManager(base string) (*SSTManager, error) {
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
*/

func (sstm *SSTManager) ListSSTables() []string {
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

func (sstm *SSTManager) NewBatch() *Batch {
	return new(Batch)
}

func (sstm *SSTManager) WriteEntry(e *binary.Entry) error {
	// TODO: implement...
	return nil
}

func (sstm *SSTManager) WriteBatch(batch *Batch) error {
	// TODO: implement...
	return nil
}

func (sstm *SSTManager) SearchSparseIndex(k string) (int64, error) {
	for _, kp := range sstm.sparse {
		if !kp.KeyBetween(k) {
			continue
		}
		return kp.index, nil
	}
	return -1, ErrSSTIndexNotFound
}

func (sstm *SSTManager) GetSparseIndex() []*KeyPair {
	return sstm.sparse
}

/*
func (sstm *SSTManager) SearchSparseIndex(k string) (*binary.Entry, error) {
	var path string
	var offset int64
	for _, index := range sstm.sparse {
		if index.HasKey(k) {
			path, offset = index.Search(k)
			break
		}
	}
	// error check
	if path == "" || offset == -1 {
		return nil, errors.New("sstable: not found")
	}
	// get base and index from path
	base := filepath.Base(path)
	index, err := IndexFromDataFileName(base)
	if err != nil {
		return nil, err
	}
	// open sstable
	sst, err := OpenSSTable(base, index)
	if err != nil {
		return nil, err
	}
	// read entry
	e, err := sst.ReadAt(offset)
	if err != nil {
		return nil, err
	}
	// close sstable
	err = sst.Close()
	if err != nil {
		return nil, err
	}
	// return entry
	return e, nil
}
*/

func (sstm *SSTManager) Close() error {
	// TODO: implement...
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
