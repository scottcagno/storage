package sstable

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// https: //play.golang.org/p/jRpPRa4Q4Nh
// https://play.golang.org/p/hTuTKen_ovK

type SSManager struct {
	base   string
	sparse []*SparseIndex
	gidx   int64
}

func OpenSSManager(base string) (*SSManager, error) {
	// make sure we are working with absolute paths
	//base, err := filepath.Abs(base)
	//if err != nil {
	//	return nil, err
	//}
	// sanitize any path separators
	base = filepath.ToSlash(base)
	ssm := &SSManager{
		base:   base,
		sparse: make([]*SparseIndex, 0),
		gidx:   -1,
	}
	ssm.gidx = ssm.GetLatestIndex()
	// load sparse index
	idxs, err := ssm.AllIndexes()
	if err != nil {
		return nil, err
	}
	log.Println("DEBUG1")
	for i := range idxs {
		spi, err := OpenSparseIndex(ssm.base, idxs[i])
		if err != nil {
			return nil, err
		}
		ssm.sparse = append(ssm.sparse, spi)
	}
	return ssm, nil
}

func (ssm *SSManager) AllIndexes() ([]int64, error) {
	ssts, err := ssm.ListSSTables()
	if err != nil {
		return nil, ErrSSTableNotFound
	}
	if len(ssts) == 0 {
		return nil, ErrSSTableNotFound
	}
	sort.Strings(ssts)
	var nn []int64
	for i := range ssts {
		index, err := IndexFromDataFileName(filepath.Base(ssts[i]))
		if err != nil {
			return nil, err
		}
		nn = append(nn, index)
	}
	return nn, nil
}

func (ssm *SSManager) GetLatestIndex() int64 {
	ssts, err := ssm.ListSSTables()
	if err != nil {
		return 0
	}
	if len(ssts) == 0 {
		return 0
	}
	sort.Strings(ssts)
	index, err := IndexFromDataFileName(filepath.Base(ssts[len(ssts)-1]))
	if err != nil {
		return 0
	}
	return index
}

func (ssm *SSManager) SearchSparseIndex(key string) (string, int64) {
	var path string
	var offset int64
	for i := range ssm.sparse {
		if !ssm.sparse[i].HasKey(key) {
			continue
		}
		path, offset = ssm.sparse[i].GetClose(key)
		break
	}
	if offset == -1 {

	}
	return path, offset
}

func (ssm *SSManager) Get(key string) ([]byte, error) {
	ssts, err := ssm.ListSSTables()
	if err != nil {
		return nil, err
	}
	var indexes []int
	for _, name := range ssts {
		index, err := IndexFromDataFileName(filepath.Base(name))
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, int(index))
	}
	sort.Ints(indexes)
	var de *sstDataEntry
	// TODO: find better way to do this
	for i := len(indexes) - 1; i > 0; i-- {
		sst, err := OpenSSTable(ssm.base, int64(indexes[i]))
		if err != nil {
			return nil, err
		}
		de, err = sst.ReadEntry(key)
		if de != nil {
			break
		}
		err = sst.Close()
		if err != nil {
			return nil, err
		}
	}
	return de.value, nil
}

func (ssm *SSManager) CompactSSTables(index int64) error {
	// load sstable
	sst, err := OpenSSTable(ssm.base, index)
	if err != nil {
		return err
	}
	// make batch
	batch := NewBatch()
	// iterate
	err = sst.Scan(func(de *sstDataEntry) bool {
		// add any data entries that are not tombstones to batch
		if de.value != nil && !bytes.Equal(de.value, TombstoneEntry) {
			batch.WriteDataEntry(de)
		}
		return true
	})
	if err != nil {
		return err
	}
	// get path
	tpath, ipath := sst.SSTablePath(), sst.SSIndexPath()
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
	sst, err = CreateSSTable(ssm.base, index)
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

func (ssm *SSManager) MergeSSTables(iA, iB int64) error {
	// load sstable A
	sstA, err := OpenSSTable(ssm.base, iA)
	if err != nil {
		return err
	}
	// and sstable B
	sstB, err := OpenSSTable(ssm.base, iB)
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
	sstC, err := CreateSSTable(ssm.base, iB+1)
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
	var de *sstDataEntry
	for i < n1 && j < n2 {
		if sstA.index.data[i].key == sstB.index.data[j].key {
			// read entry from sstB
			de, err = sstB.ReadEntryAt(sstB.index.data[j].offset)
			if err != nil {
				return err
			}
			// write entry to batch
			batch.WriteDataEntry(de)
			i++
			j++
			continue
		}
		if sstA.index.data[i].key < sstB.index.data[j].key {
			// read entry from sstA
			de, err = sstA.ReadEntryAt(sstA.index.data[i].offset)
			if err != nil {
				return err
			}
			// write entry to batch
			batch.WriteDataEntry(de)
			i++
			continue
		}
		if sstB.index.data[j].key < sstA.index.data[i].key {
			// read entry from sstB
			de, err = sstB.ReadEntryAt(sstB.index.data[j].offset)
			if err != nil {
				return err
			}
			// write entry to batch
			batch.WriteDataEntry(de)
			j++
			continue
		}
	}

	// print remaining
	for i < n1 {
		// read entry from sstA
		de, err = sstA.ReadEntryAt(sstA.index.data[i].offset)
		if err != nil {
			return err
		}
		// write entry to batch
		batch.WriteDataEntry(de)
		i++
	}

	// print remaining
	for j < n2 {
		// read entry from sstB
		de, err = sstB.ReadEntryAt(sstB.index.data[j].offset)
		if err != nil {
			return err
		}
		// write entry to batch
		batch.WriteDataEntry(de)
		j++
	}

	// return error free
	return nil
}

func (ssm *SSManager) ListSSTables() ([]string, error) {
	files, err := os.ReadDir(ssm.base)
	if err != nil {
		return nil, err
	}
	var ss []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if strings.HasSuffix(file.Name(), dataFileSuffix) {
			ss = append(ss, filepath.Join(ssm.base, file.Name()))
		}
	}
	return ss, nil
}

func (ssm *SSManager) ListSSIndexes() ([]string, error) {
	files, err := os.ReadDir(ssm.base)
	if err != nil {
		return nil, err
	}
	var ss []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if strings.HasSuffix(file.Name(), indexFileSuffix) {
			ss = append(ss, filepath.Join(ssm.base, file.Name()))
		}
	}
	return ss, nil
}

func (ssm *SSManager) Close() error {
	return nil
}
