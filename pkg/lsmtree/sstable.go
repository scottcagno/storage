package lsmtree

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	filePrefix      = "sst-"
	dataFileSuffix  = ".dat"
	indexFileSuffix = ".idx"
)

func toDataFileName(index int64) string {
	hexa := strconv.FormatInt(index, 16)
	return fmt.Sprintf("%s%010s%s", filePrefix, hexa, dataFileSuffix)
}

func toIndexFileName(index int64) string {
	hexa := strconv.FormatInt(index, 16)
	return fmt.Sprintf("%s%010s%s", filePrefix, hexa, indexFileSuffix)
}

func fromDataFileName(name string) (int64, error) {
	hexa := name[len(filePrefix) : len(name)-len(dataFileSuffix)]
	return strconv.ParseInt(hexa, 16, 32)
}

func fromIndexFileName(name string) (int64, error) {
	hexa := name[len(filePrefix) : len(name)-len(indexFileSuffix)]
	return strconv.ParseInt(hexa, 16, 32)
}

type ssTableIndex struct {
	first []byte
	last  []byte
	count int
	data  []*Index
}

type ssTable struct {
	path  string
	fd    *os.File
	index *ssTableIndex
}

// createSSAndIndexTables creates a new ss-table and ss-table-index using
// the provided entry batch, and returns nil on success.
func createSSAndIndexTables(path string, seq int64, batch *Batch) error {
	// error check
	if batch == nil {
		return ErrIncompleteSet
	}
	// make sure batch is sorted
	if !sort.IsSorted(batch) {
		// sort if not sorted
		sort.Stable(batch)
	}
	// get data file name
	dataFileName := filepath.Join(path, toDataFileName(seq))
	// open data file
	dataFile, err := os.OpenFile(dataFileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	// remember to close
	defer func(dataFile *os.File) {
		err := dataFile.Close()
		if err != nil {
			panic("closing dataFile: " + err.Error())
		}
	}(dataFile)
	// get index file name
	indexFileName := filepath.Join(path, toIndexFileName(seq))
	// open index file
	indexFile, err := os.OpenFile(indexFileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	// remember to close
	defer func(indexFile *os.File) {
		err := indexFile.Close()
		if err != nil {
			panic("closing indexFile: " + err.Error())
		}
	}(indexFile)
	// range batch and write entries and indexes
	for i := range batch.Entries {
		// entry
		e := batch.Entries[i]
		// write entry to data file
		offset, err := writeEntry(dataFile, e)
		if err != nil {
			return err
		}
		// write index to index file
		_, err = writeIndex(indexFile, &Index{
			Key:    e.Key,
			Offset: offset,
		})
		if err != nil {
			return err
		}
	}
	// sync files
	err = dataFile.Sync()
	if err != nil {
		return err
	}
	err = indexFile.Sync()
	if err != nil {
		return err
	}
	return nil
}

type ssTableManager struct {
	baseDir string
	seqnum  int64
}

func openSSTableManager(base string) (*ssTableManager, error) {
	sstm := &ssTableManager{
		baseDir: base,
	}
	return sstm, nil
}

func (sstm *ssTableManager) get(e *Entry) (*Entry, error) {
	return nil, nil
}

func (sstm *ssTableManager) flushToSSTable(memt *memTable) error {
	// create new batch
	batch := NewBatch()
	// iterate memtable adding to batch
	err := memt.scan(func(e *Entry) bool {
		err := batch.writeEntry(e)
		if err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	// reset memtable
	memt.table.reset()
	// write ss-table and ss-table index files
	err = createSSAndIndexTables(sstm.baseDir, sstm.seqnum, batch)
	if err != nil {
		return err
	}
	return nil
}
