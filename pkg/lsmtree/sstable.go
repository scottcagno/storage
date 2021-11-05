package lsmtree

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	filePrefix      = "sst-"
	dataFileSuffix  = ".dat"
	indexFileSuffix = ".idx"
)

func levelToDir(level int) string {
	return fmt.Sprintf("level-%d", level)
}

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

func createLevel0Tables(path string, batch *Batch) error {
	return nil
}

// createSSAndIndexTables creates a new ss-table and ss-table-index using
// the provided entry batch, and returns nil on success.
func createSSAndIndexTables(base string, memt *rbTree) error {
	// sanitize base path
	path, err := initBasePath(filepath.Join(base, levelToDir(0)))
	if err != nil {
		return err
	}
	// read the base dir for this level
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	// init seq
	var seq int64
	// count the files to get the sequence number
	for _, file := range files {
		// if the file is a sst-table data file, increment
		if !file.IsDir() && strings.HasSuffix(file.Name(), dataFileSuffix) {
			seq++
		}
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
	// range mem-table and write entries and indexes
	memt.rangeFront(func(e *Entry) bool {
		// write entry to data file
		offset, err := writeEntry(dataFile, e)
		if err != nil {
			// for now, just panic
			panic(err)
		}
		// write index to index file
		_, err = writeIndex(indexFile, &Index{
			Key:    e.Key,
			Offset: offset,
		})
		if err != nil {
			// for now, just panic
			panic(err)
		}
		return true
	})
	// sync data file
	err = dataFile.Sync()
	if err != nil {
		return err
	}
	// sync index file
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

func getLevelFromSize(size int64) int {
	switch {
	case size > 0<<20 && size < 1<<21: // level-0	(2 MB) max=4
		return 0
	case size > 1<<22 && size < 1<<23: // level-1   (8 MB) max=4
		return 1
	case size > 1<<24 && size < 1<<25: // level-2  (32 MB) max=4
		return 2
	case size > 1<<26 && size < 1<<27: // level-3 (128 MB) max=4
		return 3
	default:
		return 4 // oddballs that will need gc for sure
	}
}
