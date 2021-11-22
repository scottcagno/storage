package lsmtree

import (
	"bytes"
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

func dirToLevel(dir string) (int, error) {
	return strconv.Atoi(strings.Split(dir, "-")[1])
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
	name = filepath.Base(name)
	hexa := name[len(filePrefix) : len(name)-len(dataFileSuffix)]
	return strconv.ParseInt(hexa, 16, 32)
}

func fromIndexFileName(name string) (int64, error) {
	name = filepath.Base(name)
	hexa := name[len(filePrefix) : len(name)-len(indexFileSuffix)]
	return strconv.ParseInt(hexa, 16, 32)
}

// openDataFile opens the ss-table data file (read only)
func openDataFile(path string, seq int64, flag int) (*os.File, error) {
	// get data file name
	dataFileName := filepath.Join(path, toDataFileName(seq))
	// open data file
	dataFile, err := os.OpenFile(dataFileName, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	// remember to close
	//defer func(dataFile *os.File) {
	//	err := dataFile.Close()
	//	if err != nil {
	//		panic("closing dataFile: " + err.Error())
	//	}
	//}(dataFile)

	// return data file
	return dataFile, nil
}

// openIndexFile opens the ss-table index file (read only)
func openIndexFile(path string, seq int64, flag int) (*os.File, error) {
	// get index file name
	indexFileName := filepath.Join(path, toIndexFileName(seq))
	// open index file
	indexFile, err := os.OpenFile(indexFileName, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	// remember to close
	//defer func(indexFile *os.File) {
	//	err := indexFile.Close()
	//	if err != nil {
	//		panic("closing indexFile: " + err.Error())
	//	}
	//}(indexFile)

	// return index file
	return indexFile, nil
}

func compactSSTable(path string) error {
	// TODO: implement me...
	// lock and defer unlock
	// open and load ss-table
	// make new ss-table
	// iterate old ss-table and write entries to new ss-table (not adding tombstone entries to batch)
	// flush new ss-table
	// close both ss-tables
	// remove old ss-table (and index)
	return nil
}

func mergeSSTables(sstA, sstB *ssTable, batch *Batch) error {

	i, j := 0, 0
	n1, n2 := sstA.index.Len(), sstB.index.Len()

	var err error
	var de *Entry
	for i < n1 && j < n2 {
		if bytes.Compare(sstA.index.data[i].Key, sstB.index.data[j].Key) == 0 {
			// read entry from sstB
			de, err = sstB.ReadAt(sstB.index.data[j].Offset)
			if err != nil {
				return err
			}
			// write entry to batch
			err = batch.writeEntry(de)
			if err != nil {
				return err
			}
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
			err = batch.writeEntry(de)
			if err != nil {
				return err
			}
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
			err = batch.writeEntry(de)
			if err != nil {
				return err
			}
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
		err = batch.writeEntry(de)
		if err != nil {
			return err
		}
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
		err = batch.writeEntry(de)
		if err != nil {
			return err
		}
		j++
	}

	// return error free
	return nil
}
