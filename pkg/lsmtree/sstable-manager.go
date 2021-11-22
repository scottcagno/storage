package lsmtree

import (
	"os"
	"path/filepath"
	"strings"
)

type indexKey struct {
	level int
	key   string
}

type indexVal struct {
	path   string
	offset int64
}

type ssTableManager struct {
	baseDir  string
	index    map[indexKey]indexVal
	level    map[int]int
	sstcount int
}

func openSSTableManager(base string) (*ssTableManager, error) {
	// sanitize base path
	path, err := initBasePath(base)
	if err != nil {
		return nil, err
	}
	// create new ss-table-manager to return
	sstm := &ssTableManager{
		baseDir: path,
		index:   make(map[indexKey]indexVal),
		level:   make(map[int]int),
	}
	// initialize
	err = sstm.load()
	if err != nil {
		return nil, err
	}
	return sstm, nil
}

func (sstm *ssTableManager) load() error {
	// read the base dir for this level
	dirs, err := os.ReadDir(sstm.baseDir)
	if err != nil {
		return err
	}
	// iterate dirs
	for _, dir := range dirs {
		// skip anything that is not a directory
		if !dir.IsDir() {
			continue
		}
		// get level
		level, err := dirToLevel(dir.Name())
		if err != nil {
			return err
		}
		// add level to levels
		if _, ok := sstm.level[level]; !ok {
			sstm.level[level] = 0
		}
		// now let us add the file count within those levels
		files, err := os.ReadDir(dir.Name())
		if err != nil {
			return err
		}
		// count the files
		for _, file := range files {
			// if the file is a sst-table data file, increment
			if !file.IsDir() && strings.HasSuffix(file.Name(), dataFileSuffix) {
				sstm.level[level]++
				sstm.sstcount++
			}
		}
	}
	return nil
}

// createSSAndIndexTables creates a new ss-table and ss-table-index using
// the provided entry batch, and returns nil on success.
func (sstm *ssTableManager) createSSAndIndexTables(memt *rbTree) error {
	// create level-0 path for newly flushed ss-tables
	path := filepath.Join(sstm.baseDir, levelToDir(0))
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

func (sstm *ssTableManager) get(e *Entry) (*Entry, error) {
	return nil, nil
}
