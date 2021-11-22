package lsmtree

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type ssTableIndex struct {
	first []byte
	last  []byte
	count int
	data  []*Index
}

func newSSTableIndex(index []*Index) *ssTableIndex {
	if index == nil || len(index) < 1 {
		return &ssTableIndex{
			first: nil,
			last:  nil,
			count: 0,
			data:  make([]*Index, 0),
		}
	}
	return &ssTableIndex{
		first: index[0].Key,
		last:  index[len(index)-1].Key,
		count: len(index),
		data:  index,
	}
}

func (ssti *ssTableIndex) Len() int {
	return len(ssti.data)
}

func (ssti *ssTableIndex) close() {
	ssti.first = nil
	ssti.last = nil
	ssti.count = 0
	for i := range ssti.data {
		ssti.data[i] = nil
	}
	ssti.data = nil
}

type ssTable struct {
	path  string
	fd    *os.File
	index *ssTableIndex
}

func createSSTable(dir string, memt *rbTree) error {
	// create level-0 path for newly flushed ss-tables
	path := filepath.Join(dir, levelToDir(0))
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
	// create a new data file
	dataFile, err := openDataFile(path, seq, os.O_CREATE|os.O_WRONLY)
	// get data file name
	//dataFileName := filepath.Join(path, toDataFileName(seq))
	// open data file
	//dataFile, err := os.OpenFile(dataFileName, os.O_CREATE|os.O_RDWR, 0666)
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

	// create a new index file
	indexFile, err := openIndexFile(path, seq, os.O_CREATE|os.O_WRONLY)
	// get index file name
	//indexFileName := filepath.Join(path, toIndexFileName(seq))
	// open index file
	//indexFile, err := os.OpenFile(indexFileName, os.O_CREATE|os.O_RDWR, 0666)
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

func openSSTable(path string, seq int64) (*ssTable, error) {
	// open index file
	indexFile, err := openIndexFile(path, seq, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	// create an index set
	var index []*Index
	// load up the ss-table-index entries
	for {
		// read index entry from the index file
		i, err := readIndex(indexFile)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			// make sure we close!
			err = indexFile.Close()
			if err != nil {
				return nil, err
			}
			return nil, err
		}
		// add index to the index set
		index = append(index, i)
	}
	// close index file
	err = indexFile.Close()
	if err != nil {
		return nil, err
	}
	// make ss-table instance to return
	sst := &ssTable{
		path:  toDataFileName(seq),
		fd:    nil,
		index: newSSTableIndex(index),
	}
	// return ss-table instance
	return sst, nil
}

func isBetween(lo, key, hi []byte) bool {
	return bytes.Compare(lo, key) <= 0 && bytes.Compare(hi, key) >= 0
}

func (sst *ssTable) keyInRange(key []byte) bool {
	// error check
	if key == nil {
		return false
	}
	// return boolean reporting key being between the lo and hi values
	return isBetween(sst.index.first, key, sst.index.last)
}

func searchInSSTables(base string, key []byte) (*Entry, error) {
	// read the base dir for this level
	dirs, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	// iterate dirs
	for _, dir := range dirs {
		// skip anything that is not a directory
		if !dir.IsDir() {
			continue
		}
		// now let us read the files within this level
		files, err := os.ReadDir(dir.Name())
		if err != nil {
			return nil, err
		}
		// visit each file
		for _, file := range files {
			// if the file is not a ss-table data file, continue
			if file.IsDir() || !strings.HasSuffix(file.Name(), dataFileSuffix) {
				continue // skip to the next file
			}
			// get the sequence from the data file name
			seq, err := fromDataFileName(file.Name())
			if err != nil {
				return nil, err
			}
			// if the file is a ss-table, open it
			sst, err := openSSTable(dir.Name(), seq)
			if err != nil {
				return nil, err
			}
			// perform prelim check to see if the provided
			// key may fall in the range of this table
			if ok := sst.keyInRange(key); !ok {
				// if the key is not in the range, we can
				// skip to the next table straight away
				continue
			}
			// if the key does fall in the range than there
			// is a very high chance that it will be found
			// within this table. perform a search on the
			// ss-table for the provided key and return
			e, err := searchSSTable(sst.path, key)
			if err != nil {
				return nil, err
			}
			// check and return found entry
			if e != nil && !e.hasTombstone() {
				return e, nil
			}
		}
	}
	return nil, ErrNotFound
}

func searchSSTable(dir string, key []byte) (*Entry, error) {
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

func (sst *ssTable) ReadAt(offset int64) (*Entry, error) {
	// error check
	if sst.fd == nil {
		return nil, ErrFileClosed
	}
	// use offset to read entry
	e, err := readEntryAt(sst.fd, offset)
	if err != nil {
		return nil, err
	}
	// make sure entry checksum is good
	err = checkCRC(e, checksum(append(e.Key, e.Value...)))
	if err != nil {
		return nil, err
	}
	// return entry
	return e, nil
}

/*
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
*/
