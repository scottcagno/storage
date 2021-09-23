package sstable

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

const (
	filePrefix      = "sst-"
	dataFileSuffix  = ".dat"
	indexFileSuffix = ".idx"
)

var (
	ErrFileClosed   = errors.New("error: file is closed")
	ErrNotFound     = errors.New("error: not found")
	ErrEmpty        = errors.New("error: empty")
	ErrNoTableIndex = errors.New("error: no table index")
)

func DataFileNameFromIndex(index int64) string {
	hexa := strconv.FormatInt(index, 16)
	return fmt.Sprintf("%s%010s%s", filePrefix, hexa, dataFileSuffix)
}

func IndexFromDataFileName(name string) (int64, error) {
	hexa := name[len(filePrefix) : len(name)-len(dataFileSuffix)]
	return strconv.ParseInt(hexa, 16, 32)
}

type sstDataEntry struct {
	key   string
	value []byte
}

func (e *sstDataEntry) String() string {
	return fmt.Sprintf("sstDataEntry.key=%q, sstDataEntry.value=%s", e.key, e.value)
}

type Batch struct {
	data []*sstDataEntry
}

func NewBatch() *Batch {
	return &Batch{
		data: make([]*sstDataEntry, 0),
	}
}

func (b *Batch) Write(key string, value []byte) {
	b.data = append(b.data, &sstDataEntry{key: key, value: value})
}

// Len [implementing sort interface]
func (b *Batch) Len() int {
	return len(b.data)
}

// Less [implementing sort interface]
func (b *Batch) Less(i, j int) bool {
	return b.data[i].key < b.data[j].key
}

// Swap [implementing sort interface]
func (b *Batch) Swap(i, j int) {
	b.data[i], b.data[j] = b.data[j], b.data[i]
}

type SSTable struct {
	lock  sync.RWMutex
	path  string   // path is the filepath for the data
	file  *os.File // file is the file descriptor for the data
	open  bool     // open reports the status of the file
	index *SSIndex // SSIndex is an SSTableIndex file

	readOnly bool
}

func CreateSSTable(base string, index int64) (*SSTable, error) {
	// make sure we are working with absolute paths
	base, err := filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	// sanitize any path separators
	base = filepath.ToSlash(base)
	// create any directories if they are not there
	err = os.MkdirAll(base, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// create new data file path
	path := filepath.Join(base, DataFileNameFromIndex(index))
	// check to make sure file doesn't exist
	_, err = os.Stat(path)
	if os.IsExist(err) {
		return nil, err
	}
	// create new data file
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	// init sstable index
	ssi, err := OpenSSIndex(base, index)
	if err != nil {
		return nil, err
	}
	// init and return SSTable
	sst := &SSTable{
		path:  path, // path is the filepath for the data
		file:  file, // file is the file descriptor for the data
		open:  true, // open reports the status of the file
		index: ssi,  // SSIndex is an SSTableIndex file
	}
	return sst, nil
}

func (sst *SSTable) errorCheckFileAndIndex() error {
	// make sure file is not closed
	if !sst.open {
		return ErrFileClosed
	}
	// make sure index is open
	if sst.index == nil {
		return ErrNoTableIndex
	}
	return nil
}

func (sst *SSTable) WriteEntry(de *sstDataEntry) error {
	// error check
	err := sst.errorCheckFileAndIndex()
	if err != nil {
		return err
	}
	// write entry to data file
	offset, err := EncodeDataEntry(sst.file, de)
	if err != nil {
		return err
	}
	// write entry to index
	err = sst.index.WriteEntry(de.key, offset)
	if err != nil {
		return err
	}
	return nil
}

func (sst *SSTable) WriteBatch(b *Batch) error {
	// error check
	err := sst.errorCheckFileAndIndex()
	if err != nil {
		return err
	}
	// error check batch
	if b == nil {
		return ErrEmpty
	}
	// check to see if batch is sorted
	if !sort.IsSorted(b) {
		// if not, sort
		sort.Stable(b)
	}
	// range batch and write
	for i := range b.data {
		// entry
		de := b.data[i]
		// write entry to data file
		offset, err := EncodeDataEntry(sst.file, de)
		if err != nil {
			return err
		}
		// write entry info to index file
		err = sst.index.WriteEntry(de.key, offset)
		if err != nil {
			return err
		}
	}
	return nil
}

func OpenSSTable(base string, index int64) (*SSTable, error) {
	// make sure we are working with absolute paths
	base, err := filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	// sanitize any path separators
	base = filepath.ToSlash(base)
	// create new data file path
	path := filepath.Join(base, DataFileNameFromIndex(index))
	// check to make sure file exists
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		return nil, err
	}
	// open data file
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	// init sstable index
	ssi, err := OpenSSIndex(base, index)
	if err != nil {
		return nil, err
	}
	// init and return SSTable
	sst := &SSTable{
		path:     path, // path is the filepath for the data
		file:     file, // file is the file descriptor for the data
		open:     true, // open reports the status of the file
		index:    ssi,  // SSIndex is an SSTableIndex file
		readOnly: true,
	}
	return sst, nil
}

func (sst *SSTable) BuildSSTableIndexData(rebuild bool) error {
	// error check
	err := sst.errorCheckFileAndIndex()
	if err != nil {
		return err
	}
	// make sure we are using correct path
	path := sst.index.path
	// check to see if an index file exists
	_, err = os.Stat(path)
	if os.IsExist(err) {
		if !rebuild {
			return err
		}
	}
	// otherwise, we told it to rebuild, so...
	// lets close the index file descriptor
	err = sst.index.Close()
	if err != nil {
		return err
	}
	// remove index file
	err = os.Remove(path)
	if err != nil {
		return err
	}
	// create a new index file
	index, err := IndexFromDataFileName(path)
	if err != nil {
		return err
	}
	sst.index, err = OpenSSIndex(filepath.Base(path), index)
	if err != nil {
		return err
	}
	// read and decode entries
	for {
		// decode next data entry
		de, err := DecodeDataEntry(sst.file)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// get offset of data file reader for index
		offset, err := sst.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		// write index entry to file
		err = sst.index.WriteEntry(de.key, offset)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sst *SSTable) Close() error {
	if sst.open {
		err := sst.file.Sync()
		if err != nil {
			return err
		}
		err = sst.file.Close()
		if err != nil {
			return err
		}
	}
	if sst.index != nil {
		err := sst.index.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
