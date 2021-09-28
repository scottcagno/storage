package sstable

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

func IndexFileNameFromIndex(index int64) string {
	hexa := strconv.FormatInt(index, 16)
	return fmt.Sprintf("%s%010s%s", filePrefix, hexa, indexFileSuffix)
}

func IndexFromIndexFileName(name string) (int64, error) {
	hexa := name[len(filePrefix) : len(name)-len(indexFileSuffix)]
	return strconv.ParseInt(hexa, 16, 32)
}

type SSTIndex struct {
	path  string
	file  *os.File
	open  bool
	first string
	last  string
	data  []*binary.Index
}

func OpenSSTIndex(base string, index int64) (*SSTIndex, error) {
	// make sure we are working with absolute paths
	base, err := filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	// sanitize any path separators
	base = filepath.ToSlash(base)
	// create new index file path
	path := filepath.Join(base, IndexFileNameFromIndex(index))
	// open (or create) index file
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	// init and return SSTIndex
	ssi := &SSTIndex{
		path: path,
		file: file,
		open: true,
	}
	// load sst data index info
	err = ssi.LoadSSIndexData()
	if err != nil {
		return nil, err
	}
	return ssi, nil
}

func (ssi *SSTIndex) LoadSSIndexData() error {
	// check to make sure file exists
	_, err := os.Stat(ssi.path)
	if os.IsNotExist(err) {
		return err
	}
	// open file to read header
	fd, err := os.OpenFile(ssi.path, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	// make sure we close!
	defer fd.Close()
	// read and decode index entries
	for {
		// decode next index entry
		i, err := binary.DecodeIndex(fd)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// add index entry to sst index
		ssi.data = append(ssi.data, i)
	}
	// update sst first and last and then return
	if len(ssi.data) > 0 {
		ssi.first = string(ssi.data[0].Key)
		ssi.last = string(ssi.data[len(ssi.data)-1].Key)
	}
	return nil
}

func (ssi *SSTIndex) errorCheckFileAndIndex() error {
	// make sure file is not closed
	if !ssi.open {
		return binary.ErrFileClosed
	}
	// make sure index is loaded
	if ssi.data == nil {
		err := ssi.LoadSSIndexData()
		if err != nil {
			return err
		}
	}
	return nil
}

func (ssi *SSTIndex) Write(key []byte, offset int64) error {
	// error check
	err := ssi.errorCheckFileAndIndex()
	if err != nil {
		return err
	}
	// create new index
	i := &binary.Index{Key: key, Offset: offset}
	// write entry info to index file
	_, err = binary.EncodeIndex(ssi.file, i)
	if err != nil {
		return err
	}
	// add to index
	ssi.data = append(ssi.data, i)
	// check last
	last := len(ssi.data) - 1
	if ssi.last != string(ssi.data[last].Key) {
		ssi.last = string(ssi.data[last].Key)
	}
	return nil
}

func (ssi *SSTIndex) searchDataIndex(key string) int {
	// declare for later
	i, j := 0, len(ssi.data)
	// otherwise, perform binary search
	for i < j {
		h := i + (j-i)/2
		if key >= string(ssi.data[h].Key) {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

func (ssi *SSTIndex) Find(key string) (*binary.Index, error) {
	// error check
	err := ssi.errorCheckFileAndIndex()
	if err != nil {
		return nil, err
	}
	// attempt to find key
	at := ssi.searchDataIndex(key)
	if at == -1 {
		return nil, ErrSSTIndexNotFound
	}
	// check index for entry offset
	i := ssi.data[at]
	if i == nil || i.Offset == -1 {
		return nil, ErrSSTIndexNotFound
	}
	// return data entry
	return i, nil
}

func (ssi *SSTIndex) Scan(iter func(k string, off int64) bool) {
	for n := range ssi.data {
		i := ssi.data[n]
		if !iter(string(i.Key), i.Offset) {
			continue
		}
	}
}

func (ssi *SSTIndex) Len() int {
	return len(ssi.data)
}

func (ssi *SSTIndex) Close() error {
	if !ssi.open {
		return nil
	}
	err := ssi.file.Sync()
	if err != nil {
		return err
	}
	err = ssi.file.Close()
	if err != nil {
		return err
	}
	ssi.open = false
	return nil
}
