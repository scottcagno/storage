package sstable

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

func IndexFileNameFromIndex(index int64) string {
	hexa := strconv.FormatInt(index, 16)
	return fmt.Sprintf("%s%010s%s", filePrefix, hexa, indexFileSuffix)
}

func IndexFromIndexFileName(name string) (int64, error) {
	hexa := name[len(filePrefix) : len(name)-len(indexFileSuffix)]
	return strconv.ParseInt(hexa, 16, 32)
}

type sstIndexEntry struct {
	key    string // key is a key
	offset int64  // offset is the offset of this data entry
}

func (i *sstIndexEntry) String() string {
	return fmt.Sprintf("sstIndexEntry.key=%q, sstIndexEntry.offset=%d", i.key, i.offset)
}

type SSIndex struct {
	lock  sync.RWMutex
	path  string           // base is the base path of the index file
	file  *os.File         // file is the index file, file descriptor
	open  bool             // reports if the file is open or closed
	first string           // first is the first key
	last  string           // last is the last key
	data  []*sstIndexEntry // data is the sstIndexEntry
}

func OpenSSIndex(base string, index int64) (*SSIndex, error) {
	// make sure we are working with absolute paths
	base, err := filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	// sanitize any path separators
	base = filepath.ToSlash(base)
	// create new index file path
	path := filepath.Join(base, IndexFileNameFromIndex(index))
	// check to make sure file exists
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		return nil, err
	}
	// open (or create) index file
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	// init and return SSIndex
	ssi := &SSIndex{
		path: path, // path is the full path of the index file
		file: file, // file is the index file, file descriptor
		open: true,
	}
	// load sst data index info
	err = LoadSSTableIndexData(ssi)
	if err != nil {
		return nil, err
	}
	return ssi, nil
}

func LoadSSTableIndexData(idx *SSIndex) error {
	// check to make sure file exists
	_, err := os.Stat(idx.path)
	if os.IsNotExist(err) {
		return err
	}
	// open file to read header
	fd, err := os.OpenFile(idx.path, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	// make sure we close!
	defer fd.Close()
	// read and decode index entries
	for {
		// decode next index entry
		ei, err := DecodeIndexEntry(fd)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// add index entry to sst index
		idx.data = append(idx.data, ei)
	}
	// update sst first and last and then return
	if len(idx.data) > 0 {
		idx.first = idx.data[0].key
		idx.last = idx.data[len(idx.data)-1].key
	}
	return nil
}

func (ssi *SSIndex) errorCheckFileAndIndex() error {
	// make sure file is not closed
	if !ssi.open {
		return ErrFileClosed
	}
	// make sure index is loaded
	if ssi.data == nil {
		err := LoadSSTableIndexData(ssi)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ssi *SSIndex) WriteEntry(key string, offset int64) error {
	// error check
	err := ssi.errorCheckFileAndIndex()
	if err != nil {
		return err
	}
	// create new index
	ie := &sstIndexEntry{key: key, offset: offset}
	// write entry info to index file
	_, err = EncodeIndexEntry(ssi.file, ie)
	if err != nil {
		return err
	}
	// add to index
	ssi.data = append(ssi.data, ie)
	// check last
	if ssi.last != ssi.lastKey() {
		ssi.last = ssi.lastKey()
	}
	return nil
}

func (ssi *SSIndex) searchDataIndex(key string) int {
	// declare for later
	i, j := 0, len(ssi.data)
	// otherwise, perform binary search
	for i < j {
		h := i + (j-i)/2
		if key >= ssi.data[h].key {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

func (ssi *SSIndex) GetEntryOffset(key string) (int64, error) {
	// if data index is not loaded, then load it
	if len(ssi.data) == 0 {
		err := LoadSSTableIndexData(ssi)
		if err != nil {
			return -1, err
		}
	}
	// try binary search
	ki := ssi.data[ssi.searchDataIndex(key)]
	// double check we have a match
	if ki.key == key {
		return ki.offset, nil
	}
	// otherwise, we return not found
	return -1, ErrNotFound
}

func (ssi *SSIndex) lastKey() string {
	return ssi.data[len(ssi.data)-1].key
}

func (ssi *SSIndex) Close() error {
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
	return nil
}
