package sstable

import (
	"fmt"
	"io"
	"log"
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

type sstIndexEntry struct {
	key    string // key is a key
	offset int64  // offset is the offset of this data entry
}

func (i *sstIndexEntry) String() string {
	return fmt.Sprintf("sstIndexEntry.key=%q, sstIndexEntry.offset=%d", i.key, i.offset)
}

type indexEntryMeta struct {
	path string         // filepath
	data *sstIndexEntry // index entry
}

type indexEntryMetaSet []*indexEntryMeta

// Len [implementing sort interface]
func (ie indexEntryMetaSet) Len() int {
	return len(ie)
}

// Less [implementing sort interface]
func (ie indexEntryMetaSet) Less(i, j int) bool {
	return ie[i].data.key < ie[j].data.key
}

// Swap [implementing sort interface]
func (ie indexEntryMetaSet) Swap(i, j int) {
	ie[i], ie[j] = ie[j], ie[i]
}

type SSIndex struct {
	//lock  sync.RWMutex
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
	err = ssi.LoadSSIndexData()
	if err != nil {
		return nil, err
	}
	return ssi, nil
}

func (ssi *SSIndex) LoadSSIndexData() error {
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
		ei, err := DecodeIndexEntry(fd)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// add index entry to sst index
		ssi.data = append(ssi.data, ei)
	}
	// update sst first and last and then return
	if len(ssi.data) > 0 {
		ssi.first = ssi.data[0].key
		ssi.last = ssi.data[len(ssi.data)-1].key
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
		err := ssi.LoadSSIndexData()
		if err != nil {
			return err
		}
	}
	return nil
}

func (ssi *SSIndex) WriteIndexEntry(key string, offset int64) error {
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

func (ssi *SSIndex) ReadDataEntry(r io.ReaderAt, key string) (*sstDataEntry, error) {
	// error check
	err := ssi.errorCheckFileAndIndex()
	if err != nil {
		return nil, err
	}
	// check index for entry offset
	offset, err := ssi.GetEntryOffset(key)
	if err != nil || offset == -1 {
		return nil, err
	}
	// attempt to read and decode data entry using provided reader
	de, err := DecodeDataEntryAt(r, offset)
	if err != nil {
		return nil, err
	}
	// return data entry
	return de, nil
}

func (ssi *SSIndex) ReadDataEntryAt(r io.ReaderAt, offset int64) (*sstDataEntry, error) {
	// error check
	err := ssi.errorCheckFileAndIndex()
	if err != nil {
		return nil, err
	}
	// attempt to read and decode data entry using provided reader at provided offset
	de, err := DecodeDataEntryAt(r, offset)
	if err != nil {
		return nil, err
	}
	// return data entry
	return de, nil
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
	if ssi.data == nil || len(ssi.data) == 0 {
		err := ssi.LoadSSIndexData()
		if err != nil {
			return -1, err
		}
	}

	// try binary search
	in := ssi.searchDataIndex(key)
	ki := ssi.data[in]
	log.Printf("get offset for key=%q, offset=%d\n", key, ki.offset)
	// double check we have a match
	if ki.key == key {
		return ki.offset, nil
	}
	// otherwise, we return not found
	return -1, ErrIndexEntryNotFound
}

func (ssi *SSIndex) lastKey() string {
	return ssi.data[len(ssi.data)-1].key
}

func (ssi *SSIndex) Len() int {
	return len(ssi.data)
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
	ssi.open = false
	return nil
}
