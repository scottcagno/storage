package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	filePrefix = "sst-"
	fileSuffix = ".db"
)

var (
	ErrFileClosed = errors.New("error: file is closed")
	ErrNotFound   = errors.New("error: not found")
)

func DataFileNameFromIndex(index int64) string {
	hexa := strconv.FormatInt(index, 16)
	return fmt.Sprintf("%s%010s-data%s", filePrefix, hexa, fileSuffix)
}

func IndexFileNameFromIndex(index int64) string {
	hexa := strconv.FormatInt(index, 16)
	return fmt.Sprintf("%s%010s-index%s", filePrefix, hexa, fileSuffix)
}

func IndexFromDataFileName(name string) (int64, error) {
	hexa := name[len(filePrefix) : len(name)-len(fileSuffix)-5]
	return strconv.ParseInt(hexa, 16, 32)
}

func IndexFromIndexFileName(name string) (int64, error) {
	hexa := name[len(filePrefix) : len(name)-len(fileSuffix)-6]
	return strconv.ParseInt(hexa, 16, 32)
}

type sstIndex struct {
	key    string // key is a key
	offset int64  // offset is the offset of this data entry
}

func (i *sstIndex) String() string {
	return fmt.Sprintf("sstIndex.key=%q, sstIndex.offset=%d", i.key, i.offset)
}

type sstEntry struct {
	key   string
	value []byte
}

func (e *sstEntry) String() string {
	return fmt.Sprintf("sstEntry.key=%q, sstEntry.value=%s", e.key, e.value)
}

type SSTable struct {
	lock      sync.RWMutex
	dataFile  *os.File    // dataFile is the file descriptor for the data
	indexFile *os.File    // indexFile is the file descriptor for the index
	dataPath  string      // dataPath is the filepath for the data
	indexPath string      // indexPath is the filepath for the index
	first     string      // first is the first key
	last      string      // last is the last key
	data      []*sstIndex // data is the sstIndex
	readOnly  bool
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
	dataPath := filepath.Join(base, DataFileNameFromIndex(index))
	// check to make sure file doesn't exist
	_, err = os.Stat(dataPath)
	if os.IsExist(err) {
		return nil, err
	}
	// create new index file path
	indexPath := filepath.Join(base, IndexFileNameFromIndex(index))
	// check to make sure file doesn't exist
	_, err = os.Stat(indexPath)
	if os.IsExist(err) {
		return nil, err
	}
	// create new data file
	fdD, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	// create new index file
	fdI, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	// init and return SSTable
	sst := &SSTable{
		dataFile:  fdD,
		indexFile: fdI,
		dataPath:  dataPath,
		indexPath: indexPath,
	}
	return sst, nil
}

func (sst *SSTable) searchDataIndex(key string) int {
	// declare for later
	i, j := 0, len(sst.data)
	// otherwise, perform binary search
	for i < j {
		h := i + (j-i)/2
		if key >= sst.data[h].key {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

func (sst *SSTable) GetEntryOffset(key string) (int64, error) {
	// make sure file is not closed
	if sst.indexFile == nil {
		return -1, ErrFileClosed
	}
	// if data index is not loaded, then load it
	if len(sst.data) == 0 {
		err := sst.LoadSSTableDataIndex()
		if err != nil {
			return -1, err
		}
	}
	// try binary search
	ki := sst.data[sst.searchDataIndex(key)]
	// double check we have a match
	if ki.key == key {
		return ki.offset, nil
	}
	// otherwise, we return not found
	return -1, ErrNotFound
}

func (sst *SSTable) WriteEntry(e *sstEntry) error {
	// make sure file is not closed
	if sst.dataFile == nil || sst.indexFile == nil {
		return ErrFileClosed
	}
	// write entry to data file
	offset, err := EncodeDataEntry(sst.dataFile, e)
	if err != nil {
		return err
	}
	// create new index
	i := &sstIndex{key: e.key, offset: offset}
	// write entry info to index file
	_, err = EncodeIndexEntry(sst.indexFile, i)
	if err != nil {
		return err
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
	dataPath := filepath.Join(base, DataFileNameFromIndex(index))
	// check to make sure file exists
	_, err = os.Stat(dataPath)
	if os.IsNotExist(err) {
		return nil, err
	}
	// create new index file path
	indexPath := filepath.Join(base, IndexFileNameFromIndex(index))
	// check to make sure file exists
	_, err = os.Stat(indexPath)
	if os.IsNotExist(err) {
		return nil, err
	}
	// open index file
	fdI, err := os.OpenFile(indexPath, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	// init and return SSTable
	sst := &SSTable{
		dataFile:  nil,
		indexFile: fdI,
		dataPath:  dataPath,
		indexPath: indexPath,
		readOnly:  true,
	}
	// load sst data index info
	err = sst.LoadSSTableDataIndex()
	if err != nil {
		return nil, err
	}
	return sst, nil
}

func (sst *SSTable) LoadSSTableDataIndex() error {
	// make sure we are using correct path
	path := sst.indexPath
	// check to make sure file exists
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return err
	}
	// open file to read header
	fd, err := os.OpenFile(path, os.O_RDONLY, 0666)
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
		sst.data = append(sst.data, ei)
	}
	// update sst first and last and then return
	if len(sst.data) > 0 {
		sst.first = sst.data[0].key
		sst.last = sst.data[len(sst.data)-1].key
	}
	return nil
}

func EncodeIndexEntry(w io.WriteSeeker, idx *sstIndex) (int64, error) {
	// get offset of where this entry is located
	offset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	// make buffer for encoding
	buf := make([]byte, 16)
	// encode key length
	binary.LittleEndian.PutUint64(buf[0:8], uint64(len(idx.key)))
	// encode offset value
	binary.LittleEndian.PutUint64(buf[8:16], uint64(idx.offset))
	// write key length and offset value
	_, err = w.Write(buf)
	if err != nil {
		return -1, err
	}
	// write key data
	_, err = w.Write([]byte(idx.key))
	if err != nil {
		return -1, err
	}
	// return offset of entry
	return offset, nil
}

func DecodeIndexEntry(r io.Reader) (*sstIndex, error) {
	// make buffer for decoding
	buf := make([]byte, 16)
	// read key length
	_, err := r.Read(buf[0:8])
	if err != nil {
		return nil, err
	}
	// read data offset
	_, err = r.Read(buf[8:16])
	if err != nil {
		return nil, err
	}
	// decode key length
	keyLength := binary.LittleEndian.Uint64(buf[0:8])
	// decode data offset
	dataOffset := binary.LittleEndian.Uint64(buf[8:16])
	// make buffer to load the key into
	key := make([]byte, keyLength)
	// read key
	_, err = r.Read(key)
	if err != nil {
		return nil, err
	}
	// fill out sstIndex
	idx := &sstIndex{
		key:    string(key),
		offset: int64(dataOffset),
	}
	return idx, nil
}

func EncodeDataEntry(w io.WriteSeeker, ent *sstEntry) (int64, error) {
	// get offset of where this entry is located
	offset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	// make buffer for encoding
	buf := make([]byte, 16)
	// encode key length
	binary.LittleEndian.PutUint64(buf[0:8], uint64(len(ent.key)))
	// encode value length
	binary.LittleEndian.PutUint64(buf[8:16], uint64(len(ent.value)))
	// write key and value length
	_, err = w.Write(buf)
	if err != nil {
		return -1, err
	}
	// write key data
	_, err = w.Write([]byte(ent.key))
	if err != nil {
		return -1, err
	}
	// write value data
	_, err = w.Write(ent.value)
	if err != nil {
		return -1, err
	}
	// return offset of entry
	return offset, nil
}

func DecodeDataEntry(r io.Reader, ent *sstEntry) (int64, error) {
	// keep local offset
	var offset int64
	// make buffer for decoding
	buf := make([]byte, 16)
	// read key length
	n, err := r.Read(buf[0:8])
	if err != nil {
		return -1, err
	}
	// update offset
	offset += int64(n)
	// read val length
	n, err = r.Read(buf[8:16])
	if err != nil {
		return -1, err
	}
	// update offset
	offset += int64(n)
	// decode key length
	klen := binary.LittleEndian.Uint64(buf[0:8])
	// decode val length
	vlen := binary.LittleEndian.Uint64(buf[8:16])
	// make buffer to load the key and value into
	data := make([]byte, klen+vlen)
	// read key and value
	n, err = r.Read(data)
	if err != nil {
		return -1, err
	}
	// update offset
	offset += int64(n)
	// fill out sstEntry
	ent.key = string(data[0:klen])
	ent.value = data[klen : klen+vlen]
	// return
	return offset, nil
}

func (sst *SSTable) GetIndex() []*sstIndex {
	return sst.data
}

func (sst *SSTable) Close() error {
	if sst.indexFile != nil {
		if !sst.readOnly {
			err := sst.indexFile.Sync()
			if err != nil {
				return err
			}
		}
		err := sst.indexFile.Close()
		if err != nil {
			return err
		}
	}
	if sst.dataFile != nil {
		if !sst.readOnly {
			err := sst.dataFile.Sync()
			if err != nil {
				return err
			}
		}
		err := sst.dataFile.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
