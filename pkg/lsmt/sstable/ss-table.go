package sstable

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

func DataFileNameFromIndex(index int64) string {
	hexa := strconv.FormatInt(index, 16)
	return fmt.Sprintf("%s%010s%s", filePrefix, hexa, dataFileSuffix)
}

func IndexFromDataFileName(name string) (int64, error) {
	hexa := name[len(filePrefix) : len(name)-len(dataFileSuffix)]
	return strconv.ParseInt(hexa, 16, 32)
}

type SSTable struct {
	path  string
	file  *os.File
	open  bool
	index *SSTIndex
}

func OpenSSTable(base string, index int64) (*SSTable, error) {
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
	// file handler
	//var file *os.File
	// check to make sure file doesn't exist
	//_, err = os.Stat(path)
	//if os.IsNotExist(err) {
	//	// create new data file
	//	file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	//	if err != nil {
	//		return nil, err
	//	}
	//}
	// otherwise, just open new data file
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	// init sstable gindex
	ssi, err := OpenSSTIndex(base, index)
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
		return binary.ErrFileClosed
	}
	// make sure gindex is open
	if sst.index == nil {
		return ErrSSTIndexNotFound
	}
	return nil
}

func (sst *SSTable) Read(key string) (*binary.Entry, error) {
	// error check
	err := sst.errorCheckFileAndIndex()
	if err != nil {
		return nil, err
	}
	// find gindex using key
	i, err := sst.index.Find(key)
	if err != nil {
		return nil, err
	}
	// use gindex offset to read data
	e, err := binary.DecodeEntryAt(sst.file, i.Offset)
	if err != nil {
		return nil, err
	}
	// found it
	return e, nil
}

func (sst *SSTable) ReadIndex(key string) (*binary.Index, error) {
	// error check
	err := sst.errorCheckFileAndIndex()
	if err != nil {
		return nil, err
	}
	// find gindex using key
	i, err := sst.index.Find(key)
	if err != nil {
		return nil, err
	}
	// found it
	return i, nil
}

func (sst *SSTable) ReadAt(offset int64) (*binary.Entry, error) {
	// error check
	err := sst.errorCheckFileAndIndex()
	if err != nil {
		return nil, err
	}
	// use gindex offset to read data
	e, err := binary.DecodeEntryAt(sst.file, offset)
	if err != nil {
		return nil, err
	}
	// found it
	return e, nil
}

func (sst *SSTable) Write(e *binary.Entry) error {
	// error check
	err := sst.errorCheckFileAndIndex()
	if err != nil {
		return err
	}
	// write entry to data file
	offset, err := binary.EncodeEntry(sst.file, e)
	if err != nil {
		return err
	}
	// write entry to gindex
	err = sst.index.Write(e.Key, offset)
	if err != nil {
		return err
	}
	return nil
}

func (sst *SSTable) WriteBatch(b *binary.Batch) error {
	// error check
	err := sst.errorCheckFileAndIndex()
	if err != nil {
		return err
	}
	// error check batch
	if b == nil {
		return ErrSSTEmptyBatch
	}
	// check to see if batch is sorted
	if !sort.IsSorted(b) {
		// if not, sort
		sort.Stable(b)
	}
	// range batch and write
	for i := range b.Entries {
		// entry
		e := b.Entries[i]
		// write entry to data file
		offset, err := binary.EncodeEntry(sst.file, e)
		if err != nil {
			return err
		}
		// write entry info to gindex file
		err = sst.index.Write(e.Key, offset)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sst *SSTable) Scan(iter func(e *binary.Entry) bool) error {
	// error check
	err := sst.errorCheckFileAndIndex()
	if err != nil {
		return err
	}
	for {
		// decode next data entry
		e, err := binary.DecodeEntry(sst.file)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		if !iter(e) {
			break
		}
	}
	return nil
}

func (sst *SSTable) ScanAt(offset int64, iter func(e *binary.Entry) bool) error {
	// error check
	err := sst.errorCheckFileAndIndex()
	if err != nil {
		return err
	}
	// get current offset, so we can return here when were done
	cur, err := binary.Offset(sst.file)
	if err != nil {
		return err
	}
	// seek to provided location
	_, err = sst.file.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	for {
		// decode next data entry
		e, err := binary.DecodeEntry(sst.file)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		if !iter(e) {
			break
		}
	}
	// go back to where the file was at the beginning
	_, err = sst.file.Seek(cur, io.SeekStart)
	if err != nil {
		return err
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
	sst.open = false
	return nil
}
