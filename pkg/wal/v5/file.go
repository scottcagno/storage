package v5

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const TEST = 0xDEADBEEF

const fileSuffix = ".dat"

type File struct {
	mu      sync.RWMutex
	fd      *os.File
	path    string
	offset  uint64
	entries []uint64
}

func Open(path string) (*File, error) {
	// properly sanitize path
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	path = filepath.ToSlash(path)
	// check to see if directory exists
	_, err = os.Stat(path)
	var fileIsNew bool // default it to false
	if os.IsNotExist(err) {
		// create it if it does not exist
		dir, file := filepath.Split(path)
		err = os.MkdirAll(dir, os.ModeDir)
		if err != nil {
			return nil, err
		}
		// create it if it does not exist
		fd, err := os.Create(dir + file)
		if err != nil {
			return nil, err
		}
		// we are just "touching" this file
		err = fd.Close()
		if err != nil {
			return nil, err
		}
		fileIsNew = true
	}
	fd, err := os.OpenFile(path, os.O_RDWR, 0666) // os.ModeSticky
	if err != nil {
		return nil, err
	}
	// create new file instance
	f := &File{
		fd:   fd,
		path: path,
	}
	// check to see if entries need loaded
	if !fileIsNew {
		err = f.loadEntryOffsets()
		if err != nil {
			return nil, err
		}
	}
	// return new file
	return f, nil
}

func (f *File) loadEntryOffsets() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for {
		// read entry length
		var hdr [8]byte
		_, err := io.ReadFull(f.fd, hdr[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// decode entry length
		elen := binary.LittleEndian.Uint64(hdr[:])
		// add entry offset into file cache
		f.entries = append(f.entries, f.offset)
		// skip to next entry
		n, err := f.fd.Seek(int64(elen), io.SeekCurrent)
		if err != nil {
			return err
		}
		f.offset = uint64(n)
	}
	return nil
}

func (f *File) Read() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	// read entry length
	var buf [8]byte
	_, err := io.ReadFull(f.fd, buf[:])
	if err != nil {
		return nil, err
	}
	// decode entry length
	elen := binary.LittleEndian.Uint64(buf[:])
	// make byte slice of entry length size
	entry := make([]byte, elen)
	// read entry from reader into slice
	_, err = io.ReadFull(f.fd, entry)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (f *File) ReadAt(offset int64) ([]byte, error) {
	// read entry length
	var buf [8]byte
	n, err := f.fd.ReadAt(buf[:], offset)
	if err != nil {
		return nil, err
	}
	offset += int64(n)
	// decode entry length
	elen := binary.LittleEndian.Uint64(buf[:])
	// make byte slice of entry length size
	entry := make([]byte, elen)
	// read entry from reader into slice
	_, err = f.fd.ReadAt(entry, offset)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (f *File) ReadAtIndex(index int64) ([]byte, error) {
	// error check index
	if index > int64(len(f.entries)-1) || index < 0 {
		return nil, fmt.Errorf("error: out of bounds")
	}
	// get entry offset that matches index
	offset := f.entries[index]
	// read entry length
	var buf [8]byte
	n, err := f.fd.ReadAt(buf[:], int64(offset))
	if err != nil {
		return nil, err
	}
	offset += uint64(n)
	// decode entry length
	elen := binary.LittleEndian.Uint64(buf[:])
	// make byte slice of entry length size
	entry := make([]byte, elen)
	// read entry from reader into slice
	_, err = f.fd.ReadAt(entry, int64(offset))
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (f *File) Write(data []byte) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// encode entry length
	var hdr [8]byte
	binary.LittleEndian.PutUint64(hdr[:], uint64(len(data)))
	// write entry length
	_, err := f.fd.Write(hdr[:])
	if err != nil {
		return -1, err
	}
	// write entry data
	_, err = f.fd.Write(data)
	if err != nil {
		return -1, err
	}
	// ensure durability
	err = f.fd.Sync()
	if err != nil {
		return -1, err
	}
	// update position offset
	f.offset = uint64(len(hdr) + len(data))
	// add new entry index
	f.entries = append(f.entries, f.offset)
	return int64(len(f.entries) - 1), nil
}

func (f *File) Entries() []uint64 {
	return f.entries
}

func getCurrentOffset(fd *os.File) (uint64, error) {
	offset, err := fd.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	return uint64(offset), err
}

func (f *File) Sync() error {
	return nil
}

func (f *File) Close() error {
	return nil
}
