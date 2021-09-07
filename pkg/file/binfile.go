package file

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	errOutOfBounds = errors.New("error: out of bounds")
	errFileClosed  = errors.New("error: file is closed")
)

type BinFile struct {
	mu       sync.RWMutex
	path     string
	file     *os.File
	fileOpen bool
	seqn     uint64
	meta     []uint64
	coff     uint64
}

// Open opens a new BinFile
func Open(path string) (*BinFile, error) {
	// clean path, and make directory and file if they don't exist
	path = clean(path)
	err := touch(path)
	if err != nil {
		return nil, err
	}
	// open file
	fd, err := os.OpenFile(path, os.O_RDWR, 0666) // os.ModeSticky
	if err != nil {
		return nil, err
	}
	// create new file instance
	bf := &BinFile{
		path:     path,
		file:     fd,
		fileOpen: true,
	}
	// check to see if the file is new
	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	// if file is not empty
	if fi.Size() > 0 {
		// load entry meta data
		err = bf.loadEntryMeta()
		if err != nil {
			return nil, err
		}
	}
	// return new binary file
	return bf, nil
}

func (bf *BinFile) loadEntryMeta() error {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for {
		// read entry length
		var hdr [8]byte
		_, err := io.ReadFull(bf.file, hdr[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// decode entry length
		elen := binary.LittleEndian.Uint64(hdr[:])
		// add entry offset into file cache
		bf.meta = append(bf.meta, bf.coff)
		// skip to next entry
		n, err := bf.file.Seek(int64(elen), io.SeekCurrent)
		if err != nil {
			return err
		}
		bf.coff = uint64(n)
		bf.seqn++
	}
	return nil
}

func (bf *BinFile) Read(seqn uint64) ([]byte, error) {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	// error checking
	if !bf.fileOpen {
		return nil, errFileClosed
	}
	if seqn == 0 || seqn > uint64(len(bf.meta)) {
		return nil, errOutOfBounds
	}
	// get entry offset that matches index
	offset := bf.meta[seqn-1]
	// read entry length
	var buf [8]byte
	n, err := bf.file.ReadAt(buf[:], int64(offset))
	if err != nil {
		return nil, err
	}
	offset += uint64(n)
	// decode entry length
	elen := binary.LittleEndian.Uint64(buf[:])
	// make byte slice of entry length size
	entry := make([]byte, elen)
	// read entry from reader into slice
	_, err = bf.file.ReadAt(entry, int64(offset))
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (bf *BinFile) Write(data []byte) (uint64, error) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	// error checking
	if !bf.fileOpen {
		return 0, errFileClosed
	}
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint64(hdr, uint64(len(data)))
	_, err := bf.file.Write(hdr)
	if err != nil {
		return 0, err
	}
	_, err = bf.file.Write(data)
	if err != nil {
		return 0, err
	}
	bf.meta = append(bf.meta, bf.coff)
	bf.coff += uint64(len(hdr) + len(data))
	bf.seqn++
	return bf.seqn, nil
}

func (bf *BinFile) OpenFile() error {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if bf.fileOpen {
		// file is already open, do nothing
		return nil
	}
	return nil
}

func (bf *BinFile) CloseFile() error {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if !bf.fileOpen {
		// file is already closed, do nothing
		return nil
	}
	return nil
}

func (bf *BinFile) IsFileOpen() bool {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	return bf.fileOpen
}

// EntryCount returns the number of entries in the current file or -1 if there is an error
func (bf *BinFile) EntryCount() int64 {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	return int64(len(bf.meta))
}

// FirstIndex returns the first entry index, or -1 if there is an error
func (bf *BinFile) FirstIndex() error {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if !bf.fileOpen {
		return errFileClosed
	}
	return nil
}

// LastIndex return sthe last entry index or -1 if there is an error
func (bf *BinFile) LastIndex() error {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if !bf.fileOpen {
		return errFileClosed
	}
	return nil
}

func (bf *BinFile) LatestIndex() uint64 {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	return bf.seqn
}

func (bf *BinFile) LatestOffset() int64 {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if !bf.fileOpen {
		return -1
	}
	offset, err := bf.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1
	}
	return offset
}

func (bf *BinFile) Sync() error {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if !bf.fileOpen {
		return errFileClosed
	}
	return nil
}

func (bf *BinFile) Close() error {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if !bf.fileOpen {
		return errFileClosed
	}
	return nil
}

// clean sanitizes the path
func clean(path string) string {
	// properly sanitize path
	path, err := filepath.Abs(path)
	if err != nil {
		panic(err)
	}
	return filepath.ToSlash(path)
}

// touch "touches" (creates if it does not exist)
// any folders or files in the path provided
func touch(path string) error {
	// check to see if directory exists
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		// split for distinction
		dir, file := filepath.Split(path)
		// create dir it if it does not exist
		err = os.MkdirAll(dir, os.ModeDir)
		if err != nil {
			return err
		}
		// create file if it does not exist
		fd, err := os.Create(dir + file)
		if err != nil {
			return err
		}
		// we are just "touching" this file
		// so we need to close it again
		err = fd.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
