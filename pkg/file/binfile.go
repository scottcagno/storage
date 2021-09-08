package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/scottcagno/storage/pkg/common"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

var (
	errOutOfBounds = errors.New("error: out of bounds")
	errFileClosed  = errors.New("error: file is closed")
	errMaxFileSize = errors.New("error: max file size met")
)

const (
	szKB         = 1 << 10
	szMB         = 1 << 20
	szGB         = 1 << 30
	maxFileSize  = TestFileSize // (1<<21) 2097152 bytes (2MB)
	TestFileSize = 1 << 14      // (1<<14) 16384 bytes (16KB)
)

type entry struct {
	path   string
	index  uint64
	offset uint64
}

type BinFile struct {
	mu       sync.RWMutex
	path     string   // represents the base path
	file     *os.File // represents the underlying data file
	fileOpen bool     // reports if the underlying file is open or not
	coff     uint64   // latest offset pointer in the file
	gidx     uint64   // latest sequence number used as an index
	entries  []entry  // holds offset for each entry
}

// Open opens a new BinFile
func Open(path string) (*BinFile, error) {
	// clean path and create files
	path = clean(path)
	err := os.MkdirAll(path, os.ModeDir)
	if err != nil {
		return nil, err
	}

	//err := touch(path)
	//if err != nil {
	//	return nil, err
	//}

	// open file
	//fd, err := os.OpenFile(path, os.O_RDWR, 0666) // os.ModeSticky
	//if err != nil {
	//	return nil, err
	//}

	// check to see if the file is new
	//fi, err := fd.Stat()
	//if err != nil {
	//	return nil, err
	//}

	// create new file instance
	bf := &BinFile{
		path: path,
		//file:     fd,
		//fileOpen: true,
		//size:     uint64(fi.Size()),
	}

	err = bf.load()
	if err != nil {
		return nil, err
	}

	// if file is not empty
	//if bf.size > 0 {
	//	// check grow or split
	//	err = bf.checkGrowOrSplit(0)
	//	if err != nil {
	//		return nil, err
	//	}
	//	// load entry meta data
	//	err = bf.loadEntryMeta()
	//	if err != nil {
	//		return nil, err
	//	}
	//}
	// return new binary file
	return bf, nil
}

func (bf *BinFile) load() error {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	// get the files in the main directory path
	files, err := os.ReadDir(bf.path)
	if err != nil {
		return err
	}
	// list files in the main directory path and attempt to load entries
	for _, file := range files {
		// skip non data files
		if file.IsDir() { //|| len(file.Name()) < 24 {
			continue
		}
		// check file size
		fi, err := file.Info()
		if err != nil {
			return err
		}
		if fi.Size() < 1 {
			// if the file is empty skip loading entries
			continue
		}
		common.DEBUG(">>>", "load -> LOADING ENTRIES FOR "+file.Name())
		path := filepath.Join(bf.path, file.Name())
		log.Printf("loading entries from: %s (%s)\n", path, file.Name())
		// attempt to load entries from this file
		err = bf.loadEntries(path)
		if err != nil {
			return err
		}
		common.DEBUG(">>>", "load -> DONE LOADING ENTRIES FOR "+file.Name())
	}
	// check to see if we need to create a new file
	if len(bf.entries) == 0 {
		common.DEBUG(">>>", "load -> CREATING A NEW FILE")
		bf.entries = append(bf.entries, entry{
			path:   filepath.Join(bf.path, fileName(0)),
			index:  0,
			offset: 0,
		})
		bf.file, err = os.Create(bf.entries[0].path)
		bf.fileOpen = true
		return err
	}
	path := bf.entries[len(bf.entries)-1].path
	common.DEBUG(">>>", "load -> ATTEMPTING TO OPEN THE LAST ENTRY ("+path+")")
	// open last entry
	bf.file, err = os.OpenFile(path, os.O_RDWR, 0666) // os.ModeSticky
	if err != nil {
		return err
	}
	common.DEBUG(">>>", "load -> SEEKING TO END OF THE LAST ENTRY")
	n, err := bf.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	common.DEBUG(">>>", "load -> OPENED LAST ENTRY AND CALLED SEEK TO END")

	bf.fileOpen = true
	bf.coff = uint64(n)
	return nil
}

func (bf *BinFile) loadEntries(path string) error {
	// open file to read
	fd, err := os.OpenFile(path, os.O_RDONLY, 0666) // os.ModeSticky
	if err != nil {
		return err
	}
	defer fd.Close()
	// skip through entries and load entry metadata
	offset := uint64(0)
	for {
		// read entry length
		var hdr [8]byte
		_, err = io.ReadFull(fd, hdr[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// decode entry length
		elen := binary.LittleEndian.Uint64(hdr[:])
		// add entry offset into file cache
		bf.entries = append(bf.entries, entry{
			path:   fd.Name(),
			index:  bf.gidx,
			offset: offset,
		})
		// skip to next entry
		n, err := fd.Seek(int64(elen), io.SeekCurrent)
		if err != nil {
			return err
		}
		offset = uint64(n)
		bf.gidx++
	}
	return nil
}

func (bf *BinFile) checkGrowOrSplit(n int64) error {
	size, maxs := bf.coff+uint64(8+n), maxFileSize-(1<<13)
	log.Printf("%d > %d = %v\n", size, maxs, size > uint64(maxs))
	// check to see if we should grow
	if size > uint64(maxs) {
		// if the current size plus n exceeds max file size less 8 KB then it's time to grow.
		//
		// sync current file data
		err := bf.file.Sync()
		if err != nil {
			return err
		}
		// close current file
		err = bf.file.Close()
		if err != nil {
			return err
		}
		// create new file
		path := filepath.Join(bf.path, fileName(bf.gidx))
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		// assign as main file
		bf.file = file
		bf.coff = 0
	}
	// otherwise, no need to grow or split
	return nil
}

func (bf *BinFile) findEntry(index uint64) uint64 {
	i, j := 0, len(bf.entries)
	for i < j {
		h := i + (j-i)/2
		if index >= bf.entries[h].index {
			i = h + 1
		} else {
			j = h
		}
	}
	return uint64(i - 1)
}

func (bf *BinFile) Read(index uint64) ([]byte, error) {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	// error checking
	if !bf.fileOpen {
		return nil, errFileClosed
	}
	if index == 0 {
		return nil, errOutOfBounds
	}
	// get entry offset that matches index
	offset := bf.entries[bf.findEntry(index)].offset
	common.DEBUG("offset", offset)
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
	err := bf.checkGrowOrSplit(int64(len(data)))
	if err != nil {
		return 0, err
	}
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint64(hdr, uint64(len(data)))
	_, err = bf.file.Write(hdr)
	if err != nil {
		return 0, err
	}
	_, err = bf.file.Write(data)
	if err != nil {
		return 0, err
	}
	err = bf.file.Sync()
	if err != nil {
		return 0, err
	}
	bf.entries = append(bf.entries, entry{
		index:  bf.gidx,
		offset: bf.coff,
	})
	bf.coff += uint64(len(hdr) + len(data))
	bf.gidx++
	return bf.gidx, nil
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

// EntryCount returns the number of entries in the current file
func (bf *BinFile) EntryCount() uint64 {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	return uint64(len(bf.entries))
}

// FirstIndex returns the first entry index
func (bf *BinFile) FirstIndex() (uint64, error) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if !bf.fileOpen {
		return 0, errFileClosed
	}
	return bf.entries[0].index, nil
}

// LastIndex returns the last entry index
func (bf *BinFile) LastIndex() (uint64, error) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if !bf.fileOpen {
		return 0, errFileClosed
	}
	return bf.entries[len(bf.entries)-1].index, nil
}

func (bf *BinFile) LatestIndex() uint64 {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	return bf.gidx
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

func (bf *BinFile) Size() uint64 {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	fi, _ := bf.file.Stat()
	return uint64(fi.Size())
	//return bf.coff
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

func fileName(index uint64) string {
	return fmt.Sprintf("wal-%020d.seg", index)
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
