package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	errOutOfBounds = errors.New("error: out of bounds")
	errFileClosed  = errors.New("error: file is closed")
	//errMaxFileSize = errors.New("error: max file size met")
)

const (
	maxFileSize = 16 * 1 << 10 //2 * 1 << 20 // 2 mb
)

type entry struct {
	//path   string
	index  uint64
	offset uint64
}

func (e entry) String() string {
	return fmt.Sprintf("entry.index=%d\nentry.offset=%d\n\n", e.index, e.offset)
}

type segment struct {
	path    string
	index   uint64
	entries []entry
}

func (s *segment) String() string {
	return fmt.Sprintf("segment.path=%q\nsegment.index=%d\nsegment.entries=%d\n\n",
		s.path, s.index, len(s.entries))
}

type BinFile struct {
	mu       sync.RWMutex
	path     string     // represents the base path
	file     *os.File   // represents the underlying data file
	fileOpen bool       // reports if the underlying file is open or not
	offset   uint64     // latest offset pointer in the file
	index    uint64     // latest sequence number used as an index
	segments []*segment // holds offset for each entry
	segcache *segment   // currently cached segment
}

// Open opens a new BinFile
func Open(path string) (*BinFile, error) {
	// clean path and create files
	path = clean(path)
	err := os.MkdirAll(path, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// create new file instance
	bf := &BinFile{
		path: path,
	}
	// attempt to load entries
	err = bf.load()
	if err != nil {
		return nil, err
	}
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
		path := filepath.Join(bf.path, file.Name())
		// attempt to load entries from this file
		err = bf.loadEntries(path)
		if err != nil {
			return err
		}
	}
	// check to see if we need to create a new file
	if len(bf.segments) == 0 {
		bf.segments = append(bf.segments, &segment{
			path:    filepath.Join(bf.path, fileName(0)),
			index:   0,
			entries: []entry{{index: 0, offset: 0}},
		})
		bf.segcache = bf.segments[0]
		bf.file, err = os.Create(bf.segcache.path)
		bf.fileOpen = true
		return err
	}
	// open last entry
	bf.segcache = bf.segments[len(bf.segments)-1]
	bf.file, err = os.OpenFile(bf.segcache.path, os.O_RDWR, 0666) // os.ModeSticky
	if err != nil {
		return err
	}
	n, err := bf.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	bf.fileOpen = true
	bf.offset = uint64(n)
	return nil
}

func (bf *BinFile) GetSegments() []*segment {
	return bf.segments
}

func (bf *BinFile) loadEntries(path string) error {
	// open file to read
	fd, err := os.OpenFile(path, os.O_RDONLY, 0666) // os.ModeSticky
	if err != nil {
		return err
	}
	defer func(fd *os.File) {
		err := fd.Close()
		if err != nil {

		}
	}(fd)
	// create segment
	seg := &segment{
		path:    path,
		index:   bf.index,
		entries: make([]entry, 0),
	}
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
		//bf.segments = append(bf.segments, &segment{
		//	path:   fd.Name(),
		//	index:  bf.index,
		//	offset: offset,
		//})
		// add entry to segment
		seg.entries = append(seg.entries, entry{
			index:  bf.index,
			offset: offset,
		})
		// skip to next entry
		n, err := fd.Seek(int64(elen), io.SeekCurrent)
		if err != nil {
			return err
		}
		offset = uint64(n)
		bf.index++
	}
	// add new segment to segment cache
	bf.segments = append(bf.segments, seg)
	return nil
}

func (bf *BinFile) doSplit(n int64) error {
	// break this down into manageable bites
	size, maxs := bf.offset+uint64(8+n), maxFileSize-(1<<13)
	// check to see if we should grow
	if size > uint64(maxs) {
		// if the current size plus n exceeds max file size
		// less 8 KB then it's time to grow. so first we...
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
		path := filepath.Join(bf.path, fileName(bf.index))
		file, err := os.Create(path)
		if err != nil {
			return err
		}
		// assign as main file and reset the global offset
		bf.segments = append(bf.segments, &segment{
			path:    path,
			index:   bf.index,
			entries: make([]entry, 0),
		})
		bf.segcache = bf.segments[len(bf.segments)-1]
		bf.file = file
		bf.offset = 0
	}
	// otherwise, no need to grow or split
	return nil
}

func (bf *BinFile) fileOffset() uint64 {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if !bf.fileOpen {
		return 0
	}
	offset, err := getOffset(bf.file)
	//offset, err := bf.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0
	}
	return offset
}

func getOffset(fd *os.File) (uint64, error) {
	offset, err := fd.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	return uint64(offset), nil
}

func (bf *BinFile) findSegment(index uint64) *segment {
	i, j := 0, len(bf.segments)
	for i < j {
		h := i + (j-i)/2
		if index >= bf.segments[h].index {
			i = h + 1
		} else {
			j = h
		}
	}
	return bf.segments[i-1]
}

func (s *segment) findEntry(index uint64) uint64 {
	i, j := 0, len(s.entries)
	for i < j {
		h := i + (j-i)/2
		if index >= s.entries[h].index {
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
	seg := bf.findSegment(index)
	offset := seg.entries[seg.findEntry(index)].offset
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
	// check if we need to split
	err := bf.doSplit(int64(len(data)))
	if err != nil {
		return 0, err
	}
	// get entry offset pointer
	bf.offset, err = getOffset(bf.file)
	if err != nil {
		return 0, err
	}
	// write entry header
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint64(hdr, uint64(len(data)))
	_, err = bf.file.Write(hdr)
	if err != nil {
		return 0, err
	}
	// write entry data
	_, err = bf.file.Write(data)
	if err != nil {
		return 0, err
	}
	// for a sync / flush to disk
	err = bf.file.Sync()
	if err != nil {
		return 0, err
	}
	// add new data entry to the entries cache
	bf.segcache.entries = append(bf.segcache.entries, entry{
		index:  bf.index,
		offset: bf.offset,
	})
	bf.offset, err = getOffset(bf.file)
	if err != nil {
		return 0, err
	}
	//bf.offset += uint64(len(hdr) + len(data))
	bf.index++
	return bf.index, nil
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
	count := 0
	for _, seg := range bf.segments {
		count += len(seg.entries)
	}
	return uint64(count)
}

// FirstIndex returns the first entry index
func (bf *BinFile) FirstIndex() (uint64, error) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if !bf.fileOpen {
		return 0, errFileClosed
	}
	return bf.segments[0].index, nil
}

// LastIndex returns the last entry index
func (bf *BinFile) LastIndex() (uint64, error) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if !bf.fileOpen {
		return 0, errFileClosed
	}
	return bf.segcache.entries[len(bf.segcache.entries)-1].index, nil
}

func (bf *BinFile) LatestIndex() (uint64, error) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if !bf.fileOpen {
		return 0, errFileClosed
	}
	return bf.index, nil
}

func (bf *BinFile) LatestOffset() (uint64, error) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	if !bf.fileOpen {
		return 0, errFileClosed
	}
	offset, err := getOffset(bf.file)
	//offset, err := bf.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (bf *BinFile) Size() uint64 {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	fi, _ := bf.file.Stat()
	return uint64(fi.Size())
	//return bf.offset
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
