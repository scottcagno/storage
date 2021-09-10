package v2

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

const (
	maxFileSize = 16 << 10 // 16 KB
	memPageSize = 4 << 10  // 4 KB
	logPrefix   = "wal-"
	logSuffix   = ".seg"
)

var (
	ErrOutOfBounds = errors.New("error: out of bounds")
	ErrSegmentFull = errors.New("error: segment is full")
	ErrFileClosed  = errors.New("error: file closed")
)

// entry metadata for this entry within the segment
type entry struct {
	index  uint64
	offset uint64
}

// String is the stringer method for entry
func (e entry) String() string {
	return fmt.Sprintf("\tentry.index=%d, entry.offset=%d\n", e.index, e.offset)
}

// entry size calculates the size of the entry
func entrySize(datalen int) uint64 {
	// return size of entry which is an 8 byte
	// header, plus the length of the data
	return uint64(8 + datalen)
}

// segment holds the metadata for this file segment
type segment struct {
	path      string  // full path to this segment file
	index     uint64  // starting index of this segment
	entries   []entry // entry metadata for this segment
	remaining uint64  // bytes remaining after max file size minus any entry data
}

// findEntry returns the offset metadata of the
// entry in the segment associated with the provided index
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

// needsCycle returns a boolean value indicating true if
// the current segment needs to be cycled
func (s *segment) needsCycle(datalen int) bool {
	return s.remaining-entrySize(datalen) < 1
}

// Log represents a write-ahead log structure
type Log struct {
	mu       sync.RWMutex
	base     string     // base directory for the logs
	fd       *os.File   // file descriptor for the active log file
	open     bool       // true if the current file descriptor is open
	index    uint64     // this is the global index number or the next number in the sequence
	segments []*segment // each log file segment metadata
	active   *segment   // the active (usually last) segment
}

// Open opens and returns a new write-ahead logger. It automatically calls the load() method
// which should load (or create) any and all segments and entry metadata.
func Open(base string) (*Log, error) {
	// clean path and create directory structure
	base = clean(base)
	err := os.MkdirAll(base, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// create new log instance
	l := &Log{
		base:     base,
		segments: make([]*segment, 0),
	}
	// attempt to load segments
	err = l.load()
	if err != nil {
		return nil, err
	}
	return l, nil
}

// load looks at the files in the base directory and iterates and
// instantiates any log segment files (and associated entries) it
// finds. If this is a new instance, it sets up an initial segment.
func (l *Log) load() error {
	// lock
	l.mu.Lock()
	defer l.mu.Unlock()
	// get the files in the main directory path
	files, err := os.ReadDir(l.base)
	if err != nil {
		return err
	}
	// list files in the main directory path and attempt to load entries
	for _, file := range files {
		// skip non data files
		if file.IsDir() || !strings.HasSuffix(file.Name(), logSuffix) {
			continue
		}
		// attempt to load segment from file
		s, err := l.loadSegment(filepath.Join(l.base, file.Name()))
		//s, err := l.openSegment(filepath.Join(l.base, file.Name()))
		if err != nil {
			return err
		}
		// add segment to segment list
		l.segments = append(l.segments, s)
	}
	// if no segments were found, we need to initialize a new one
	if len(l.segments) == 0 {
		// create new segment file
		s, err := l.makeSegment(filepath.Join(l.base, fileName(0)))
		if err != nil {
			return err
		}
		// add segment to segment list
		l.segments = append(l.segments, s)
	}
	// update the active segment pointer
	l.active = l.findSegment(-1)
	// at this point everything has been successfully created or loaded,
	// so it is time to open the file associated with the active segment
	l.fd, err = os.OpenFile(l.active.path, os.O_RDWR, 0666)
	if err != nil {
		LogLineErr(136, err)
		return err
	}
	// seek to the end of the current file to continue appending data
	_, err = l.fd.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	// update the log file descriptor boolean
	l.open = true
	return nil
}

// makeSegment creates a segment at the path provided. On
// success, it will simply return segment, and a nil error // 25 loc
func (l *Log) makeSegment(path string) (*segment, error) {
	// init segment to fill out
	s := &segment{
		path:    path,
		index:   l.index,
		entries: make([]entry, 0),
	}
	// create new file
	fd, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	// close, because we are just "touching" it
	err = fd.Close()
	if err != nil {
		return nil, err
	}
	// update segment remaining, and add first entry
	s.remaining = maxFileSize
	s.entries = append(s.entries, entry{
		index:  0,
		offset: 0,
	})
	// return new segment
	return s, nil
}

// loadSegment opens the segment at the path provided. it will return an
// io.ErrUnexpectedEOF if the file exists but is empty and has no data to
// read, ErrSegmentFull if the file has met the maxFileSize or on success
// will simply return segment, and a nil error // 51 loc
func (l *Log) loadSegment(path string) (*segment, error) {
	// init segment to fill out
	s := &segment{
		path:    path,
		index:   l.index,
		entries: make([]entry, 0),
	}
	// open existing segment file for reading
	fd, err := os.OpenFile(path, os.O_RDONLY, 0666) // os.ModeSticky
	if err != nil {
		return nil, err
	}
	// defer close
	defer fd.Close()
	// iterate segment entries and load metadata
	offset := uint64(0)
	for {
		// read entry length
		var hdr [8]byte
		_, err = io.ReadFull(fd, hdr[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		// decode entry length
		elen := binary.LittleEndian.Uint64(hdr[:])
		// add entry to segment
		s.entries = append(s.entries, entry{
			index:  l.index,
			offset: offset,
		})
		// skip to next entry
		n, err := fd.Seek(int64(elen), io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		// update the file pointer offset
		offset = uint64(n)
		// increment global index
		l.index++
		// continue on to process the next entry, or exit loop and return segment
	}
	// update segment remaining
	s.remaining = maxFileSize - offset
	// check to bytes remaining before continuing
	if s.remaining < 1 {
		return nil, ErrSegmentFull
	}
	// return loaded segment
	return s, nil
}

// TODO: this method may be deprecated
// openSegment opens or creates the segment at the path provided. it will
// return io.ErrUnexpectedEOF if the file exists but is empty and has no
// data to read, ErrSegmentFull if the file has met the maxFileSize and a
// new segment needs to be created, otherwise returning a segment and nil // 76 loc
func (l *Log) openSegment(path string) (*segment, error) {
	// init segment to fill out
	s := &segment{
		path:    path,
		index:   l.index,
		entries: make([]entry, 0),
	}
	// check to see if the file needs to be created, or read
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		// create new file
		fd, err := os.Create(path)
		if err != nil {
			return nil, err
		}
		// close, because we are just "touching" it
		err = fd.Close()
		if err != nil {
			return nil, err
		}
		// update segment remaining, and add first entry
		s.remaining = maxFileSize
		s.entries = append(s.entries, entry{
			index:  0,
			offset: 0,
		})
		// return new segment
		return s, nil
	}
	// otherwise, check file size to make sure there is something worth reading
	if info.Size() < 1 {
		return s, nil
	}
	// update segment remaining
	s.remaining = uint64(maxFileSize - info.Size())
	// check to bytes remaining before continuing
	if s.remaining < 1 {
		return nil, ErrSegmentFull
	}
	// open existing segment file for reading
	fd, err := os.OpenFile(path, os.O_RDONLY, 0666) // os.ModeSticky
	if err != nil {
		return nil, err
	}
	// defer close
	defer fd.Close()
	// iterate segment entries and load metadata
	offset := uint64(0)
	for {
		// read entry length
		var hdr [8]byte
		_, err = io.ReadFull(fd, hdr[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		// decode entry length
		elen := binary.LittleEndian.Uint64(hdr[:])
		// add entry to segment
		s.entries = append(s.entries, entry{
			index:  l.index,
			offset: offset,
		})
		// skip to next entry
		n, err := fd.Seek(int64(elen), io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		// update the file pointer offset
		offset = uint64(n)
		// increment global index
		l.index++
		// continue on to process the next entry, or exit loop and return segment
	}
	return s, nil
}

// findSegment returns last segment, or performs binary search to find matching index
func (l *Log) findSegment(index int64) *segment {
	// declare for later
	i, j := 0, len(l.segments)
	// -1 represents the last segment
	if index == -1 {
		i = j
		goto SkipBinsearch
	}
	// otherwise, perform binary search
	for i < j {
		h := i + (j-i)/2
		if index >= int64(l.segments[h].index) {
			i = h + 1
		} else {
			j = h
		}
	}
SkipBinsearch:
	// update index
	index = int64(i - 1)
	// return segment
	return l.segments[index]
}

func (l *Log) setActiveSegment(s *segment) error {
	// return early if there is nothing to update
	if l.active.path == s.path && l.active == s {
		return nil
	}
	// update the active segment pointer
	l.active = s
	// sync current file segment
	err := l.fd.Sync()
	if err != nil {
		return err
	}
	// close current file segment
	err = l.fd.Close()
	if err != nil {
		return err
	}
	// open the file associated with the active segment
	l.fd, err = os.OpenFile(l.active.path, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	return nil
}

// cycle adds a new segment to replace the current active (full) segment
func (l *Log) cycle() error {
	log.Printf("attempting to cycle: old=%q, ", l.fd.Name())
	// sync current file segment
	err := l.fd.Sync()
	if err != nil {
		return err
	}
	// close current file segment
	err = l.fd.Close()
	if err != nil {
		return err
	}
	// update global index
	l.index++
	// create new segment
	s, err := l.makeSegment(filepath.Join(l.base, fileName(l.index)))
	if err != nil {
		return err
	}
	// add segment to segment list
	l.segments = append(l.segments, s)
	// update the active segment pointer
	l.active = l.findSegment(-1)
	// open the file associated with the active segment
	l.fd, err = os.OpenFile(l.active.path, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	log.Printf("new=%q\n", l.fd.Name())
	return nil
}

// Read attempts to read the entry at the index provided. It will
// return ErrFileClosed if the file is closed, ErrOutOfBounds if
// the index is incorrect, otherwise the entry is returned, and nil
func (l *Log) Read(index uint64) ([]byte, error) {
	// read lock
	l.mu.RLock()
	defer l.mu.RUnlock()
	// error checking
	if !l.open {
		return nil, ErrFileClosed
	}
	if index == 0 {
		return nil, ErrOutOfBounds
	}
	// get entry that matches index
	s := l.findSegment(int64(index))
	// update active segment if they aren't the same
	err := l.setActiveSegment(s)
	if err != nil {
		return nil, err
	}
	offset := s.entries[s.findEntry(index)].offset
	// read entry length
	var buf [8]byte
	n, err := l.fd.ReadAt(buf[:], int64(offset))
	if err != nil {
		return nil, err
	}
	// update offset for reading entry into slice
	offset += uint64(n)
	// decode entry length
	elen := binary.LittleEndian.Uint64(buf[:])

	// DEBUGGING...
	if elen > 1024 {
		seg := l.findSegment(int64(index))
		should := seg.findEntry(index)
		off := seg.entries[should].offset
		fmt.Printf("offset: %d, should be: %d\n", offset, off)
		s := l.findSegment(int64(index))
		fmt.Printf("index %d is located in the following segment\n%s", index, s)
		e := s.findEntry(index)
		fmt.Printf("\tin entry number %d\n\t%s\n", e, s.entries[e])
	}

	// make byte slice of entry length size
	data := make([]byte, elen)
	// read entry from reader into slice
	_, err = l.fd.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}
	// return entry data
	return data, nil
}

// Write attempts to write a new entry in an append-only fashion. It
// will return ErrFileClosed if the file is not open to write, otherwise
// it will return the global index of the entry written, and nil
func (l *Log) Write(data []byte) (uint64, error) {
	// lock
	l.mu.Lock()
	defer l.mu.Unlock()
	// error checking
	if !l.open {
		return 0, ErrFileClosed
	}
	// get the file pointer offset for the entry
	offset, err := l.fd.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	// write entry header
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint64(hdr, uint64(len(data)))
	_, err = l.fd.Write(hdr)
	if err != nil {
		return 0, err
	}
	// write entry data
	_, err = l.fd.Write(data)
	if err != nil {
		return 0, err
	}
	// perform a sync / flush to disk
	err = l.fd.Sync()
	if err != nil {
		return 0, err
	}
	// add new data entry to the entries cache
	l.active.entries = append(l.active.entries, entry{
		index:  l.index,
		offset: uint64(offset),
	})
	// increment global index
	l.index++
	// update segment remaining
	l.active.remaining -= uint64(len(hdr) + len(data))
	// check to see if the active segment needs to be cycled
	if l.active.remaining < memPageSize {
		err = l.cycle()
		if err != nil {
			return 0, err
		}
	}
	return l.index, nil
}

// Sync commits the current contents of the file to stable storage
func (l *Log) Sync() error {
	// lock
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.open {
		return ErrFileClosed
	}
	// sync file
	err := l.fd.Sync()
	if err != nil {
		return err
	}
	return nil
}

// Close first commits any in-memory copy of recently written data
// to disk and then closes the File, rendering it unusable for I/O
func (l *Log) Close() error {
	// lock
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.open {
		return ErrFileClosed
	}
	// sync file first, before closing
	err := l.fd.Sync()
	if err != nil {
		return err
	}
	// attempt to close file
	err = l.fd.Close()
	if err != nil {
		return err
	}
	// clean up everything
	l.base = ""
	l.fd = nil
	l.open = false
	l.index = 0
	l.segments = nil
	l.active = nil
	runtime.GC()
	return nil
}

// Path returns the base path that the write-ahead logger is using
func (l *Log) Path() string {
	// lock
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.base
}

// String is a stringer method for a Log
func (l *Log) String() string {
	var ss string
	ss += fmt.Sprintf("\n\n[write-ahead log]\n")
	ss += fmt.Sprintf("base: %q\n", l.base)
	ss += fmt.Sprintf("index: %d\n", l.index)
	ss += fmt.Sprintf("segments: %d\n", len(l.segments))
	ss += fmt.Sprintf("active: %q\n", filepath.Base(l.active.path))
	for i, s := range l.segments {
		ss += fmt.Sprintf("segment[%d]:\n", i)
		ss += fmt.Sprintf("\tpath: %q\n", filepath.Base(s.path))
		ss += fmt.Sprintf("\tindex: %d\n", s.index)
		ss += fmt.Sprintf("\tentries: %d\n", len(s.entries))
		ss += fmt.Sprintf("\tremaining: %d\n", s.remaining)
	}
	ss += "\n"
	return ss
}

// String is the stringer method for a segment
func (s *segment) String() string {
	var ss string
	ss += fmt.Sprintf("path: %q\n", filepath.Base(s.path))
	ss += fmt.Sprintf("index: %d\n", s.index)
	ss += fmt.Sprintf("entries: %d\n", len(s.entries))
	ss += fmt.Sprintf("remaining: %d\n", s.remaining)
	return ss
}
