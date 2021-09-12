package wal

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
	maxFileSize = 4 << 20 // 4 MB
	memPageSize = 8 << 10 // 8 KB
	logPrefix   = "wal-"
	logSuffix   = ".seg"
	firstIndex  = 1
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

// reader provides a read-only file descriptor
type reader struct {
	path string   // path of the file that is currently open
	fd   *os.File // underlying file to read from
	open bool     // is the file open
}

// openReader returns a *reader for the file at the provided path
func openReader(path string) (*reader, error) {
	fd, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	return &reader{
		path: path,
		fd:   fd,
		open: true,
	}, nil
}

// readFrom checks the given path and if it matches, simply returns
// the same reader, but if it is different it opens a new one recycling
// the same file descriptor. this allows you to read from multiple files
// fairly quickly and pain free.
func (r *reader) readFrom(path string) (*reader, error) {
	// if there is already a file opened
	if r.open {
		// and if that file has the same path, simply return r
		if r.path == path {
			return r, nil
		}
		// otherwise, a file is still opened at a different
		// location, so we must close it before we continue
		err := r.close()
		if err != nil {
			return nil, err
		}
	}
	// open a file at a new path (if we're here then the file is closed)
	fd, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	r.path = path
	r.fd = fd
	r.open = true
	return r, nil
}

// close simply closes the *reader
func (r *reader) close() error {
	if !r.open {
		return ErrFileClosed
	}
	err := r.fd.Close()
	if err != nil {
		return err
	}
	r.open = false
	r.path = ""
	return nil
}

// writer provides a write-only file descriptor
type writer struct {
	path string   // path of the file that is currently open
	fd   *os.File // underlying file to write to
	open bool     // is the file open
}

// openWriter returns a *writer for the file at the provided path
func openWriter(path string) (*writer, error) {
	fd, err := os.OpenFile(path, os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	// seek to the end of the current file to continue appending data
	_, err = fd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	return &writer{
		path: path,
		fd:   fd,
		open: true,
	}, nil
}

// offset returns the *writer's current file pointer offset
func (w *writer) offset() (int64, error) {
	if !w.open {
		return -1, ErrFileClosed
	}
	return w.fd.Seek(0, io.SeekCurrent)
}

// close syncs and closes the *writer
func (w *writer) close() error {
	if !w.open {
		return ErrFileClosed
	}
	err := w.fd.Sync()
	if err != nil {
		return err
	}
	err = w.fd.Close()
	if err != nil {
		return err
	}
	w.open = false
	w.path = ""
	return nil
}

// Log represents a write-ahead log structure
type Log struct {
	mu       sync.RWMutex
	base     string     // base directory for the logs
	r        *reader    // read only wrapper for an *os.File and reading
	w        *writer    // write only wrapper for an *os.File and writing
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
		index:    firstIndex,
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
		// attempt to load segment and entry metadata from file
		s, err := l.loadSegment(filepath.Join(l.base, file.Name()))
		if err != nil {
			return err
		}
		// add segment along with metadata to segment list
		l.segments = append(l.segments, s)
	}
	// if no segments were found, we need to initialize a new one
	if len(l.segments) == 0 {
		// create new segment file
		s, err := l.makeSegment(filepath.Join(l.base, fileName(firstIndex)))
		if err != nil {
			return err
		}
		// add segment to segment list
		l.segments = append(l.segments, s)
	}
	// update the active segment pointer
	l.active = l.lastSegment()
	// TODO: consider removing commented code below
	//l.active = l.findSegment(-1)
	// at this point everything has been successfully created or loaded,
	// so let us open a file reader associated with the active segment
	l.r, err = openReader(l.active.path)
	if err != nil {
		return err
	}
	// and also let us open a file writer associated with the active
	// segment. openWriter automatically seeks to the end of the file
	// so that the appending of data can start as soon as possible
	l.w, err = openWriter(l.active.path)
	if err != nil {
		return err
	}
	return nil
}

// makeSegment creates a segment at the path provided. On
// success, it will simply return segment, and a nil error
func (l *Log) makeSegment(path string) (*segment, error) {
	// init segment to fill out
	s := &segment{
		path:    path,
		index:   l.index,
		entries: make([]entry, 0),
	}
	// "touch" a new file
	fd, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	err = fd.Close()
	if err != nil {
		return nil, err
	}
	// update segment remaining, and add first entry
	s.remaining = maxFileSize
	// TODO: consider removing commented code below
	//s.entries = append(s.entries, entry{
	//	index:  l.index,
	//	offset: 0,
	//})
	// return new segment
	return s, nil
}

// loadSegment opens the segment at the path provided. it will return an
// io.ErrUnexpectedEOF if the file exists but is empty and has no data to
// read, ErrSegmentFull if the file has met the maxFileSize or on success
// will simply return segment, and a nil error
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
	// defer (explicitly in closure) the file close
	defer func(fd *os.File) {
		_ = fd.Close()
	}(fd)
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

// findSegment returns last segment, or performs binary search to find matching index
func (l *Log) findSegment(index int64) *segment {
	// declare for later
	i, j := 0, len(l.segments)
	// -1 represents the last segment
	if index == -1 {
		i = j
		goto SkipBinarySearch
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
SkipBinarySearch:
	// update index
	index = int64(i - 1)
	// return segment
	return l.segments[index]
}

// lastSegment returns the last segment in the segment list
func (l *Log) lastSegment() *segment {
	return l.segments[len(l.segments)-1]
}

// cycle adds a new segment to replace the current active (full) segment
func (l *Log) cycle() error {
	// sync and close current file segment
	err := l.w.close()
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
	l.active = l.lastSegment()
	// TODO: consider removing commented code below
	//l.active = l.findSegment(-1)
	// open file writer associated with the active segment
	l.w, err = openWriter(l.active.path)
	if err != nil {
		return err
	}
	// update file reader associated with the active segment
	l.r, err = l.r.readFrom(l.active.path)
	if err != nil {
		return err
	}
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
	if !l.r.open {
		return nil, ErrFileClosed
	}
	if index < firstIndex {
		return nil, ErrOutOfBounds
	}
	var err error
	// get entry that matches index
	s := l.findSegment(int64(index))
	log.Printf("look up index: %d\n%s\n", index, s)
	// make sure we are reading from the correct file
	if l.r.path != s.path {
		// update the reader with file associated with found segment
		l.r, err = l.r.readFrom(s.path)
		if err != nil {
			return nil, err
		}
	}
	// get the entry offset that matches index
	offset := s.entries[s.findEntry(index)].offset
	log.Printf("entry: %s\n", s.entries[s.findEntry(index)])
	// read entry length
	var buf [8]byte
	n, err := l.r.fd.ReadAt(buf[:], int64(offset))
	if err != nil {
		return nil, err
	}
	// update offset for reading entry into slice
	offset += uint64(n)
	// decode entry length
	elen := binary.LittleEndian.Uint64(buf[:])
	// make byte slice of entry length size
	data := make([]byte, elen)
	// read entry from reader into slice
	_, err = l.r.fd.ReadAt(data, int64(offset))
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
	if !l.w.open {
		return 0, ErrFileClosed
	}
	// get the file pointer offset for the entry
	offset, err := l.w.offset()
	if err != nil {
		return 0, err
	}
	// write entry header
	var hdr [8]byte
	binary.LittleEndian.PutUint64(hdr[:], uint64(len(data)))
	_, err = l.w.fd.Write(hdr[:])
	if err != nil {
		return 0, err
	}
	// write entry data
	_, err = l.w.fd.Write(data)
	if err != nil {
		return 0, err
	}
	// perform a sync / flush to disk
	err = l.w.fd.Sync()
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
	return l.index - 1, nil
}

// Scan is a log iterator
func (l *Log) Scan(iter func(index uint64, data []byte) bool) error {
	// read lock
	l.mu.RLock()
	defer l.mu.RUnlock()
	if !l.w.open {
		return ErrFileClosed
	}
	// init for any errors
	var err error
	// range the in memory segment index
	for _, seg := range l.segments {
		// make sure we are reading the right data
		l.r, err = l.r.readFrom(seg.path)
		if err != nil {
			return err
		}
		// iterate segment entries
		for _, ent := range seg.entries {
			// get local offset for each entry
			offset := ent.offset
			// read entry length
			var hdr [8]byte
			_, err = l.r.fd.ReadAt(hdr[:], int64(offset))
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				return err
			}
			// update offset pointer for this entry
			offset += uint64(len(hdr))
			// decode entry length
			elen := binary.LittleEndian.Uint64(hdr[:])
			// make byte slice of entry length size
			data := make([]byte, elen)
			// read entry from reader into slice
			_, err = l.r.fd.ReadAt(data, int64(offset))
			if err != nil {
				return err
			}
			// check entry against iterator boolean function
			if !iter(ent.index, data) {
				// if it returns false, then process next entry
				continue
			}
		}
		// outside entry loop
	}
	// outside segment loop
	return nil
}

// Sync commits the current contents of the file to stable storage
func (l *Log) Sync() error {
	// lock
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.w.open {
		return ErrFileClosed
	}
	// sync file
	err := l.w.fd.Sync()
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
	if !l.w.open {
		return ErrFileClosed
	}
	// sync and close
	err := l.w.close()
	if err != nil {
		return err
	}
	// attempt to close file
	err = l.r.close()
	if err != nil {
		return err
	}
	// clean up everything
	l.base = ""
	l.r = nil
	l.w = nil
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

// Count returns the number of entries the log has written
func (l *Log) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	var count int
	for _, s := range l.segments {
		count += len(s.entries)
	}
	return count
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

// clean sanitizes a given path
func clean(path string) string {
	path, err := filepath.Abs(path)
	if err != nil {
		panic(err)
	}
	return filepath.ToSlash(path)
}

// name formats and returns a log name based on an index
func fileName(index uint64) string {
	return fmt.Sprintf("%s%020d%s", logPrefix, index, logSuffix)
}
