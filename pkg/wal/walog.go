package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

const (
	defaultFileSize = uint64(4 << 20) // 4 MB
	memPageSize     = 256             //8 << 10 // 8 KB
	logPrefix       = "wal-"
	logSuffix       = ".seg"
	firstIndex      = uint64(1)
)

var maxFileSize = defaultFileSize

var (
	ErrOutOfBounds    = errors.New("error: out of bounds")
	ErrSegmentFull    = errors.New("error: segment is full")
	ErrFileClosed     = errors.New("error: file closed")
	ErrBadArgument    = errors.New("error: bad argument")
	ErrNoPathProvided = errors.New("error: no path provided")
	ErrOptionsMissing = errors.New("error: options missing")
)

// Options is a configurable options struct
// which can be passed to a new logger instance
type Options struct {
	BasePath    string
	MaxFileSize uint64
}

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

// OpenWithOptions opens a new write-ahead logger with configurable options set
func OpenWithOptions(op Options) (*Log, error) {
	if op.BasePath == "" {
		return nil, ErrOptionsMissing
	}
	if op.MaxFileSize == 0 {
		op.MaxFileSize = defaultFileSize
	}
	maxFileSize = op.MaxFileSize
	// clean path and create directory structure
	base := clean(op.BasePath)
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
		s, err := l.makeSegmentFromIndex(firstIndex)
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

// makeSegmentFromIndex creates a segment using the index provided.
// On success, it will simply return segment, and a nil error
func (l *Log) makeSegmentFromIndex(index uint64) (*segment, error) {
	// init segment to fill out
	s := &segment{
		path:    filepath.Join(l.base, fileName(index)),
		index:   index,
		entries: make([]entry, 0),
	}
	// "touch" a new file
	fd, err := os.Create(s.path)
	if err != nil {
		return nil, err
	}
	// write segment header
	var shdr [8]byte
	binary.LittleEndian.PutUint64(shdr[:], index)
	_, err = l.w.fd.Write(shdr[:])
	if err != nil {
		return nil, err
	}
	err = fd.Sync()
	if err != nil {
		return nil, err
	}
	err = fd.Close()
	if err != nil {
		return nil, err
	}
	// update segment remaining, and add first entry
	s.remaining = maxFileSize
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
	// read segment header
	var shdr [8]byte
	_, err = io.ReadFull(fd, shdr[:])
	if err != nil {
		return nil, err
	}
	// decode starting index into current segment
	s.index = binary.LittleEndian.Uint64(shdr[:])
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
func (l *Log) findSegmentIndex(index int64) int {
	// declare for later
	i, j := 0, len(l.segments)
	// otherwise, perform binary search
	for i < j {
		h := i + (j-i)/2
		if index >= int64(l.segments[h].index) {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

// lastSegment returns the last segment in the segment list
func (l *Log) lastSegment() *segment {
	return l.segments[len(l.segments)-1]
}

// lastIndex returns the last index in the provided segment
func (s *segment) lastIndex() uint64 {
	e := s.entries[len(s.entries)-1]
	return e.index
}

// cycle adds a new segment to replace the current active (full) segment
func (l *Log) cycle() error {
	// sync and close current file segment
	err := l.w.close()
	if err != nil {
		return err
	}
	// update global index
	//l.index++
	// create new segment
	//s, err := l.makeSegment(filepath.Join(l.base, fileName(l.index+1)))
	s, err := l.makeSegmentFromIndex(l.index + 1)
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

func (l *Log) readEntry(offset int64) ([]byte, error) {
	// read entry length
	var buf [8]byte
	n, err := l.r.fd.ReadAt(buf[:], offset)
	if err != nil {
		return nil, err
	}
	// update offset for reading entry into slice
	offset += int64(n)
	// decode entry length
	elen := binary.LittleEndian.Uint64(buf[:])
	// make byte slice of entry length size
	data := make([]byte, elen)
	// read entry from reader into slice
	_, err = l.r.fd.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}
	// return entry data
	return data, nil
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
	s := l.segments[l.findSegmentIndex(int64(index))]
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

func (l *Log) writeEntry(data []byte) (int64, error) {
	// get the file pointer offset for the entry
	offset, err := l.w.offset()
	if err != nil {
		return -1, err
	}
	// write entry header
	var hdr [8]byte
	binary.LittleEndian.PutUint64(hdr[:], uint64(len(data)))
	_, err = l.w.fd.Write(hdr[:])
	if err != nil {
		return -1, err
	}
	// write entry data
	_, err = l.w.fd.Write(data)
	if err != nil {
		return -1, err
	}
	// perform a sync / flush to disk
	err = l.w.fd.Sync()
	if err != nil {
		return -1, err
	}
	return offset, nil
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
	err := l.scan(iter)
	if err != nil {
		return err
	}
	return nil
}

// scan is a log iterator for "internal use only"
func (l *Log) scan(iter func(index uint64, data []byte) bool) error {
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

func (l *Log) truncateFront(index uint64) error {
	// do bounds check
	lastIndex := l.lastSegment().lastIndex()
	if index == 0 ||
		lastIndex == 0 ||
		index < firstIndex || index > lastIndex {
		return ErrOutOfBounds
	}
	if index == firstIndex {
		return nil // nothing to truncate
	}

	fmt.Printf("truncate-front: %d\n", index)
	segidx := l.findSegmentIndex(int64(index))

	// remove all segments
	for i := 0; i < segidx; i++ {
		s := l.segments[i]
		fmt.Printf("remove segment: %q (%d)\n", filepath.Base(s.path), s.index)
		err := os.Remove(s.path)
		if err != nil {
			return err
		}
	}
	return nil

	s := l.segments[segidx]
	fmt.Printf("prune entries within segment: %q (%d)\n", filepath.Base(s.path), s.index)
	// create temp buffer
	var buf bytes.Buffer
	// scan through entries
	for _, ent := range s.entries {
		if index > ent.index {
			fmt.Printf("re-write entry: %s", ent)
			offset := ent.offset
			// read entry length
			var hdr [8]byte
			n, err := l.r.fd.ReadAt(hdr[:], int64(offset))
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				return err
			}
			// update offset for reading entry into slice
			offset += uint64(n)
			// decode entry length
			elen := binary.LittleEndian.Uint64(hdr[:])
			// make byte slice of entry length size
			data := make([]byte, elen)
			// read entry from reader into slice
			_, err = l.r.fd.ReadAt(data, int64(offset))
			if err != nil {
				return err
			}
			// write entry length into temp file
			_, err = buf.Write(hdr[:])
			if err != nil {
				return err
			}
			// write entry into buffer
			_, err = buf.Write(data)
			if err != nil {
				return err
			}
			fmt.Printf("bytes buffer length: %d\n", buf.Len())
			continue
		}
		fmt.Printf("remove entry: %s", ent)
	}
	// create a temp file to write into
	f, err := os.Create(filepath.Join(l.base, fileName(0)))
	if err != nil {
		return err
	}
	_, err = f.Write(buf.Bytes())
	if err != nil {
		return err
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}
	err = os.Remove(s.path)
	if err != nil {
		return err
	}
	return nil
}

func (l *Log) truncateBack(index uint64) error {
	// do bounds check
	lastIndex := l.lastSegment().lastIndex()
	if index == 0 ||
		lastIndex == 0 ||
		index < firstIndex || index > lastIndex {
		return ErrOutOfBounds
	}
	if index == lastIndex {
		return nil // nothing to truncate
	}

	fmt.Printf("truncate-back: %d\n", index)
	segidx := l.findSegmentIndex(int64(index))
	for i := len(l.segments) - 1; i > segidx; i-- {
		s := l.segments[i]
		fmt.Printf("remove segment: %q (%d)\n", filepath.Base(s.path), s.index)
	}

	return nil
}

// Truncate truncates the entries according to whence
func (l *Log) Truncate(index uint64, whence int) error {
	// lock
	l.mu.Lock()
	defer l.mu.Unlock()
	// error checking
	if !l.r.open {
		return ErrFileClosed
	}
	if index < firstIndex || index > l.lastSegment().lastIndex() {
		return ErrOutOfBounds
	}
	if whence != io.SeekStart && whence != io.SeekEnd {
		return ErrBadArgument
	}
	var err error
	// get entry that matches index
	s := l.segments[l.findSegmentIndex(int64(index))]
	// make sure we are reading from the correct file
	if l.r.path != s.path {
		// update the reader with file associated with found segment
		l.r, err = l.r.readFrom(s.path)
		if err != nil {
			return err
		}
	}
	// set up the indexes according to the whence
	var begIndex, endIndex uint64
	if whence == io.SeekStart {
		begIndex, endIndex = firstIndex, index
	}
	if whence == io.SeekEnd {
		begIndex, endIndex = index, l.lastSegment().lastIndex()
	}
	// create a temp file to write into
	tempf, err := os.Create(filepath.Join(l.base, "TEMP"))
	if err != nil {
		return err
	}
	defer tempf.Close()
	// keep track of which segment indexes need cleaned up
	var segi []int
	// range the in memory segment index
	for i, seg := range l.segments {
		// check to make sure we can't skip right away
		if seg.index < begIndex || seg.lastIndex() > endIndex {
			continue
		}
		// note that segment at index "i" will need cleaning up later
		segi = append(segi, i)
		// now, assuming we are in an applicable
		// segment, let's make sure we are switching
		// out reader to the correct segment as well
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
			// write entry header to temp file
			_, err = tempf.Write(hdr[:])
			if err != nil {
				return err
			}
			// write entry data to temp file
			_, err = tempf.Write(data)
			if err != nil {
				return err
			}
			// make sure to sync
			err = tempf.Sync()
			if err != nil {
				return err
			}
			// on to the next entry
		}
		// outside entry loop
	}
	// close temp file
	err = tempf.Close()
	if err != nil {
		return err
	}
	// close file handlers and clean stuff up
	err = l.r.close()
	if err != nil {
		return err
	}
	// clean up segments that are no longer being used
	for _, i := range segi {
		fmt.Printf("removing segment: %q\n", l.segments[i].path)
		err = os.RemoveAll(l.segments[i].path)
		if err != nil {
			return err
		}
		if i < len(l.segments)-1 {
			copy(l.segments[i:], l.segments[i+1:])
		}
		l.segments[len(l.segments)-1] = nil // or the zero value of T
		l.segments = l.segments[:len(l.segments)-1]
	}
	// re-activate last segment
	l.active = l.lastSegment()
	// re-open reader again
	l.r, err = l.r.readFrom(l.active.path)
	if err != nil {
		return err
	}
	// rename temp file
	err = os.Rename(filepath.Join(l.base, "TEMP"), filepath.Join(l.base, fileName(begIndex)))
	if err != nil {
		return err
	}
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
