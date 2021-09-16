package v2

import (
	"errors"
	"github.com/scottcagno/storage/pkg/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	logPrefix = ""
	logSuffix = ""

	defaultMaxFileSize uint64 = 4 << 20 // 4 MB
)

var (
	maxFileSize = defaultMaxFileSize

	ErrOutOfBounds    = errors.New("error: out of bounds")
	ErrSegmentFull    = errors.New("error: segment is full")
	ErrFileClosed     = errors.New("error: file closed")
	ErrBadArgument    = errors.New("error: bad argument")
	ErrNoPathProvided = errors.New("error: no path provided")
	ErrOptionsMissing = errors.New("error: options missing")
)

// WAL is a write-ahead log structure
type WAL struct {
	lock       sync.RWMutex   // lock is a mutual exclusion lock
	base       string         // base is the base filepath
	r          *binary.Reader // r is a binary reader
	w          *binary.Writer // w is a binary writer
	firstIndex uint64         // firstIndex is the index of the first entry
	lastIndex  uint64         // lastIndex is the index of the last entry
	segments   []*segment     // segments is an index of the current file segments
	active     *segment       // active is the current active segment
}

// OpenWAL opens and returns a new write-ahead log structure
func OpenWAL(path string) (*WAL, error) {
	// make sure we are working with absolute paths
	base, err := filepath.Abs(path)
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
	// create a new write-ahead log instance
	l := &WAL{
		base:       base,
		firstIndex: 1,
		segments:   make([]*segment, 0),
	}
	// attempt to load segments
	err = l.loadIndex()
	if err != nil {
		return nil, err
	}
	// return write-ahead log
	return l, nil
}

// loadIndex initializes the segment index. It looks for segment
// files in the base directory and attempts to index the segment as
// well as any of the entries within the segment. If this is a new
// instance, it will create a new segment that is ready for writing.
func (l *WAL) loadIndex() error {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	// get the files in the base directory path
	files, err := os.ReadDir(l.base)
	if err != nil {
		return err
	}
	// list the files in the base directory path and attempt to index the entries
	for _, file := range files {
		// skip non data files
		if file.IsDir() ||
			!strings.HasPrefix(file.Name(), logPrefix) ||
			!strings.HasSuffix(file.Name(), logSuffix) {
			continue // skip this, continue on to the next file
		}
		// attempt to load segment (and index entries in segment)
		s, err := l.loadSegmentFile(filepath.Join(l.base, file.Name()))
		if err != nil {
			return err
		}
		// segment has been loaded successfully, append to the segments list
		l.segments = append(l.segments, s)
	}
	// check to see if any segments were found. If not, initialize a new one
	if len(l.segments) == 0 {
		// create a new segment file
		s, err := l.makeSegmentFile()
		if err != nil {
			return err
		}
		// segment has been created successfully, append to the segments list
		l.segments = append(l.segments, s)
	}
	// segments have either been loaded or created, so now we
	// should go about updating the active segment pointer to
	// point to the "tail" (the last segment in the segment list)
	l.active = l.getLastSegment()
	// we should be good to go, lets attempt to open a file
	// reader to work with the active segment
	l.r, err = binary.OpenReader(l.active.path)
	if err != nil {
		return err
	}
	// and then attempt to open a file writer to also work
	// with the active segment, so we can begin appending data
	l.w, err = binary.OpenWriter(l.active.path)
	if err != nil {
		return err
	}
	// finally, update the firstIndex and lastIndex
	l.firstIndex = l.segments[0].entries[0].index
	l.lastIndex = l.getLastSegment().getLastIndex()
	return nil
}

// loadSegment attempts to open the segment file at the path provided
// and index the entries within the segment. It will return an os.PathError
// if the file does not exist, an io.ErrUnexpectedEOF if the file exists
// but is empty and has no data to read, and ErrSegmentFull if the file
// has met the maxFileSize. It will return the segment and nil error on success.
func (l *WAL) loadSegmentFile(path string) (*segment, error) {
	// check to make sure path exists before continuing
	_, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	// attempt to open existing segment file for reading
	fd, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	// defer file close
	defer func(fd *os.File) {
		_ = fd.Close()
	}(fd)
	// create a new segment to append indexed entries to
	s := &segment{
		path:    path,
		entries: make([]entry, 0),
	}
	// read segment file and index entries
	for {
		// get the current offset of the
		// reader for the entry later
		offset, err := binary.Offset(fd)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		// read and decode entry
		e, err := binary.DecodeEntry(fd)
		if err != nil {
			return nil, err
		}
		// get current offset
		// add entry index to segment entries list
		s.entries = append(s.entries, entry{
			index:  e.Id,
			offset: offset,
		})
		// continue to process the next entry
	}
	// get the offset of the reader to calculate bytes remaining
	offset, err := binary.Offset(fd)
	if err != nil {
		return nil, err
	}
	// update the segment remaining bytes
	s.remaining = maxFileSize - uint64(offset)
	return s, nil
}

// makeSegment attempts to make a new segment automatically using the timestamp
// as the segment name. On success, it will simply return a new segment and a nil error
func (l *WAL) makeSegmentFile() (*segment, error) {
	// create a new file
	path := filepath.Join(l.base, makeFileName(time.Now()))
	fd, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	// don't forget to close it
	err = fd.Close()
	if err != nil {
		return nil, err
	}
	// create and return new segment
	return &segment{
		path:      path,
		index:     l.lastIndex + 1,
		remaining: maxFileSize,
	}, nil
}

// findSegmentIndex performs binary search to find the segment containing provided index
func (l *WAL) findSegmentIndex(index uint64) int {
	// declare for later
	i, j := 0, len(l.segments)
	// otherwise, perform binary search
	for i < j {
		h := i + (j-i)/2
		if index >= l.segments[h].index {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

// getLastSegment returns the tail segment in the segments index list
func (l *WAL) getLastSegment() *segment {
	return l.segments[len(l.segments)-1]
}

// cycleSegment adds a new segment to replace the current (active) segment
func (l *WAL) cycleSegment() error {
	// TODO: implement
	return nil
}

// Read reads an entry from the write-ahead log at the specified index
func (l *WAL) Read(index uint64) ([]byte, []byte, error) {
	// read lock
	l.lock.RLock()
	defer l.lock.RUnlock()
	// error checking
	if index < l.firstIndex || index > l.lastIndex {
		return nil, nil, ErrOutOfBounds
	}
	var err error
	// find the segment containing the provided index
	s := l.segments[l.findSegmentIndex(index)]
	// make sure we are reading from the correct file
	l.r, err = l.r.ReadFrom(s.path)
	if err != nil {
		return nil, nil, err
	}
	// find the offset for the entry containing the provided index
	offset := s.entries[s.findEntryIndex(index)].offset
	// read entry at offset
	e, err := l.r.ReadEntryAt(offset)
	if err != nil {
		return nil, nil, err
	}
	return e.Key, e.Value, nil
}

// WriteEntry writes an entry to the write-ahead log in an append-only fashion
func (l *WAL) Write(key []byte, value []byte) (uint64, error) {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	// write entry
	offset, err := l.w.WriteEntry(&binary.Entry{
		Id:    l.lastIndex,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return 0, err
	}
	// add new entry to the segment index
	l.active.entries = append(l.active.entries, entry{
		index:  l.lastIndex,
		offset: offset,
	})
	// update lastIndex
	l.lastIndex++
	// grab the current offset written
	offset2, err := l.w.Offset()
	if err != nil {
		return 0, err
	}
	// update segment remaining
	l.active.remaining -= uint64(offset2 - offset)
	// check to see if the active segment needs to be cycled
	if l.active.remaining < 64 {
		err = l.cycleSegment()
		if err != nil {
			return 0, err
		}
	}
	return l.lastIndex - 1, nil
}

// Scan provides an iterator method for the write-ahead log
func (l *WAL) Scan() error {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	// TODO: implement
	return nil
}

// Close closes the write-ahead log
func (l *WAL) Close() error {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	// TODO: implement
	return nil
}

// String is the stringer method for the write-ahead log
func (l *WAL) String() string {
	// TODO: implement
	return ""
}
