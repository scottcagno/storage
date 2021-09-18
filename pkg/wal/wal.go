package wal

import (
	"errors"
	"fmt"
	"github.com/scottcagno/storage/pkg/binary"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	LogPrefix = "wal-"
	LogSuffix = ".seg"

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

// entry contains the metadata for a single entry within the file segment
type entry struct {
	index  int64 // index is the "id" of this entry
	offset int64 // offset is the actual offset of this entry in the segment file
}

// String is the stringer method for an entry
func (e entry) String() string {
	return fmt.Sprintf("entry.index=%d, entry.offset=%d", e.index, e.offset)
}

// segment contains the metadata for the file segment
type segment struct {
	path      string  // path is the full path to this segment file
	index     int64   // starting index of the segment
	entries   []entry // entries is an index of the entries in the segment
	remaining uint64  // remaining is the bytes left after max file size minus entry data
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

// makeFileName returns a file name using the provided timestamp.
// If t is nil, it will create a new name using time.Now()
func makeFileName(t time.Time) string {
	//tf := t.Format("2006-01-03_15:04:05:000000")
	//return fmt.Sprintf("%s%s%s", LogPrefix, time.RFC3339Nano, LogSuffix)
	return fmt.Sprintf("%s%d%s", LogPrefix, time.Now().UnixMicro(), LogSuffix)
}

// getFirstIndex returns the first index in the entries list
func (s *segment) getFirstIndex() int64 {
	return s.index
}

// getLastIndex returns the last index in the entries list
func (s *segment) getLastIndex() int64 {
	if len(s.entries) > 0 {
		return s.entries[len(s.entries)-1].index
	}
	return s.index
}

// findEntryIndex performs binary search to find the entry containing provided index
func (s *segment) findEntryIndex(index int64) int {
	// declare for later
	i, j := 0, len(s.entries)
	// otherwise, perform binary search
	for i < j {
		h := i + (j-i)/2
		if index >= s.entries[h].index {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

// WriteAheadLog is a write-ahead log structure
type WriteAheadLog struct {
	lock       sync.RWMutex   // lock is a mutual exclusion lock
	base       string         // base is the base filepath
	r          *binary.Reader // r is a binary reader
	w          *binary.Writer // w is a binary writer
	firstIndex int64          // firstIndex is the index of the first entry
	lastIndex  int64          // lastIndex is the index of the last entry
	segments   []*segment     // segments is an index of the current file segments
	active     *segment       // active is the current active segment
}

// Open opens and returns a new write-ahead log structure
func Open(path string) (*WriteAheadLog, error) {
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
	l := &WriteAheadLog{
		base:       base,
		firstIndex: 0,
		lastIndex:  1,
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
func (l *WriteAheadLog) loadIndex() error {
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
			!strings.HasPrefix(file.Name(), LogPrefix) ||
			!strings.HasSuffix(file.Name(), LogSuffix) {
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
	l.firstIndex = l.segments[0].index
	// and update last index
	l.lastIndex = l.getLastSegment().getLastIndex()
	return nil
}

// loadSegment attempts to open the segment file at the path provided
// and index the entries within the segment. It will return an os.PathError
// if the file does not exist, an io.ErrUnexpectedEOF if the file exists
// but is empty and has no data to read, and ErrSegmentFull if the file
// has met the maxFileSize. It will return the segment and nil error on success.
func (l *WriteAheadLog) loadSegmentFile(path string) (*segment, error) {
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
			return nil, err
		}
		// read and decode entry
		e, err := binary.DecodeEntry(fd)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
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
	// make sure to fill out the segment index from the first entry index
	s.index = s.entries[0].index
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
func (l *WriteAheadLog) makeSegmentFile() (*segment, error) {
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
	s := &segment{
		path:      path,
		index:     l.lastIndex,
		entries:   make([]entry, 0),
		remaining: maxFileSize,
	}
	return s, nil
}

// findSegmentIndex performs binary search to find the segment containing provided index
func (l *WriteAheadLog) findSegmentIndex(index int64) int {
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
func (l *WriteAheadLog) getLastSegment() *segment {
	return l.segments[len(l.segments)-1]
}

// cycleSegment adds a new segment to replace the current (active) segment
func (l *WriteAheadLog) cycleSegment() error {
	// sync and close current file segment
	err := l.w.Close()
	if err != nil {
		return err
	}
	// create a new segment file
	s, err := l.makeSegmentFile()
	if err != nil {
		return err
	}
	// add segment to segment index list
	l.segments = append(l.segments, s)
	// update the active segment pointer
	l.active = l.getLastSegment()
	// open file writer associated with active segment
	l.w, err = binary.OpenWriter(l.active.path)
	if err != nil {
		return err
	}
	// update file reader associated with the active segment
	l.r, err = binary.OpenReader(l.active.path)
	if err != nil {
		return err
	}
	return nil
}

// Read reads an entry from the write-ahead log at the specified index
func (l *WriteAheadLog) Read(index int64) (string, []byte, error) {
	// read lock
	l.lock.RLock()
	defer l.lock.RUnlock()
	// error checking
	if index < l.firstIndex || index > l.lastIndex {
		return "", nil, ErrOutOfBounds
	}
	var err error
	// find the segment containing the provided index
	s := l.segments[l.findSegmentIndex(index)]
	// make sure we are reading from the correct file
	l.r, err = l.r.ReadFrom(s.path)
	if err != nil {
		return "", nil, err
	}
	// find the offset for the entry containing the provided index
	offset := s.entries[s.findEntryIndex(index)].offset
	// read entry at offset
	e, err := l.r.ReadEntryAt(offset)
	if err != nil {
		return "", nil, err
	}
	return string(e.Key), e.Value, nil
}

// WriteEntry writes an entry to the write-ahead log in an append-only fashion
func (l *WriteAheadLog) Write(key string, value []byte) (int64, error) {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	// write entry
	offset, err := l.w.WriteEntry(&binary.Entry{
		Id:    l.lastIndex,
		Key:   []byte(key),
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
func (l *WriteAheadLog) Scan(iter func(index int64, key string, value []byte) bool) error {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	// init for any errors
	var err error
	// range the segment index
	for _, sidx := range l.segments {
		fmt.Printf("segment: %s\n", sidx)
		// make sure we are reading the right data
		l.r, err = l.r.ReadFrom(sidx.path)
		if err != nil {
			return err
		}
		// range the segment entries index
		for _, eidx := range sidx.entries {
			// read entry
			e, err := l.r.ReadEntryAt(eidx.offset)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				return err
			}
			// check entry against iterator boolean function
			if !iter(e.Id, string(e.Key), e.Value) {
				// if it returns false, then process next entry
				continue
			}
		}
		// outside entry loop
	}
	// outside segment loop
	return nil
}

// TruncateFront removes all segments and entries before specified index
func (l *WriteAheadLog) TruncateFront(index int64) error {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	// perform bounds check
	if index == 0 ||
		l.lastIndex == 0 ||
		index < l.firstIndex || index > l.lastIndex {
		return ErrOutOfBounds
	}
	if index == l.firstIndex {
		return nil // nothing to truncate
	}
	// locate segment in segment index list containing specified index
	sidx := l.findSegmentIndex(index)
	// isolate whole segments that can be removed
	for i := 0; i < sidx; i++ {
		// remove segment file
		err := os.Remove(l.segments[i].path)
		if err != nil {
			return err
		}
	}
	// remove segments from segment index (cut, i-j)
	i, j := 0, sidx
	copy(l.segments[i:], l.segments[j:])
	for k, n := len(l.segments)-j+i, len(l.segments); k < n; k++ {
		l.segments[k] = nil // or the zero value of T
	}
	l.segments = l.segments[:len(l.segments)-j+i]
	// update firstIndex
	l.firstIndex = l.segments[0].index
	// prepare to re-write partial segment
	var err error
	var entries []entry
	tmpfd, err := os.Create(filepath.Join(l.base, "tmp-partial.seg"))
	if err != nil {
		return err
	}
	// after the segment index cut, segment 0 will
	// contain the partials that we must re-write
	if l.segments[0].index < index {
		// make sure we are reading from the correct path
		l.r, err = l.r.ReadFrom(l.segments[0].path)
		if err != nil {
			return err
		}
		// range the entries within this segment to find
		// the ones that are greater than the index and
		// write those to a temporary buffer....
		for _, ent := range l.segments[0].entries {
			if ent.index < index {
				continue // skip
			}
			// read entry
			e, err := l.r.ReadEntryAt(ent.offset)
			if err != nil {
				return err
			}
			// write entry to temp file
			ent.offset, err = binary.EncodeEntry(tmpfd, e)
			if err != nil {
				return err
			}
			// sync write
			err = tmpfd.Sync()
			if err != nil {
				return err
			}
			// append to a new entries list
			entries = append(entries, ent)
		}
		// move reader back to active segment file
		l.r, err = l.r.ReadFrom(l.active.path)
		if err != nil {
			return err
		}
		// close temp file
		err = tmpfd.Close()
		if err != nil {
			return err
		}
		// remove partial segment file
		err = os.Remove(l.segments[0].path)
		if err != nil {
			return err
		}
		// change temp file name
		err = os.Rename(tmpfd.Name(), l.segments[0].path)
		if err != nil {
			return err
		}
		// update segment
		l.segments[0].entries = entries
		l.segments[0].index = entries[0].index
	}
	return nil
}

// Count returns the number of entries currently in the write-ahead log
func (l *WriteAheadLog) Count() int {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	// get count
	var count int
	for _, s := range l.segments {
		count += len(s.entries)
	}
	// return count
	return count
}

// FirstIndex returns the write-ahead logs first index
func (l *WriteAheadLog) FirstIndex() int64 {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.firstIndex
}

// LastIndex returns the write-ahead logs first index
func (l *WriteAheadLog) LastIndex() int64 {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.lastIndex
}

// Close syncs and closes the write-ahead log
func (l *WriteAheadLog) Close() error {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	// sync and close writer
	err := l.w.Close()
	if err != nil {
		return err
	}
	// close reader
	err = l.r.Close()
	if err != nil {
		return err
	}
	// clean everything else up
	l.base = ""
	l.r = nil
	l.w = nil
	l.firstIndex = 0
	l.lastIndex = 0
	l.segments = nil
	l.active = nil
	// force gc for good measure
	runtime.GC()
	return nil
}

func (l *WriteAheadLog) Path() string {
	return l.base
}

// String is the stringer method for the write-ahead log
func (l *WriteAheadLog) String() string {
	var ss string
	ss += fmt.Sprintf("\n\n[write-ahead log]\n")
	ss += fmt.Sprintf("base: %q\n", l.base)
	ss += fmt.Sprintf("firstIndex: %d\n", l.firstIndex)
	ss += fmt.Sprintf("lastIndex: %d\n", l.lastIndex)
	ss += fmt.Sprintf("segments: %d\n", len(l.segments))
	if l.active != nil {
		ss += fmt.Sprintf("active: %q\n", filepath.Base(l.active.path))
	}
	if len(l.segments) > 0 {
		for i, s := range l.segments {
			ss += fmt.Sprintf("segment[%d]:\n", i)
			ss += fmt.Sprintf("\tpath: %q\n", filepath.Base(s.path))
			ss += fmt.Sprintf("\tindex: %d\n", s.index)
			ss += fmt.Sprintf("\tentries: %d\n", len(s.entries))
			ss += fmt.Sprintf("\tremaining: %d\n", s.remaining)
		}
	}
	ss += "\n"
	return ss
}
