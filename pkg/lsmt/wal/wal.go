package wal

import (
	"errors"
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

const (
	FilePrefix               = "dat-"
	FileSuffix               = ".seg"
	defaultMaxFileSize int64 = 16 << 10 // 16 KB
	defaultBasePath          = "log"
	defaultSyncOnWrite       = false
	remainingTrigger         = 64
)

var (
	maxFileSize       = defaultMaxFileSize
	ErrOutOfBounds    = errors.New("error: out of bounds")
	ErrSegmentFull    = errors.New("error: segment is full")
	ErrFileClosed     = errors.New("error: file closed")
	ErrBadArgument    = errors.New("error: bad argument")
	ErrNoPathProvided = errors.New("error: no path provided")
	ErrOptionsMissing = errors.New("error: options missing")
)

// segEntry contains the metadata for a single segEntry within the file segment
type segEntry struct {
	index  int64 // index is the "id" of this segEntry
	offset int64 // offset is the actual offset of this segEntry in the segment file
}

// String is the stringer method for an segEntry
func (e segEntry) String() string {
	return fmt.Sprintf("segEntry.index=%d, segEntry.offset=%d", e.index, e.offset)
}

// segment contains the metadata for the file segment
type segment struct {
	path      string     // path is the full path to this segment file
	index     int64      // starting index of the segment
	entries   []segEntry // entries is an index of the entries in the segment
	remaining int64      // remaining is the bytes left after max file size minus segEntry data
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

func MakeFileNameFromIndex(index int64) string {
	hexa := strconv.FormatInt(index, 16)
	return fmt.Sprintf("%s%010s%s", FilePrefix, hexa, FileSuffix)
}

func GetIndexFromFileName(name string) (int64, error) {
	hexa := name[len(FilePrefix) : len(name)-len(FileSuffix)]
	return strconv.ParseInt(hexa, 16, 32)
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

// findEntryIndex performs binary search to find the segEntry containing provided index
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

var defaultWALConfig = &WALConfig{
	BasePath:    defaultBasePath,
	MaxFileSize: defaultMaxFileSize,
	SyncOnWrite: defaultSyncOnWrite,
}

type WALConfig struct {
	BasePath    string // base storage path
	MaxFileSize int64  // memtable flush threshold in KB
	SyncOnWrite bool   // perform sync every time an entry is write
}

func checkWALConfig(conf *WALConfig) *WALConfig {
	if conf == nil {
		return defaultWALConfig
	}
	if conf.BasePath == *new(string) {
		conf.BasePath = defaultBasePath
	}
	if conf.MaxFileSize < 1 {
		conf.MaxFileSize = defaultMaxFileSize
	}
	return conf
}

// WAL is a write-ahead log structure
type WAL struct {
	lock       sync.RWMutex // lock is a mutual exclusion lock
	conf       *WALConfig
	r          *binary.Reader // r is a binary reader
	w          *binary.Writer // w is a binary writer
	firstIndex int64          // firstIndex is the index of the first segEntry
	lastIndex  int64          // lastIndex is the index of the last segEntry
	segments   []*segment     // segments is an index of the current file segments
	active     *segment       // active is the current active segment
}

// OpenWAL opens and returns a new write-ahead log structure
func OpenWAL(c *WALConfig) (*WAL, error) {
	// check config
	conf := checkWALConfig(c)
	// TODO: consider replacing `filepath.Abs()`, and `filepath.ToSlash()`
	// TODO: with `filepath.Clean()` at some point or another. It should
	// TODO: close enough to the same (possibly even better), so yeah.
	// make sure we are working with absolute paths
	base, err := filepath.Abs(conf.BasePath)
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
		conf:       conf,
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

func (l *WAL) CloseAndRemove() error {
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
	// reset the segments
	l.segments = make([]*segment, 0)
	// reset first and last index
	l.firstIndex = 0
	l.lastIndex = 1
	// erase all files
	err = os.RemoveAll(l.conf.BasePath)
	if err != nil {
		return err
	}
	return nil
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
	files, err := os.ReadDir(l.conf.BasePath)
	if err != nil {
		return err
	}
	// list the files in the base directory path and attempt to index the entries
	for _, file := range files {
		// skip non data files
		if file.IsDir() ||
			!strings.HasPrefix(file.Name(), FilePrefix) ||
			!strings.HasSuffix(file.Name(), FileSuffix) {
			continue // skip this, continue on to the next file
		}
		// check the size of segment file
		fi, err := file.Info()
		if err != nil {
			return err
		}
		// if the file is empty, remove it and skip to next file
		if fi.Size() == 0 {
			err = os.Remove(filepath.Join(l.conf.BasePath, file.Name()))
			if err != nil {
				return err
			}
			continue // make sure we skip to next segment
		}
		// attempt to load segment (and index entries in segment)
		s, err := l.loadSegmentFile(filepath.Join(l.conf.BasePath, file.Name()))
		if err != nil {
			return err
		}
		// segment has been loaded successfully, append to the segments list
		l.segments = append(l.segments, s)
	}
	// check to see if any segments were found. If not, initialize a new one
	if len(l.segments) == 0 {
		// create a new segment file
		s, err := l.makeSegmentFile(l.lastIndex)
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
	l.w, err = binary.OpenWriterWithSync(l.active.path, l.conf.SyncOnWrite)
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
		entries: make([]segEntry, 0),
	}
	// read segment file and index entries
	index, err := GetIndexFromFileName(filepath.Base(fd.Name()))
	if err != nil {
		return nil, err
	}
	for {
		// get the current offset of the
		// reader for the segEntry later
		offset, err := binary.Offset(fd)
		if err != nil {
			return nil, err
		}
		// read and decode segEntry
		_, err = binary.DecodeEntry(fd)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		// get current offset
		// add segEntry index to segment entries list
		s.entries = append(s.entries, segEntry{
			index:  index,
			offset: offset,
		})
		// continue to process the next segEntry
		index++
	}
	// make sure to fill out the segment index from the first segEntry index
	s.index = s.entries[0].index
	// get the offset of the reader to calculate bytes remaining
	offset, err := binary.Offset(fd)
	if err != nil {
		return nil, err
	}
	// update the segment remaining bytes
	s.remaining = maxFileSize - offset
	return s, nil
}

// makeSegment attempts to make a new segment automatically using the timestamp
// as the segment name. On success, it will simply return a new segment and a nil error
func (l *WAL) makeSegmentFile(index int64) (*segment, error) {
	// create a new file
	path := filepath.Join(l.conf.BasePath, MakeFileNameFromIndex(index))
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
		entries:   make([]segEntry, 0),
		remaining: l.conf.MaxFileSize,
	}
	return s, nil
}

// findSegmentIndex performs binary search to find the segment containing provided index
func (l *WAL) findSegmentIndex(index int64) int {
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
	// sync and close current file segment
	err := l.w.Close()
	if err != nil {
		return err
	}
	// create a new segment file
	s, err := l.makeSegmentFile(l.lastIndex)
	if err != nil {
		return err
	}
	// add segment to segment index list
	l.segments = append(l.segments, s)
	// update the active segment pointer
	l.active = l.getLastSegment()
	// open file writer associated with active segment
	l.w, err = binary.OpenWriterWithSync(l.active.path, l.conf.SyncOnWrite)
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

// Read reads an segEntry from the write-ahead log at the specified index
func (l *WAL) Read(index int64) (*binary.Entry, error) {
	// read lock
	l.lock.RLock()
	defer l.lock.RUnlock()
	// error checking
	if index < l.firstIndex || index > l.lastIndex {
		return nil, ErrOutOfBounds
	}
	var err error
	// find the segment containing the provided index
	s := l.segments[l.findSegmentIndex(index)]
	// make sure we are reading from the correct file
	l.r, err = l.r.ReadFrom(s.path)
	if err != nil {
		return nil, err
	}
	// find the offset for the segEntry containing the provided index
	offset := s.entries[s.findEntryIndex(index)].offset
	// read segEntry at offset
	e, err := l.r.ReadEntryAt(offset)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// Write writes an segEntry to the write-ahead log in an append-only fashion
func (l *WAL) Write(e *binary.Entry) (int64, error) {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	// write segEntry
	offset, err := l.w.WriteEntry(e)
	if err != nil {
		return 0, err
	}
	// add new segEntry to the segment index
	l.active.entries = append(l.active.entries, segEntry{
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
	l.active.remaining -= offset2 - offset
	// check to see if the active segment needs to be cycled
	if l.active.remaining < remainingTrigger {
		err = l.cycleSegment()
		if err != nil {
			return 0, err
		}
	}
	return l.lastIndex - 1, nil
}

// WriteBatch writes a batch of entries performing no syncing until the end of the batch
func (l *WAL) WriteBatch(batch *binary.Batch) error {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	// check sync policy
	changedSyncPolicy := false
	if l.conf.SyncOnWrite == true {
		l.conf.SyncOnWrite = false // if it's on, temporarily disable
		l.w.SetSyncOnWrite(false)
		changedSyncPolicy = true
	}
	// iterate batch
	for i := range batch.Entries {
		// entry
		e := batch.Entries[i]
		// write entry to data file
		offset, err := l.w.WriteEntry(e)
		if err != nil {
			return err
		}
		// add new segEntry to the segment index
		l.active.entries = append(l.active.entries, segEntry{
			index:  l.lastIndex,
			offset: offset,
		})
		// update lastIndex
		l.lastIndex++
		// grab the current offset written
		offset2, err := l.w.Offset()
		if err != nil {
			return err
		}
		// update segment remaining
		l.active.remaining -= offset2 - offset
		// check to see if the active segment needs to be cycled
		if l.active.remaining < remainingTrigger {
			err = l.cycleSegment()
			if err != nil {
				return err
			}
		}
	}
	// after batch, set everything back how it was
	if changedSyncPolicy {
		l.conf.SyncOnWrite = true
		l.w.SetSyncOnWrite(true)
	}
	// after batch has been written, do sync
	err := l.w.Sync()
	if err != nil {
		return err
	}
	return nil
}

// Scan provides an iterator method for the write-ahead log
func (l *WAL) Scan(iter func(e *binary.Entry) bool) error {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	// init for any errors
	var err error
	// range the segment index
	for _, sidx := range l.segments {
		//fmt.Printf("segment: %s\n", sidx)
		// make sure we are reading the right data
		l.r, err = l.r.ReadFrom(sidx.path)
		if err != nil {
			return err
		}
		// range the segment entries index
		for _, eidx := range sidx.entries {
			// read segEntry
			e, err := l.r.ReadEntryAt(eidx.offset)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				return err
			}
			// check segEntry against iterator boolean function
			if !iter(e) {
				// if it returns false, then process next segEntry
				continue
			}
		}
		// outside segEntry loop
	}
	// outside segment loop
	return nil
}

// TruncateFront removes all segments and entries before specified index
func (l *WAL) TruncateFront(index int64) error {
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
	var entries []segEntry
	tmpfd, err := os.Create(filepath.Join(l.conf.BasePath, "tmp-partial.seg"))
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
			// read segEntry
			e, err := l.r.ReadEntryAt(ent.offset)
			if err != nil {
				return err
			}
			// write segEntry to temp file
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

func (l *WAL) GetConfig() *WALConfig {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.conf
}

func (l *WAL) Sync() error {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	err := l.w.Sync()
	if err != nil {
		return err
	}
	return nil
}

// Count returns the number of entries currently in the write-ahead log
func (l *WAL) Count() int {
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
func (l *WAL) FirstIndex() int64 {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.firstIndex
}

// LastIndex returns the write-ahead logs first index
func (l *WAL) LastIndex() int64 {
	// lock
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.lastIndex
}

// Close syncs and closes the write-ahead log
func (l *WAL) Close() error {
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

// String is the stringer method for the write-ahead log
func (l *WAL) String() string {
	var ss string
	ss += fmt.Sprintf("\n\n[write-ahead log]\n")
	ss += fmt.Sprintf("base: %q\n", l.conf.BasePath)
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
