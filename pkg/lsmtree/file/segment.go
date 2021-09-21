package file

import (
	"errors"
	"fmt"
	"github.com/scottcagno/storage/pkg/binary"
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
	defaultMaxFileSize int64 = 4 << 20 // 4 MB
)

var (
	maxFileSize       = defaultMaxFileSize
	ErrOutOfBounds    = errors.New("error: out of bounds")
	ErrSegmentFull    = errors.New("error: Segment is full")
	ErrFileClosed     = errors.New("error: file closed")
	ErrBadArgument    = errors.New("error: bad argument")
	ErrNoPathProvided = errors.New("error: no path provided")
	ErrOptionsMissing = errors.New("error: options missing")
)

// entry contains the metadata for a single entry within the file Segment
type entry struct {
	index  int64 // index is the "id" of this entry
	offset int64 // offset is the actual offset of this entry in the Segment file
}

// String is the stringer method for an entry
func (e entry) String() string {
	return fmt.Sprintf("entry.index=%d, entry.offset=%d", e.index, e.offset)
}

// Segment contains the metadata for the file Segment
type Segment struct {
	path        string  // path is the full path to this Segment file
	index       int64   // starting index of the Segment
	entries     []entry // entries is an index of the entries in the Segment
	firstOffset int64
	lastOffset  int64
	remaining   int64 // remaining is the bytes left after max file size minus entry data
}

// String is the stringer method for a Segment
func (s *Segment) String() string {
	var ss string
	ss += fmt.Sprintf("path: %q\n", filepath.Base(s.path))
	ss += fmt.Sprintf("index: %d\n", s.index)
	ss += fmt.Sprintf("entries: %d\n", len(s.entries))
	ss += fmt.Sprintf("remaining: %d\n", s.remaining)
	return ss
}

// getFirstIndex returns the first index in the entries list
func (s *Segment) getFirstIndex() int64 {
	return s.index
}

// getLastIndex returns the last index in the entries list
func (s *Segment) getLastIndex() int64 {
	if len(s.entries) > 0 {
		return s.entries[len(s.entries)-1].index
	}
	return s.index
}

// findEntryIndex performs binary search to find the entry containing provided index
func (s *Segment) findEntryIndex(index int64) int {
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

// SegmentIndex is a segmented file structure
type SegmentIndex struct {
	lock       sync.RWMutex   // lock is a mutual exclusion lock
	base       string         // base is the base filepath
	r          *binary.Reader // r is a binary reader
	w          *binary.Writer // w is a binary writer
	firstIndex int64          // firstIndex is the index of the first entry
	lastIndex  int64          // lastIndex is the index of the last entry
	segments   []*Segment     // segments is an index of the current file segments
	active     *Segment       // active is the current active Segment
}

// Open opens and returns a new segmented file structure
func Open(path string) (*SegmentIndex, error) {
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
	// create a new segmented file instance
	sf := &SegmentIndex{
		base:       base,
		firstIndex: 0,
		lastIndex:  1,
		segments:   make([]*Segment, 0),
	}
	// attempt to load segments
	err = sf.loadSegmentIndex()
	if err != nil {
		return nil, err
	}
	// return segmented file
	return sf, nil
}

// loadIndex initializes the Segment index. It looks for Segment
// files in the base directory and attempts to index the Segment as
// well as any of the entries within the Segment. If this is a new
// instance, it will create a new Segment that is ready for writing.
func (sf *SegmentIndex) loadSegmentIndex() error {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	// get the files in the base directory path
	files, err := os.ReadDir(sf.base)
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
		// attempt to load Segment (and index entries in Segment)
		s, err := OpenSegment2(filepath.Join(sf.base, file.Name()))
		if err != nil {
			return err
		}
		// Segment has been loaded successfully, append to the segments list
		sf.segments = append(sf.segments, s)
	}
	// check to see if any segments were found. If not, initialize a new one
	if len(sf.segments) == 0 {
		// create a new Segment file
		s, err := CreateSegment(sf.base, sf.lastIndex)
		if err != nil {
			return err
		}
		// Segment has been created successfully, append to the segments list
		sf.segments = append(sf.segments, s)
	}
	// segments have either been loaded or created, so now we
	// should go about updating the active Segment pointer to
	// point to the "tail" (the last Segment in the Segment list)
	sf.active = sf.getLastSegment()
	// load active Segment entry index
	sf.active.loadEntryIndex()
	// we should be good to go, lets attempt to open a file
	// reader to work with the active Segment
	sf.r, err = binary.OpenReader(sf.active.path)
	if err != nil {
		return err
	}
	// and then attempt to open a file writer to also work
	// with the active Segment, so we can begin appending data
	sf.w, err = binary.OpenWriter(sf.active.path)
	if err != nil {
		return err
	}
	// finally, update the firstIndex and lastIndex
	sf.firstIndex = sf.segments[0].index
	// and update last index
	sf.lastIndex = sf.getLastSegment().getLastIndex()
	return nil
}

// OpenSegment attempts to open the Segment file at the path provided
// and index the entries within the Segment. It will return an os.PathError
// if the file does not exist, an io.ErrUnexpectedEOF if the file exists
// but is empty and has no data to read, and ErrSegmentFull if the file
// has met the maxFileSize. It will return the Segment and nil error on success.
func OpenSegment(path string) (*Segment, error) {
	// check to make sure path exists before continuing
	_, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	// attempt to open existing Segment file for reading
	fd, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	// defer file close
	defer func(fd *os.File) {
		_ = fd.Close()
	}(fd)
	// get Segment index
	index, err := readSegmentIndex(filepath.Base(path))
	if err != nil {
		return nil, err
	}
	// create a new Segment to append indexed entries to
	s := &Segment{
		path:    path,
		index:   index,
		entries: make([]entry, 0),
	}
	// read Segment file and index entries
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
		// add entry index to Segment entries list
		s.entries = append(s.entries, entry{
			index:  e.Id,
			offset: offset,
		})
		// continue to process the next entry
	}
	// make sure to fill out the Segment index from the first entry index
	//s.index = s.entries[0].index
	// get the offset of the reader to calculate bytes remaining
	offset, err := binary.Offset(fd)
	if err != nil {
		return nil, err
	}
	// update the Segment remaining bytes
	s.remaining = maxFileSize - offset
	return s, nil
}

func OpenSegment2(path string) (*Segment, error) {
	// check to make sure path exists before continuing
	_, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	// get Segment index
	index, err := readSegmentIndex(filepath.Base(path))
	if err != nil {
		return nil, err
	}
	// create a new Segment to append indexed entries to
	s := &Segment{
		path:    path,
		index:   index,
		entries: make([]entry, 0),
	}
	//offset, err := loadEntryIndex(s)
	//if err != nil {
	//	return nil, err
	//}
	// update the Segment remaining bytes
	//s.remaining = maxFileSize - uint64(offset)
	return s, nil
}

func (s *Segment) loadEntryIndex() (int64, error) {
	// attempt to open existing Segment file for reading
	fd, err := os.OpenFile(s.path, os.O_RDONLY, 0666)
	if err != nil {
		return -1, err
	}
	// defer file close
	defer func(fd *os.File) {
		_ = fd.Close()
	}(fd)
	// read Segment file and index entries
	for {
		// get the current offset of the
		// reader for the entry later
		offset, err := binary.Offset(fd)
		if err != nil {
			return -1, err
		}
		// read and decode entry
		e, err := binary.DecodeEntry(fd)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return -1, err
		}
		// add entry index to Segment entries list
		s.entries = append(s.entries, entry{
			index:  e.Id,
			offset: offset,
		})
		// continue to process the next entry
	}
	// return offset
	offset, err := binary.Offset(fd)
	if err != nil {
		return -1, err
	}
	return offset, nil
}

func makeSegmentName(index int64) string {
	hexa := strconv.FormatInt(index, 16)
	return fmt.Sprintf("%s%010s%s", FilePrefix, hexa, FileSuffix)
}

func readSegmentIndex(name string) (int64, error) {
	hexa := name[len(FilePrefix) : len(name)-len(FileSuffix)]
	return strconv.ParseInt(hexa, 16, 32)
}

// CreateSegment attempts to make a new Segment automatically using the timestamp
// as the Segment name. On success, it will simply return a new Segment and a nil error
func CreateSegment(base string, lastIndex int64) (*Segment, error) {
	// create a new file
	path := filepath.Join(base, makeSegmentName(lastIndex))
	fd, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	// don't forget to close it
	err = fd.Close()
	if err != nil {
		return nil, err
	}
	// create and return new Segment
	s := &Segment{
		path:      path,
		index:     lastIndex,
		entries:   make([]entry, 0),
		remaining: maxFileSize,
	}
	return s, nil
}

func (sf *SegmentIndex) LoadSegment(index int64) (*Segment, error) {
	s := sf.active
	if index >= s.index {
		return s, nil
	}
	s = sf.segments[sf.findSegmentIndex(index)]
	if len(s.entries) == 0 {
		_, err := s.loadEntryIndex()
		if err != nil {
			return nil, err
		}
	}
	sf.active = s
	return s, nil
}

// findSegmentIndex performs binary search to find the Segment containing provided index
func (sf *SegmentIndex) findSegmentIndex(index int64) int {
	// declare for later
	i, j := 0, len(sf.segments)
	// otherwise, perform binary search
	for i < j {
		h := i + (j-i)/2
		if index >= sf.segments[h].index {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

// getLastSegment returns the tail Segment in the segments index list
func (sf *SegmentIndex) getLastSegment() *Segment {
	return sf.segments[len(sf.segments)-1]
}

// cycleSegment adds a new Segment to replace the current (active) Segment
func (sf *SegmentIndex) cycleSegment(remaining int64) error {
	// check to see if we need to cycle
	if remaining > 0 {
		return nil
	}
	// sync and close current file Segment
	err := sf.w.Close()
	if err != nil {
		return err
	}
	// create a new Segment file
	s, err := CreateSegment(sf.base, sf.lastIndex)
	if err != nil {
		return err
	}
	// add Segment to Segment index list
	sf.segments = append(sf.segments, s)
	// update the active Segment pointer
	sf.active = sf.getLastSegment()
	// open file writer associated with active Segment
	sf.w, err = binary.OpenWriter(sf.active.path)
	if err != nil {
		return err
	}
	// update file reader associated with the active Segment
	sf.r, err = binary.OpenReader(sf.active.path)
	if err != nil {
		return err
	}
	return nil
}

// Read reads an entry from the segmented file at the specified index
func (sf *SegmentIndex) Read(index int64) (string, []byte, error) {
	// read lock
	sf.lock.RLock()
	defer sf.lock.RUnlock()
	// error checking
	if index < sf.firstIndex || index > sf.lastIndex {
		return "", nil, ErrOutOfBounds
	}
	var err error
	// find the Segment containing the provided index
	//s := sf.segments[sf.findSegmentIndex(index)]
	s, err := sf.LoadSegment(index)
	if err != nil {
		return "", nil, err
	}
	// make sure we are reading from the correct file
	sf.r, err = sf.r.ReadFrom(s.path)
	if err != nil {
		return "", nil, err
	}
	// find the offset for the entry containing the provided index
	offset := s.entries[s.findEntryIndex(index)].offset
	// read entry at offset
	e, err := sf.r.ReadEntryAt(offset)
	if err != nil {
		return "", nil, err
	}
	return string(e.Key), e.Value, nil
}

// WriteEntry writes an entry to the segmented file in an append-only fashion
func (sf *SegmentIndex) Write(key string, value []byte) (int64, error) {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	// write entry
	offset, err := sf.w.WriteEntry(&binary.Entry{
		Id:    sf.lastIndex,
		Key:   []byte(key),
		Value: value,
	})
	if err != nil {
		return 0, err
	}
	// add new entry to the Segment index
	sf.active.entries = append(sf.active.entries, entry{
		index:  sf.lastIndex,
		offset: offset,
	})
	// update lastIndex
	sf.lastIndex++
	// grab the current offset written
	offset2, err := sf.w.Offset()
	if err != nil {
		return 0, err
	}
	// update Segment remaining
	sf.active.remaining -= offset2 - offset
	// check to see if the active Segment needs to be cycled
	if sf.active.remaining < 64 {
		err = sf.cycleSegment(int64(sf.active.remaining - 64))
		if err != nil {
			return 0, err
		}
	}
	return sf.lastIndex - 1, nil
}

// Write2 writes an entry to the segmented file in an append-only fashion
func (sf *SegmentIndex) Write2(key string, value []byte) (int64, error) {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	// write entry
	offset, err := sf.w.WriteEntry(&binary.Entry{
		Id:    sf.lastIndex,
		Key:   []byte(key),
		Value: value,
	})
	if err != nil {
		return -1, err
	}
	// add new entry to the Segment index
	sf.active.entries = append(sf.active.entries, entry{
		index:  sf.lastIndex,
		offset: offset,
	})
	// update lastIndex
	sf.lastIndex++
	// get updated offset to check cycle
	offset, err = sf.w.Offset()
	if err != nil {
		return -1, err
	}
	// check to see if the active Segment needs to be cycled
	err = sf.cycleSegment(int64(maxFileSize) - offset)
	if err != nil {
		return -1, err
	}
	return sf.lastIndex - 1, nil
}

// Scan provides an iterator method for the segmented file
func (sf *SegmentIndex) Scan(iter func(index int64, key string, value []byte) bool) error {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	// init for any errors
	var err error
	// range the Segment index
	for _, sidx := range sf.segments {
		fmt.Printf("Segment: %s\n", sidx)
		// make sure we are reading the right data
		sf.r, err = sf.r.ReadFrom(sidx.path)
		if err != nil {
			return err
		}
		// range the Segment entries index
		for _, eidx := range sidx.entries {
			// read entry
			e, err := sf.r.ReadEntryAt(eidx.offset)
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
	// outside Segment loop
	return nil
}

// TruncateFront removes all segments and entries before specified index
func (sf *SegmentIndex) TruncateFront(index int64) error {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	// perform bounds check
	if index == 0 ||
		sf.lastIndex == 0 ||
		index < sf.firstIndex || index > sf.lastIndex {
		return ErrOutOfBounds
	}
	if index == sf.firstIndex {
		return nil // nothing to truncate
	}
	// locate Segment in Segment index list containing specified index
	sidx := sf.findSegmentIndex(index)
	// isolate whole segments that can be removed
	for i := 0; i < sidx; i++ {
		// remove Segment file
		err := os.Remove(sf.segments[i].path)
		if err != nil {
			return err
		}
	}
	// remove segments from Segment index (cut, i-j)
	i, j := 0, sidx
	copy(sf.segments[i:], sf.segments[j:])
	for k, n := len(sf.segments)-j+i, len(sf.segments); k < n; k++ {
		sf.segments[k] = nil // or the zero value of T
	}
	sf.segments = sf.segments[:len(sf.segments)-j+i]
	// update firstIndex
	sf.firstIndex = sf.segments[0].index
	// prepare to re-write partial Segment
	var err error
	var entries []entry
	tmpfd, err := os.Create(filepath.Join(sf.base, "tmp-partiasf.seg"))
	if err != nil {
		return err
	}
	// after the Segment index cut, Segment 0 will
	// contain the partials that we must re-write
	if sf.segments[0].index < index {
		// make sure we are reading from the correct path
		sf.r, err = sf.r.ReadFrom(sf.segments[0].path)
		if err != nil {
			return err
		}
		// range the entries within this Segment to find
		// the ones that are greater than the index and
		// write those to a temporary buffer....
		for _, ent := range sf.segments[0].entries {
			if ent.index < index {
				continue // skip
			}
			// read entry
			e, err := sf.r.ReadEntryAt(ent.offset)
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
		// move reader back to active Segment file
		sf.r, err = sf.r.ReadFrom(sf.active.path)
		if err != nil {
			return err
		}
		// close temp file
		err = tmpfd.Close()
		if err != nil {
			return err
		}
		// remove partial Segment file
		err = os.Remove(sf.segments[0].path)
		if err != nil {
			return err
		}
		// change temp file name
		err = os.Rename(tmpfd.Name(), sf.segments[0].path)
		if err != nil {
			return err
		}
		// update Segment
		sf.segments[0].entries = entries
		sf.segments[0].index = entries[0].index
	}
	return nil
}

func (sf *SegmentIndex) TruncateBack(index int64) error {
	// TODO: implement
	return nil
}

// Sort (stable) sorts entries (and re-writes them) in forward or reverse Lexicographic order
func (sf *SegmentIndex) Sort() error {
	// TODO: implement
	return nil
}

// CompactAndMerge removes any blank sections or duplicate entries and then merges (re-writes)
// the data into a different Segment size using the maxSegSize provided
func (sf *SegmentIndex) CompactAndMerge(maxSegSize int64) error {
	// TODO: implement
	return nil
}

// Count returns the number of entries currently in the segmented file
func (sf *SegmentIndex) Count() int {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	// get count
	var count int
	for _, s := range sf.segments {
		count += len(s.entries)
	}
	// return count
	return count
}

// FirstIndex returns the segmented files first index
func (sf *SegmentIndex) FirstIndex() int64 {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	return sf.firstIndex
}

// LastIndex returns the segmented files first index
func (sf *SegmentIndex) LastIndex() int64 {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	return sf.lastIndex
}

// Close syncs and closes the segmented file
func (sf *SegmentIndex) Close() error {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	// sync and close writer
	err := sf.w.Close()
	if err != nil {
		return err
	}
	// close reader
	err = sf.r.Close()
	if err != nil {
		return err
	}
	// clean everything else up
	sf.base = ""
	sf.r = nil
	sf.w = nil
	sf.firstIndex = 0
	sf.lastIndex = 0
	sf.segments = nil
	sf.active = nil
	// force gc for good measure
	runtime.GC()
	return nil
}

func (sf *SegmentIndex) Path() string {
	return sf.base
}

// String is the stringer method for the segmented file
func (sf *SegmentIndex) String() string {
	var ss string
	ss += fmt.Sprintf("\n\n[segmented file]\n")
	ss += fmt.Sprintf("base: %q\n", sf.base)
	ss += fmt.Sprintf("firstIndex: %d\n", sf.firstIndex)
	ss += fmt.Sprintf("lastIndex: %d\n", sf.lastIndex)
	ss += fmt.Sprintf("segments: %d\n", len(sf.segments))
	if sf.active != nil {
		ss += fmt.Sprintf("active: %q\n", filepath.Base(sf.active.path))
	}
	if len(sf.segments) > 0 {
		for i, s := range sf.segments {
			ss += fmt.Sprintf("Segment[%d]:\n", i)
			ss += fmt.Sprintf("\tpath: %q\n", filepath.Base(s.path))
			ss += fmt.Sprintf("\tindex: %d\n", s.index)
			ss += fmt.Sprintf("\tentries: %d\n", len(s.entries))
			ss += fmt.Sprintf("\tremaining: %d\n", s.remaining)
		}
	}
	ss += "\n"
	return ss
}
