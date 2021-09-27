package sfile

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmtree/encoding/binary"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// SegmentedFile is a segmented file structure
type SegmentedFile struct {
	lock       sync.RWMutex   // lock is a mutual exclusion lock
	base       string         // base is the base filepath
	r          *binary.Reader // r is a binary reader
	w          *binary.Writer // w is a binary writer
	firstIndex int64          // firstIndex is the index of the first entry
	lastIndex  int64          // lastIndex is the index of the last entry
	segments   []*FileSegment // segments is an index of the current file segments
	active     *FileSegment   // active is the current active FileSegment
}

// Open opens and returns a new segmented file structure
func Open(path string) (*SegmentedFile, error) {
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
	sf := &SegmentedFile{
		base:       base,
		firstIndex: 0,
		lastIndex:  1,
		segments:   make([]*FileSegment, 0),
	}
	// attempt to load segments
	err = sf.loadSegmentIndex()
	if err != nil {
		return nil, err
	}
	// return segmented file
	return sf, nil
}

// loadIndex initializes the FileSegment index. It looks for FileSegment
// files in the base directory and attempts to index the FileSegment as
// well as any of the entries within the FileSegment. If this is a new
// instance, it will create a new FileSegment that is ready for writing.
func (sf *SegmentedFile) loadSegmentIndex() error {
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
		// attempt to load FileSegment (and index entries in FileSegment)
		s, err := OpenSegment(filepath.Join(sf.base, file.Name()))
		if err != nil {
			return err
		}
		// FileSegment has been loaded successfully, append to the segments list
		sf.segments = append(sf.segments, s)
	}
	// check to see if any segments were found. If not, initialize a new one
	if len(sf.segments) == 0 {
		// create a new FileSegment file
		s, err := CreateSegment(sf.base, sf.lastIndex)
		if err != nil {
			return err
		}
		// FileSegment has been created successfully, append to the segments list
		sf.segments = append(sf.segments, s)
	}
	// segments have either been loaded or created, so now we
	// should go about updating the active FileSegment pointer to
	// point to the "tail" (the last FileSegment in the FileSegment list)
	sf.active = sf.getLastSegment()
	// load active FileSegment entry index
	sf.active.loadEntryIndex()
	// we should be good to go, lets attempt to open a file
	// reader to work with the active FileSegment
	sf.r, err = binary.OpenReader(sf.active.path)
	if err != nil {
		return err
	}
	// and then attempt to open a file writer to also work
	// with the active FileSegment, so we can begin appending data
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

func (sf *SegmentedFile) LoadSegment(index int64) (*FileSegment, error) {
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

// findSegmentIndex performs binary search to find the FileSegment containing provided index
func (sf *SegmentedFile) findSegmentIndex(index int64) int {
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

// getLastSegment returns the tail FileSegment in the segments index list
func (sf *SegmentedFile) getLastSegment() *FileSegment {
	return sf.segments[len(sf.segments)-1]
}

// cycleSegment adds a new FileSegment to replace the current (active) FileSegment
func (sf *SegmentedFile) cycleSegment2(err error) error {
	// check to see if we need to cycle
	if err == nil && err != ErrSegmentFull {
		return nil
	}
	// sync and close current file FileSegment
	err = sf.w.Close()
	if err != nil {
		return err
	}
	// create a new FileSegment file
	s, err := CreateSegment(sf.base, sf.lastIndex)
	if err != nil {
		return err
	}
	// add FileSegment to FileSegment index list
	sf.segments = append(sf.segments, s)
	// update the active FileSegment pointer
	sf.active = sf.getLastSegment()
	// open file writer associated with active FileSegment
	sf.w, err = binary.OpenWriter(sf.active.path)
	if err != nil {
		return err
	}
	// update file reader associated with the active FileSegment
	sf.r, err = binary.OpenReader(sf.active.path)
	if err != nil {
		return err
	}
	return nil
}

// cycleSegment adds a new FileSegment to replace the current (active) FileSegment
func (sf *SegmentedFile) cycleSegment(remaining int64) error {
	// check to see if we need to cycle
	if remaining > 0 {
		return nil
	}
	// sync and close current file FileSegment
	err := sf.w.Close()
	if err != nil {
		return err
	}
	// create a new FileSegment file
	s, err := CreateSegment(sf.base, sf.lastIndex)
	if err != nil {
		return err
	}
	// add FileSegment to FileSegment index list
	sf.segments = append(sf.segments, s)
	// update the active FileSegment pointer
	sf.active = sf.getLastSegment()
	// open file writer associated with active FileSegment
	sf.w, err = binary.OpenWriter(sf.active.path)
	if err != nil {
		return err
	}
	// update file reader associated with the active FileSegment
	sf.r, err = binary.OpenReader(sf.active.path)
	if err != nil {
		return err
	}
	return nil
}

// Read reads an entry from the segmented file at the specified index
func (sf *SegmentedFile) Read(index int64) (string, []byte, error) {
	// read lock
	sf.lock.RLock()
	defer sf.lock.RUnlock()
	// error checking
	if index < sf.firstIndex || index > sf.lastIndex {
		return "", nil, ErrOutOfBounds
	}
	var err error
	// find the FileSegment containing the provided index
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

// ReadDataEntryUsingSegment reads an entry from the segmented file at the specified index
func (sf *SegmentedFile) ReadDataEntryUsingSegment(index int64) (string, []byte, error) {
	// read lock
	sf.lock.RLock()
	defer sf.lock.RUnlock()
	// error checking
	if index < sf.firstIndex || index > sf.lastIndex {
		return "", nil, ErrOutOfBounds
	}
	var err error
	// find the FileSegment containing the provided index
	s, err := sf.LoadSegment(index)
	if err != nil {
		return "", nil, err
	}
	e, err := s.ReadDataEntry(index)
	if err != nil {
		return "", nil, err
	}
	return string(e.Key), e.Value, nil
}

// WriteIndexEntry writes an entry to the segmented file in an append-only fashion
func (sf *SegmentedFile) _Write(key string, value []byte) (int64, error) {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	// write entry
	e := &binary.DataEntry{
		Id:    sf.lastIndex,
		Key:   []byte(key),
		Value: value,
	}
	offset, err := sf.w.WriteEntry(e)
	if err != nil {
		return 0, err
	}
	// add new entry to the FileSegment index
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
	// update FileSegment remaining
	sf.active.remaining -= offset2 - offset
	// check to see if the active FileSegment needs to be cycled
	if sf.active.remaining < 64 {
		err = sf.cycleSegment(int64(sf.active.remaining - 64))
		if err != nil {
			return 0, err
		}
	}
	return sf.lastIndex - 1, nil
}

// Write2 writes an entry to the segmented file in an append-only fashion
func (sf *SegmentedFile) Write(key string, value []byte) (int64, error) {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	e := &binary.DataEntry{
		Id:    sf.lastIndex,
		Key:   []byte(key),
		Value: value,
	}
	// write entry
	offset, err := sf.w.WriteEntry(e)
	if err != nil {
		return -1, err
	}
	// add new entry to the FileSegment index
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
	// check to see if the active FileSegment needs to be cycled
	err = sf.cycleSegment(int64(maxFileSize) - offset)
	if err != nil {
		return -1, err
	}
	return sf.lastIndex - 1, nil
}

func (sf *SegmentedFile) WriteDataEntryUsingSegment(key string, value []byte) (int64, error) {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	e := &binary.DataEntry{
		Id:    sf.lastIndex,
		Key:   []byte(key),
		Value: value,
	}
	// write entry
	offset, err := sf.active.WriteDataEntry(e)
	if err != nil {
		return -1, err
	}
	// check cycle segment
	err = sf.cycleSegment(maxFileSize - offset + 64)
	// update lastIndex
	sf.lastIndex++
	// return index, and nil
	return sf.lastIndex - 1, nil
}

// Scan provides an iterator method for the segmented file
func (sf *SegmentedFile) Scan(iter func(index int64, key string, value []byte) bool) error {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	// init for any errors
	var err error
	// range the FileSegment index
	for _, sidx := range sf.segments {
		fmt.Printf("FileSegment: %s\n", sidx)
		// make sure we are reading the right data
		sf.r, err = sf.r.ReadFrom(sidx.path)
		if err != nil {
			return err
		}
		// range the FileSegment entries index
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
	// outside FileSegment loop
	return nil
}

// TruncateFront removes all segments and entries before specified index
func (sf *SegmentedFile) TruncateFront(index int64) error {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	// perform bounds check
	if index == 0 ||
		sf.lastIndex == 0 ||
		index < sf.firstIndex || index > sf.lastIndex {
		return ErrOutOfBounds
	}
	// more easy checking
	if index == sf.firstIndex {
		return nil // nothing to truncate
	}
	// locate segment in the segment index list containing specified index
	sidx := sf.findSegmentIndex(index)
	// remove all whole segments before index "sidx"
	for i := 0; i < sidx; i++ {
		// remove FileSegment file
		err := os.Remove(sf.segments[i].path)
		if err != nil {
			return err
		}
	}
	// remove segments from FileSegment index (cut, i-j)
	i, j := 0, sidx
	copy(sf.segments[i:], sf.segments[j:])
	for k, n := len(sf.segments)-j+i, len(sf.segments); k < n; k++ {
		sf.segments[k] = nil // or the zero value of T
	}
	sf.segments = sf.segments[:len(sf.segments)-j+i]
	// update firstIndex
	sf.firstIndex = sf.segments[0].index
	// prepare to re-write partial FileSegment
	//var err error
	tmpfd, err := os.Create(filepath.Join(sf.base,
		fmt.Sprintf("%stmp-part%s", FilePrefix, FileSuffix)))
	if err != nil {
		return err
	}
	// after the FileSegment index cut, FileSegment 0 will
	// contain the partials that we must re-write
	if sf.segments[0].index < index {
		// make sure we are reading from the correct path
		sf.r, err = sf.r.ReadFrom(sf.segments[0].path)
		if err != nil {
			return err
		}
		// init temp entries list
		var entries []entry
		// make sure entry index is loaded
		if !sf.segments[0].hasEntriesLoaded() {
			_, err := sf.segments[0].loadEntryIndex()
			if err != nil {
				return err
			}
		}
		// range the entries within this FileSegment to find
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
		// move reader back to active FileSegment file
		sf.r, err = sf.r.ReadFrom(sf.active.path)
		if err != nil {
			return err
		}
		// close temp file
		err = tmpfd.Close()
		if err != nil {
			return err
		}
		// remove partial FileSegment file
		err = os.Remove(sf.segments[0].path)
		if err != nil {
			return err
		}
		// change temp file name
		err = os.Rename(tmpfd.Name(), sf.segments[0].path)
		if err != nil {
			return err
		}
		// update FileSegment
		sf.segments[0].entries = entries
		sf.segments[0].index = entries[0].index
	}
	return nil
}

func (sf *SegmentedFile) TruncateBack(index int64) error {
	// TODO: implement
	return nil
}

// Sort (stable) sorts entries (and re-writes them) in forward or reverse Lexicographic order
func (sf *SegmentedFile) Sort() error {
	// TODO: implement
	return nil
}

// CompactAndMerge removes any blank sections or duplicate entries and then merges (re-writes)
// the data into a different FileSegment size using the maxSegSize provided
func (sf *SegmentedFile) CompactAndMerge(maxSegSize int64) error {
	// TODO: implement
	return nil
}

// Count returns the number of entries currently in the segmented file
func (sf *SegmentedFile) Count() int {
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

func (sf *SegmentedFile) Path() string {
	return sf.base
}

// FirstIndex returns the segmented files first index
func (sf *SegmentedFile) FirstIndex() int64 {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	return sf.firstIndex
}

// LastIndex returns the segmented files first index
func (sf *SegmentedFile) LastIndex() int64 {
	// lock
	sf.lock.Lock()
	defer sf.lock.Unlock()
	return sf.lastIndex
}

// Close syncs and closes the segmented file
func (sf *SegmentedFile) Close() error {
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

// String is the stringer method for the segmented file
func (sf *SegmentedFile) String() string {
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
			ss += fmt.Sprintf("FileSegment[%d]:\n", i)
			ss += fmt.Sprintf("\tpath: %q\n", filepath.Base(s.path))
			ss += fmt.Sprintf("\tindex: %d\n", s.index)
			ss += fmt.Sprintf("\tentries: %d\n", len(s.entries))
			ss += fmt.Sprintf("\tremaining: %d\n", s.remaining)
		}
	}
	ss += "\n"
	return ss
}
