package sfile

import (
	"errors"
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

const (
	FilePrefix               = "dat-"
	FileSuffix               = ".seg"
	defaultMaxFileSize int64 = 4 << 20 // 4 MB
)

var (
	maxFileSize       = defaultMaxFileSize
	ErrOutOfBounds    = errors.New("error: out of bounds")
	ErrSegmentFull    = errors.New("error: FileSegment is full")
	ErrFileClosed     = errors.New("error: file closed")
	ErrBadArgument    = errors.New("error: bad argument")
	ErrNoPathProvided = errors.New("error: no path provided")
	ErrOptionsMissing = errors.New("error: options missing")
)

// entry contains the metadata for a single entry within the file FileSegment
type entry struct {
	index  int64 // index is the "id" of this entry
	offset int64 // offset is the actual offset of this entry in the FileSegment file
}

// String is the stringer method for an entry
func (e entry) String() string {
	return fmt.Sprintf("entry.index=%d, entry.offset=%d", e.index, e.offset)
}

// FileSegment contains the metadata for the file FileSegment
type FileSegment struct {
	path        string  // path is the full path to this FileSegment file
	index       int64   // starting index of the FileSegment
	entries     []entry // entries is an index of the entries in the FileSegment
	firstOffset int64
	lastOffset  int64
	remaining   int64 // remaining is the bytes left after max file size minus entry data
}

// OpenSegment attempts to open the FileSegment file at the path provided
// and index the entries within the FileSegment. It will return an os.PathError
// if the file does not exist, an io.ErrUnexpectedEOF if the file exists
// but is empty and has no data to read, and ErrSegmentFull if the file
// has met the maxFileSize. It will return the FileSegment and nil error on success.
func _OpenSegment(path string) (*FileSegment, error) {
	// check to make sure path exists before continuing
	_, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	// attempt to open existing FileSegment file for reading
	fd, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	// defer file close
	defer func(fd *os.File) {
		_ = fd.Close()
	}(fd)
	// get FileSegment index
	index, err := GetIndexFromFileName(filepath.Base(path))
	if err != nil {
		return nil, err
	}
	// create a new FileSegment to append indexed entries to
	s := &FileSegment{
		path:    path,
		index:   index,
		entries: make([]entry, 0),
	}
	// read FileSegment file and index entries
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
		// add entry index to FileSegment entries list
		s.entries = append(s.entries, entry{
			index:  e.Id,
			offset: offset,
		})
		// continue to process the next entry
	}
	// make sure to fill out the FileSegment index from the first entry index
	//s.index = s.entries[0].index
	// get the offset of the reader to calculate bytes remaining
	offset, err := binary.Offset(fd)
	if err != nil {
		return nil, err
	}
	// update the FileSegment remaining bytes
	s.remaining = maxFileSize - offset
	return s, nil
}

// getFirstIndex returns the first index in the entries list
func (s *FileSegment) getFirstIndex() int64 {
	return s.index
}

// getLastIndex returns the last index in the entries list
func (s *FileSegment) getLastIndex() int64 {
	if len(s.entries) > 0 {
		return s.entries[len(s.entries)-1].index
	}
	return s.index
}

// findEntryIndex performs binary search to find the entry containing provided index
func (s *FileSegment) findEntryIndex(index int64) int {
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

func OpenSegment(path string) (*FileSegment, error) {
	// check to make sure path exists before continuing
	_, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	// get FileSegment index
	index, err := GetIndexFromFileName(filepath.Base(path))
	if err != nil {
		return nil, err
	}
	// create a new FileSegment to append indexed entries to
	s := &FileSegment{
		path:    path,
		index:   index,
		entries: make([]entry, 0),
	}
	return s, nil
}

func (s *FileSegment) WriteDataEntry(e *binary.Entry) (int64, error) {
	// check to see if the entries are loaded
	if !s.hasEntriesLoaded() {
		// load the entry index
		_, err := s.loadEntryIndex()
		if err != nil {
			return -1, err
		}
	}
	// open writer
	w, err := binary.OpenWriter(s.path)
	if err != nil {
		return -1, err
	}
	defer w.Close()
	// write entry
	offset, err := w.WriteEntry(e)
	if err != nil {
		return -1, err
	}
	// get "last index" TODO: might be a potential bug here
	//lastIndex := s.entries[len(s.entries)-1].index
	// add new entry to the entry index
	s.entries = append(s.entries, entry{
		index:  de.Id, // DataEntry.Id should == last index
		offset: offset,
	})
	// return offset, and nil
	return offset, nil
}

func (s *FileSegment) ReadDataEntry(index int64) (*binary.DataEntry, error) {
	// check to see if the entries are loaded
	if !s.hasEntriesLoaded() {
		// load the entry index
		_, err := s.loadEntryIndex()
		if err != nil {
			return nil, err
		}
	}
	// open reader
	r, err := binary.OpenReader(s.path)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	// find correct entry offset to read from
	offset := s.entries[s.findEntryIndex(index)].offset
	// attempt to read entry at offset
	de, err := r.ReadEntryAt(offset)
	if err != nil {
		return nil, err
	}
	// return entry
	return de, nil
}

func (s *FileSegment) hasEntriesLoaded() bool {
	return len(s.entries) > 0
}

func (s *FileSegment) loadEntryIndex() (int64, error) {
	// attempt to open existing FileSegment file for reading
	fd, err := os.OpenFile(s.path, os.O_RDONLY, 0666)
	if err != nil {
		return -1, err
	}
	// defer file close
	defer func(fd *os.File) {
		_ = fd.Close()
	}(fd)
	// read FileSegment file and index entries
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
		// add entry index to FileSegment entries list
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

func MakeFileNameFromIndex(index int64) string {
	hexa := strconv.FormatInt(index, 16)
	return fmt.Sprintf("%s%010s%s", FilePrefix, hexa, FileSuffix)
}

func GetIndexFromFileName(name string) (int64, error) {
	hexa := name[len(FilePrefix) : len(name)-len(FileSuffix)]
	return strconv.ParseInt(hexa, 16, 32)
}

// CreateSegment attempts to make a new FileSegment automatically using the timestamp
// as the FileSegment name. On success, it will simply return a new FileSegment and a nil error
func CreateSegment(base string, lastIndex int64) (*FileSegment, error) {
	// create a new file
	path := filepath.Join(base, MakeFileNameFromIndex(lastIndex))
	fd, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	// don't forget to close it
	err = fd.Close()
	if err != nil {
		return nil, err
	}
	// create and return new FileSegment
	s := &FileSegment{
		path:      path,
		index:     lastIndex,
		entries:   make([]entry, 0),
		remaining: maxFileSize,
	}
	return s, nil
}

// String is the stringer method for a FileSegment
func (s *FileSegment) String() string {
	var ss string
	ss += fmt.Sprintf("path: %q\n", filepath.Base(s.path))
	ss += fmt.Sprintf("index: %d\n", s.index)
	ss += fmt.Sprintf("entries: %d\n", len(s.entries))
	ss += fmt.Sprintf("remaining: %d\n", s.remaining)
	return ss
}
