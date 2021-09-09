package v2

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	maxFileSize = 16 << 10 // 16KB
	logPrefix   = "wal-"
	logSuffix   = ".seg"
)

// entry metadata for this entry within the segment
type entry struct {
	index  uint64
	offset uint64
}

// segment holds the metadata for this file segment
type segment struct {
	path    string  // full path to this segment file
	index   uint64  // starting index of this segment
	entries []entry // entry metadata for this segment
}

// Log represents a write-ahead log structure
type Log struct {
	mu       sync.RWMutex
	base     string     // base directory for the logs
	fd       *os.File   // file descriptor for the active log file
	fdOpen   bool       // true if the current file descriptor is open
	gindex   uint64     // this is the global index number or the next number in the sequence
	segments []*segment // each log file segment metadata
	active   int        // the active segment index
}

func Open(base string) (*Log, error) {
	// clean path and create directory structure
	base = clean(base)
	err := os.MkdirAll(base, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// create new file instance
	bf := &Log{
		base: base,
	}
	// attempt to load entries
	err = bf.load()
	if err != nil {
		return nil, err
	}
	return bf, nil
}

// load looks at the files in the base directory and iterates and
// instantiates any log segment files (and associated entries) it
// finds. If this is a new instance, it sets up an initial segment.
func (l *Log) load() error {
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
		// check file size
		fi, err := file.Info()
		if err != nil {
			return err
		}
		if fi.Size() < 1 {
			// if the file is empty skip loading
			continue
		}
		// attempt to load segment from file
		err = l.loadSegment(filepath.Join(l.base, file.Name()))
		if err != nil {
			return err
		}
		return nil
	}
	// if no segments were found, we need to initialize a new one
	if len(l.segments) == 0 {
		return l.addSegment()
	}
	// otherwise, we open the last entry
	l.useSegment()
	return nil
}

func (l *Log) loadSegment(path string) error {
	// open file to read
	fd, err := os.OpenFile(path, os.O_RDONLY, 0666) // os.ModeSticky
	if err != nil {
		return err
	}
	// defer close
	defer fd.Close()
	// init segment to fill out
	seg := &segment{
		path:    path,
		index:   l.gindex,
		entries: make([]entry, 0),
	}
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
			return err
		}
		// decode entry length
		elen := binary.LittleEndian.Uint64(hdr[:])
		// add entry to segment
		seg.entries = append(seg.entries, entry{l.gindex, offset})
		// skip to next entry
		n, err := fd.Seek(int64(elen), io.SeekCurrent)
		if err != nil {
			return err
		}
		offset = uint64(n) // update file pointer offset
		l.gindex++         // increment global index
	}
	// add new segment to segment list
	l.segments = append(l.segments, seg)
	return nil
}

func (l *Log) addSegment() error {
	// create filename for new segment
	name := fmt.Sprintf("%s%020d%s", logPrefix, len(l.segments), logSuffix)
	// add it to the current segment list
	l.segments = append(l.segments, &segment{
		path:    filepath.Join(l.base, name),
		index:   0,
		entries: []entry{{index: 0, offset: 0}},
	})
	// check to see if there is an open file
	if l.fdOpen {
		err := l.fd.Sync()
		if err != nil {
			return err
		}
		err = l.fd.Close()
		if err != nil {
			return err
		}
		l.fdOpen = false
	}
	// update the new active segment
	l.active = len(l.segments) - 1
	// create new segment file
	fd, err := os.OpenFile(l.segments[l.active].path, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	// update the file descriptor
	l.fd = fd
	l.fdOpen = true
	return err
}

func (l *Log) useSegment(index uint64) {}

func (s *segment) loadEntryMeta(fd *os.File, offset uint64) uint64 {
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
	// add entry to segment
	s.entries = append(s.entries, entry{
		index:  s.index,
		offset: offset,
	})
}
