package v3

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/scottcagno/storage/pkg/common"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrCorrupt    = errors.New("log corrupt")
	ErrClosed     = errors.New("log closed")
	ErrNotFound   = errors.New("not found")
	ErrOutOfOrder = errors.New("out of order")
	ErrOutOfRange = errors.New("out of range")
	ErrEmpty      = errors.New("empty or nil value")
)

type WAL struct {
	mu         sync.RWMutex
	path       string
	closed     bool
	firstIndex uint64
	lastIndex  uint64
	segfile    *os.File
	segments   []*segment
	segcache   *segment
}

func Open(path string) (*WAL, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	wal := &WAL{
		path:   path,
		closed: true,
	}
	err = wal.load()
	if err != nil {
		return nil, err
	}
	return wal, nil
}

func (wal *WAL) load() error {
	// check to see if directory exists
	_, err := os.Stat(wal.path)
	if os.IsNotExist(err) {
		// create it if it does not exist
		err = os.MkdirAll(wal.path, os.ModeDir)
		if err != nil {
			return err
		}
	}
	// get the files in the main directory path
	files, err := os.ReadDir(wal.path)
	if err != nil {
		return err
	}
	// list files in the main directory path and attempt to load segments
	for _, file := range files {
		// skip files that are not log segments
		if file.IsDir() ||
			!strings.HasPrefix(file.Name(), "wal-") &&
				!strings.HasSuffix(file.Name(), ".seg") {
			continue
		}
		// validate log segment file index
		index, err := wal.validateLogSegment(file.Name())
		if err != nil {
			return err
		}
		if index == 0 {
			continue
		}
		// add valid log segments to the log segment cache
		wal.segments = append(wal.segments, &segment{
			index: index,
			path:  filepath.Join(wal.path, file.Name()),
		})
	}
	// if no segments, create a new log segment and return
	if len(wal.segments) == 0 {
		wal.segments = append(wal.segments, &segment{
			index: 1,
			path:  filepath.Join(wal.path, segmentName(1)),
		})
		wal.segfile, err = os.Create(wal.segments[0].path)
		if err != nil {
			return err
		}
		// write log segment header
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, wal.segments[0].index)
		_, err = wal.segfile.Write(buf)
		return err
	}
	wal.firstIndex = wal.firstSegment().index
	// open last log segment for appending
	lastseg := wal.lastSegment()
	wal.segfile, err = os.OpenFile(lastseg.path, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	_, err = wal.segfile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	// load last segment entry spans
	err = wal.loadSegmentSpans(lastseg)
	if err != nil {
		return err
	}
	wal.lastIndex = lastseg.index + uint64(len(lastseg.spans)) - 1
	if wal.segcache == nil {
		wal.segcache = wal.lastSegment()
	}
	return nil
}

func (wal *WAL) Read() ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	// read entry length
	var buf [8]byte
	_, err := io.ReadFull(wal.segfile, buf[:])
	if err != nil {
		return nil, err
	}
	// decode entry length
	elen := binary.LittleEndian.Uint64(buf[:])
	// make byte slice of entry length size
	entry := make([]byte, elen)
	// read entry from reader into slice
	_, err = io.ReadFull(wal.segfile, entry)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (wal *WAL) ReadEntry(index uint64) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	// save current position in case we need it later
	pos1, err := wal.segfile.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	// make sure provided index is within the bounds
	if index > uint64(len(wal.segcache.spans))+1 {
		return nil, ErrOutOfRange
	}
	// span represents the offset and length of entry in log segment at index provided
	span := wal.segcache.spans[index-1]
	// make byte slice of span length
	entry := make([]byte, span.end-span.start)
	// read entry from file into "entry buffer"
	_, err = wal.segfile.ReadAt(entry, int64(span.start))
	if err != nil {
		return nil, err
	}
	// check the file cursor
	pos2, err := wal.segfile.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	if pos1 != pos2 {
		common.DEBUG("RESETTING FILE CURSOR", fmt.Sprintf("pos1: %d, pos2: %d", pos1, pos2))
		_, err = wal.segfile.Seek(pos1, io.SeekStart)
		if err != nil {
			return nil, err
		}
	}
	return entry, nil
}

func (wal *WAL) Write(data []byte) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	// encode entry length
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(len(data)))
	// write entry length
	_, err := wal.segfile.Write(buf[:])
	if err != nil {
		return err
	}
	// write entry data
	_, err = wal.segfile.Write(data)
	if err != nil {
		return err
	}
	// add new span to log segment
	err = wal.addSegmentSpan(wal.segcache, uint64(len(data)))
	if err != nil {
		return err
	}
	return nil
}

func (wal *WAL) Seek(offset int64, whence int) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	_, err := wal.segfile.Seek(offset, whence)
	return err
}

func (wal *WAL) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	return wal.segfile.Sync()
}

func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	err := wal.segfile.Sync()
	if err != nil {
		return err
	}
	err = wal.segfile.Close()
	if err != nil {
		return err
	}
	wal.closed = true
	return nil
}

func (wal *WAL) validateLogSegment(fileName string) (uint64, error) {
	// get log segment file index
	index, err := strconv.ParseUint(fileName[4:24], 10, 64)
	if err != nil || index == 0 {
		return 0, err
	}
	// open log segment file and read header to validate the index
	fd, err := os.Open(filepath.Join(wal.path, fileName))
	if err != nil {
		return 0, err
	}
	header := make([]byte, 8)
	_, err = io.ReadFull(fd, header)
	if err != nil {
		fd.Close()
		return 0, err
	}
	// validate log segment header...
	hindex := binary.LittleEndian.Uint64(header)
	if index != hindex {
		// we have some kind of corruption
		fd.Close()
		return 0, ErrCorrupt
	}
	// make sure to close file--didn't want to defer in a loop
	err = fd.Close()
	if err != nil {
		return 0, err
	}
	return index, nil
}

func (wal *WAL) firstSegment() *segment {
	return wal.segments[0]
}

func (wal *WAL) lastSegment() *segment {
	return wal.segments[len(wal.segments)-1]
}

func (wal *WAL) loadSegmentSpans(s *segment) error {
	// open log segment file
	fd, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer fd.Close()
	// skip log segment header
	_, err = fd.Seek(8, io.SeekStart)
	if err != nil {
		return err
	}
	// iterate log segment file entries and populate spans

	for {
		// read entry length
		var buf [8]byte
		_, err := io.ReadFull(fd, buf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// decode entry length
		elen := binary.LittleEndian.Uint64(buf[:])
		pos, err := fd.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		// add entry span to segment
		s.spans = append(s.spans, span{
			start: uint64(pos),
			end:   uint64(pos) + elen,
		})
		// skip to next entry
		_, err = fd.Seek(int64(elen), io.SeekCurrent)
		if err != nil {
			return err
		}
	}
	return nil
}

func (wal *WAL) addSegmentSpan(s *segment, datalen uint64) error {
	// get current position
	offset, err := wal.segfile.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	s.spans = append(s.spans, span{
		start: uint64(offset),
		end:   uint64(offset) + datalen,
	})
	return nil
}

func (wal *WAL) String() string {
	s := fmt.Sprintf("path: %T, %s\n", wal.path, wal.path)
	s += fmt.Sprintf("closed: %T, %v\n", wal.closed, wal.closed)
	s += fmt.Sprintf("firstIndex: %T, %d\n", wal.firstIndex, wal.firstIndex)
	s += fmt.Sprintf("lastIndex: %T, %d\n", wal.lastIndex, wal.lastIndex)
	s += fmt.Sprintf("segfile: %T, %s\n", wal.segfile, wal.segfile.Name())
	s += fmt.Sprintf("segcache: %T, %s\n", wal.segcache, wal.segcache)
	s += fmt.Sprintf("segments: %T, %s\n", wal.segments, wal.segments)
	return s
}
