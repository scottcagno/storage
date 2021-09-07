package v4

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type Log struct {
	mu       sync.RWMutex
	path     string     // full sanitized file path
	first    uint64     // first sequence number in the log
	seqnum   uint64     // seqnum is the latest sequence number
	active   int        // index of the active segment
	segments []*segment // list of segments
}

func Open(path string) (*Log, error) {
	// properly sanitize path
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	path = filepath.ToSlash(path)
	// create a new log instance
	wal := &Log{
		path:   path,
		first:  1,
		seqnum: 0,
	}
	// start sanning and loading
	err = wal.load()
	if err != nil {
		return nil, err
	}
	return wal, nil
}

func isLogSegmentFile(name string) bool {
	return len(name) == 28 &&
		strings.HasPrefix(name, "wal-") &&
		strings.HasSuffix(name, ".seg")
}

func (l *Log) load() error {
	// check to see if directory exists
	_, err := os.Stat(l.path)
	if os.IsNotExist(err) {
		// create it if it does not exist
		err = os.MkdirAll(l.path, os.ModeDir)
		if err != nil {
			return err
		}
	}
	// get the files in the main directory path
	files, err := os.ReadDir(l.path)
	if err != nil {
		return err
	}
	// list files in the main directory path and attempt to load segments
	for _, file := range files {
		// skip files that are not log segments
		if file.IsDir() || !isLogSegmentFile(file.Name()) {
			continue
		}
		// read segment sequence number
		seq, err := strconv.ParseUint(file.Name()[4:24], 10, 64)
		if err != nil || seq == 0 {
			return err
		}
		// open segment
		seg, err := openSegment(l.path, seq)
		if err != nil {
			return err
		}
		// add log segments to the log segment cache
		l.segments = append(l.segments, seg)
	}
	// if no segments, create a new log segment and return
	if len(l.segments) == 0 {
		// open new segment
		seg, err := openSegment(l.path, l.first)
		if err != nil {
			return err
		}
		// add log segment to the log segment cache
		l.segments = append(l.segments, seg)
	}
	l.first = l.segments[0].index
	segpos := l.segments[len(l.segments)-1]
	l.seqnum = segpos.seqnum + uint64(len(segpos.entries)) - 1
	l.active = len(l.segments) - 1
	return nil
}

func (l *Log) Write(data []byte) error {
	seg := l.segments[l.active]
	return seg.Write(data)
}
