package wal

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Logger struct {
	mu         sync.RWMutex
	gidx       uint64 // global atomic index
	path       string
	segments   []*segment
	firstIndex uint64
	lastIndex  uint64
	closed     bool
	file       *os.File
}

func Open(path string) (*Logger, error) {
	err := os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	// load stuff here
	return &Logger{
		path:   path,
		closed: false,
		file:   file,
	}, nil
}

/*
func (l *Logger) load() error {
	dirEntries, err := os.ReadDir(l.path)
	if err != nil {
		return err
	}
	var file *os.File
	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() {
			continue // skip all files that are not log files
		}
		file, err = os.OpenFile(dirEntry.Name(), os.O_RDONLY|os.O_CREATE, 0600)
		if err != nil {
			return err
		}
		defer file.Close()

		scanner, err := record.NewScanner(f, maxRecordSize)
		if err != nil {
			return nil, err
		}

		for scanner.Scan() {
			record := scanner.Record()
			idx.put(record.Key(), int64(record.Size()))
		}

		if scanner.Err() != nil {
			return nil, fmt.Errorf("could not scan entry, %w", err)
		}

	}
}
*/

// Read returns the raw data associated with the provided index
func (l *Logger) Read() ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	// read entry length
	var buf [8]byte
	_, err := io.ReadFull(l.file, buf[:])
	if err != nil {
		return nil, err
	}
	// decode entry length
	elen := binary.LittleEndian.Uint64(buf[:])
	// make byte slice of entry length size
	entry := make([]byte, elen)
	// read entry from reader into slice
	_, err = io.ReadFull(l.file, entry)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

// Write appends a raw data to the log
func (l *Logger) Write(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	// encode entry length
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(len(data)))
	// write entry length
	_, err := l.file.Write(buf[:])
	if err != nil {
		return err
	}
	// write entry data
	_, err = l.file.Write(data)
	if err != nil {
		return err
	}
	// add segment to file
	err = l.addSegment(len(data))
	if err != nil {
		return err
	}
	// incr global index
	l.gidx++
	return nil
}

func (l *Logger) addSegment(datalen int) error {
	// get current position
	pos, err := l.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	s := &segment{
		path:  l.path,
		index: l.gidx,
		span: span{
			start: int(pos) - datalen,
			end:   int(pos),
		},
	}
	l.segments = append(l.segments, s)
	return nil
}

func (l *Logger) PrintLoggerSegments() {
	// TODO
}

func (l *Logger) Seek(offset int64, whence int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err := l.file.Seek(offset, whence)
	return err
}

// First returns the first wal.Entry in the log file
func (l *Logger) First() (*Entry, error) {
	return nil, nil
}

// Last returns the first wal.Entry in the log file
func (l *Logger) Last() (*Entry, error) {
	return nil, nil
}

// Truncate truncates the log file from whence to the provided index
func (l *Logger) Truncate(index uint64, whence int) error {
	return nil
}

func (l *Logger) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Sync()
}

func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	err := l.file.Sync()
	if err != nil {
		return err
	}
	err = l.file.Close()
	if err != nil {
		return err
	}
	l.closed = true
	return nil
}
