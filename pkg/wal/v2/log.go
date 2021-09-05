package v2

import (
	"encoding/binary"
	"errors"
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
	segments   []*segment
	firstIndex uint64
	lastIndex  uint64
	sfile      *os.File
	scache     *segment
}

func Open(path string) (*WAL, error) {
	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(path, 0777); err != nil {
		return nil, err
	}
	l := &WAL{
		path:   path,
		scache: new(segment),
	}
	if err = l.load(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *WAL) load() error {
	items, err := os.ReadDir(l.path)
	if err != nil {
		return err
	}
	startIdx, endIdx := -1, -1
	for _, item := range items {
		name := item.Name()
		if item.IsDir() || len(name) < 20 {
			continue
		}
		index, err := strconv.ParseUint(name[:20], 10, 64)
		if err != nil || index == 0 {
			continue
		}
		isStart := len(name) == 26 && strings.HasSuffix(name, ".START")
		isEnd := len(name) == 24 && strings.HasSuffix(name, ".END")
		if len(name) == 20 || isStart || isEnd {
			if isStart {
				startIdx = len(l.segments)
			} else if isEnd && endIdx == -1 {
				endIdx = len(l.segments)
			}
			l.segments = append(l.segments, &segment{
				index: index,
				path:  filepath.Join(l.path, name),
			})
		}
	}
	if len(l.segments) == 0 {
		// Create a new log
		l.segments = append(l.segments, &segment{
			index: 1,
			path:  filepath.Join(l.path, segmentName(1)),
		})
		l.firstIndex = 1
		l.lastIndex = 0
		l.sfile, err = os.Create(l.segments[0].path)
		return err
	}
	// Open existing log. Clean up log if START of END segments exists.
	if startIdx != -1 {
		if endIdx != -1 {
			// There should not be a START and END at the same time
			return ErrCorrupt
		}
		// Delete all files leading up to START
		for i := 0; i < startIdx; i++ {
			if err := os.Remove(l.segments[i].path); err != nil {
				return err
			}
		}
		l.segments = append([]*segment{}, l.segments[startIdx:]...)
		// Rename the START segment
		orgPath := l.segments[0].path
		finalPath := orgPath[:len(orgPath)-len(".START")]
		err := os.Rename(orgPath, finalPath)
		if err != nil {
			return err
		}
		l.segments[0].path = finalPath
	}
	if endIdx != -1 {
		// Delete all files following END
		for i := len(l.segments) - 1; i > endIdx; i-- {
			if err := os.Remove(l.segments[i].path); err != nil {
				return err
			}
		}
		l.segments = append([]*segment{}, l.segments[:endIdx+1]...)
		if len(l.segments) > 1 && l.segments[len(l.segments)-2].index ==
			l.segments[len(l.segments)-1].index {
			// remove the segment prior to the END segment because it shares
			// the same starting index.
			l.segments[len(l.segments)-2] = l.segments[len(l.segments)-1]
			l.segments = l.segments[:len(l.segments)-1]
		}
		// Rename the END segment
		orgPath := l.segments[len(l.segments)-1].path
		finalPath := orgPath[:len(orgPath)-len(".END")]
		err := os.Rename(orgPath, finalPath)
		if err != nil {
			return err
		}
		l.segments[len(l.segments)-1].path = finalPath
	}
	l.firstIndex = l.segments[0].index
	// Open the last segment for appending
	lseg := l.segments[len(l.segments)-1]
	l.sfile, err = os.OpenFile(lseg.path, os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	if _, err := l.sfile.Seek(0, 2); err != nil {
		return err
	}
	// Load the last segment entries
	if err := l.loadSegmentEntries(lseg); err != nil {
		return err
	}
	l.lastIndex = lseg.index + uint64(len(lseg.spans)) - 1
	if l.scache == nil {
		l.scache = lseg
	}
	return nil
}

func (l *WAL) Read(index uint64) (data []byte, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, ErrClosed
	}
	if index == 0 || index < l.firstIndex || index > l.lastIndex {
		return nil, ErrNotFound
	}
	s, err := l.loadSegment(index)
	if err != nil {
		return nil, err
	}
	spans := s.spans[index-s.index]
	edata := s.ebuf[spans.start:spans.end]

	// binary read
	size, n := binary.Uvarint(edata)
	if n <= 0 {
		return nil, ErrCorrupt
	}
	if uint64(len(edata)-n) < size {
		return nil, ErrCorrupt
	}

	data = make([]byte, size)
	copy(data, edata[n:])

	return data, nil
}

func (l *WAL) Write(index uint64, data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	l.wbatch.Clear()
	l.wbatch.Write(index, data)
	return l.writeEntryBatch(&l.wbatch)
}

func (l *WAL) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	return l.sfile.Sync()
}

func (l *WAL) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if err := l.sfile.Sync(); err != nil {
		return err
	}
	if err := l.sfile.Close(); err != nil {
		return err
	}
	l.closed = true
	return nil
}

func (l *WAL) cycle() error {
	if err := l.sfile.Sync(); err != nil {
		return err
	}
	if err := l.sfile.Close(); err != nil {
		return err
	}
	// cache the previous segment
	l.cacheSegment(len(l.segments) - 1)
	s := &segment{
		index: l.lastIndex + 1,
		path:  filepath.Join(l.path, segmentName(l.lastIndex+1)),
	}
	var err error
	l.sfile, err = os.Create(s.path)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	return nil
}

func (l *WAL) writeEntryBatch(b *EntryBatch) error {
	// check that all indexes in batch are sane
	for i := 0; i < len(b.entries); i++ {
		if b.entries[i].index != l.lastIndex+uint64(i+1) {
			return ErrOutOfOrder
		}
	}
	// load the tail segment
	s := l.segments[len(l.segments)-1]
	if len(s.ebuf) > l.conf.SegmentSize {
		// tail segment has reached capacity. Close it and create a new one.
		if err := l.cycle(); err != nil {
			return err
		}
		s = l.segments[len(l.segments)-1]
	}
	mark := len(s.ebuf)
	datas := b.datas
	for i := 0; i < len(b.entries); i++ {
		// prepare entry to append to the current segment buffer
		data := datas[:b.entries[i].size]
		var epos bpos
		// append entry to the segment buffer along with position/offsets
		s.ebuf, epos = l.appendEntry(s.ebuf, data)
		s.epos = append(s.epos, epos)
		// check segment grow
		if len(s.ebuf) >= l.conf.SegmentSize {
			// segment has reached capacity, cycle now
			if _, err := l.sfile.Write(s.ebuf[mark:]); err != nil {
				return err
			}
			l.lastIndex = b.entries[i].index
			if err := l.cycle(); err != nil {
				return err
			}
			s = l.segments[len(l.segments)-1]
			mark = 0
		}
		datas = datas[b.entries[i].size:]
	}
	// if the segment buffer contains anything, write to the segment file
	if len(s.ebuf)-mark > 0 {
		if _, err := l.sfile.Write(s.ebuf[mark:]); err != nil {
			return err
		}
		l.lastIndex = b.entries[len(b.entries)-1].index
	}
	if !l.conf.NoSync {
		if err := l.sfile.Sync(); err != nil {
			return err
		}
	}
	b.Clear()
	return nil
}

func (l *WAL) findSegment(index uint64) int {
	i, j := 0, len(l.segments)
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

func (l *WAL) loadSegmentEntries(s *segment) error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	//ebuf := data
	var spans []span
	var start int
	for exidx := s.index; len(data) > 0; exidx++ {
		var n int
		n, err = loadNextEntry(data)
		if err != nil {
			return err
		}
		data = data[n:]
		spans = append(spans, span{start, pos + n})
		start += n
	}
	//s.ebuf = ebuf
	s.spans = spans
	return nil
}

func (l *WAL) loadSegment(index uint64) (*segment, error) {
	// check the last segment first.
	lseg := l.segments[len(l.segments)-1]
	if index >= lseg.index {
		return lseg, nil
	}
	// check the most recent cached segment
	var rseg *segment
	if index >= l.scache.index && index < l.scache.index+uint64(len(l.scache.spans)) {
		rseg = l.scache
	}
	if rseg != nil {
		return rseg, nil
	}
	// find in the segment array
	idx := l.findSegment(index)
	s := l.segments[idx]
	if len(s.spans) == 0 {
		// load the entries from cache
		if err := l.loadSegmentEntries(s); err != nil {
			return nil, err
		}
	}
	// cache this segment
	l.cacheSegment(idx)
	return s, nil
}

func (l *WAL) cacheSegment(idx int) {
	l.scache = l.segments[idx]
}

func appendEntry(dst []byte, data []byte) (out []byte, spans span) {
	// data_size + data
	pos := len(dst)
	dst = appendUvarint(dst, uint64(len(data)))
	dst = append(dst, data...)
	return dst, span{pos, len(dst)}
}

func appendUvarint(dst []byte, x uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], x)
	dst = append(dst, buf[:n]...)
	return dst
}

func loadNextEntry(data []byte) (n int, err error) {
	size, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, ErrCorrupt
	}
	if uint64(len(data)-n) < size {
		return 0, ErrCorrupt
	}
	return n + int(size), nil
}
