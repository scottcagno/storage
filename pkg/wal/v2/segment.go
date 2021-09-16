package v2

import (
	"fmt"
	"path/filepath"
	"time"
)

// entry contains the metadata for a single entry within the file segment
type entry struct {
	index  uint64 // index is the "id" of this entry
	offset int64  // offset is the actual offset of this entry in the segment file
}

// segment contains the metadata for the file segment
type segment struct {
	path      string  // path is the full path to this segment file
	index     uint64  // starting index of the segment
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
	//return fmt.Sprintf("%s%s%s", logPrefix, time.RFC3339Nano, logSuffix)
	return fmt.Sprintf("%s%d%s", logPrefix, time.Now().UnixMicro(), logSuffix)
}

// getLastIndex returns the last index in the entries list
func (s *segment) getLastIndex() uint64 {
	if len(s.entries) > 0 {
		return s.entries[len(s.entries)-1].index
	}
	return s.index
}

// findEntryIndex performs binary search to find the entry containing provided index
func (s *segment) findEntryIndex(index uint64) int {
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
