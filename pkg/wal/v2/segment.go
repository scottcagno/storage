package v2

import (
	"fmt"
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

// makeFileName returns a file name using the provided timestamp.
// If t is nil, it will create a new name using time.Now()
func makeFileName(t time.Time) string {
	tf := t.Format("2006-01-03-15:04:05.000000")
	return fmt.Sprintf("%s-%s-%s", logPrefix, tf, logSuffix)
}

// getLastIndex returns the last index in the entries list
func (s *segment) getLastIndex() uint64 {
	return s.entries[len(s.entries)-1].index
}
