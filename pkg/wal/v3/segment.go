package v3

import (
	"fmt"
	"path/filepath"
)

// segment represents a segment file, which
// is a single file representing a segment
// of the entire log file.
type segment struct {
	path  string // path of segment file
	index uint64 // first index of segment file
	spans []span // cached entry spans in buffer
}

func (s *segment) String() string {
	path := filepath.Base(s.path)
	str := fmt.Sprintf("\nsegment:\n{\n\tpath: %q,\n\tindex: %d,\n\tspans: [ ", path, s.index)
	for i := range s.spans {
		str += fmt.Sprintf("%d", s.spans[i])
	}
	return str + " ]\n}\n"
}

// span represents the offset span of a single
// entry within the segment file.
type span struct {
	start uint64
	end   uint64
}

func segmentName(index uint64) string {
	return fmt.Sprintf("wal-%020d.seg", index)
}

type segmentHeader struct {
}

func (sh *segmentHeader) hasValidChecksum() bool {
	return true
}

func (sh *segmentHeader) getIndex() uint64 {
	return 1
}

func readLogSegmentHeader(path string) (*segmentHeader, error) {
	return nil, nil
}

func writeLogSegmentHeader(path string, sh *segmentHeader) error {
	return nil
}
