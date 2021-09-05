package v3

import "fmt"

// segment represents a segment file, which
// is a single file representing a segment
// of the entire log file.
type segment struct {
	path  string // path of segment file
	index uint64 // first index of segment file
	spans []span // cached entry spans in buffer
}

// span represents the offset span of a single
// entry within the segment file.
type span struct {
	start int
	end   int
}

func segmentName(index uint64) string {
	return fmt.Sprintf("%020d", index)
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
