package swal

import (
	"fmt"
	"path/filepath"
	"strconv"
)

func MakeFileNameFromIndex(index int64) string {
	hexa := strconv.FormatInt(index, 16)
	return fmt.Sprintf("%s%010s%s", FilePrefix, hexa, FileSuffix)
}

func GetIndexFromFileName(name string) (int64, error) {
	hexa := name[len(FilePrefix) : len(name)-len(FileSuffix)]
	return strconv.ParseInt(hexa, 16, 32)
}

// segEntry contains the metadata for a single segEntry within the file segment
type segEntry struct {
	index  int64 // index is the "id" of this segEntry
	offset int64 // offset is the actual offset of this segEntry in the segment file
}

// String is the stringer method for an segEntry
func (e segEntry) String() string {
	return fmt.Sprintf("segEntry.index=%d, segEntry.offset=%d", e.index, e.offset)
}

// segment contains the metadata for the file segment
type segment struct {
	path      string     // path is the full path to this segment file
	index     int64      // starting index of the segment
	entries   []segEntry // entries is an index of the entries in the segment
	remaining int64      // remaining is the bytes left after max file size minus segEntry data
}

// getFirstIndex returns the first index in the entries list
func (s *segment) getFirstIndex() int64 {
	return s.index
}

// getLastIndex returns the last index in the entries list
func (s *segment) getLastIndex() int64 {
	if len(s.entries) > 0 {
		return s.entries[len(s.entries)-1].index
	}
	return s.index
}

// findEntryIndex performs binary search to find the segEntry containing provided index
func (s *segment) findEntryIndex(index int64) int {
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

// String is the stringer method for a segment
func (s *segment) String() string {
	var ss string
	ss += fmt.Sprintf("path: %q\n", filepath.Base(s.path))
	ss += fmt.Sprintf("index: %d\n", s.index)
	ss += fmt.Sprintf("entries: %d\n", len(s.entries))
	ss += fmt.Sprintf("remaining: %d\n", s.remaining)
	return ss
}
