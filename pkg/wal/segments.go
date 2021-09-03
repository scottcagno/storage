package wal

import "fmt"

var DefaultFn = func(b byte) bool {
	if b == '\n' || b == ' ' || b == '\t' {
		return true
	}
	return false
}

type segment struct {
	path  string // path of segment file
	index uint64 // first index of segment
	span
}

type span struct {
	start int
	end   int
}

func (s span) Start() int {
	return s.start
}

func (s span) End() int {
	return s.end
}

func Segments(s []byte, f func(byte) bool) []span {
	spans := make([]span, 0, 32)
	start := -1 // valid span start if >= 0
	for i := 0; i < len(s); {
		size := 1
		r := s[i]
		if f(r) {
			if start >= 0 {
				spans = append(spans, span{start, i})
				start = -1
			}
		} else {
			if start < 0 {
				start = i
			}
		}
		i += size
	}
	// Last field might end at EOF.
	if start >= 0 {
		spans = append(spans, span{start, len(s)})
	}
	return spans
}

// SegmentName returns a 20-byte textual representation of an index
// for lexical ordering. This is used for the file names of log segments.
func SegmentName(index uint64) string {
	return fmt.Sprintf("%020d", index)
}
