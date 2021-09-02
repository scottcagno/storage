package wal

var fn = func(b byte) bool {
	if b == '\n' || b == ' ' || b == '\t' {
		return true
	}
	return false
}

type span struct {
	start int
	end   int
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
