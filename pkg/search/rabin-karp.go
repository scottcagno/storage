package search

import "bytes"

// RabinKarp algorithm is inferior for single pattern searching to Knuth–Morris–Pratt algorithm or the
// Boyer–Moore string search algorithm (and other faster single pattern string searching algorithms) because
// of its slow worst case behavior. However, it is a useful algorithm for multiple pattern searches.
type RabinKarp struct{}

func NewRabinKarp() *RabinKarp {
	return new(RabinKarp)
}

func (rk *RabinKarp) String() string {
	return "RABIN-KARP"
}

func (rk *RabinKarp) FindIndex(text, pattern []byte) int {
	if text == nil || pattern == nil {
		return -1
	}
	return rabinKarpFinder(pattern, text)
}

func (rk *RabinKarp) FindIndexString(text, pattern string) int {
	return rabinKarpFinderString(pattern, text)
}

func rabinKarpFinder(pattern, text []byte) int {
	return indexRabinKarpBytes(text, pattern)
}

func rabinKarpFinderString(pattern, text string) int {
	return indexRabinKarpString(text, pattern)
}

// PrimeRK is the prime base used in Rabin-Karp algorithm.
const PrimeRK = 16777619

// indexRabinKarpBytes uses the Rabin-Karp search algorithm to return the index of the
// first occurrence of substr in s, or -1 if not present.
func indexRabinKarpBytes(s, sep []byte) int {
	// Rabin-Karp search
	hashsep, pow := hashBytes(sep)
	n := len(sep)
	var h uint32
	for i := 0; i < n; i++ {
		h = h*PrimeRK + uint32(s[i])
	}
	if h == hashsep && bytes.Equal(s[:n], sep) {
		return 0
	}
	for i := n; i < len(s); {
		h *= PrimeRK
		h += uint32(s[i])
		h -= pow * uint32(s[i-n])
		i++
		if h == hashsep && bytes.Equal(s[i-n:i], sep) {
			return i - n
		}
	}
	return -1
}

// hashBytes returns the hash and the appropriate multiplicative
// factor for use in Rabin-Karp algorithm.
func hashBytes(sep []byte) (uint32, uint32) {
	hash := uint32(0)
	for i := 0; i < len(sep); i++ {
		hash = hash*PrimeRK + uint32(sep[i])
	}
	var pow, sq uint32 = 1, PrimeRK
	for i := len(sep); i > 0; i >>= 1 {
		if i&1 != 0 {
			pow *= sq
		}
		sq *= sq
	}
	return hash, pow
}

// hashBytesRev returns the hash of the reverse of sep and the
// appropriate multiplicative factor for use in Rabin-Karp algorithm.
func hashBytesRev(sep []byte) (uint32, uint32) {
	hash := uint32(0)
	for i := len(sep) - 1; i >= 0; i-- {
		hash = hash*PrimeRK + uint32(sep[i])
	}
	var pow, sq uint32 = 1, PrimeRK
	for i := len(sep); i > 0; i >>= 1 {
		if i&1 != 0 {
			pow *= sq
		}
		sq *= sq
	}
	return hash, pow
}

// indexRabinKarpString uses the Rabin-Karp search algorithm to return the index of the
// first occurrence of substr in s, or -1 if not present.
func indexRabinKarpString(s, substr string) int {
	// Rabin-Karp search
	hashss, pow := hashString(substr)
	n := len(substr)
	var h uint32
	for i := 0; i < n; i++ {
		h = h*PrimeRK + uint32(s[i])
	}
	if h == hashss && s[:n] == substr {
		return 0
	}
	for i := n; i < len(s); {
		h *= PrimeRK
		h += uint32(s[i])
		h -= pow * uint32(s[i-n])
		i++
		if h == hashss && s[i-n:i] == substr {
			return i - n
		}
	}
	return -1
}

// hashString returns the hash and the appropriate multiplicative
// factor for use in Rabin-Karp algorithm.
func hashString(sep string) (uint32, uint32) {
	hash := uint32(0)
	for i := 0; i < len(sep); i++ {
		hash = hash*PrimeRK + uint32(sep[i])
	}
	var pow, sq uint32 = 1, PrimeRK
	for i := len(sep); i > 0; i >>= 1 {
		if i&1 != 0 {
			pow *= sq
		}
		sq *= sq
	}
	return hash, pow
}

// hashStringRev returns the hash of the reverse of sep and the
// appropriate multiplicative factor for use in Rabin-Karp algorithm.
func hashStringRev(sep string) (uint32, uint32) {
	hash := uint32(0)
	for i := len(sep) - 1; i >= 0; i-- {
		hash = hash*PrimeRK + uint32(sep[i])
	}
	var pow, sq uint32 = 1, PrimeRK
	for i := len(sep); i > 0; i >>= 1 {
		if i&1 != 0 {
			pow *= sq
		}
		sq *= sq
	}
	return hash, pow
}
