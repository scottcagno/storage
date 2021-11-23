package search

type Searcher interface {
	FindIndex(text, pattern []byte) int
	FindIndexString(text, pattern string) int
}

// Boyer-Moore:
// Works by pre-analyzing the pattern and comparing from right-to-left. If a mismatch occurs, the
// initial analysis is used to determine how far the pattern can be shifted w.r.t. the  text being
// searched. This works particularly well for long search patterns. In particular, it can be
// sublinear, as you do not need to read every single character of your text. So if your pattern is one
// or two characters, then it literally becomes linear searching. The length of your pattern you are
// trying to search is in theory equal to the best case scenario of how many characters you
// can skip for something that doesn't match.

// Knuth-Morris-Pratt:
// Also works by pre-analyzing the pattern, but tries to re-use whatever was already matched in the
// initial part of the pattern to avoid having to rematch that. This can work quite well, if your
// alphabet is small (f.ex. DNA bases), as you get a higher chance that your search patterns
// contain re-usable sub-patterns. KMP is best suited for searching texts that have a lot of tight
// repetition.

// Rabin-Karp:
// Works by utilizing efficient computation of hash values of the successive substrings of the text,
// which it then uses for comparing matches. It is best on large text in which you are finding multiple
// pattern matches, like detecting plagiarism.
