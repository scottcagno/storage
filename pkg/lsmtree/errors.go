package lsmtree

import "errors"

var (
	ErrKeyNotFound    = errors.New("lsmtree: key not found")
	ErrFoundTombstone = errors.New("lsmtree: found tombstone or empty value")

	ErrNoDataFound = errors.New("lsmtree: no data found")

	ErrNotFound       = errors.New("lsmtree: entry not found")
	ErrIncompleteSet  = errors.New("lsmtree: incomplete batch or set")
	ErrFlushThreshold = errors.New("lsmtree: flush threshold has been reached")

	ErrBadKey        = errors.New("lsmtree: bad key")
	ErrKeyTooLarge   = errors.New("lsmtree: key too large")
	ErrBadValue      = errors.New("lsmtree: bad value")
	ErrValueTooLarge = errors.New("lsmtree: value too large")

	ErrWritingEntry = errors.New("lsmtree: error write entry")
	ErrReadingEntry = errors.New("lsmtree: error reading entry")

	ErrNilEntry = errors.New("lsmtree: error got nil entry")
	ErrNilIndex = errors.New("lsmtree: error got nil index")

	ErrBadChecksum = errors.New("lsmtree: bad checksum")

	ErrFileClosed = errors.New("lsmtree: file is closed")
)
