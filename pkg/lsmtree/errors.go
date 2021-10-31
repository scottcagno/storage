package lsmtree

import "errors"

var (
	ErrKeyNotFound    = errors.New("lsmtree: key not found")
	ErrFoundTombstone = errors.New("lsmtree: found tombstone or empty value")
	ErrDeleted        = ErrFoundTombstone

	ErrNotFound       = errors.New("lsmtree: not found")
	ErrIncompleteSet  = errors.New("lsmtree: incomplete batch or set")
	ErrFlushThreshold = errors.New("lsmtree: flush threshold has been reached")

	ErrBadKey        = errors.New("lsmtree: bad key")
	ErrKeyTooLarge   = errors.New("lsmtree: key too large")
	ErrBadValue      = errors.New("lsmtree: bad value")
	ErrValueTooLarge = errors.New("lsmtree: value too large")

	ErrWritingEntry = errors.New("lsmtree: error write entry")
	ErrReadingEntry = errors.New("lsmtree: error reading entry")

	ErrNilEntry = errors.New("lsmtree: error got nil entry")

	ErrBadChecksum = errors.New("lsmtree: bad checksum")
)
