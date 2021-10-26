package lsmt

import (
	"errors"
)

var (
	ErrKeyNotFound    = errors.New("lsmt: key not found")
	ErrFoundTombstone = errors.New("lsmt: found tombstone or empty value")
	ErrDeleted        = ErrFoundTombstone

	ErrNotFound       = errors.New("lsmt: not found")
	ErrIncompleteSet  = errors.New("lsmt: incomplete batch or set")
	ErrFlushThreshold = errors.New("lsmt: flush threshold has been reached")

	ErrBadKey        = errors.New("lsmt: bad key")
	ErrKeyTooLarge   = errors.New("lsmt: key too large")
	ErrBadValue      = errors.New("lsmt: bad value")
	ErrValueTooLarge = errors.New("lsmt: value too large")
)
