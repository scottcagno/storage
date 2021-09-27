package lsmt

import "errors"

var (
	ErrKeyNotFound = errors.New("lsmt: key not found")

	ErrFlushThreshold = errors.New("memtable: flush threshold has been reached")
	ErrFoundTombstone = errors.New("lsmt: found tombstone or empty value")
)
