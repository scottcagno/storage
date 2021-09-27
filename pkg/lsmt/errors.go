package lsmt

import "errors"

var (
	ErrKeyNotFound    = errors.New("lsmt: key not found")
	ErrFileClosed     = errors.New("binary: file closed")
	ErrBadEntry       = errors.New("binary: bad entry")
	ErrFlushThreshold = errors.New("memtable: flush threshold has been reached")
)
