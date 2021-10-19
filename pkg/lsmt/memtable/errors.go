package memtable

import "errors"

var (
	ErrFlushThreshold = errors.New("memtable: flush threshold has been reached")
	ErrKeyNotFound    = errors.New("memtable: key not found")
	ErrFoundTombstone = errors.New("memtable: found tombstone; entry was deleted")
)
