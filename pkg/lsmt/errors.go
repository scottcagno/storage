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
)

func (lsm *LSMTree) checkMemtableSize(memTableSize int64) error {
	if memTableSize > lsm.conf.FlushThreshold {
		return ErrFlushThreshold
	}
	return nil
}
