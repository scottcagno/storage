package sstable

import "errors"

var (
	ErrSSTIndexNotFound     = errors.New("sstable: gindex not found")
	ErrSSTEmptyBatch        = errors.New("sstable: batch is empty or nil")
	ErrInvalidScanDirection = errors.New("sstable: invalid scan direction")
)
