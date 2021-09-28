package sstable

import "errors"

var (
	ErrSSTIndexNotFound = errors.New("sstable: index not found")
	ErrSSTEmptyBatch    = errors.New("sstable: batch is empty or nil")
)
