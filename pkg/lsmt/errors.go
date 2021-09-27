package lsmt

import "errors"

var (
	ErrKeyNotFound = errors.New("lsmt: key not found")

	ErrFoundTombstone = errors.New("lsmt: found tombstone or empty value")
)
