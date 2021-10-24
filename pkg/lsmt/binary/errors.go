package binary

import "errors"

var (
	ErrFileClosed    = errors.New("binary: file closed")
	ErrBadEntry      = errors.New("binary: bad entry")
	ErrEntryNotFound = errors.New("binary: entry not found")
	ErrKeyTooLarge   = errors.New("binary: key too large")
	ErrValueTooLarge = errors.New("binary: value too large")
)
