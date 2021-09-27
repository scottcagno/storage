package binary

import "errors"

var (
	ErrFileClosed = errors.New("binary: file closed")
	ErrBadEntry   = errors.New("binary: bad entry")
)
