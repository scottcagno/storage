package mmap

import (
	"errors"
	"fmt"
)

var (
	ErrBadMode     = fmt.Errorf("mmap: bad mode")
	ErrBadOffset   = fmt.Errorf("mmap: bad offset")
	ErrBadLength   = fmt.Errorf("mmap: bad length")
	ErrClosed      = fmt.Errorf("mmap: mapping closed")
	ErrLocked      = fmt.Errorf("mmap: mapping already locked")
	ErrNotLocked   = fmt.Errorf("mmap: mapping is not locked")
	ErrOutOfBounds = fmt.Errorf("mmap: out of bounds")
	ErrReadOnly    = fmt.Errorf("mmap: mapping is read only")
	ErrSeekWhence  = errors.New("mmap: invalid seek whence")
	ErrSeekOffset  = errors.New("mmap: invalid seek offset")
)
