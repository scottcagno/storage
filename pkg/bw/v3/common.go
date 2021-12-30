package v3

import "errors"

var (
	ErrNotEnoughRoom = errors.New("not enough room left in the buffer")
	ErrBadChecksum   = errors.New("bad checksum")
)

const (
	defaultPageSize  = 512
	defaultPageAlign = true
	defaultAutoFlush = true
)

type page struct {
	data [defaultPageSize]byte
}

var defaultOptions = &Options{
	pageSize:  defaultPageSize,
	pageAlign: defaultPageAlign,
	autoFlush: defaultAutoFlush,
}

type Options struct {
	pageSize  int
	pageAlign bool
	autoFlush bool
}
