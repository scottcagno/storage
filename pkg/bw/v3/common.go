package v3

import "errors"

var (
	ErrNotEnoughRoom = errors.New("not enough room left in the buffer")
	ErrBadChecksum   = errors.New("bad checksum")
	ErrBadPageSize   = errors.New("bad page size; must be evenly divisible")
)

const (
	defaultPageSize  = 512
	defaultPageAlign = true
	defaultAutoFlush = true
)

const (
	minPageSize = 64
	maxPageSize = 64 << 10
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

func checkOptions(opt *Options) error {
	if opt == nil {
		opt = defaultOptions
	}
	if opt.pageSize < minPageSize {
		opt.pageSize = minPageSize
	}
	if opt.pageSize > maxPageSize {
		opt.pageSize = maxPageSize
	}
	if opt.pageSize > minPageSize && opt.pageSize < maxPageSize {
		if r := opt.pageSize % minPageSize; r != 0 {
			opt.pageSize = (opt.pageSize + minPageSize - 1) &^ (minPageSize - 1)
		}
	}
	return nil
}
