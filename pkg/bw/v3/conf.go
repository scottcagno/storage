package v3

const (
	defaultPageSize  = 512
	defaultPageAlign = true
	defaultAutoFlush = true
)

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
