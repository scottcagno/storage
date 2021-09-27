package memtable

import "errors"

var ErrFlushThreshold = errors.New("memtable: flush threshold has been reached")
