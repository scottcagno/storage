package v3

import "errors"

var ErrNotEnoughRoom = errors.New("not enough room left in the buffer")
var ErrBadChecksum = errors.New("bad checksum")
