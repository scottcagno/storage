package v1

import (
	"time"
)

type Entry struct {
	ID        uint64
	Type      uint8
	Timestamp time.Time
	Data      []byte
}
