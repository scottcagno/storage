package v2

import (
	"os"
	"sync"
)

type Log struct {
	mu   sync.RWMutex
	base string
	fd   *os.File
}
