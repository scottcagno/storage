package storage

import "github.com/scottcagno/storage/pkg/lsmt/binary"

// Config is an interface for this package
type Config interface {
	CheckConfig() Config
}

// Storage is an interface for this package
type Storage interface {
	Has(key string) bool
	Put(key string, value []byte) error
	PutBatch(batch *binary.Batch) error
	Get(key string) ([]byte, error)
	GetBatch(keys ...string) (*binary.Batch, error)
	Del(key string) error
	Stats() (StorageStats, error)
	Close() error
}

// StorageStats is an interface for this package
type StorageStats interface {
	Size() int64
}
