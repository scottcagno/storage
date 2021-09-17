package storage

// Storage is an interface for this package
type Storage interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Del(key string) error
}
