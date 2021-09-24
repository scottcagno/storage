package openaddr

const (
	DefaultLoadFactor = 0.90 // load factor must exceed 50%
	DefaultMapSize    = 16
)

// alignBucketCount aligns buckets to ensure all sizes are powers of two
func alignBucketCount(size uint) uint64 {
	count := uint(DefaultMapSize)
	for count < size {
		count *= 2
	}
	return uint64(count)
}
