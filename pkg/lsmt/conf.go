package lsmt

import (
	"math"
)

const (
	SizeKB   = 1<<10 - 1
	SizeMB   = 1<<20 - 1
	Size64KB = math.MaxUint16
	Size4GB  = math.MaxUint32
)

const (

	// path defaults
	defaultWalPath  = "log"
	defaultSstPath  = "data"
	defaultBasePath = "lsm-db"

	// syncing
	defaultSyncOnWrite = false

	// default sizes
	defaultFlushThreshold   = 2 * SizeMB  //   2 MB
	defaultBloomFilterSize  = 8 * SizeMB  //   8 MB
	defaultKeySizeAllowed   = 256         // 256 B
	defaultValueSizeAllowed = 64 * SizeKB //  64 KB

	// min and max sizes
	minFlushThresholdAllowed  = 1 * SizeMB  //   1 MB
	maxFlushThresholdAllowed  = 32 * SizeMB //  32 MB
	minBloomFilterSizeAllowed = 1 * SizeMB  //   1 MB
	maxBloomFilterSizeAllowed = 16 * SizeMB //  16 MB
	minKeySizeAllowed         = 8           //   8 B
	maxKeySizeAllowed         = 2 * SizeMB  //   2 MB
	minValueSizeAllowed       = 8           //   8 B
	maxValueSizeAllowed       = 16 * SizeMB //  16 MB
)

var defaultLSMConfig = &LSMConfig{
	BasePath:        defaultBasePath,
	SyncOnWrite:     defaultSyncOnWrite,
	FlushThreshold:  defaultFlushThreshold,
	BloomFilterSize: defaultBloomFilterSize,
	KeySize:         defaultKeySizeAllowed,
	ValueSize:       defaultValueSizeAllowed,
}

// LSMConfig holds configuration settings for an LSMTree instance
type LSMConfig struct {
	BasePath        string // base storage path
	SyncOnWrite     bool   // perform sync every time an entry is written
	FlushThreshold  int64  // mem-table flush threshold
	BloomFilterSize uint   // specify the bloom filter size
	KeySize         int64  // the max allowed key size
	ValueSize       int64  // the maximum allowed value size
}

// checkLSMConfig is a helper to make sure the configuration
// options are correct and handles and missing options
func checkLSMConfig(conf *LSMConfig) *LSMConfig {
	if conf == nil {
		return defaultLSMConfig
	}
	if conf.BasePath == *new(string) {
		conf.BasePath = defaultBasePath
	}
	if conf.FlushThreshold <= 0 {
		conf.FlushThreshold = defaultFlushThreshold // 2 MB
	}
	if conf.FlushThreshold < minFlushThresholdAllowed {
		conf.FlushThreshold = minFlushThresholdAllowed // 1 MB
	}
	if conf.FlushThreshold > maxFlushThresholdAllowed {
		conf.FlushThreshold = maxFlushThresholdAllowed // 32 MB
	}
	if conf.BloomFilterSize <= 0 {
		conf.BloomFilterSize = defaultBloomFilterSize // 8 MB
	}
	if conf.BloomFilterSize < minBloomFilterSizeAllowed {
		conf.BloomFilterSize = minBloomFilterSizeAllowed // 1 MB
	}
	if conf.BloomFilterSize > maxBloomFilterSizeAllowed {
		conf.BloomFilterSize = maxBloomFilterSizeAllowed // 16 MB
	}
	if conf.KeySize <= 0 {
		conf.KeySize = defaultKeySizeAllowed // 256 B
	}
	if conf.ValueSize <= 0 {
		conf.ValueSize = defaultValueSizeAllowed // 64 KB
	}
	if conf.KeySize < minKeySizeAllowed {
		conf.KeySize = minKeySizeAllowed // 8 B
	}
	if conf.KeySize > maxKeySizeAllowed {
		conf.KeySize = maxKeySizeAllowed // 2 MB
	}
	if conf.ValueSize < minValueSizeAllowed {
		conf.ValueSize = minValueSizeAllowed // 8
	}
	if conf.ValueSize > maxValueSizeAllowed {
		conf.ValueSize = maxValueSizeAllowed // 16 MB
	}
	if conf.ValueSize+conf.KeySize > conf.FlushThreshold {
		conf.FlushThreshold = maxFlushThresholdAllowed // 32 MB
	}
	return conf
}
