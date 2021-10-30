package lsmtree

import "math"

const (

	// path defaults
	defaultBaseDir = "lsmtree-data"
	defaultWalDir  = "log"
	defaultSstDir  = "sst"

	// syncing
	defaultSyncOnWrite  = false
	defaultLoggingLevel = LevelError

	// default sizes
	defaultFlushThreshold  = 2 << 20 // 2 MB
	defaultBloomFilterSize = 4 << 20 // 4 MB
	defaultMaxKeySize      = maxKeySizeAllowed
	defaultMaxValueSize    = maxValueSizeAllowed

	// minimum size bounds
	minFlushThresholdAllowed  = maxValueSizeAllowed * 16
	minBloomFilterSizeAllowed = minFlushThresholdAllowed
	minKeySizeAllowed         = 1
	minValueSizeAllowed       = 1

	// maximum size bounds
	maxFlushThresholdAllowed  = 8 << 20        //      8 MB
	maxBloomFilterSizeAllowed = 8 << 20        //      8 MB
	maxKeySizeAllowed         = math.MaxUint8  //    255 B
	maxValueSizeAllowed       = math.MaxUint16 // 65,535 B
)

var defaultOptions = &Options{
	BaseDir:      defaultBaseDir,
	SyncOnWrite:  defaultSyncOnWrite,
	LoggingLevel: defaultLoggingLevel,
}

func DefaultOptions(base string) *Options {
	defaultOptions.BaseDir = base
	return defaultOptions
}

type Options struct {
	BaseDir      string
	SyncOnWrite  bool
	LoggingLevel uint8
	MaxKeySize   int64
	MaxValueSize int64
}

func checkOptions(options *Options) *Options {
	if options == nil {
		return defaultOptions
	}
	if options.BaseDir == *new(string) {
		options.BaseDir = defaultBaseDir
	}
	if options.LoggingLevel <= 0 {
		options.LoggingLevel = defaultLoggingLevel
	}
	if options.MaxKeySize <= 0 {
		options.MaxKeySize = defaultMaxKeySize
	}
	if options.MaxKeySize < minKeySizeAllowed {
		options.MaxKeySize = minKeySizeAllowed
	}
	if options.MaxKeySize > maxKeySizeAllowed {
		options.MaxKeySize = maxKeySizeAllowed
	}
	if options.MaxValueSize <= 0 {
		options.MaxValueSize = defaultMaxValueSize
	}
	if options.MaxValueSize < minValueSizeAllowed {
		options.MaxValueSize = minValueSizeAllowed
	}
	if options.MaxValueSize > maxValueSizeAllowed {
		options.MaxValueSize = maxValueSizeAllowed
	}
	// NOTE: ensure that the key + value size is ALWAYS < mem-table
	return options
}
