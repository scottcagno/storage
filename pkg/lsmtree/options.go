package lsmtree

import "math"

const (

	// path defaults
	defaultBaseDir = "lsmtree-data"
	defaultWalDir  = "log"
	defaultSstDir  = "sst"

	// other defaults
	defaultSyncOnWrite    = false
	defaultLoggingLevel   = LevelError
	defaultFlushThreshold = 2 << 20 // 2 MB (min=1 MB, max=8 MB)

	// min and max
	minKeySizeAllowed   = 1
	minValueSizeAllowed = 1
	maxKeySizeAllowed   = math.MaxUint8  //    255 B
	maxValueSizeAllowed = math.MaxUint16 // 65,535 B
)

var defaultOptions = &Options{
	BaseDir:      defaultBaseDir,
	SyncOnWrite:  defaultSyncOnWrite,
	LoggingLevel: defaultLoggingLevel,
}

func DefaultOptions(base string) *Options {
	defaultOptions.BaseDir = base
	defaultOptions.flushThreshold = defaultFlushThreshold
	return defaultOptions
}

type Options struct {
	BaseDir        string
	SyncOnWrite    bool
	LoggingLevel   uint8
	flushThreshold int64
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
	options.flushThreshold = defaultFlushThreshold
	return options
}
