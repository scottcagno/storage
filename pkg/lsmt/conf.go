package lsmt

import (
	"math"
	"strconv"
	"strings"
)

const (
	SizeKB   = 1<<10 - 1
	SizeMB   = 1<<20 - 1
	Size64KB = math.MaxUint16
	Size4GB  = math.MaxUint32
)

const (

	// path defaults
	defaultBaseDir = "data"
	defaultWalDir  = "log"
	defaultSstDir  = "sst"

	// syncing
	defaultSyncOnWrite   = false
	defaultEnableLogging = false

	// default sizes
	defaultFlushThreshold  = 2 * SizeMB
	defaultBloomFilterSize = 4 * SizeMB
	defaultMaxKeySize      = maxKeySizeAllowed
	defaultMaxValueSize    = maxValueSizeAllowed

	// minimum size bounds
	minFlushThresholdAllowed  = maxValueSizeAllowed * 16
	minBloomFilterSizeAllowed = minFlushThresholdAllowed
	minKeySizeAllowed         = 1
	minValueSizeAllowed       = 1

	// maximum size bounds
	maxFlushThresholdAllowed  = 8 * SizeMB
	maxBloomFilterSizeAllowed = 8 * SizeMB
	maxKeySizeAllowed         = math.MaxUint8  //    255 B
	maxValueSizeAllowed       = math.MaxUint16 // 65,535 B
)

// default config
var defaultLSMConfig = &LSMConfig{
	BaseDir:         defaultBaseDir,
	SyncOnWrite:     defaultSyncOnWrite,
	EnableLogging:   defaultEnableLogging,
	FlushThreshold:  defaultFlushThreshold,
	BloomFilterSize: defaultBloomFilterSize,
	MaxKeySize:      defaultMaxKeySize,
	MaxValueSize:    defaultMaxValueSize,
}

// LSMConfig holds configuration settings for an LSMTree instance
type LSMConfig struct {
	BaseDir         string // base directory
	SyncOnWrite     bool   // perform sync every time an entry is written
	EnableLogging   bool   // enable logging
	FlushThreshold  int64  // mem-table flush threshold
	BloomFilterSize uint   // specify the bloom filter size
	MaxKeySize      int64  // the max allowed key size
	MaxValueSize    int64  // the maximum allowed value size
}

func (conf *LSMConfig) String() string {
	var sb strings.Builder
	sb.WriteString("BaseDir: ")
	sb.WriteString(conf.BaseDir)
	sb.WriteString("\n")
	sb.WriteString("SyncOnWrite: ")
	if conf.SyncOnWrite {
		sb.WriteString("true")
	} else {
		sb.WriteString("false")
	}
	sb.WriteString("\n")
	sb.WriteString("FlushThreshold: ")
	sb.WriteString(strconv.Itoa(int(conf.FlushThreshold)))
	sb.WriteString("\n")
	sb.WriteString("BloomFilterSize: ")
	sb.WriteString(strconv.Itoa(int(conf.BloomFilterSize)))
	sb.WriteString("\n")
	sb.WriteString("KeySize: ")
	sb.WriteString(strconv.Itoa(int(conf.MaxKeySize)))
	sb.WriteString("\n")
	sb.WriteString("ValueSize: ")
	sb.WriteString(strconv.Itoa(int(conf.MaxValueSize)))
	return sb.String()
}

// checkLSMConfig is a helper to make sure the configuration
// options are correct and handles and missing options
func checkLSMConfig(conf *LSMConfig) *LSMConfig {
	if conf == nil {
		return defaultLSMConfig
	}
	if conf.BaseDir == *new(string) {
		conf.BaseDir = defaultBaseDir
	}
	if conf.FlushThreshold <= 0 {
		conf.FlushThreshold = defaultFlushThreshold
	}
	if conf.FlushThreshold < minFlushThresholdAllowed {
		conf.FlushThreshold = minFlushThresholdAllowed
	}
	if conf.FlushThreshold > maxFlushThresholdAllowed {
		conf.FlushThreshold = maxFlushThresholdAllowed
	}
	if conf.BloomFilterSize <= 0 {
		conf.BloomFilterSize = defaultBloomFilterSize
	}
	if conf.BloomFilterSize < minBloomFilterSizeAllowed {
		conf.BloomFilterSize = minBloomFilterSizeAllowed
	}
	if conf.BloomFilterSize > maxBloomFilterSizeAllowed {
		conf.BloomFilterSize = maxBloomFilterSizeAllowed
	}
	if conf.MaxKeySize <= 0 {
		conf.MaxKeySize = defaultMaxKeySize
	}
	if conf.MaxKeySize < minKeySizeAllowed {
		conf.MaxKeySize = minKeySizeAllowed
	}
	if conf.MaxKeySize > maxKeySizeAllowed {
		conf.MaxKeySize = maxKeySizeAllowed
	}
	if conf.MaxValueSize <= 0 {
		conf.MaxValueSize = defaultMaxValueSize
	}
	if conf.MaxValueSize < minValueSizeAllowed {
		conf.MaxValueSize = minValueSizeAllowed
	}
	if conf.MaxValueSize > maxValueSizeAllowed {
		conf.MaxValueSize = maxValueSizeAllowed
	}
	if conf.MaxValueSize+conf.MaxKeySize >= conf.FlushThreshold {
		conf.FlushThreshold = maxFlushThresholdAllowed
	}
	return conf
}
