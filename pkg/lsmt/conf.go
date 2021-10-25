package lsmt

import (
	"github.com/scottcagno/storage/pkg/lsmt/logger"
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
	defaultWalPath  = "log"
	defaultSstPath  = "data"
	defaultBasePath = "lsm-db"

	// syncing
	defaultSyncOnWrite = false

	// default sizes
	defaultFlushThreshold   = 2 * SizeMB  //   2 MB
	defaultBloomFilterSize  = 8 * SizeMB  //   8 MB
	defaultKeySizeAllowed   = 1 * SizeKB  //   1 KB
	defaultValueSizeAllowed = 64 * SizeKB //  64 KB

	// minimum size bounds
	minFlushThresholdAllowed  = 1 * SizeMB //   1 MB
	minBloomFilterSizeAllowed = 1 * SizeMB //   1 MB
	minKeySizeAllowed         = 1          //   1 B
	minValueSizeAllowed       = 1          //   1 B

	// maximum size bounds
	maxFlushThresholdAllowed  = 32 * SizeMB //  32 MB
	maxBloomFilterSizeAllowed = 16 * SizeMB //  16 MB
	maxKeySizeAllowed         = 2 * SizeMB  //   2 MB
	maxValueSizeAllowed       = 16 * SizeMB //  16 MB
)

// default config
var defaultLSMConfig = &LSMConfig{
	BaseDir:         defaultBasePath,
	Logger:          logger.DefaultLogger,
	SyncOnWrite:     defaultSyncOnWrite,
	FlushThreshold:  defaultFlushThreshold,
	BloomFilterSize: defaultBloomFilterSize,
	KeySize:         defaultKeySizeAllowed,
	ValueSize:       defaultValueSizeAllowed,
}

// LSMConfig holds configuration settings for an LSMTree instance
type LSMConfig struct {
	BaseDir         string         // base directory
	Logger          *logger.Logger // logger
	SyncOnWrite     bool           // perform sync every time an entry is written
	FlushThreshold  int64          // mem-table flush threshold
	BloomFilterSize uint           // specify the bloom filter size
	KeySize         int64          // the max allowed key size
	ValueSize       int64          // the maximum allowed value size
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
	sb.WriteString(strconv.Itoa(int(conf.KeySize)))
	sb.WriteString("\n")
	sb.WriteString("ValueSize: ")
	sb.WriteString(strconv.Itoa(int(conf.ValueSize)))
	return sb.String()
}

// checkLSMConfig is a helper to make sure the configuration
// options are correct and handles and missing options
func checkLSMConfig(conf *LSMConfig) *LSMConfig {
	if conf == nil {
		return defaultLSMConfig
	}
	if conf.BaseDir == *new(string) {
		conf.BaseDir = defaultBasePath
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
