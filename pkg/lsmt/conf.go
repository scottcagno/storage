package lsmt

const (
	walPath                = "log"
	sstPath                = "data"
	defaultBasePath        = "lsm-db"
	defaultFlushThreshold  = 1 << 20 // 1 MB
	defaultBloomFilterSize = 1 << 16 // 64 KB
	defaultSyncOnWrite     = false
)

var defaultLSMConfig = &LSMConfig{
	BasePath:        defaultBasePath,
	FlushThreshold:  defaultFlushThreshold,
	SyncOnWrite:     defaultSyncOnWrite,
	BloomFilterSize: defaultBloomFilterSize,
}

// LSMConfig holds configuration settings for an LSMTree instance
type LSMConfig struct {
	BasePath        string // base storage path
	FlushThreshold  int64  // memtable flush threshold in KB
	SyncOnWrite     bool   // perform sync every time an entry is write
	BloomFilterSize uint   // specify the bloom filter size
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
	if conf.FlushThreshold < 1 {
		conf.FlushThreshold = defaultFlushThreshold
	}
	if conf.BloomFilterSize < 1 {
		conf.BloomFilterSize = defaultBloomFilterSize
	}
	return conf
}
