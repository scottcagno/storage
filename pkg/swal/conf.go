package swal

const (
	defaultBasePath             = "log"
	defaultMaxSegmentSize int64 = 128 << 10 // 128 KB
	defaultSyncOnWrite          = false
)

var defaultWALConfig = &SWALConfig{
	BasePath:       defaultBasePath,
	MaxSegmentSize: defaultMaxSegmentSize,
	SyncOnWrite:    defaultSyncOnWrite,
}

type SWALConfig struct {
	BasePath       string // base storage path
	MaxSegmentSize int64  // max segment size
	SyncOnWrite    bool   // perform sync every write
}

func checkWALConfig(conf *SWALConfig) *SWALConfig {
	if conf == nil {
		return defaultWALConfig
	}
	if conf.BasePath == *new(string) {
		conf.BasePath = defaultBasePath
	}
	if conf.MaxSegmentSize < 1 {
		conf.MaxSegmentSize = defaultMaxSegmentSize
	}
	return conf
}
