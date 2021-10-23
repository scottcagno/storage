package lsmt

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/trees/rbtree"
	"strings"
)

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

type memtableEntry struct {
	Key   string
	Entry *binary.Entry
}

func (m memtableEntry) Compare(that rbtree.RBEntry) int {
	return strings.Compare(m.Key, that.(memtableEntry).Key)
}

func (m memtableEntry) Size() int {
	return len(m.Key) + len(m.Entry.Key) + len(m.Entry.Value)
}

func (m memtableEntry) String() string {
	return fmt.Sprintf("entry.key=%q", m.Key)
}
