package lsmtree

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/bloom"
	"os"
	"path/filepath"
	"strconv"
)

const (
	filePrefix      = "sst-"
	dataFileSuffix  = ".dat"
	indexFileSuffix = ".idx"
)

func indexToFileName(index int64) string {
	hexa := strconv.FormatInt(index, 16)
	return fmt.Sprintf("%s%010s%s", filePrefix, hexa, dataFileSuffix)
}

func fileNameToIndex(name string) (int64, error) {
	hexa := name[len(filePrefix) : len(name)-len(dataFileSuffix)]
	return strconv.ParseInt(hexa, 16, 32)
}

type SSTable struct {
	path  string
	bloom *ssTableBloom
	fd    *os.File
	//index *ssTableIndex
}

type ssTableBloom struct {
	*bloom.BloomFilter
}

type ssTableIndex struct {
	*ssTableBloom
}

type ssTable struct {
	path string
	fd   *os.File
	*ssTableBloom
}

type ssTableManager struct {
	baseDir string
	seqnum  int64
}

func openSSTableManager(base string) (*ssTableManager, error) {
	sstm := &ssTableManager{
		baseDir: base,
	}
	return sstm, nil
}

func (sstm *ssTableManager) createSSTable(memt *memTable) error {
	// create new data file path
	path := filepath.Join(sstm.baseDir, indexToFileName(sstm.seqnum))
	// open data file
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	// create new bloom filter
	bf := bloom.NewBloomFilter(1 << 13) // 8192 bytes
	// fill bloom filter
	// iterate mem-table entries
	err = memt.scan(func(e *Entry) bool {
		// check entry for "deleted" one
		if e.hasTombstone() {
			return true // ignore "deleted" values
		}
		// write entry to bloom filter
		bf.Set(e.Key)
		// marshal bloom filter info data to be written
		data, err := bf.MarshalBinary()
		if err != nil {
			return false
		}
		// write bloom filter data to head of ss-table
		_, err = file.Write(data)
		if err != nil {
			return false
		}
		return true
	})
	// init and return SSTable
	sst := &SSTable{
		path: path,
		fd:   file,
		bloom: &ssTableBloom{
			BloomFilter: bf,
		},
	}
	// flush data
	err = sst.fd.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (sstm *ssTableManager) get(e *Entry) (*Entry, error) {
	return nil, nil
}

func (sstm *ssTableManager) flushToSSTable(memt *memTable) error {
	return nil
}
