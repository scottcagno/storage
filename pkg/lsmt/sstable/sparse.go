package sstable

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/rbtree/augmented"
	"log"
	"math"
	"path/filepath"
	"strings"
)

type rbTreeEntry struct {
	Key   string
	Path  string
	Index *binary.Index
}

func (r rbTreeEntry) Compare(that augmented.RBEntry) int {
	return strings.Compare(r.Key, that.(rbTreeEntry).Key)
}

func (r rbTreeEntry) Size() int {
	return len(r.Key) + 8
}

func (r rbTreeEntry) String() string {
	return fmt.Sprintf("entry.Key=%q", r.Key)
}

type SparseIndex struct {
	base  string
	index int64
	rbt   *augmented.RBTree
}

func ratio(n int64) int64 {
	if n < 1 {
		return 0
	}
	if n == 1 {
		n++
	}
	return int64(math.Log2(float64(n)))
}

func OpenSparseIndex(base string, index int64) (*SparseIndex, error) {
	spi := &SparseIndex{
		base:  base,
		index: index,
		rbt:   augmented.NewRBTree(),
	}
	ssi, err := OpenSSTIndex(base, index)
	if err != nil {
		return nil, err
	}
	count := int64(ssi.Len())
	n, i := ratio(count), int64(0)
	path := filepath.Join(base, DataFileNameFromIndex(index))
	ssi.Scan(func(k string, off int64) bool {
		if i%(count/n) == 0 {
			log.Printf("adding to sparse index: k=%q, v=%d\n", k, off)
			spi.rbt.Put(rbTreeEntry{Key: k, Path: path, Index: &binary.Index{Key: []byte(k), Offset: off}})
		}
		i++
		return true
	})
	return spi, nil
}

func (spi *SparseIndex) Search(k string) (string, int64) {
	v, _ := spi.rbt.GetNearMin(rbTreeEntry{Key: k})
	return v.(rbTreeEntry).Path, v.(rbTreeEntry).Index.Offset
}

func (spi *SparseIndex) HasKey(k string) bool {
	key := rbTreeEntry{Key: k}
	_, prev, next, ok := spi.rbt.GetApproxPrevNext(key)
	if ok {
		return true
	}
	if prev.Compare(key) == -1 && key.Compare(next) == -1 {
		return true
	}
	return false
}
