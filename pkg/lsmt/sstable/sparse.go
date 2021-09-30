package sstable

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/trees/rbtree"
	"math"
	"path/filepath"
	"strings"
)

type sparseIndexEntry struct {
	Key   string
	Path  string
	Index *binary.Index
}

func (r sparseIndexEntry) Compare(that rbtree.RBEntry) int {
	return strings.Compare(r.Key, that.(sparseIndexEntry).Key)
}

func (r sparseIndexEntry) Size() int {
	return len(r.Key) + 8
}

func (r sparseIndexEntry) String() string {
	return fmt.Sprintf("entry.Key=%q", r.Key)
}

type SparseIndex struct {
	base  string
	index int64
	rbt   *rbtree.RBTree
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
		rbt:   rbtree.NewRBTree(),
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
			//log.Printf("adding to sparse index: k=%q, v=%d\n", k, off)
			spi.rbt.Put(sparseIndexEntry{Key: k, Path: path, Index: &binary.Index{Key: []byte(k), Offset: off}})
		}
		i++
		return true
	})
	// DON'T FORGET TO CLOSE STUFF!
	err = ssi.Close()
	if err != nil {
		return nil, err
	}
	return spi, nil
}

func (spi *SparseIndex) Search(k string) (string, int64) {
	v, _ := spi.rbt.GetNearMin(sparseIndexEntry{Key: k})
	return v.(sparseIndexEntry).Path, v.(sparseIndexEntry).Index.Offset
}

func (spi *SparseIndex) HasKey(k string) bool {
	key := sparseIndexEntry{Key: k}
	_, prev, next, ok := spi.rbt.GetApproxPrevNext(key)
	if ok {
		return true
	}
	if prev.Compare(key) == -1 && key.Compare(next) == -1 {
		return true
	}
	return false
}
