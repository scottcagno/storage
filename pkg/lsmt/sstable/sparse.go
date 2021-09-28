package sstable

import (
	"github.com/scottcagno/storage/pkg/lsmt/rbtree"
	"log"
	"math"
	"path/filepath"
)

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
	ssi.Scan(func(k string, off int64) bool {
		if i%(count/n) == 0 {
			log.Printf("adding to sparse index: k=%q, v=%d\n", k, off)
			spi.rbt.Put(k, rbtree.IntToVal(off))
		}
		i++
		return true
	})
	return spi, nil
}

func (spi *SparseIndex) Search(k string) (string, int64) {
	v, _ := spi.rbt.GetNearMin(k)
	path := filepath.Join(spi.base, DataFileNameFromIndex(spi.index))
	offset := rbtree.ValToInt(v)
	return path, offset
}

func (spi *SparseIndex) First() string {
	k, _, _ := spi.rbt.Min()
	return k
}

func (spi *SparseIndex) Last() string {
	k, _, _ := spi.rbt.Max()
	return k
}

func (spi *SparseIndex) HasKey(k string) bool {
	_, prev, next, ok := spi.rbt.GetApproxKeyPrevNext(k)
	if ok {
		return true
	}
	if prev < k && k < next {
		return true
	}
	return false
}
