package sstable

import (
	"github.com/scottcagno/storage/pkg/lsmtree/container/rbtree"
	"math"
)

type SparseIndex struct {
	rbt *rbtree.RBTree
}

func log2(n int64) int64 {
	if n < 1 {
		return 0
	}
	if n == 1 {
		n++
	}
	return int64(math.Log2(float64(n)))
}

func OpenSparseIndex(base string, index int64) (*SparseIndex, error) {
	sdi := &SparseIndex{
		rbt: rbtree.NewRBTree(),
	}
	ssi, err := OpenSSIndex(base, index)
	if err != nil {
		return nil, err
	}
	count := int64(ssi.Len())
	n, i := log2(count), int64(0)
	ssi.Scan(func(k string, off int64) bool {
		if i%(count/n) == 0 {
			sdi.rbt.Put(k, rbtree.IntToVal(off))
		}
		i++
		return true
	})
	return sdi, nil
}
