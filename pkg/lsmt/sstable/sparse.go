package sstable

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/trees/rbtree"
	"strings"
)

type sparseIndexEntry struct {
	LastKey    string
	SSTIndex   int64
	IndexEntry *binary.Index
}

func (r sparseIndexEntry) Compare(that rbtree.RBEntry) int {
	return strings.Compare(r.LastKey, that.(sparseIndexEntry).LastKey)
}

func (r sparseIndexEntry) Size() int {
	return len(r.LastKey) + 8
}

func (r sparseIndexEntry) String() string {
	return fmt.Sprintf("entry.LastKey=%q", r.LastKey)
}

type SparseIndex struct {
	rbt *rbtree.RBTree
}

func NewSparseIndex() *SparseIndex {
	return &SparseIndex{
		rbt: rbtree.NewRBTree(),
	}
}

func (s *SparseIndex) Put(last string, index int64) {
	s.rbt.Put(sparseIndexEntry{
		LastKey:  last,
		SSTIndex: index,
	})
}

/*

func ratio(n int64) int64 {
	if n < 1 {
		return 0
	}
	if n == 1 {
		n++
	}
	return int64(math.Log2(float64(n)))
}

func makeNewSparseIndex(index int64, ssi *SSTIndex) *SparseIndex {
	spi := &SparseIndex{
		rbt:   rbtree.NewRBTree(),
	}
	count := int64(ssi.Len())
	n, i := ratio(count), int64(0)
	ssi.Scan(func(k string, off int64) bool {
		if i%(count/n) == 0 {
			spi.rbt.Put(sparseIndexEntry{Key: k, Path: index, Index: &binary.Index{Key: []byte(k), Offset: off}})
		}
		i++
		return true
	})
	return spi
}

func (spi *SparseIndex) Search(k string) (int64, int64) {
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
*/
