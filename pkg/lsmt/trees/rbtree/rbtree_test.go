package rbtree

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
	"log"
	"strconv"
	"testing"
)

const (
	thousand = 1000
	n        = 1
)

func NewEntry(k, v string) rbStringBytes {
	return rbStringBytes{
		Key:   k,
		Value: []byte(v),
	}
}

func TestFindNearest(t *testing.T) {
	tree := NewRBTree()
	// insert A, E, J, O, T, Z

	tree.Put(NewEntry("e", "e"))
	tree.Put(NewEntry("a", "a"))
	tree.Put(NewEntry("t", "t"))
	tree.Put(NewEntry("z", "z"))
	tree.Put(NewEntry("j", "j"))
	tree.Put(NewEntry("o", "o"))

	// print tree
	tree.Scan(func(e RBEntry) bool {
		fmt.Printf("%s\n", e)
		return true
	})

	// find O
	key := rbStringBytes{Key: "o"}
	a, b, c, d := tree.GetApproxPrevNext(key)
	fmt.Printf("find-%s: a=%s, b=%s, c=%s, d=%v\n", key, a, b, c, d)

	// find K
	key = rbStringBytes{Key: "k"}
	a, b, c, d = tree.GetApproxPrevNext(key)
	fmt.Printf("find-%s: a=%q, b=%q, c=%q, d=%v\n", key, a, b, c, d)

	// find F
	key = rbStringBytes{Key: "f"}
	a, b, c, d = tree.GetApproxPrevNext(key)
	fmt.Printf("find-%s: a=%q, b=%q, c=%q, d=%v\n", key, a, b, c, d)

	tree.Close()
}

func TestNewRBTree(t *testing.T) {
	var tree *RBTree
	tree = NewRBTree()
	util.AssertNotNil(t, tree)
	tree.Close()
}

func makeKey(i int) rbStringInt64 {
	return rbStringInt64{
		Key:   strconv.Itoa(i),
		Value: int64(i),
	}
}

// signature: Has(key string) (bool, int64)
func TestRbTree_Has(t *testing.T) {
	tree := NewRBTree()
	util.AssertLen(t, 0, tree.Len())
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i))
	}
	for i := 0; i < n*thousand; i++ {
		ok := tree.Has(makeKey(i))
		if !ok { // existing=updated
			t.Errorf("has: %v", ok)
		}
	}
	util.AssertLen(t, n*thousand, tree.Len())
	tree.Close()
}

// signature: Put(key string, val []byte) ([]byte, bool)
func TestRbTree_Put(t *testing.T) {
	tree := NewRBTree()
	util.AssertLen(t, 0, tree.Len())
	for i := 0; i < n*thousand; i++ {
		_, existing := tree.Put(makeKey(i))
		if existing { // existing=updated
			t.Errorf("putting: %v", existing)
		}
	}
	util.AssertLen(t, n*thousand, tree.Len())
	tree.Close()
}

// signature: Get(key string) ([]byte, bool)
func TestRbTree_Get(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	for i := 0; i < n*thousand; i++ {
		val, ok := tree.Get(makeKey(i))
		if !ok {
			t.Errorf("getting: %v", ok)
		}
		util.AssertEqual(t, makeKey(i), val)
	}
	tree.Close()
}

// signature: Del(key string) ([]byte, bool)
func TestRbTree_Del(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	for i := 0; i < n*thousand; i++ {
		_, ok := tree.Del(makeKey(i))
		if !ok {
			t.Errorf("delete: %v", ok)
		}
	}
	util.AssertLen(t, 0, tree.Len())
	tree.Close()
}

// signature: Len() int
func TestRbTree_Len(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	tree.Close()
}

// signature: Size() int64
func TestRbTree_Size(t *testing.T) {
	tree := NewRBTree()
	var numBytes int64
	for i := 0; i < n*thousand; i++ {
		key := makeKey(i)
		numBytes += int64(key.Size())
		tree.Put(key)
	}
	util.AssertLen(t, numBytes, tree.Size())
	log.Printf("size=%d\n", numBytes)
	tree.Close()
}

// signature: Min() (string, []byte, bool)
func TestRbTree_Min(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	k, ok := tree.Min()
	if !ok {
		t.Errorf("min: %v", tree)
	}
	util.AssertEqual(t, makeKey(0), k)
	tree.Close()
}

// signature: Max() (string, []byte, bool)
func TestRbTree_Max(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	k, ok := tree.Max()
	if !ok {
		t.Errorf("min: %v", tree)
	}
	util.AssertEqual(t, makeKey(n*thousand-1), k)
	tree.Close()
}

// signature: ScanFront(iter Iterator)
func TestRbTree_ScanFront(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())

	printInfo := true

	// do scan front
	tree.Scan(func(e RBEntry) bool {
		if e.(rbStringInt64).Key == "" {
			t.Errorf("scan front, issue with key: %s", e)
			return false
		}
		if printInfo {
			log.Printf("entry: %s\n", e)
		}
		return true
	})

	tree.Close()
}

// signature: ScanRange(start Entry, end Entry, iter Iterator)
func TestRbTree_ScanRange(t *testing.T) {
	tree := NewRBTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())

	printInfo := true

	start, stop := makeKey(300), makeKey(700)
	tree.ScanRange(start, stop, func(e RBEntry) bool {
		if e.(rbStringInt64).Key == "" && e.(rbStringInt64).Compare(start) == -1 && e.(rbStringInt64).Compare(stop) == 1 {
			t.Errorf("scan range, issue with key: %s", e)
			return false
		}
		if printInfo {
			log.Printf("entry: %s\n", e)
		}
		return true
	})

	tree.Close()
}

// signature: Close()
func TestRbTree_Close(t *testing.T) {
	var tree *RBTree
	tree = NewRBTree()
	tree.Close()
}
