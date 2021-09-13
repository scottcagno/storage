package bptree

import (
	"fmt"
	"github.com/scottcagno/leviathan/pkg/util"
	"log"
	"testing"
)

const (
	thousand = 1000
	n        = 1
)

func TestNewBPTree(t *testing.T) {
	var tree *BPTree
	tree = NewBPTree()
	util.AssertNotNil(t, tree)
	tree.Close()
}

// signature: Has(key string) (bool, int64)
func TestRbTree_Has(t *testing.T) {
	tree := NewBPTree()
	util.AssertLen(t, 0, tree.Len())
	for i := 0; i < n*thousand; i++ {
		tree.Set(makeKey(i), makeVal(i))
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

// signature: HasInt(key int64) (bool, int64)
func TestRbTree_HasInt(t *testing.T) {
	tree := NewBPTree()
	util.AssertLen(t, 0, tree.Len())
	for i := 0; i < n*thousand; i++ {
		tree.SetInt(int64(i), int64(i))
	}
	for i := 0; i < n*thousand; i++ {
		ok := tree.HasInt(int64(i))
		if !ok { // existing=updated
			t.Errorf("has: %v", ok)
		}
	}
	util.AssertLen(t, n*thousand, tree.Len())
	tree.Close()
}

// signature: Put(key string, val []byte) ([]byte, bool)
func TestRbTree_Put(t *testing.T) {
	tree := NewBPTree()
	util.AssertLen(t, 0, tree.Len())
	for i := 0; i < n*thousand; i++ {
		_, existing := tree.Set(makeKey(i), makeVal(i))
		if existing { // existing=updated
			t.Errorf("putting: %v", existing)
		}
	}
	util.AssertLen(t, n*thousand, tree.Len())
	tree.Close()
}

// signature: PutInt(key int64, val int64) (int64, bool)
func TestRbTree_PutInt(t *testing.T) {
	tree := NewBPTree()
	util.AssertLen(t, 0, tree.Len())
	for i := 0; i < n*thousand; i++ {
		_, existing := tree.SetInt(int64(i), int64(i))
		if existing { // existing=updated
			t.Errorf("putting: %v", existing)
		}
	}
	util.AssertLen(t, n*thousand, tree.Len())
	tree.Close()
}

// signature: Get(key string) ([]byte, bool)
func TestRbTree_Get(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Set(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	for i := 0; i < n*thousand; i++ {
		val, ok := tree.Get(makeKey(i))
		if !ok {
			t.Errorf("getting: %v", ok)
		}
		util.AssertEqual(t, makeVal(i), val)
	}
	tree.Close()
}

// signature: GetInt(key int64) (int64, bool)
func TestRbTree_GetInt(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.SetInt(int64(i), int64(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	for i := 0; i < n*thousand; i++ {
		val, ok := tree.GetInt(int64(i))
		if !ok {
			t.Errorf("getting: %v", ok)
		}
		util.AssertEqual(t, int64(i), val)
	}
	tree.Close()
}

// signature: Del(key string) ([]byte, bool)
func TestRbTree_Del(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Set(makeKey(i), makeVal(i))
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

// signature: DelInt(key int64) (int64, bool)
func TestRbTree_DelInt(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.SetInt(int64(i), int64(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	for i := 0; i < n*thousand; i++ {
		_, ok := tree.DelInt(int64(i))
		if !ok {
			t.Errorf("delete: %v", ok)
		}
	}
	util.AssertLen(t, 0, tree.Len())
	tree.Close()
}

// signature: Len() int
func TestRbTree_Len(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Set(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	tree.Close()
}

// signature: Size() int64
func TestRbTree_Size(t *testing.T) {
	tree := NewBPTree()
	var numBytes int64
	for i := 0; i < n*thousand; i++ {
		key, val := makeKey(i), makeVal(i)
		numBytes += int64(len(key) + len(val))
		tree.Set(key, val)
	}
	util.AssertLen(t, numBytes, tree.Size())
	log.Printf("size=%d\n", numBytes)
	tree.Close()
}

// signature: Min() (string, []byte, bool)
func TestRbTree_Min(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Set(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	k, _, ok := tree.Min()
	if !ok {
		t.Errorf("min: %v", tree)
	}
	util.AssertEqual(t, makeKey(0), k)
	tree.Close()
}

// signature: Max() (string, []byte, bool)
func TestRbTree_Max(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Set(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	k, _, ok := tree.Max()
	if !ok {
		t.Errorf("min: %v", tree)
	}
	util.AssertEqual(t, makeKey(n*thousand-1), k)
	tree.Close()
}

// signature: ScanFront(iter Iterator)
func TestRbTree_ScanFront(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Set(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())

	printInfo := true

	// do scan front
	tree.ScanFront(func(key string, value []byte) bool {
		if key == "" {
			t.Errorf("scan front, issue with key: %v", key)
			return false
		}
		if printInfo {
			log.Printf("key: %s\n", key)
		}
		return true
	})

	tree.Close()
}

// signature: ScanBack(iter Iterator)
func TestRbTree_ScanBack(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Set(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())

	printInfo := true

	tree.ScanBack(func(key string, value []byte) bool {
		if key == "" {
			t.Errorf("scan back, issue with key: %v", key)
			return false
		}
		if printInfo {
			log.Printf("key: %s\n", key)
		}
		return true
	})

	tree.Close()
}

// signature: ScanRange(start Entry, end Entry, iter Iterator)
func TestRbTree_ScanRange(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Set(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())

	printInfo := true

	start, stop := makeKey(300), makeKey(700)
	tree.ScanRange(start, stop, func(key string, value []byte) bool {
		if key == "" && key < start && key > stop {
			t.Errorf("scan range, issue with key: %v", key)
			return false
		}
		if printInfo {
			log.Printf("key: %s\n", key)
		}
		return true
	})

	tree.Close()
}

// signature: ToList() (*list.List, error)
func TestRbTree_ToList(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Set(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())

	l, err := tree.ToList()
	if err != nil {
		t.Errorf("tolist: %v", err)
	}
	util.AssertLen(t, n*thousand, l.Len())
	l = nil
	tree.Close()
}

// signature: FromList(li *list.List) error
func TestRbTree_FromList(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Set(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	treeList, err := tree.ToList()
	if err != nil {
		t.Errorf("to list: %v", err)
	}
	util.AssertLen(t, n*thousand, treeList.Len())
	tree.Close()

	tree = NewBPTree()
	util.AssertLen(t, 0, tree.Len())

	err = tree.FromList(treeList)
	if err != nil {
		t.Errorf("from list: %v", err)
	}
	treeList = nil
	util.AssertLen(t, n*thousand, tree.Len())

	tree.Close()
}

// signature: Close()
func TestRbTree_Close(t *testing.T) {
	var tree *RBTree
	tree = NewBPTree()
	tree.Close()
}

func makeKey(i int) string {
	return fmt.Sprintf("key-%.6d", i)
}

func makeVal(i int) []byte {
	return []byte(fmt.Sprintf("{\"id\":%.6d,\"key\":\"key-%.6d\",\"value\":\"val-%.6d\"}", i, i, i))
}
