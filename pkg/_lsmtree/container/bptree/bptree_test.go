package bptree

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
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
func TestBPTree_Has(t *testing.T) {
	tree := NewBPTree()
	util.AssertLen(t, 0, tree.Len())
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
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
func TestBPTree_HasInt(t *testing.T) {
	tree := NewBPTree()
	util.AssertLen(t, 0, tree.Len())
	for i := 0; i < n*thousand; i++ {
		tree.PutInt(int64(i), int64(i))
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
func TestBPTree_Put(t *testing.T) {
	tree := NewBPTree()
	util.AssertLen(t, 0, tree.Len())
	for i := 0; i < n*thousand; i++ {
		existing := tree.Put(makeKey(i), makeVal(i))
		if existing { // existing=updated
			t.Errorf("putting: %v", existing)
		}
	}
	util.AssertLen(t, n*thousand, tree.Len())
	tree.Close()
}

// signature: PutInt(key int64, val int64) (int64, bool)
func TestBPTree_PutInt(t *testing.T) {
	tree := NewBPTree()
	util.AssertLen(t, 0, tree.Len())
	for i := 0; i < n*thousand; i++ {
		existing := tree.PutInt(int64(i), int64(i))
		if existing { // existing=updated
			t.Errorf("putting: %v", existing)
		}
	}
	util.AssertLen(t, n*thousand, tree.Len())
	tree.Close()
}

// signature: Get(key string) ([]byte, bool)
func TestBPTree_Get(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	for i := 0; i < n*thousand; i++ {
		_, v := tree.Get(makeKey(i))
		if v == nil {
			t.Errorf("getting: %v", v)
		}
		util.AssertEqual(t, makeVal(i), v)
	}
	tree.Close()
}

// signature: GetInt(key int64) (int64, bool)
func TestBPTree_GetInt(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.PutInt(int64(i), int64(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	for i := 0; i < n*thousand; i++ {
		_, v := tree.GetInt(int64(i))
		if v == -1 {
			t.Errorf("getting: %v", v)
		}
		util.AssertEqual(t, int64(i), v)
	}
	tree.Close()
}

// signature: Del(key string) ([]byte, bool)
func TestBPTree_Del(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	for i := 0; i < n*thousand; i++ {
		_, v := tree.Del(makeKey(i))
		if v == nil {
			t.Errorf("delete: %v", v)
		}
	}
	util.AssertLen(t, 0, tree.Len())
	tree.Close()
}

// signature: DelInt(key int64) (int64, bool)
func TestBPTree_DelInt(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.PutInt(int64(i), int64(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	for i := 0; i < n*thousand; i++ {
		_, v := tree.DelInt(int64(i))
		if v == -1 {
			t.Errorf("delete: %v", v)
		}
	}
	util.AssertLen(t, 0, tree.Len())
	tree.Close()
}

// signature: Len() int
func TestBPTree_Len(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	tree.Close()
}

// signature: Size() int64
func TestBPTree_Size(t *testing.T) {
	tree := NewBPTree()
	var numBytes int64
	for i := 0; i < n*thousand; i++ {
		key, val := makeKey(i), makeVal(i)
		numBytes += int64(len(key) + len(val))
		tree.Put(key, val)
	}
	util.AssertLen(t, numBytes, tree.Size())
	log.Printf("size=%d\n", numBytes)
	tree.Close()
}

// signature: Min() (string, []byte, bool)
func TestBPTree_Min(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	k, v := tree.Min()
	if v == nil {
		t.Errorf("min: %v", tree)
	}
	util.AssertEqual(t, makeKey(0), k)
	tree.Close()
}

// signature: Max() (string, []byte, bool)
func TestBPTree_Max(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())
	k, v := tree.Max()
	if v == nil {
		t.Errorf("min: %v", tree)
	}
	util.AssertEqual(t, makeKey(n*thousand-1), k)
	tree.Close()
}

func TestBPTree_Range(t *testing.T) {
	tree := NewBPTree()
	for i := 0; i < n*thousand; i++ {
		tree.Put(makeKey(i), makeVal(i))
	}
	util.AssertLen(t, n*thousand, tree.Len())

	printInfo := false

	// do scan front
	tree.Range(func(key string, value []byte) bool {
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

func TestBPTree_Close(t *testing.T) {
	var tree *BPTree
	tree = NewBPTree()
	tree.Close()
}

func makeKey(i int) string {
	return fmt.Sprintf("key-%.6d", i)
}

func makeVal(i int) []byte {
	return []byte(fmt.Sprintf("{\"id\":%.6d,\"key\":\"key-%.6d\",\"value\":\"val-%.6d\"}", i, i, i))
}
