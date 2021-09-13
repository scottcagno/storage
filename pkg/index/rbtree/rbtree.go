package rbtree

import (
	"container/list"
	"fmt"
	"runtime"
	"strings"
)

// Entry represents a key value pair for the ordered map.
// If the user decides to change any of this, take care
// to ensure you implement the Item interface properly in
// order to keep everything in working order.
type Entry struct {
	Key   string
	Value []byte
}

// Compare is a comparator function for an Entry
func (e *Entry) Compare(that Entry) int {
	if len(e.Key) < len(that.Key) {
		return -1
	}
	if len(e.Key) > len(that.Key) {
		return +1
	}
	if e.Key < that.Key {
		return -1
	}
	if e.Key > that.Key {
		return 1
	}
	return 0
}

var empty = *new(Entry)

func isempty(e Entry) bool {
	return e.Key == ""
}

func compare(this, that Entry) int {
	return this.Compare(that)
}

const (
	RED   = 0
	BLACK = 1
)

type rbNode struct {
	left   *rbNode
	right  *rbNode
	parent *rbNode
	color  uint
	entry  Entry
}

type RBTree = rbTree

// rbTree is a struct representing a rbTree
type rbTree struct {
	NIL   *rbNode
	root  *rbNode
	count int
	size  int64
}

func NewRBTree() *rbTree {
	return newRBTree()
}

// NewTree creates and returns a new rbTree
func newRBTree() *rbTree {
	n := &rbNode{
		left:   nil,
		right:  nil,
		parent: nil,
		color:  BLACK,
		entry:  empty,
	}
	return &rbTree{
		NIL:   n,
		root:  n,
		count: 0,
	}
}

func (t *rbTree) Put(key string, val []byte) ([]byte, bool) {
	ret, ok := t.PutEntry(Entry{Key: key, Value: val})
	return ret.Value, ok
}

func (t *rbTree) PutEntry(entry Entry) (Entry, bool) {
	if isempty(entry) {
		return empty, false
	}
	// insert returns the node inserted
	// and if the node returned already
	// existed and/or was updated
	ret, ok := t.insert(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  entry,
	})
	return ret.entry, ok
}

func (t *rbTree) Has(key string) (bool, int64) {
	ret, ok := t.GetEntry(Entry{Key: key})
	return ok, int64(len(ret.Value))
}

func (t *rbTree) Get(key string) ([]byte, bool) {
	ret, ok := t.GetEntry(Entry{Key: key})
	return ret.Value, ok
}

func (t *rbTree) GetEntry(entry Entry) (Entry, bool) {
	if isempty(entry) {
		return empty, false
	}
	ret := t.search(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  entry,
	})
	return ret.entry, !isempty(ret.entry)
}

func (t *rbTree) Del(key string) ([]byte, bool) {
	siz := t.count
	ret := t.DelEntry(Entry{Key: key})
	return ret.Value, siz == t.count+1
}

func (t *rbTree) DelEntry(entry Entry) Entry {
	if isempty(entry) {
		return *new(Entry)
	}
	ret := t.delete(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  entry,
	})
	return ret.entry
}

func (t *rbTree) Len() int {
	return t.count
}

// Size returns the size in bytes
func (t *rbTree) Size() int64 {
	return t.size
}

func (t *rbTree) Min() (Entry, bool) {
	x := t.min(t.root)
	if x == t.NIL {
		return *new(Entry), false
	}
	return x.entry, true
}

func (t *rbTree) Max() (Entry, bool) {
	x := t.max(t.root)
	if x == t.NIL {
		return *new(Entry), false
	}
	return x.entry, true
}

type Iterator func(entry Entry) bool

func (t *rbTree) ScanFront(iter Iterator) {
	t.ascend(t.root, t.min(t.root).entry, iter)
}

func (t *rbTree) ScanBack(iter Iterator) {
	t.descend(t.root, t.max(t.root).entry, iter)
}

func (t *rbTree) ScanRange(start, end Entry, iter Iterator) {
	t.ascendRange(t.root, start, end, iter)
}

func (t *rbTree) Ascend(entry Entry, iter Iterator) {
	t.ascend(t.root, entry, iter)
}

func (t *rbTree) AscendRange(ge, lt Entry, iter Iterator) {
	t.ascendRange(t.root, ge, lt, iter)
}

func (t *rbTree) ToList() (*list.List, error) {
	if t.count < 1 {
		return nil, fmt.Errorf("Error: there are not enough entrys in the tree\n")
	}
	li := list.New()
	t.ascend(t.root, t.min(t.root).entry, func(entry Entry) bool {
		li.PushBack(entry)
		return true
	})
	return li, nil
}

func (t *rbTree) FromList(li *list.List) error {
	for e := li.Front(); e != nil; e = e.Next() {
		entry, ok := e.Value.(Entry)
		if !ok {
			return fmt.Errorf("Error: cannot add to tree, element (%T) "+
				"does not implement the Entry interface\n", e.Value)
		}
		t.PutEntry(entry)
	}
	return nil
}

func (t *rbTree) String() string {
	var sb strings.Builder
	t.ascend(t.root, t.min(t.root).entry, func(entry Entry) bool {
		sb.WriteString(entry.String())
		return true
	})
	return sb.String()
}

func (e Entry) String() string {
	return fmt.Sprintf("Entry.Key=%q, Entry.Value=%q\n", e.Key, e.Value)
}

func (t *rbTree) Close() {
	t.NIL = nil
	t.root = nil
	t.count = 0
	return
}

func (t *rbTree) Reset() *rbTree {
	// clear all data
	t.NIL = nil
	t.root = nil
	t.count = 0
	// collect
	runtime.GC()
	// re-initialize
	return newRBTree()
}

func (t *rbTree) insert(z *rbNode) (*rbNode, bool) {
	x := t.root
	y := t.NIL
	for x != t.NIL {
		y = x
		if compare(z.entry, x.entry) == -1 {
			x = x.left
		} else if compare(x.entry, z.entry) == -1 {
			x = x.right
		} else {
			t.size -= int64(len(x.entry.Key) + len(x.entry.Value))
			t.size += int64(len(z.entry.Key) + len(z.entry.Value))
			// originally we were just returning x
			// without updating the entry, but if we
			// want it to have similar behavior to
			// a hashmap then we need to update any
			// entries that already exist in the tree
			x.entry = z.entry
			return x, true // true means an existing
			// value was found and updated. It should
			// be noted that we don't need to re-balance
			// the tree because they keys are not changing
			// and the tree is balance is maintained by
			// the keys and not their values.
		}
	}
	z.parent = y
	if y == t.NIL {
		t.root = z
	} else if compare(z.entry, y.entry) == -1 {
		y.left = z
	} else {
		y.right = z
	}
	t.count++
	t.size += int64(len(z.entry.Key) + len(z.entry.Value))
	t.insertFixup(z)
	return z, false
}

func (t *rbTree) leftRotate(x *rbNode) {
	if x.right == t.NIL {
		return
	}
	y := x.right
	x.right = y.left
	if y.left != t.NIL {
		y.left.parent = x
	}
	y.parent = x.parent
	if x.parent == t.NIL {
		t.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}
	y.left = x
	x.parent = y
}

func (t *rbTree) rightRotate(x *rbNode) {
	if x.left == t.NIL {
		return
	}
	y := x.left
	x.left = y.right
	if y.right != t.NIL {
		y.right.parent = x
	}
	y.parent = x.parent

	if x.parent == t.NIL {
		t.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}

	y.right = x
	x.parent = y
}

func (t *rbTree) insertFixup(z *rbNode) {
	for z.parent.color == RED {
		if z.parent == z.parent.parent.left {
			y := z.parent.parent.right
			if y.color == RED {
				z.parent.color = BLACK
				y.color = BLACK
				z.parent.parent.color = RED
				z = z.parent.parent
			} else {
				if z == z.parent.right {
					z = z.parent
					t.leftRotate(z)
				}
				z.parent.color = BLACK
				z.parent.parent.color = RED
				t.rightRotate(z.parent.parent)
			}
		} else {
			y := z.parent.parent.left
			if y.color == RED {
				z.parent.color = BLACK
				y.color = BLACK
				z.parent.parent.color = RED
				z = z.parent.parent
			} else {
				if z == z.parent.left {
					z = z.parent
					t.rightRotate(z)
				}
				z.parent.color = BLACK
				z.parent.parent.color = RED
				t.leftRotate(z.parent.parent)
			}
		}
	}
	t.root.color = BLACK
}

func (t *rbTree) search(x *rbNode) *rbNode {
	p := t.root
	for p != t.NIL {
		if compare(p.entry, x.entry) == -1 {
			p = p.right
		} else if compare(x.entry, p.entry) == -1 {
			p = p.left
		} else {
			break
		}
	}
	return p
}

// min traverses from root to left recursively until left is NIL
func (t *rbTree) min(x *rbNode) *rbNode {
	if x == t.NIL {
		return t.NIL
	}
	for x.left != t.NIL {
		x = x.left
	}
	return x
}

// max traverses from root to right recursively until right is NIL
func (t *rbTree) max(x *rbNode) *rbNode {
	if x == t.NIL {
		return t.NIL
	}
	for x.right != t.NIL {
		x = x.right
	}
	return x
}

func (t *rbTree) successor(x *rbNode) *rbNode {
	if x == t.NIL {
		return t.NIL
	}
	if x.right != t.NIL {
		return t.min(x.right)
	}
	y := x.parent
	for y != t.NIL && x == y.right {
		x = y
		y = y.parent
	}
	return y
}

func (t *rbTree) delete(key *rbNode) *rbNode {
	z := t.search(key)
	if z == t.NIL {
		return t.NIL
	}
	ret := &rbNode{t.NIL, t.NIL, t.NIL, z.color, z.entry}
	var y *rbNode
	var x *rbNode
	if z.left == t.NIL || z.right == t.NIL {
		y = z
	} else {
		y = t.successor(z)
	}
	if y.left != t.NIL {
		x = y.left
	} else {
		x = y.right
	}
	x.parent = y.parent

	if y.parent == t.NIL {
		t.root = x
	} else if y == y.parent.left {
		y.parent.left = x
	} else {
		y.parent.right = x
	}
	if y != z {
		z.entry = y.entry
	}
	if y.color == BLACK {
		t.deleteFixup(x)
	}
	t.size -= int64(len(ret.entry.Key) + len(ret.entry.Value))
	t.count--
	return ret
}

func (t *rbTree) deleteFixup(x *rbNode) {
	for x != t.root && x.color == BLACK {
		if x == x.parent.left {
			w := x.parent.right
			if w.color == RED {
				w.color = BLACK
				x.parent.color = RED
				t.leftRotate(x.parent)
				w = x.parent.right
			}
			if w.left.color == BLACK && w.right.color == BLACK {
				w.color = RED
				x = x.parent
			} else {
				if w.right.color == BLACK {
					w.left.color = BLACK
					w.color = RED
					t.rightRotate(w)
					w = x.parent.right
				}
				w.color = x.parent.color
				x.parent.color = BLACK
				w.right.color = BLACK
				t.leftRotate(x.parent)
				// this is to exit while loop
				x = t.root
			}
		} else {
			w := x.parent.left
			if w.color == RED {
				w.color = BLACK
				x.parent.color = RED
				t.rightRotate(x.parent)
				w = x.parent.left
			}
			if w.left.color == BLACK && w.right.color == BLACK {
				w.color = RED
				x = x.parent
			} else {
				if w.left.color == BLACK {
					w.right.color = BLACK
					w.color = RED
					t.leftRotate(w)
					w = x.parent.left
				}
				w.color = x.parent.color
				x.parent.color = BLACK
				w.left.color = BLACK
				t.rightRotate(x.parent)
				x = t.root
			}
		}
	}
	x.color = BLACK
}

func (t *rbTree) ascend(x *rbNode, entry Entry, iter Iterator) bool {
	if x == t.NIL {
		return true
	}
	if !(compare(x.entry, entry) == -1) {
		if !t.ascend(x.left, entry, iter) {
			return false
		}
		if !iter(x.entry) {
			return false
		}
	}
	return t.ascend(x.right, entry, iter)
}

func (t *rbTree) Descend(pivot Entry, iter Iterator) {
	t.descend(t.root, pivot, iter)
}

func (t *rbTree) descend(x *rbNode, pivot Entry, iter Iterator) bool {
	if x == t.NIL {
		return true
	}
	if !(compare(pivot, x.entry) == -1) {
		if !t.descend(x.right, pivot, iter) {
			return false
		}
		if !iter(x.entry) {
			return false
		}
	}
	return t.descend(x.left, pivot, iter)
}

func (t *rbTree) ascendRange(x *rbNode, inf, sup Entry, iter Iterator) bool {
	if x == t.NIL {
		return true
	}
	if !(compare(x.entry, sup) == -1) {
		return t.ascendRange(x.left, inf, sup, iter)
	}
	if compare(x.entry, inf) == -1 {
		return t.ascendRange(x.right, inf, sup, iter)
	}
	if !t.ascendRange(x.left, inf, sup, iter) {
		return false
	}
	if !iter(x.entry) {
		return false
	}
	return t.ascendRange(x.right, inf, sup, iter)
}
