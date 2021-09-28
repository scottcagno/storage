package rbtree

import (
	"fmt"
	"runtime"
	"strings"
)

type rbEntry struct {
	key   string
	value []byte
}

func (e rbEntry) String() string {
	return fmt.Sprintf("rbEntry.key=%q, rbEntry.value=%q\n", e.key, e.value)
}

var empty = *new(rbEntry)

func isempty(e rbEntry) bool {
	return e.key == ""
}

func compare(this, that rbEntry) int {
	if len(this.key) < len(that.key) {
		return -1
	}
	if len(this.key) > len(that.key) {
		return +1
	}
	if this.key < that.key {
		return -1
	}
	if this.key > that.key {
		return 1
	}
	return 0
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
	entry  rbEntry
}

func (n *rbNode) String() string {
	left, right, parent := "", "", ""
	if n.left != nil {
		left = n.left.entry.key
	}
	if n.right != nil {
		right = n.right.entry.key
	}
	if n.parent != nil {
		parent = n.parent.entry.key
	}
	return fmt.Sprintf("node.key=%q, node.left=%q, node.right=%q, node.parent=%q",
		n.entry.key, left, right, parent)

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

// Has tests and returns a boolean value if the
// provided key exists in the tree
func (t *rbTree) Has(key string) bool {
	_, ok := t.getInternal(key)
	return ok
}

// Add adds the provided key and value only if it does not
// already exist in the tree. It returns false if the key and
// value was not able to be added, and true if it was added
// successfully
func (t *rbTree) Add(key string, value []byte) bool {
	_, ok := t.getInternal(key)
	if ok {
		// key already exists, so we are not adding
		return false
	}
	t.putInternal(key, value)
	return true
}

func (t *rbTree) Put(key string, value []byte) ([]byte, bool) {
	return t.putInternal(key, value)
}

func (t *rbTree) putInternal(key string, value []byte) ([]byte, bool) {
	e := rbEntry{key: key, value: value}
	if isempty(e) {
		return nil, false
	}
	// insert returns the node inserted
	// and if the node returned already
	// existed and/or was updated
	ret, ok := t.insert(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  e,
	})
	return ret.entry.value, ok
}

func (t *rbTree) Get(key string) ([]byte, bool) {
	return t.getInternal(key)
}

// GetNearMin performs an approximate search for the specified key
// and returns the closest key that is less than (the predecessor)
// to the searched key as well as a boolean reporting true if an
// exact match was found for the key, and false if it is unknown
// or and exact match was not found
func (t *rbTree) GetNearMin(key string) ([]byte, bool) {
	e := rbEntry{key: key}
	if isempty(e) {
		return nil, false
	}
	ret := t.searchApprox(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  e,
	})
	prev := t.predecessor(ret).entry.value
	if prev == nil {
		_, prev, _ = t.Min()
	}
	return prev, ret.entry.key == key
}

// GetNearMax performs an approximate search for the specified key
// and returns the closest key that is greater than (the successor)
// to the searched key as well as a boolean reporting true if an
// exact match was found for the key, and false if it is unknown or
// and exact match was not found
func (t *rbTree) GetNearMax(key string) ([]byte, bool) {
	e := rbEntry{key: key}
	if isempty(e) {
		return nil, false
	}
	ret := t.searchApprox(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  e,
	})
	return t.successor(ret).entry.value, ret.entry.key == key
}

// GetApproxPrevNext performs an approximate search for the specified key
// and returns the searched key, the predecessor, and the successor and a
// boolean reporting true if an exact match was found for the key, and false
// if it is unknown or and exact match was not found
func (t *rbTree) GetApproxPrevNext(key string) (rbEntry, rbEntry, rbEntry, bool) {
	e := rbEntry{key: key}
	if isempty(e) {
		return rbEntry{}, rbEntry{}, rbEntry{}, false
	}
	ret := t.searchApprox(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  e,
	})
	return ret.entry, t.predecessor(ret).entry, t.successor(ret).entry,
		ret.entry.key == key
}

func (t *rbTree) getInternal(key string) ([]byte, bool) {
	e := rbEntry{key: key}
	if isempty(e) {
		return nil, false
	}
	ret := t.search(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  e,
	})
	return ret.entry.value, !isempty(ret.entry)
}

func (t *rbTree) Del(key string) ([]byte, bool) {
	return t.delInternal(key)
}

func (t *rbTree) delInternal(key string) ([]byte, bool) {
	e := rbEntry{key: key}
	if isempty(e) {
		return nil, false
	}
	cnt := t.count
	ret := t.delete(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  e,
	})
	return ret.entry.value, cnt == t.count+1
}

func (t *rbTree) Len() int {
	return t.count
}

// Size returns the size in bytes
func (t *rbTree) Size() int64 {
	return t.size
}

func (t *rbTree) Min() (string, []byte, bool) {
	x := t.min(t.root)
	if x == t.NIL {
		return "", nil, false
	}
	return x.entry.key, x.entry.value, true
}

func (t *rbTree) Max() (string, []byte, bool) {
	x := t.max(t.root)
	if x == t.NIL {
		return "", nil, false
	}
	return x.entry.key, x.entry.value, true
}

type Iterator func(key string, value []byte) bool

func (t *rbTree) Scan(iter func(key string, value []byte) bool) {
	t.ascend(t.root, t.min(t.root).entry, iter)
}

func (t *rbTree) ScanBack(iter Iterator) {
	t.descend(t.root, t.max(t.root).entry, iter)
}

func (t *rbTree) ScanRange(startKey, endKey string, iter Iterator) {
	t.ascendRange(t.root, startKey, endKey, iter)
}

func (t *rbTree) String() string {
	var sb strings.Builder
	t.ascend(t.root, t.min(t.root).entry, func(key string, value []byte) bool {
		sb.WriteString(rbEntry{key: key, value: value}.String())
		return true
	})
	return sb.String()
}

func (t *rbTree) Close() {
	t.NIL = nil
	t.root = nil
	t.count = 0
	return
}

func (t *rbTree) Reset() {
	t.NIL = nil
	t.root = nil
	t.count = 0
	runtime.GC()
	n := &rbNode{
		left:   nil,
		right:  nil,
		parent: nil,
		color:  BLACK,
		entry:  empty,
	}
	t.NIL = n
	t.root = n
	t.count = 0
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
			t.size -= int64(len(x.entry.key) + len(x.entry.value))
			t.size += int64(len(z.entry.key) + len(z.entry.value))
			// originally we were just returning x
			// without updating the rbEntry, but if we
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
	t.size += int64(len(z.entry.key) + len(z.entry.value))
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

// trying out a slightly different search method
// that (hopefully) will not return nil values and
// instead will return approximate node matches
func (t *rbTree) searchApprox(x *rbNode) *rbNode {
	p := t.root
	for p != t.NIL {
		if compare(p.entry, x.entry) == -1 {
			if p.right == t.NIL {
				break
			}
			p = p.right
		} else if compare(x.entry, p.entry) == -1 {
			if p.left == t.NIL {
				break
			}
			p = p.left
		} else {
			break
		}
	}
	return p
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

func (t *rbTree) predecessor(x *rbNode) *rbNode {
	if x == t.NIL {
		return t.NIL
	}
	if x.left != t.NIL {
		return t.max(x.left)
	}
	y := x.parent
	for y != t.NIL && x == y.left {
		x = y
		y = y.parent
	}
	return y
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
	t.size -= int64(len(ret.entry.key) + len(ret.entry.value))
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

func (t *rbTree) ascend(x *rbNode, entry rbEntry, iter Iterator) bool {
	if x == t.NIL {
		return true
	}
	if !(compare(x.entry, entry) == -1) {
		if !t.ascend(x.left, entry, iter) {
			return false
		}
		if !iter(x.entry.key, x.entry.value) {
			return false
		}
	}
	return t.ascend(x.right, entry, iter)
}

func (t *rbTree) __Descend(pivot rbEntry, iter Iterator) {
	t.descend(t.root, pivot, iter)
}

func (t *rbTree) descend(x *rbNode, pivot rbEntry, iter Iterator) bool {
	if x == t.NIL {
		return true
	}
	if !(compare(pivot, x.entry) == -1) {
		if !t.descend(x.right, pivot, iter) {
			return false
		}
		if !iter(x.entry.key, x.entry.value) {
			return false
		}
	}
	return t.descend(x.left, pivot, iter)
}

func (t *rbTree) ascendRange(x *rbNode, inf, sup string, iter Iterator) bool {
	if x == t.NIL {
		return true
	}
	if !(compare(x.entry, rbEntry{key: sup}) == -1) {
		return t.ascendRange(x.left, inf, sup, iter)
	}
	if compare(x.entry, rbEntry{key: inf}) == -1 {
		return t.ascendRange(x.right, inf, sup, iter)
	}
	if !t.ascendRange(x.left, inf, sup, iter) {
		return false
	}
	if !iter(x.entry.key, x.entry.value) {
		return false
	}
	return t.ascendRange(x.right, inf, sup, iter)
}