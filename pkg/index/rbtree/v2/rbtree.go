package v2

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

const (
	RED   = 0
	BLACK = 1
)

type rbNode struct {
	left   *rbNode
	right  *rbNode
	parent *rbNode
	color  uint
	entry  entry
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

func (t *rbTree) Has(key string) bool {
	_, ok := t.get(key)
	return ok
}

func (t *rbTree) HasInt(key int64) bool {
	_, ok := t.get(IntToKey(key))
	return ok
}

func (t *rbTree) Put(key string, value []byte) ([]byte, bool) {
	return t.put(key, value)
}

func (t *rbTree) PutInt(key int64, value int64) (int64, bool) {
	val, ok := t.put(IntToKey(key), IntToVal(value))
	return ValToInt(val), ok
}

func (t *rbTree) put(key string, value []byte) ([]byte, bool) {
	e := entry{key: key, value: value}
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
	return t.get(key)
}

func (t *rbTree) GetInt(key int64) (int64, bool) {
	val, ok := t.get(IntToKey(key))
	return ValToInt(val), ok
}

func (t *rbTree) get(key string) ([]byte, bool) {
	e := entry{key: key}
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
	return t.del(key)
}

func (t *rbTree) DelInt(key int64) (int64, bool) {
	val, ok := t.del(IntToKey(key))
	return ValToInt(val), ok
}

func (t *rbTree) del(key string) ([]byte, bool) {
	e := entry{key: key}
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

func (t *rbTree) ScanFront(iter Iterator) {
	t.ascend(t.root, t.min(t.root).entry, iter)
}

func (t *rbTree) ScanBack(iter Iterator) {
	t.descend(t.root, t.max(t.root).entry, iter)
}

func (t *rbTree) ScanRange(startKey, endKey string, iter Iterator) {
	t.ascendRange(t.root, startKey, endKey, iter)
}

func (t *rbTree) ToList() (*list.List, error) {
	if t.count < 1 {
		return nil, fmt.Errorf("Error: there are not enough entrys in the tree\n")
	}
	li := list.New()
	t.ascend(t.root, t.min(t.root).entry, func(key string, value []byte) bool {
		li.PushBack(entry{key: key, value: value})
		return true
	})
	return li, nil
}

func (t *rbTree) FromList(li *list.List) error {
	for e := li.Front(); e != nil; e = e.Next() {
		ent, ok := e.Value.(entry)
		if !ok {
			return fmt.Errorf("Error: cannot add to tree, element (%T) "+
				"does not implement the entry interface\n", ent.value)
		}
		t.put(ent.key, ent.value)
	}
	return nil
}

func (t *rbTree) String() string {
	var sb strings.Builder
	t.ascend(t.root, t.min(t.root).entry, func(key string, value []byte) bool {
		sb.WriteString(entry{key: key, value: value}.String())
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

func (t *rbTree) ascend(x *rbNode, entry entry, iter Iterator) bool {
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

func (t *rbTree) Descend(pivot entry, iter Iterator) {
	t.descend(t.root, pivot, iter)
}

func (t *rbTree) descend(x *rbNode, pivot entry, iter Iterator) bool {
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
	if !(compare(x.entry, entry{key: sup}) == -1) {
		return t.ascendRange(x.left, inf, sup, iter)
	}
	if compare(x.entry, entry{key: inf}) == -1 {
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

func IntToKey(key int64) string {
	return "i" + strconv.FormatInt(key, 10)
}

func KeyToInt(key string) int64 {
	if len(key) != 11 || key[0] != 'i' {
		return -1
	}
	ikey, err := strconv.ParseInt(key[1:], 10, 0)
	if err != nil {
		return -1
	}
	return ikey
}

func IntToVal(val int64) []byte {
	buf := make([]byte, 1+binary.MaxVarintLen64)
	buf[0] = 'i'
	_ = binary.PutVarint(buf[1:], val)
	return buf
}

func ValToInt(val []byte) int64 {
	if len(val) != 11 || val[0] != 'i' {
		return -1
	}
	ival, n := binary.Varint(val[1:])
	if ival == 0 && n <= 0 {
		return -1
	}
	return ival
}
