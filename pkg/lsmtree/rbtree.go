package lsmtree

import (
	"bytes"
	"runtime"
	"strings"
	"sync"
)

const (
	colorRED uint8 = iota
	colorBLK
)

var lock sync.RWMutex

// rbNode is a node of a rbtree
type rbNode struct {
	left   *rbNode // left is a left child node
	right  *rbNode // right is a right child node
	parent *rbNode // parent is a parent node
	color  uint8   // color is the color if this node
	entry  *Entry  // entry is the data this node holds
}

// rbTree is a struct representing a rbTree
type rbTree struct {
	nilNode *rbNode // NIL is a "leaf"; end of the line
	root    *rbNode // root is the head of the tree
	count   int     // count is the number of items in the tree
	size    int64   // size is the estimated size (in bytes) the tree is holding
}

// NewTree creates and returns a new rbTree
func newRBTree() *rbTree {
	n := &rbNode{
		left:   nil,
		right:  nil,
		parent: nil,
		color:  colorBLK,
		entry:  nil,
	}
	return &rbTree{
		nilNode: n,
		root:    n,
		count:   0,
		size:    0,
	}
}

// compare is the main comparator for the tree
func compare(this, that *Entry) int {
	return bytes.Compare(this.Key, that.Key)
}

// upsertAndCheckSize updates the provided entry if it already
// exists or inserts the supplied entry as a new entry if it
// does not exist. It returns the current size in bytes after
// performing the insert or update. It also returns a boolean
// reporting true if the tree has met or exceeded the provided
// threshold, and false if the current size is less than the
// provided threshold.
func (t *rbTree) upsertAndCheckSize(entry *Entry, threshold int64) (int64, bool) {
	// insert the entry in to the mem-table
	t.putInternal(entry)
	if t.size >= threshold {
		// size is greater or equal to supplied threshold
		// return size along with a true value (need flush)
		return t.size, true
	}
	// size has not met or exceeded supplied threshold
	// simply return the current size, and a false value
	return t.size, false
}

// getNearMin performs an approximate search for the key of the
// entry provided and returns the closest entry that contains a
// key that is less than (the predecessor) the searched entry key
// as well as a boolean reporting true if an exact match was found,
// and false if it is unknown or an exact match was not found.
func (t *rbTree) getNearMin(entry *Entry) (*Entry, bool) {
	if entry == nil {
		return nil, false
	}
	ret := t.searchApprox(&rbNode{
		left:   t.nilNode,
		right:  t.nilNode,
		parent: t.nilNode,
		color:  colorRED,
		entry:  entry,
	})
	prev := t.predecessor(ret).entry
	if prev == nil {
		prev, _ = t.firstEntry()
	}
	return prev, compare(ret.entry, entry) == 0
}

// getNearMax performs an approximate search for the key of the
// entry provided and returns the closest entry that contains a
// key that is greater than (the successor) the searched entry key
// as well as a boolean reporting true if an exact match was found,
// and false if it is unknown or an exact match was not found.
func (t *rbTree) getNearMax(entry *Entry) (*Entry, bool) {
	if entry == nil {
		return nil, false
	}
	ret := t.searchApprox(&rbNode{
		left:   t.nilNode,
		right:  t.nilNode,
		parent: t.nilNode,
		color:  colorRED,
		entry:  entry,
	})
	next := t.successor(ret).entry
	if next == nil {
		next, _ = t.lastEntry()
	}
	return next, compare(ret.entry, entry) == 0
}

// hasEntry tests and returns a boolean value if the
// provided key exists in the tree
func (t *rbTree) hasEntry(entry *Entry) bool {
	_, ok := t.getInternal(entry)
	return ok
}

// addEntry adds the provided key and value only if it does not
// already exist in the tree. It returns false if the key and
// value was not able to be added, and true if it was added
// successfully
func (t *rbTree) addEntry(entry *Entry) bool {
	_, ok := t.getInternal(entry)
	if ok {
		// key already exists, so we are not adding
		return false
	}
	t.putInternal(entry)
	return true
}

// putEntry acts as a regular upsert. It returns true if
// the entry was updated and false if it was added.
func (t *rbTree) putEntry(entry *Entry) (*Entry, bool) {
	return t.putInternal(entry)
}

// getEntry attempts to locate the entry with the matching
// the provided entry's key. It returns false if a matching
// entry could not be found.
func (t *rbTree) getEntry(entry *Entry) (*Entry, bool) {
	return t.getInternal(entry)
}

// delEntry attempts to locate the entry matching the
// provided entry's key and remove it from the tree.
// It returns true if the correct entry was found and
// removed and false if it could not be found or removed.
func (t *rbTree) delEntry(entry *Entry) (*Entry, bool) {
	return t.delInternal(entry)
}

// putInternal inserts and return the node along with a
// boolean value signaling true if the node was updated,
// and false if the node was a new addition.
func (t *rbTree) putInternal(entry *Entry) (*Entry, bool) {
	if entry == nil {
		return nil, false
	}
	// insert return the node along with
	// a boolean value signaling true if
	// the node was updated, and false if
	// the node was newly added.
	ret, ok := t.insert(&rbNode{
		left:   t.nilNode,
		right:  t.nilNode,
		parent: t.nilNode,
		color:  colorRED,
		entry:  entry,
	})
	return ret.entry, ok
}

// getInternal is the internal search wrapper. It
// attempts to locate the entry with a matching key and
// return it. If it succeeds it will return true, if it
// cannot find a matching entry it will return false.
func (t *rbTree) getInternal(entry *Entry) (*Entry, bool) {
	if entry == nil {
		return nil, false
	}
	ret := t.search(&rbNode{
		left:   t.nilNode,
		right:  t.nilNode,
		parent: t.nilNode,
		color:  colorRED,
		entry:  entry,
	})
	return ret.entry, ret.entry != nil
}

// delInternal is the internal delete wrapper. It attempts
// to locate the entry with the matching key and remove it
// from the tree. It returns true if the correct entry was
// found and removed; false if a  matching entry could not
// be found or removed.
func (t *rbTree) delInternal(entry *Entry) (*Entry, bool) {
	if entry == nil {
		return nil, false
	}
	cnt := t.count
	ret := t.delete(&rbNode{
		left:   t.nilNode,
		right:  t.nilNode,
		parent: t.nilNode,
		color:  colorRED,
		entry:  entry,
	})
	return ret.entry, cnt == t.count+1
}

// firstEntry returns the first (or min) entry
func (t *rbTree) firstEntry() (*Entry, bool) {
	x := t.min(t.root)
	if x == t.nilNode {
		return nil, false
	}
	return x.entry, true
}

// lastEntry returns the last (or max) entry
func (t *rbTree) lastEntry() (*Entry, bool) {
	x := t.max(t.root)
	if x == t.nilNode {
		return nil, false
	}
	return x.entry, true
}

// rangeFront calls f sequentially in a "forward" direction
// for each entry present in the tree (going from min, to
// max.) If f returns false, the iteration stops.
func (t *rbTree) rangeFront(f func(entry *Entry) bool) {
	t.ascend(t.root, t.min(t.root).entry, f)
}

// rangeFront calls f sequentially in a "reverse" direction
// for each entry present in the tree (going from max, to
// min.) If f returns false, the iteration stops.
func (t *rbTree) rangeBack(f func(entry *Entry) bool) {
	t.descend(t.root, t.max(t.root).entry, f)
}

// sizeOfEntries returns the total size in bytes that the
// entries are occupying
func (t *rbTree) sizeOfEntries() int64 {
	return t.size
}

// countOfEntries returns the total number of entries in
// the tree currently
func (t *rbTree) countOfEntries() int {
	return t.count
}

// close is an internal close method
// that frees up the tree.
func (t *rbTree) close() {
	t.nilNode = nil
	t.root = nil
	t.count = 0
	return
}

// reset is an internal reset that wipes
// the tree data and then "resets" it back
// to a newly created state
func (t *rbTree) reset() {
	t.nilNode = nil
	t.root = nil
	t.count = 0
	runtime.GC()
	n := &rbNode{
		left:   nil,
		right:  nil,
		parent: nil,
		color:  colorBLK,
		entry:  nil,
	}
	t.nilNode = n
	t.root = n
	t.count = 0
	t.size = 0
}

// searchApprox will not return nil values and
// instead will return approximate node matches
// if it cannot find an exact match
func (t *rbTree) searchApprox(x *rbNode) *rbNode {
	p := t.root
	for p != t.nilNode {
		if compare(p.entry, x.entry) == -1 {
			if p.right == t.nilNode {
				break
			}
			p = p.right
		} else if compare(x.entry, p.entry) == -1 {
			if p.left == t.nilNode {
				break
			}
			p = p.left
		} else {
			break
		}
	}
	return p
}

// insert is the inner-most insert call for the tree.
// It inserts the provided node, updating the entry
// if it already exists, or adding a new one if it
// is not currently in the tree. It returns true if
// an existing entry was found and updated, and false
// if an entry was simply added.
func (t *rbTree) insert(z *rbNode) (*rbNode, bool) {
	x := t.root
	y := t.nilNode
	for x != t.nilNode {
		y = x
		if compare(z.entry, x.entry) == -1 {
			x = x.left
		} else if compare(x.entry, z.entry) == -1 {
			x = x.right
		} else {
			t.size -= int64(x.entry.Size())
			t.size += int64(z.entry.Size())
			// originally we were just returning x
			// without updating the RBEntry, but if we
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
	if y == t.nilNode {
		t.root = z
	} else if compare(z.entry, y.entry) == -1 {
		y.left = z
	} else {
		y.right = z
	}
	t.count++
	t.size += int64(z.entry.Size())
	t.insertFixup(z)
	return z, false
}

// leftRotate is the inner leftRotate method (standard)
func (t *rbTree) leftRotate(x *rbNode) {
	if x.right == t.nilNode {
		return
	}
	y := x.right
	x.right = y.left
	if y.left != t.nilNode {
		y.left.parent = x
	}
	y.parent = x.parent
	if x.parent == t.nilNode {
		t.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}
	y.left = x
	x.parent = y
}

// leftRotate is the inner leftRotate method (standard)
func (t *rbTree) rightRotate(x *rbNode) {
	if x.left == t.nilNode {
		return
	}
	y := x.left
	x.left = y.right
	if y.right != t.nilNode {
		y.right.parent = x
	}
	y.parent = x.parent

	if x.parent == t.nilNode {
		t.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}

	y.right = x
	x.parent = y
}

// insertFixup is the internal fixup after insert (standard)
func (t *rbTree) insertFixup(z *rbNode) {
	for z.parent.color == colorRED {
		if z.parent == z.parent.parent.left {
			y := z.parent.parent.right
			if y.color == colorRED {
				z.parent.color = colorBLK
				y.color = colorBLK
				z.parent.parent.color = colorRED
				z = z.parent.parent
			} else {
				if z == z.parent.right {
					z = z.parent
					t.leftRotate(z)
				}
				z.parent.color = colorBLK
				z.parent.parent.color = colorRED
				t.rightRotate(z.parent.parent)
			}
		} else {
			y := z.parent.parent.left
			if y.color == colorRED {
				z.parent.color = colorBLK
				y.color = colorBLK
				z.parent.parent.color = colorRED
				z = z.parent.parent
			} else {
				if z == z.parent.left {
					z = z.parent
					t.rightRotate(z)
				}
				z.parent.color = colorBLK
				z.parent.parent.color = colorRED
				t.leftRotate(z.parent.parent)
			}
		}
	}
	t.root.color = colorBLK
}

// search is the internal search method (standard)
func (t *rbTree) search(x *rbNode) *rbNode {
	p := t.root
	for p != t.nilNode {
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
	if x == t.nilNode {
		return t.nilNode
	}
	for x.left != t.nilNode {
		x = x.left
	}
	return x
}

// max traverses from root to right recursively until right is NIL
func (t *rbTree) max(x *rbNode) *rbNode {
	if x == t.nilNode {
		return t.nilNode
	}
	for x.right != t.nilNode {
		x = x.right
	}
	return x
}

// predecessor returns the first node that is less than the one provided.
func (t *rbTree) predecessor(x *rbNode) *rbNode {
	if x == t.nilNode {
		return t.nilNode
	}
	if x.left != t.nilNode {
		return t.max(x.left)
	}
	y := x.parent
	for y != t.nilNode && x == y.left {
		x = y
		y = y.parent
	}
	return y
}

// successor returns the first node the is greater than the one provided.
func (t *rbTree) successor(x *rbNode) *rbNode {
	if x == t.nilNode {
		return t.nilNode
	}
	if x.right != t.nilNode {
		return t.min(x.right)
	}
	y := x.parent
	for y != t.nilNode && x == y.right {
		x = y
		y = y.parent
	}
	return y
}

// delete is the internal delete method (standard)
func (t *rbTree) delete(key *rbNode) *rbNode {
	z := t.search(key)
	if z == t.nilNode {
		return t.nilNode
	}
	ret := &rbNode{t.nilNode, t.nilNode, t.nilNode, z.color, z.entry}
	var y *rbNode
	var x *rbNode
	if z.left == t.nilNode || z.right == t.nilNode {
		y = z
	} else {
		y = t.successor(z)
	}
	if y.left != t.nilNode {
		x = y.left
	} else {
		x = y.right
	}
	x.parent = y.parent

	if y.parent == t.nilNode {
		t.root = x
	} else if y == y.parent.left {
		y.parent.left = x
	} else {
		y.parent.right = x
	}
	if y != z {
		z.entry = y.entry
	}
	if y.color == colorBLK {
		t.deleteFixup(x)
	}
	t.size -= int64(ret.entry.Size())
	t.count--
	return ret
}

// deleteFixup is the internal fixup after delete (standard)
func (t *rbTree) deleteFixup(x *rbNode) {
	for x != t.root && x.color == colorBLK {
		if x == x.parent.left {
			w := x.parent.right
			if w.color == colorRED {
				w.color = colorBLK
				x.parent.color = colorRED
				t.leftRotate(x.parent)
				w = x.parent.right
			}
			if w.left.color == colorBLK && w.right.color == colorBLK {
				w.color = colorRED
				x = x.parent
			} else {
				if w.right.color == colorBLK {
					w.left.color = colorBLK
					w.color = colorRED
					t.rightRotate(w)
					w = x.parent.right
				}
				w.color = x.parent.color
				x.parent.color = colorBLK
				w.right.color = colorBLK
				t.leftRotate(x.parent)
				// this is to exit while loop
				x = t.root
			}
		} else {
			w := x.parent.left
			if w.color == colorRED {
				w.color = colorBLK
				x.parent.color = colorRED
				t.rightRotate(x.parent)
				w = x.parent.left
			}
			if w.left.color == colorBLK && w.right.color == colorBLK {
				w.color = colorRED
				x = x.parent
			} else {
				if w.left.color == colorBLK {
					w.right.color = colorBLK
					w.color = colorRED
					t.leftRotate(w)
					w = x.parent.left
				}
				w.color = x.parent.color
				x.parent.color = colorBLK
				w.left.color = colorBLK
				t.rightRotate(x.parent)
				x = t.root
			}
		}
	}
	x.color = colorBLK
}

// ascend traverses the tree in ascending entry order
func (t *rbTree) ascend(x *rbNode, entry *Entry, f func(e *Entry) bool) bool {
	if x == t.nilNode {
		return true
	}
	if !(compare(x.entry, entry) == -1) {
		if !t.ascend(x.left, entry, f) {
			return false
		}
		if !f(x.entry) {
			return false
		}
	}
	return t.ascend(x.right, entry, f)
}

// descend traverses the tree in descending entry order
func (t *rbTree) descend(x *rbNode, pivot *Entry, f func(e *Entry) bool) bool {
	if x == t.nilNode {
		return true
	}
	if !(compare(pivot, x.entry) == -1) {
		if !t.descend(x.right, pivot, f) {
			return false
		}
		if !f(x.entry) {
			return false
		}
	}
	return t.descend(x.left, pivot, f)
}

// ascendRange traverses the tree in ascending entry order within the bounds
// of the inferior to the superior entries provided
func (t *rbTree) ascendRange(x *rbNode, inf, sup *Entry, f func(e *Entry) bool) bool {
	if x == t.nilNode {
		return true
	}
	if !(compare(x.entry, sup) == -1) {
		return t.ascendRange(x.left, inf, sup, f)
	}
	if compare(x.entry, inf) == -1 {
		return t.ascendRange(x.right, inf, sup, f)
	}
	if !t.ascendRange(x.left, inf, sup, f) {
		return false
	}
	if !f(x.entry) {
		return false
	}
	return t.ascendRange(x.right, inf, sup, f)
}

func (t *rbTree) Lock() {
	lock.Lock()
}

func (t *rbTree) Unlock() {
	lock.Unlock()
}

func (t *rbTree) MarshalBinary() ([]byte, error) {
	// lock in this case (for now)
	lock.Lock()
	defer lock.Unlock()
	// TODO: implement...
	return nil, nil
}

func (t *rbTree) UnmarshalBinary(data []byte) error {
	// lock in this case (for now)
	lock.Lock()
	defer lock.Unlock()
	// TODO: implement...
	return nil
}

func (t *rbTree) String() string {
	var sb strings.Builder
	t.ascend(t.root, t.min(t.root).entry, func(entry *Entry) bool {
		sb.WriteString(entry.String())
		return true
	})
	return sb.String()
}
