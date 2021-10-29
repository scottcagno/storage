package mtbl

import (
	"bytes"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"runtime"
	"strings"
)

var empty *binary.Entry = nil

func compare(this, that *binary.Entry) int {
	return bytes.Compare(this.Key, that.Key)
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
	entry  *binary.Entry
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
		size:  0,
	}
}

func (t *rbTree) Count() int {
	return t.count
}

// Has tests and returns a boolean value if the
// provided key exists in the tree
func (t *rbTree) Has(entry *binary.Entry) bool {
	_, ok := t.getInternal(entry)
	return ok
}

// HasKey tests and returns a boolean value if the
// provided key exists in the tree
func (t *rbTree) HasKey(k string) bool {
	e, ok := t.getInternal(&binary.Entry{Key: []byte(k)})
	return ok && e != nil && e.Value != nil
}

// Add adds the provided key and value only if it does not
// already exist in the tree. It returns false if the key and
// value was not able to be added, and true if it was added
// successfully
func (t *rbTree) Add(entry *binary.Entry) bool {
	_, ok := t.getInternal(entry)
	if ok {
		// key already exists, so we are not adding
		return false
	}
	t.putInternal(entry)
	return true
}

func (t *rbTree) Put(entry *binary.Entry) (*binary.Entry, bool) {
	return t.putInternal(entry)
}

// UpsertAndCheckIfFull updates the provided entry if it already
// exists or inserts the supplied entry as a new entry if it
// does not exist. UpsertAndCheckIfFull returns the current size
// in bytes after performing the insert or update. It also returns
// a boolean reporting true if the tree has met or exceeded the
// provided threshold, and false if the current size is less than
// the provided threshold.
func (t *rbTree) UpsertAndCheckIfFull(entry *binary.Entry, threshold int64) (int64, bool) {
	// TODO: possibly perform pre-check in future somehow??
	//
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

// UpsertBatchAndCheckIfFull ranges the batch of entries, and it
// updates the provided entry if it already exists or inserts the
// supplied entry as a new entry if it does not exist. When it's
// finished, UpsertBatchAndCheckIfFull returns the current size in
// bytes after performing the insert or update. It also returns a
// boolean value reporting true if the tree has met or exceeded the
// provided threshold, and false if the current size is less than
// the provided threshold.
func (t *rbTree) UpsertBatchAndCheckIfFull(batch *binary.Batch, threshold int64) (int64, bool) {
	// TODO: possibly perform pre-check in future somehow??
	//
	// range the batch entries
	for _, e := range batch.Entries {
		// insert the entry in to the mem-table
		t.putInternal(e)
	}
	// TODO: possibly think about dealing with partial batches??
	if t.size >= threshold {
		// size is greater or equal to supplied threshold
		// return size along with a true value (need flush)
		return t.size, true
	}
	// size has not met or exceeded supplied threshold
	// simply return the current size, and a false value
	return t.size, false
}

func (t *rbTree) PutBatch(batch *binary.Batch) {
	for _, entry := range batch.Entries {
		t.putInternal(entry)
	}
}

func (t *rbTree) putInternal(entry *binary.Entry) (*binary.Entry, bool) {
	if entry == nil {
		return nil, false
	}
	// insert return the node along with
	// a boolean value signaling true if
	// the node was updated, and false if
	// the node was newly added.
	ret, ok := t.insert(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  entry,
	})
	return ret.entry, ok
}

func (t *rbTree) Get(entry *binary.Entry) (*binary.Entry, bool) {
	return t.getInternal(entry)
}

// GetNearMin performs an approximate search for the specified key
// and returns the closest key that is less than (the predecessor)
// to the searched key as well as a boolean reporting true if an
// exact match was found for the key, and false if it is unknown
// or and exact match was not found
func (t *rbTree) GetNearMin(entry *binary.Entry) (*binary.Entry, bool) {
	if entry == nil {
		return nil, false
	}
	ret := t.searchApprox(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  entry,
	})
	prev := t.predecessor(ret).entry
	if prev == nil {
		prev, _ = t.Min()
	}
	return prev, compare(ret.entry, entry) == 0
}

// GetNearMax performs an approximate search for the specified key
// and returns the closest key that is greater than (the successor)
// to the searched key as well as a boolean reporting true if an
// exact match was found for the key, and false if it is unknown or
// and exact match was not found
func (t *rbTree) GetNearMax(entry *binary.Entry) (*binary.Entry, bool) {
	if entry == nil {
		return nil, false
	}
	ret := t.searchApprox(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  entry,
	})
	return t.successor(ret).entry, compare(ret.entry, entry) == 0
}

// GetApproxPrevNext performs an approximate search for the specified key
// and returns the searched key, the predecessor, and the successor and a
// boolean reporting true if an exact match was found for the key, and false
// if it is unknown or and exact match was not found
func (t *rbTree) GetApproxPrevNext(entry *binary.Entry) (*binary.Entry, *binary.Entry, *binary.Entry, bool) {
	if entry == nil {
		return nil, nil, nil, false
	}
	ret := t.searchApprox(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  entry,
	})
	return ret.entry, t.predecessor(ret).entry, t.successor(ret).entry,
		compare(ret.entry, entry) == 0
}

func (t *rbTree) getInternal(entry *binary.Entry) (*binary.Entry, bool) {
	if entry == nil {
		return nil, false
	}
	ret := t.search(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  entry,
	})
	return ret.entry, ret.entry != nil
}

func (t *rbTree) Del(entry *binary.Entry) (*binary.Entry, bool) {
	return t.delInternal(entry)
}

func (t *rbTree) delInternal(entry *binary.Entry) (*binary.Entry, bool) {
	if entry == nil {
		return nil, false
	}
	cnt := t.count
	ret := t.delete(&rbNode{
		left:   t.NIL,
		right:  t.NIL,
		parent: t.NIL,
		color:  RED,
		entry:  entry,
	})
	return ret.entry, cnt == t.count+1
}

func (t *rbTree) Len() int {
	return t.count
}

// Size returns the size in bytes
func (t *rbTree) Size() int64 {
	return t.size
}

func (t *rbTree) Min() (*binary.Entry, bool) {
	x := t.min(t.root)
	if x == t.NIL {
		return nil, false
	}
	return x.entry, true
}

func (t *rbTree) Max() (*binary.Entry, bool) {
	x := t.max(t.root)
	if x == t.NIL {
		return nil, false
	}
	return x.entry, true
}

// helper function for clone
func (t *rbTree) cloneEntries(t2 *rbTree) {
	t.ascend(t.root, t.min(t.root).entry, func(e *binary.Entry) bool {
		t2.putInternal(e)
		return true
	})
}

type Iterator func(entry *binary.Entry) bool

func (t *rbTree) Scan(iter Iterator) {
	t.ascend(t.root, t.min(t.root).entry, iter)
}

func (t *rbTree) ScanBack(iter Iterator) {
	t.descend(t.root, t.max(t.root).entry, iter)
}

func (t *rbTree) ScanRange(start, end *binary.Entry, iter Iterator) {
	t.ascendRange(t.root, start, end, iter)
}

func (t *rbTree) String() string {
	var sb strings.Builder
	t.ascend(t.root, t.min(t.root).entry, func(entry *binary.Entry) bool {
		sb.WriteString(entry.String())
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
	t.size = 0
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
	if y == t.NIL {
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
	t.size -= int64(ret.entry.Size())
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

func (t *rbTree) ascend(x *rbNode, entry *binary.Entry, iter Iterator) bool {
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

func (t *rbTree) descend(x *rbNode, pivot *binary.Entry, iter Iterator) bool {
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

func (t *rbTree) ascendRange(x *rbNode, inf, sup *binary.Entry, iter Iterator) bool {
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
