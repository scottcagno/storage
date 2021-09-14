/*
 * // Copyright (c) 2021. Scott Cagno. All rights reserved.
 * // The license can be found in the root of this project; see LICENSE.
 */

package bptree

import (
	"encoding/binary"
	"log"
	"strconv"
	"strings"
	"unsafe"
)

var stringZero = *new(string)
var ValTypeZero = *new([]byte)

func Compare(a, b string) int {
	return strings.Compare(a, b)
}

func Equal(a, b string) bool {
	return a == b
}

const (
	defaultOrder = orderSize32
	orderSize4   = 4
	orderSize8   = 8
	orderSize16  = 16
	orderSize32  = 32
	orderSize64  = 64
	orderSize128 = 128
	orderSize256 = 256
	orderSize512 = 512
)

// bpNode represents a bpNode of the BPTree
type bpNode struct {
	numKeys  int
	keys     [defaultOrder - 1]string
	pointers [defaultOrder]unsafe.Pointer
	parent   *bpNode
	isLeaf   bool
}

func (n *bpNode) hasKey(key string) bool {
	if n.isLeaf {
		for i := 0; i < n.numKeys; i++ {
			if Equal(key, n.keys[i]) {
				return true
			}
		}
	}
	return false
}

func (n *bpNode) record(key string) (*entry, bool) {
	if n.isLeaf {
		for i := 0; i < n.numKeys; i++ {
			if Equal(key, n.keys[i]) {
				return (*entry)(n.pointers[i]), true
			}
		}
	}
	return nil, false
}

// entry represents an entry pointed to by a leaf bpNode
type entry struct {
	Key   string
	Value []byte
}

// BPTree represents the root of a b+tree
type BPTree struct {
	root *bpNode
}

// NewBPTree creates and returns a new tree
func NewBPTree() *BPTree {
	return new(BPTree)
}

// Has returns a boolean indicating weather or not
// the provided key and associated record exists.
func (t *BPTree) Has(key string) bool {
	return t.findEntry(key) != nil
}

// HasInt tests and returns a boolean value if the
// provided key exists in the tree
func (t *BPTree) HasInt(key int64) bool {
	return t.findEntry(IntToKey(key)) != nil
}

// Add inserts a new record using the provided key. It
// only inserts an entry if the key does not already exist.
func (t *BPTree) Add(key string, value []byte) {
	// master insertUnique method only inserts if the key
	// does not currently exist in the tree
	t.insertUnique(key, value)
}

// AddInt inserts a new record using the provided integer key
// and value. It only inserts an entry if the key does not
// already exist.
func (t *BPTree) AddInt(key int64, value int64) {
	// master insertUnique method only inserts if the key
	// does not currently exist in the tree
	t.insertUnique(IntToKey(key), IntToVal(value))
}

// insertUnique inserts a new record using the provided key. It
// only inserts an entry if the key does not already exist.
func (t *BPTree) insertUnique(key string, value []byte) {
	// If the tree is empty, start a new one and return.
	if t.root == nil {
		t.root = startNewTree(key, &entry{key, value})
		return
	}
	// If the tree already exists, let's see what we
	// get when we try to find the correct leaf.
	leaf := findLeaf(t.root, key)
	// check to ensure the leaf node does already contain the key
	if leaf.hasKey(key) {
		return // Key already exists! So lets just return.
	}
	// If the tree already exists and the key has not been
	// found, then let's insert it into the tree and return!
	if leaf.numKeys < defaultOrder-1 {
		insertIntoLeaf(leaf, key, &entry{key, value})
		return
	}
	// Otherwise, insert, split and balance returning the updated root.
	t.root = insertIntoLeafAfterSplitting(t.root, leaf, key, &entry{key, value})
}

// Put is mainly used when you wish to upsert as it assumes the
// data to already be contained the tree. It will  overwrite
// duplicate keys, as it does not check to see if the key exists
func (t *BPTree) Put(key string, value []byte) bool {
	// master insert method treats insertion much like
	// "setting" in a hashmap (an upsert) by default
	return t.insert(key, value)
}

// PutInt is mainly used when you wish to upsert as it assumes the
// data to already be contained the tree. It will overwrite
// duplicate keys, as it does not check to see if the key exists
func (t *BPTree) PutInt(key int64, value int64) bool {
	// master insert method treats insertion much like
	// "setting" in a hashmap (an upsert) by default
	return t.insert(IntToKey(key), IntToVal(value))
}

// Get returns the record for a given key if it exists
func (t *BPTree) Get(key string) (string, []byte) {
	e := t.findEntry(key)
	if e == nil {
		return "", nil
	}
	return e.Key, e.Value
}

// GetInt returns the record for a given key if it exists
func (t *BPTree) GetInt(key int64) (int64, int64) {
	e := t.findEntry(IntToKey(key))
	if e == nil {
		return -1, -1
	}
	return KeyToInt(e.Key), ValToInt(e.Value)
}

func (t *BPTree) Del(key string) (string, []byte) {
	e := t.delete(key)
	if e == nil {
		return "", nil
	}
	return e.Key, e.Value
}

func (t *BPTree) DelInt(key int64) (int64, int64) {
	e := t.delete(IntToKey(key))
	if e == nil {
		return -1, -1
	}
	return KeyToInt(e.Key), ValToInt(e.Value)
}

func (t *BPTree) Range(iter func(k string, v []byte) bool) {
	c := findFirstLeaf(t.root)
	if c == nil {
		return
	}
	var e *entry
	for {
		for i := 0; i < c.numKeys; i++ {
			e = (*entry)(c.pointers[i])
			if e != nil && !iter(e.Key, e.Value) {
				continue
			}
		}
		if c.pointers[defaultOrder-1] != nil {
			c = (*bpNode)(c.pointers[defaultOrder-1])
		} else {
			break
		}
	}
}

func (t *BPTree) Min() (string, []byte) {
	c := findFirstLeaf(t.root)
	if c == nil {
		return "", nil
	}
	e := (*entry)(c.pointers[0])
	return e.Key, e.Value
}

func (t *BPTree) Max() (string, []byte) {
	c := findLastLeaf(t.root)
	if c == nil {
		return "", nil
	}
	e := (*entry)(c.pointers[c.numKeys-1])
	return e.Key, e.Value
}

func (t *BPTree) Len() int {
	var count int
	for n := findFirstLeaf(t.root); n != nil; n = n.nextLeaf() {
		count += n.numKeys
	}
	return count
}

func (t *BPTree) Size() int64 {
	c := findFirstLeaf(t.root)
	if c == nil {
		return 0
	}
	var s int64
	var e *entry
	for {
		for i := 0; i < c.numKeys; i++ {
			e = (*entry)(c.pointers[i])
			if e != nil {
				s += int64(len(e.Key) + len(e.Value))
			}
		}
		if c.pointers[defaultOrder-1] != nil {
			c = (*bpNode)(c.pointers[defaultOrder-1])
		} else {
			break
		}
	}
	return s
}

func (t *BPTree) Close() {
	//t.destroyTree()
	t.root = nil
}

// insert is the "master" insertion function.
// Inserts a key and an associated Value into
// the B+ tree, causing the tree to be adjusted
// however necessary to maintain the B+ tree
// properties
func (t *BPTree) insert(key string, value []byte) bool {

	// CASE: BPTree does not exist yet, start a new tree
	if t.root == nil {
		t.root = startNewTree(key, &entry{key, value})
		return false
	}

	// The current implementation ignores duplicates (will treat it kind of like a set operation)
	leaf, recordPointer := t.find(key)
	if recordPointer != nil {

		// If the key already exists in this tree then proceed to update Value and return
		recordPointer.Value = value
		return true
	}

	// No Record found, so create a new Record for the Value. NOTE: Normally t.find would not return
	// a record pointer in which case we would need the line below this.
	//recordPointer = makeRecord(Value)

	// CASE: BPTree already exists (continue through the rest of the function)

	// Leaf has room for the key and recordPointer--insert into leaf and return
	if leaf.numKeys < defaultOrder-1 {
		insertIntoLeaf(leaf, key, &entry{key, value})
		return false
	}

	// Leaf does not have enough room and needs to be split
	t.root = insertIntoLeafAfterSplitting(t.root, leaf, key, &entry{key, value})
	return false
}

// startNewTree first insertion case: starts a new tree
func startNewTree(key string, pointer *entry) *bpNode {
	root := &bpNode{isLeaf: true} // makeLeaf()
	root.keys[0] = key
	root.pointers[0] = unsafe.Pointer(pointer)
	root.pointers[defaultOrder-1] = nil
	root.parent = nil
	root.numKeys++
	return root
}

// insertIntoNewRoot creates a new root for two subtrees and inserts the appropriate key into the new root
func insertIntoNewRoot(left *bpNode, key string, right *bpNode) *bpNode {
	root := &bpNode{} // makeNode()
	root.keys[0] = key
	root.pointers[0] = unsafe.Pointer(left)
	root.pointers[1] = unsafe.Pointer(right)
	root.numKeys++
	root.parent = nil
	left.parent = root
	right.parent = root
	return root
}

/*
 * insertIntoParent inserts a new bpNode (leaf or internal bpNode) into the tree--returns the root of
 * the tree after insertion is complete
 */
func insertIntoParent(root *bpNode, left *bpNode, key string, right *bpNode) *bpNode {

	/*
	 * Case: new root
	 */
	if left.parent == nil {
		return insertIntoNewRoot(left, key, right)
	}

	/*
	 * Case: leaf or bpNode. (Remainder of function body.)
	 * Find the parents pointer to the left bpNode
	 */
	leftIndex := getLeftIndex(left.parent, left)

	/*
	 * Simple case: the new key fits into the bpNode.
	 */
	if left.parent.numKeys < defaultOrder-1 {
		return insertIntoNode(root, left.parent, leftIndex, key, right)
	}

	/* Harder case:  split a bpNode in order
	 * to preserve the B+ tree properties.
	 */
	return insertIntoNodeAfterSplitting(root, left.parent, leftIndex, key, right)
}

/*
 *	getLeftIndex helper function used in insertIntoParent to find the index of the parents
 *  pointer to the bpNode to the left of the key to be inserted
 */
func getLeftIndex(parent, left *bpNode) int {
	var leftIndex int
	for leftIndex <= parent.numKeys && (*bpNode)(parent.pointers[leftIndex]) != left {
		leftIndex++
	}
	return leftIndex
}

/*
 * insertIntoNode inserts a new key and pointer to a bpNode into a bpNode into which these can fit without violating the
 * tree's properties
 */
func insertIntoNode(root, n *bpNode, leftIndex int, key string, right *bpNode) *bpNode {
	// Consider using copy, it might be better
	copy(n.pointers[leftIndex+2:], n.pointers[leftIndex+1:])
	copy(n.keys[leftIndex+1:], n.keys[leftIndex:])

	/* // ORIG
	for i := n.numKeys; i > leftIndex; i-- {
		n.pointers[i+1] = n.pointers[i]
		n.keys[i] = n.keys[i-1]
	}
	*/

	n.pointers[leftIndex+1] = unsafe.Pointer(right)
	n.keys[leftIndex] = key
	n.numKeys++
	return root
}

// insertIntoNodeAfterSplitting inserts a new key and pointer to a bpNode into a bpNode, causing
// the nodes size to exceed the ORDER, and causing the bpNode to split
func insertIntoNodeAfterSplitting(root, oldNode *bpNode, leftIndex int, key string, right *bpNode) *bpNode {

	// First create a temp set of keys and pointers to hold everything in ORDER, including
	// the new key and pointer, inserted in their correct places--then create a new bpNode
	// and copy half of the keys and pointers to the old bpNode and the other half to the new

	var i, j int
	var tempKeys [defaultOrder]string                 //tempKeys := make([]int, ORDER)
	var tempPointers [defaultOrder + 1]unsafe.Pointer //tempPointers := make([]interface{}, ORDER+1)

	for i, j = 0, 0; i < oldNode.numKeys+1; i, j = i+1, j+1 {
		if j == leftIndex+1 {
			j++
		}
		tempPointers[j] = oldNode.pointers[i]
	}

	for i, j = 0, 0; i < oldNode.numKeys; i, j = i+1, j+1 {
		if j == leftIndex {
			j++
		}
		tempKeys[j] = oldNode.keys[i]
	}

	tempPointers[leftIndex+1] = unsafe.Pointer(right)
	tempKeys[leftIndex] = key

	/*
	 * copy half the keys and pointers to the old bpNode...
	 */
	split := cut(defaultOrder)

	oldNode.numKeys = 0

	for i = 0; i < split-1; i++ {
		oldNode.pointers[i] = tempPointers[i]
		oldNode.keys[i] = tempKeys[i]
		oldNode.numKeys++
	}
	oldNode.pointers[i] = tempPointers[i]
	kPrime := tempKeys[split-1]

	/*
	 * ...create the new bpNode and copy the other half the keys and pointers
	 */
	newNode := &bpNode{} // makeNode()

	for i, j = i+1, 0; i < defaultOrder; i, j = i+1, j+1 {
		newNode.pointers[j] = tempPointers[i]
		newNode.keys[j] = tempKeys[i]
		newNode.numKeys++
	}
	newNode.pointers[j] = tempPointers[i]
	newNode.parent = oldNode.parent

	/*
	 * Free up tempPointers and tempKeys
	 */
	for i = 0; i < defaultOrder; i++ {
		tempKeys[i] = *new(string) // zero values
		tempPointers[i] = nil      // zero values
	}
	tempPointers[defaultOrder] = nil

	var child *bpNode
	for i = 0; i <= newNode.numKeys; i++ {
		child = (*bpNode)(newNode.pointers[i])
		child.parent = newNode
	}

	/* Insert a new key into the parent of the two
	 * nodes resulting from the split, with
	 * the old bpNode to the left and the new to the right.
	 */
	return insertIntoParent(root, oldNode, kPrime, newNode)
}

// insertIntoLeaf inserts a new pointer to a Record and its
// corresponding key into a leaf.
func insertIntoLeaf(leaf *bpNode, key string, pointer *entry) /* *bpNode */ {
	var i, insertionPoint int
	for insertionPoint < leaf.numKeys && Compare(leaf.keys[insertionPoint], key) == -1 {
		insertionPoint++
	}
	for i = leaf.numKeys; i > insertionPoint; i-- {
		leaf.keys[i] = leaf.keys[i-1]
		leaf.pointers[i] = leaf.pointers[i-1]
	}
	leaf.keys[insertionPoint] = key
	leaf.pointers[insertionPoint] = unsafe.Pointer(pointer)
	leaf.numKeys++
	//return leaf // might not need to return this leaf
}

// insertIntoLeafAfterSplitting inserts a new key and pointer to a new Record into a leaf
// so as to exceed the tree's ORDER, causing the leaf to be split in half
func insertIntoLeafAfterSplitting(root, leaf *bpNode, key string, pointer *entry) *bpNode {

	// perform linear search to find index to insert new record
	var insertionIndex int
	for insertionIndex < defaultOrder-1 && Compare(leaf.keys[insertionIndex], key) == -1 {
		insertionIndex++
	}

	var i, j int
	var tempKeys [defaultOrder]string
	var tempPointers [defaultOrder]unsafe.Pointer

	// copy leaf keys and pointers to temp sets
	// reserve space at insertion index for new record
	for i, j = 0, 0; i < leaf.numKeys; i, j = i+1, j+1 {
		if j == insertionIndex {
			j++
		}
		tempKeys[j] = leaf.keys[i]
		tempPointers[j] = leaf.pointers[i]
	}

	tempKeys[insertionIndex] = key
	tempPointers[insertionIndex] = unsafe.Pointer(pointer)

	leaf.numKeys = 0

	// find pivot index where to split leaf
	split := cut(defaultOrder - 1)

	// overwrite original leaf up to the split point
	for i = 0; i < split; i++ {
		leaf.keys[i] = tempKeys[i]
		leaf.pointers[i] = tempPointers[i]
		leaf.numKeys++
	}

	// create new leaf
	newLeaf := &bpNode{isLeaf: true} // makeLeaf()

	// writing to new leaf from split point to end of original leaf pre split
	for i, j = split, 0; i < defaultOrder; i, j = i+1, j+1 {
		newLeaf.keys[j] = tempKeys[i]
		newLeaf.pointers[j] = tempPointers[i]
		newLeaf.numKeys++
	}

	// free temps
	for i = 0; i < defaultOrder; i++ {
		tempKeys[i] = *new(string) // zero Value
		tempPointers[i] = nil      // zero Value
	}

	newLeaf.pointers[defaultOrder-1] = leaf.pointers[defaultOrder-1]
	leaf.pointers[defaultOrder-1] = unsafe.Pointer(newLeaf)

	for i = leaf.numKeys; i < defaultOrder-1; i++ {
		leaf.pointers[i] = nil
	}
	for i = newLeaf.numKeys; i < defaultOrder-1; i++ {
		newLeaf.pointers[i] = nil
	}

	newLeaf.parent = leaf.parent
	newKey := newLeaf.keys[0]

	return insertIntoParent(root, leaf, newKey, newLeaf)
}

/*
 *	findRecord finds and returns the Record to which a key refers
 */
func (t *BPTree) find(key string) (*bpNode, *entry) {
	leaf := findLeaf(t.root, key)
	if leaf == nil {
		return nil, nil
	}
	/*
	 * If root/leaf != nil, leaf must have a Value, even if it does not contain the desired key.
	 * The leaf holds the range of keys that would include the desired key
	 */
	var i int
	for i = 0; i < leaf.numKeys; i++ {
		if Equal(leaf.keys[i], key) {
			break
		}
	}
	if i == leaf.numKeys {
		return leaf, nil
	}
	return leaf, (*entry)(leaf.pointers[i])
}

/*
 *	findEntry finds and returns the entry to which a key refers
 */
func (t *BPTree) findEntry(key string) *entry {
	leaf := findLeaf(t.root, key)
	if leaf == nil {
		return nil
	}
	/*
	 * If root/leaf != nil, leaf must have a Value, even if it does not contain the desired key.
	 * The leaf holds the range of keys that would include the desired key
	 */
	var i int
	for i = 0; i < leaf.numKeys; i++ {
		if Equal(leaf.keys[i], key) {
			break
		}
	}
	if i == leaf.numKeys {
		return nil
	}
	return (*entry)(leaf.pointers[i])
}

/*
 *	findLeaf traces the path from the root to a leaf, searching by key and displaying information about the path if the
 *  verbose flag is set--findLeaf returns the leaf containing the given key
 */
func findLeaf(root *bpNode, key string) *bpNode {
	if root == nil {
		return root
	}
	i, c := 0, root
	for !c.isLeaf {
		i = 0
		for i < c.numKeys {
			if Compare(key, c.keys[i]) >= 0 {
				i++
			} else {
				break
			}
		}
		c = (*bpNode)(c.pointers[i])
	}
	// c is the found leaf bpNode
	return c
}

/*
 * findFirstLeaf traces the path from the root to the leftmost leaf in the tree
 */
func findFirstLeaf(root *bpNode) *bpNode {
	if root == nil {
		return root
	}
	c := root
	for !c.isLeaf {
		c = (*bpNode)(c.pointers[0])
	}
	return c
}

func findLastLeaf(root *bpNode) *bpNode {
	if root == nil {
		return root
	}
	c := root
	for !c.isLeaf {
		c = (*bpNode)(c.pointers[c.numKeys])
	}
	return c
}

/*
 *  nextLeaf returns the next non-nil leaf in the chain (to the right) of the current leaf
 */
func (n *bpNode) nextLeaf() *bpNode {
	if p := (*bpNode)(n.pointers[defaultOrder-1]); p != nil && p.isLeaf {
		return p
	}
	return nil
}

/*
 * delete is the master delete function
 */
func (t *BPTree) delete(key string) *entry {
	var old *entry
	keyLeaf, keyEntry := t.find(key)
	if keyEntry != nil && keyLeaf != nil {
		t.root = deleteEntry(t.root, keyLeaf, key, unsafe.Pointer(keyEntry))
		old = keyEntry
		keyEntry = nil
	}
	return old // return the old entry we just deleted
}

/*
 * getNeighborIndex is a utility function for deletion. It gets the index of a bpNode's nearest
 * sibling (that exists) to the left. If not then bpNode is already the leftmost child and (in
 * such a case the bpNode) will return -1
 */
func getNeighborIndex(n *bpNode) int {
	var i int
	for i = 0; i <= n.parent.numKeys; i++ {
		if (*bpNode)(n.parent.pointers[i]) == n {
			return i - 1
		}
	}
	log.Panicf("getNeighborIndex: Search for nonexistent pointer to bpNode in parent.\nNode: %#v\n", n)
	return i
}

/*
 * removeEntryFromNode does just that
 */
func removeEntryFromNode(n *bpNode, key string, pointer unsafe.Pointer) *bpNode {

	/*
	 * Remove the key and shift the other keys accordingly
	 */
	var i, numPointers int
	for !Equal(n.keys[i], key) {
		i++
	}
	for i++; i < n.numKeys; i++ { // was for i+=1;
		n.keys[i-1] = n.keys[i]
	}

	/*
	 * Remove the pointer and shift other pointers accordingly
	 */
	if n.isLeaf {
		numPointers = n.numKeys
	} else {
		numPointers = n.numKeys + 1
	}

	i = 0
	for n.pointers[i] != pointer {
		i++
	}
	for i++; i < numPointers; i++ { // was for i+=1;
		n.pointers[i-1] = n.pointers[i]
	}

	/*
	 * One key fewer
	 */
	n.numKeys--

	/*
	 * Set the other pointers to nil for tidiness. A leaf uses
	 * the last pointer to point to the next leaf
	 */
	if n.isLeaf {
		for i = n.numKeys; i < defaultOrder-1; i++ {
			n.pointers[i] = nil
		}
	} else {
		for i = n.numKeys + 1; i < defaultOrder; i++ {
			n.pointers[i] = nil
		}
	}
	return n
}

/*
 * deleteEntry deletes and entry from the tree. Removes the Record and it's key and pointer from
 * the leaf, and then makes all appropriate changes to preserve the tree's properties
 */
func deleteEntry(root, n *bpNode, key string, pointer unsafe.Pointer) *bpNode {

	var minKeys, kPrimeIndex, capacity int

	/*
	 * Remove key and pointer from bpNode
	 */
	n = removeEntryFromNode(n, key, pointer)

	/*
	 * CASE: deletion from the root bpNode
	 */
	if n == root {
		return adjustRoot(root)
	}

	/*
	 * CASE: deletion from a bpNode below the root (continue rest of function)
	 *
	 * Determine minimum allowable size of bpNode to be preserved after deletion
	 */
	if n.isLeaf {
		minKeys = cut(defaultOrder - 1)
	} else {
		minKeys = cut(defaultOrder) - 1
	}

	/*
	 * CASE: bpNode stays at or above minimum (simple case)
	 */
	if n.numKeys >= minKeys {
		return root
	}

	/*
			 * CASE: bpNode falls below minimum. Either coalescence or redistribution is needed
		     *
			 * Find the appropriate neighbor bpNode with which to coalesce. Also find the key (kPrime)
			 * in the parent between the pointer to bpNode n and the pointer to the neighbor
	*/
	neighborIndex := getNeighborIndex(n)
	if neighborIndex == -1 {
		kPrimeIndex = 0
	} else {
		kPrimeIndex = neighborIndex
	}

	kPrime := n.parent.keys[kPrimeIndex]

	var neighbor *bpNode
	if neighborIndex == -1 {
		neighbor = (*bpNode)(n.parent.pointers[1])
	} else {
		neighbor = (*bpNode)(n.parent.pointers[neighborIndex])
	}

	if n.isLeaf {
		capacity = defaultOrder
	} else {
		capacity = defaultOrder - 1
	}

	/*
	 * Coalescence
	 */
	if neighbor.numKeys+n.numKeys < capacity {
		return coalesceNodes(root, n, neighbor, neighborIndex, kPrime)
	}

	/*S
	 * Redistribution
	 */
	return redistributeNodes(root, n, neighbor, neighborIndex, kPrimeIndex, kPrime)
}

/*
 * adjustRoot does some magic in the root bpNode (not really)
 */
func adjustRoot(root *bpNode) *bpNode {
	/*
	 * CASE: nonempty root. key and pointer have already been deleted, so nothing to be done
	 */
	if root.numKeys > 0 {
		return root
	}

	/*
	 * CASE: empty root. If it has a child, promote the first (only) child as the new root
	 */
	var newRoot *bpNode
	if !root.isLeaf {
		newRoot = (*bpNode)(root.pointers[0])
		newRoot.parent = nil
	} else {
		/*
		 * If it is a leaf (has no children), then the whole tree is in fact empty
		 */
		newRoot = nil // free
	}
	root = nil
	return newRoot
}

/*
 * coalesceNodes coalesces a bpNode that has become too small after deletion with a
 * neighboring bpNode that can accept the additional entries without exceeing the maximum
 */
func coalesceNodes(root, n, neighbor *bpNode, neighborIndex int, kPrime string) *bpNode {

	var tmp *bpNode

	/*
	 * Swap neighbor with bpNode if bpNode is on the extreme left and neighbor is to it's right
	 */
	if neighborIndex == -1 {
		tmp = n
		n = neighbor
		neighbor = tmp
	}

	/*
	 * Starting point in the neighbor for copying keys and pointers from n. Recall that n and
	 * neighbor have swapped places in the special case of n being a leftmost child
	 */
	neighborInsertionIndex := neighbor.numKeys
	var i, j, nEnd int

	/*
	 * CASE: Nonleaf bpNode. Append kPrime and the following pointer and append all pointers and keys from the neighbor
	 */
	if !n.isLeaf {
		/*
		 * Append kPrime
		 */
		neighbor.keys[neighborInsertionIndex] = kPrime
		neighbor.numKeys++
		nEnd = n.numKeys

		for i, j = neighborInsertionIndex+1, 0; j < nEnd; i, j = i+1, j+1 {
			neighbor.keys[i] = n.keys[j]
			neighbor.pointers[i] = n.pointers[j]
			neighbor.numKeys++
			n.numKeys--
		}

		/*
		 * The number of pointers is always one more than the number of keys
		 */
		neighbor.pointers[i] = n.pointers[j]

		/*
		 * All children must now point up to the same parent
		 */
		for i = 0; i < neighbor.numKeys+1; i++ {
			tmp = (*bpNode)(neighbor.pointers[i])
			tmp.parent = neighbor
		}
		/*
		 * CASE: In a leaf, append the keys and pointers of n to the neighbor.
		 * Set the neighbor's last pointer to point to what had been n's right neighbor.
		 */
	} else {
		for i, j = neighborInsertionIndex, 0; j < n.numKeys; i, j = i+1, j+1 {
			neighbor.keys[i] = n.keys[j]
			neighbor.pointers[i] = n.pointers[j]
			neighbor.numKeys++
		}
		neighbor.pointers[defaultOrder-1] = n.pointers[defaultOrder-1]
	}
	root = deleteEntry(root, n.parent, kPrime, unsafe.Pointer(n))
	n = nil // free
	return root
}

/*
 * redistributeNodes redistributes entries between two nodes when one has become too small
 * after deletion but its neighbor is too big to append the small bpNode's entries without
 * exceeding the maximum
 */
func redistributeNodes(root, n, neighbor *bpNode, neighborIndex, kPrimeIndex int, kPrime string) *bpNode {

	var i int
	var tmp *bpNode

	/*
	 * CASE: n has a neighbor to the left. Pull the neighbor's last key-pointer pair over
	 * from the neighbor's right end to n's lef end
	 */
	if neighborIndex != -1 {
		if !n.isLeaf {
			n.pointers[n.numKeys+1] = n.pointers[n.numKeys]
		}
		for i = n.numKeys; i > 0; i-- {
			n.keys[i] = n.keys[i-1]
			n.pointers[i] = n.pointers[i-1]
		}
		if !n.isLeaf {
			n.pointers[0] = neighbor.pointers[neighbor.numKeys]
			tmp = (*bpNode)(n.pointers[0])
			tmp.parent = n
			neighbor.pointers[neighbor.numKeys] = nil
			n.keys[0] = kPrime
			n.parent.keys[kPrimeIndex] = neighbor.keys[neighbor.numKeys-1]
		} else {
			n.pointers[0] = neighbor.pointers[neighbor.numKeys-1]
			neighbor.pointers[neighbor.numKeys-1] = nil
			n.keys[0] = neighbor.keys[neighbor.numKeys-1]
			n.parent.keys[kPrimeIndex] = n.keys[0]
		}

		/*
		 * CASE: n is the leftmost child. Take a key-pointer pair from the neighbor
		 * to the right.  Move the neighbor's leftmost key-pointer pair to n's rightmost position
		 */
	} else {
		if n.isLeaf {
			n.keys[n.numKeys] = neighbor.keys[0]
			n.pointers[n.numKeys] = neighbor.pointers[0]
			n.parent.keys[kPrimeIndex] = neighbor.keys[1]
		} else {
			n.keys[n.numKeys] = kPrime
			n.pointers[n.numKeys+1] = neighbor.pointers[0]
			tmp = (*bpNode)(n.pointers[n.numKeys+1])
			tmp.parent = n
			n.parent.keys[kPrimeIndex] = neighbor.keys[0]
		}
		for i = 0; i < neighbor.numKeys-1; i++ {
			neighbor.keys[i] = neighbor.keys[i+1]
			neighbor.pointers[i] = neighbor.pointers[i+1]
		}
		if !n.isLeaf {
			neighbor.pointers[i] = neighbor.pointers[i+1]
		}
	}

	/*
	 * n now has one more key and one more pointer; the neighbor has one fewer of each
	 */
	n.numKeys++
	neighbor.numKeys--
	return root
}

func destroyTreeNodes(n *bpNode) {
	if n == nil {
		return
	}
	if n.isLeaf {
		for i := 0; i < n.numKeys; i++ {
			n.pointers[i] = nil
		}
	} else {
		for i := 0; i < n.numKeys+1; i++ {
			destroyTreeNodes((*bpNode)(n.pointers[i]))
		}
	}
	n = nil
}

func (t *BPTree) destroyTree() {
	destroyTreeNodes(t.root)
}

/*
 *	cut finds the appropriate place to split a bpNode that is too big
 */
func cut(length int) int {
	if length%2 == 0 {
		return length / 2
	}
	return length/2 + 1
}

// makeRecord create a new Record to hold the Value to which a key refers
func makeRecord(value []byte) *entry {
	return &entry{
		Value: value,
	}
}

// makeNode creates a new general bpNode, which can be adapted to serve as either a leaf or internal bpNode
func makeNode() *bpNode {
	return &bpNode{}
}

// makeLeaf creates a new leaf by creating a bpNode and then adapting it appropriately
func makeLeaf() *bpNode {
	leaf := &bpNode{} // makeNode()
	leaf.isLeaf = true
	return leaf
}

func Btoi(b []byte) int64 {
	return int64(b[7]) |
		int64(b[6])<<8 |
		int64(b[5])<<16 |
		int64(b[4])<<24 |
		int64(b[3])<<32 |
		int64(b[2])<<40 |
		int64(b[1])<<48 |
		int64(b[0])<<56
}

func Itob(i int64) []byte {
	return []byte{
		byte(i >> 56),
		byte(i >> 48),
		byte(i >> 40),
		byte(i >> 32),
		byte(i >> 24),
		byte(i >> 16),
		byte(i >> 8),
		byte(i),
	}
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
