package chained

import "github.com/scottcagno/storage/pkg/hash/murmur3"

const (
	loadFactor     = 0.85 // load factor must exceed 50%
	defaultMapSize = 16
)

// user specified key and value types
type keyType = string
type valType = []byte

var keyZeroType = *new(keyType)
var valZeroType = *new(valType)

// entry is a key value pair that is found in each bucket
type entry struct {
	key keyType
	val valType
}

// entry node is a node in part of our linked list
type entryNode struct {
	entry
	next *entryNode
}

// bucket represents a single slot in the HashMap table
type bucket struct {
	hashkey uint64
	head    *entryNode
}

func (b *bucket) insert(key keyType, val valType) (valType, bool) {
	if val, ok := b.search(key); ok {
		// already exists
		return val, true
	}
	newNode := &entryNode{
		entry: entry{
			key: key,
			val: val,
		},
		next: b.head,
	}
	b.head = newNode
	// no previous value, so return false
	return b.head.entry.val, false
}

func (b *bucket) search(key keyType) (valType, bool) {
	current := b.head
	for current != nil {
		if current.entry.key == key {
			return current.entry.val, true
		}
		current = current.next
	}
	return valZeroType, false
}

func (b *bucket) scan(it Iterator) {
	current := b.head
	for current != nil {
		if !it(current.entry.key, current.entry.val) {
			return
		}
		current = current.next
	}
}

func (b *bucket) delete(key keyType) (valType, bool) {
	var ret valType
	if b.head.entry.key == key {
		ret = b.head.entry.val
		b.head = b.head.next
		return ret, true
	}
	previous := b.head
	for previous.next != nil {
		if previous.next.entry.key == key {
			ret = previous.next.entry.val
			previous.next = previous.next.next
			return ret, true
		}
		previous = previous.next
	}
	return valZeroType, false
}

// HashMap represents a closed hashing hashtable implementation
type HashMap struct {
	hash    hashFunc
	mask    uint64
	expand  uint
	shrink  uint
	keys    uint
	size    uint
	buckets []bucket
}

// alignBucketCount aligns buckets to ensure all sizes are powers of two
func alignBucketCount(size uint) uint64 {
	count := uint(defaultMapSize)
	for count < size {
		count *= 2
	}
	return uint64(count)
}

// defaultHashFunc is the default hashFunc used. This is here mainly as
// a convenience for the sharded hashmap to utilize
func defaultHashFunc(key keyType) uint64 {
	return murmur3.Sum64([]byte(key))
}

// hashFunc is a type definition for what a hash function should look like
type hashFunc func(key keyType) uint64

// NewHashMap returns a new HashMap instantiated with the specified size or
// the defaultMapSize, whichever is larger
func NewHashMap(size uint) *HashMap {
	return newHashMap(size, defaultHashFunc)
}

// newHashMap is the internal variant of the previous function
// and is mainly used internally
func newHashMap(size uint, hash hashFunc) *HashMap {
	bukCnt := alignBucketCount(size)
	if hash == nil {
		hash = defaultHashFunc
	}
	m := &HashMap{
		hash:    hash,
		mask:    bukCnt - 1,
		expand:  uint(float64(bukCnt) * loadFactor),
		shrink:  uint(float64(bukCnt) * (1 - loadFactor)),
		keys:    0,
		size:    size,
		buckets: make([]bucket, bukCnt),
	}
	return m
}

// resize grows or shrinks the HashMap by the newSize provided. It makes a
// new map with the new size, copies everything over, and then frees the old map
func (m *HashMap) resize(newSize uint) {
	newHM := newHashMap(newSize, m.hash)
	var buk bucket
	for i := 0; i < len(m.buckets); i++ {
		buk = m.buckets[i]
		if buk.head != nil {
			buk.scan(func(key keyType, value valType) bool {
				newHM.insert(buk.hashkey, key, value)
				return true
			})
		}
	}
	tsize := m.size
	*m = *newHM
	m.size = tsize
}

// Get returns a value for a given key, or returns false if none could be found
// Get can be considered the exported version of the lookup call
func (m *HashMap) Get(key keyType) (valType, bool) {
	return m.lookup(0, key)
}

// lookup returns a value for a given key, or returns false if none could be found
func (m *HashMap) lookup(hashkey uint64, key keyType) (valType, bool) {
	// check if map is empty
	if len(m.buckets) == 0 {
		// hopefully this should never really happen
		// do we really need to check this here?
		*m = *newHashMap(defaultMapSize, m.hash)
	}
	if hashkey == 0 {
		// calculate the hashkey value
		hashkey = m.hash(key)
	}
	// mask the hashkey to get the initial index
	i := hashkey & m.mask
	// check if the chain is empty
	if m.buckets[i].head == nil {
		return *new(valType), false
	}
	// not empty, lets look for it in the list
	return m.buckets[i].search(key)
}

// Put inserts a key value entry and returns the previous value or false
// Put can be considered the exported version of the insert call
func (m *HashMap) Put(key keyType, value valType) (valType, bool) {
	return m.insert(0, key, value)
}

// insert inserts a key value entry and returns the previous value, or false
func (m *HashMap) insert(hashkey uint64, key keyType, value valType) (valType, bool) {
	// check if map is empty
	if len(m.buckets) == 0 {
		// create a new map with default size
		*m = *newHashMap(defaultMapSize, m.hash)
	}
	// check and see if we need to resize
	if m.keys >= m.expand {
		// if we do, then double the map size
		m.resize(uint(len(m.buckets)) * 2)
	}
	if hashkey == 0 {
		// calculate the hashkey value
		hashkey = m.hash(key)
	}
	// mask the hashkey to get the initial index
	i := hashkey & m.mask
	// insert key and value
	val, ok := m.buckets[i].insert(key, value)
	if !ok { // means not updated, aka a new one was inserted
		m.keys++
	}
	return val, !ok
}

// Del removes a value for a given key and returns the deleted value, or false
// Del can be considered the exported version of the delete call
func (m *HashMap) Del(key keyType) (valType, bool) {
	return m.delete(0, key)
}

// delete removes a value for a given key and returns the deleted value, or false
func (m *HashMap) delete(hashkey uint64, key keyType) (valType, bool) {
	// check if map is empty
	if len(m.buckets) == 0 {
		// nothing to see here folks
		return valZeroType, false
	}
	if hashkey == 0 {
		// calculate the hashkey value
		hashkey = m.hash(key)
	}
	// mask the hashkey to get the initial index
	i := hashkey & m.mask
	// try deleting from the chain
	val, ok := m.buckets[i].delete(key)
	if ok { // means it was deleted, aka...
		// ...decrement entry count
		m.keys--
	}
	// check and see if we need to resize
	if m.keys <= m.shrink && uint(len(m.buckets)) > m.size {
		// if it checks out, then resize down by 25%-ish
		m.resize(m.keys)
	}
	return val, ok
}

// Iterator is an iterator function type
type Iterator func(key keyType, value valType) bool

// Range takes an Iterator and ranges the HashMap as long as long
// as the iterator function continues to be true. Range is not
// safe to perform an insert or remove operation while ranging!
func (m *HashMap) Range(it Iterator) {
	for i := 0; i < len(m.buckets); i++ {
		m.buckets[i].scan(it)
	}
}

// PercentFull returns the current load factor of the HashMap
func (m *HashMap) PercentFull() float64 {
	return float64(m.keys) / float64(len(m.buckets))
}

// Len returns the number of entries currently in the HashMap
func (m *HashMap) Len() int {
	return int(m.keys)
}

// Close closes and frees the current hashmap. Calling any method
// on the HashMap after this will most likely result in a panic
func (m *HashMap) Close() {
	destroy(m)
}

// destroy does exactly what is sounds like it does
func destroy(m *HashMap) {
	m = nil
}
