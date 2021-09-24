package openaddr

import (
	"github.com/scottcagno/storage/pkg/hash/murmur3"
	"reflect"
	"unsafe"
)

// entryU64 is a key value pair that is found in each bucketU64
type entryU64 struct {
	key uint64
	val uint64
}

// bucketU64 represents a single slot in the HashMapU64 table
type bucketU64 struct {
	dib     uint8
	hashkey uint64
	entryU64
}

// checkHashAndKey checks if this bucketU64 matches the specified hashkey and key
func (b *bucketU64) checkHashAndKey(hashkey uint64, key uint64) bool {
	return b.hashkey == hashkey && b.entryU64.key == key
}

// HashMapU64 represents a closed hashing hashtable implementation
type HashMapU64 struct {
	hash    hashFuncU64
	mask    uint64
	expand  uint
	shrink  uint
	keys    uint
	size    uint
	buckets []bucketU64
}

// defaultHashFunc is the default hashFunc used. This is here mainly as
// a convenience for the sharded hashmap to utilize
func defaultHashFuncU64(key uint64) uint64 {
	return murmur3.Sum64(*(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&key)),
		Len:  8,
		Cap:  8,
	})))
}

// hashFunc is a type definition for what a hash function should look like
type hashFuncU64 func(key uint64) uint64

// NewHashMapU64 returns a new HashMapU64 instantiated with the specified size or
// the DefaultMapSize, whichever is larger
func NewHashMapU64(size uint) *HashMapU64 {
	return newHashMapU64(size, defaultHashFuncU64)
}

// newHashMap is the internal variant of the previous function
// and is mainly used internally
func newHashMapU64(size uint, hash hashFuncU64) *HashMapU64 {
	bukCnt := alignBucketCount(size)
	if hash == nil {
		hash = defaultHashFuncU64
	}
	m := &HashMapU64{
		hash:    hash,
		mask:    bukCnt - 1, // this minus one is extremely important for using a mask over modulo
		expand:  uint(float64(bukCnt) * DefaultLoadFactor),
		shrink:  uint(float64(bukCnt) * (1 - DefaultLoadFactor)),
		keys:    0,
		size:    size,
		buckets: make([]bucketU64, bukCnt),
	}
	return m
}

// resize grows or shrinks the HashMapU64 by the newSize provided. It makes a
// new map with the new size, copies everything over, and then frees the old map
func (m *HashMapU64) resize(newSize uint) {
	newHM := newHashMapU64(newSize, m.hash)
	var buk bucketU64
	for i := 0; i < len(m.buckets); i++ {
		buk = m.buckets[i]
		if buk.dib > 0 {
			newHM.insertInternal(buk.hashkey, buk.entryU64.key, buk.entryU64.val)
		}
	}
	tsize := m.size
	*m = *newHM
	m.size = tsize
}

// Get returns a value for a given key, or returns false if none could be found
// Get can be considered the exported version of the lookup call
func (m *HashMapU64) Get(key uint64) (uint64, bool) {
	return m.lookup(0, key)
}

// lookup returns a value for a given key, or returns false if none could be found
func (m *HashMapU64) lookup(hashkey uint64, key uint64) (uint64, bool) {
	// check if map is empty
	if len(m.buckets) == 0 {
		// hopefully this should never really happen
		// do we really need to check this here?
		*m = *newHashMapU64(DefaultMapSize, m.hash)
	}
	if hashkey == 0 {
		// calculate the hashkey value
		hashkey = m.hash(key)
	}
	// mask the hashkey to get the initial index
	i := hashkey & m.mask
	// search the position linearly
	for {
		// havent located anything
		if m.buckets[i].dib == 0 {
			return 0, false
		}
		// check for matching hashes and keys
		if m.buckets[i].checkHashAndKey(hashkey, key) {
			return m.buckets[i].entryU64.val, true
		}
		// keep on probing
		i = (i + 1) & m.mask
	}
}

// Set inserts a key value entryU64 and returns the previous value or false
// Set can be considered the exported version of the insert call
func (m *HashMapU64) Set(key uint64, value uint64) (uint64, bool) {
	return m.insert(0, key, value)
}

// insert inserts a key value entryU64 and returns the previous value, or false
func (m *HashMapU64) insert(hashkey uint64, key uint64, value uint64) (uint64, bool) {
	// check if map is empty
	if len(m.buckets) == 0 {
		// create a new map with default size
		*m = *newHashMapU64(DefaultMapSize, m.hash)
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
	// call the internal insert to insert the entryU64
	return m.insertInternal(hashkey, key, value)
}

// insertInternal inserts a key value entryU64 and returns the previous value, or false
func (m *HashMapU64) insertInternal(hashkey uint64, key uint64, value uint64) (uint64, bool) {
	// create a new entryU64 to insert
	newb := bucketU64{
		dib:     1,
		hashkey: hashkey,
		entryU64: entryU64{
			key: key,
			val: value,
		},
	}
	// mask the hashkey to get the initial index
	i := newb.hashkey & m.mask
	// search the position linearly
	for {
		// we found a spot, insert a new entryU64
		if m.buckets[i].dib == 0 {
			m.buckets[i] = newb
			m.keys++
			// no previous value to return, as this is a new entryU64
			return 0, false
		}
		// found existing entryU64, check hashes and keys
		if m.buckets[i].checkHashAndKey(newb.hashkey, newb.entryU64.key) {
			// hashes and keys are a match--update entryU64 and return previous values
			oldval := m.buckets[i].entryU64.val
			m.buckets[i].val = newb.entryU64.val
			return oldval, true
		}
		// we did not find an empty slot or an existing matching entryU64
		// so check this entries dib against our new entryU64's dib
		if m.buckets[i].dib < newb.dib {
			// current position's dib is less than our new entryU64's, swap
			newb, m.buckets[i] = m.buckets[i], newb
		}
		// keep on probing until we find what we're looking for.
		// increase our search index by one as well as our new
		// entryU64's dib, then continue with the linear probe.
		i = (i + 1) & m.mask
		newb.dib = newb.dib + 1
	}
}

// Del removes a value for a given key and returns the deleted value, or false
// Del can be considered the exported version of the delete call
func (m *HashMapU64) Del(key uint64) (uint64, bool) {
	return m.delete(0, key)
}

// delete removes a value for a given key and returns the deleted value, or false
func (m *HashMapU64) delete(hashkey uint64, key uint64) (uint64, bool) {
	// check if map is empty
	if len(m.buckets) == 0 {
		// nothing to see here folks
		return 0, false
	}
	if hashkey == 0 {
		// calculate the hashkey value
		hashkey = m.hash(key)
	}
	// mask the hashkey to get the initial index
	i := hashkey & m.mask
	// search the position linearly
	for {
		// havent located anything
		if m.buckets[i].dib == 0 {
			return 0, false
		}
		// found existing entryU64, check hashes and keys
		if m.buckets[i].checkHashAndKey(hashkey, key) {
			// hashes and keys are a match--delete entryU64 and return previous values
			oldval := m.buckets[i].entryU64.val
			m.deleteInternal(i)
			return oldval, true
		}
		// keep on probing until we find what we're looking for.
		// increase our search index by one as well as our new
		// entryU64's dib, then continue with the linear probe.
		i = (i + 1) & m.mask
	}
}

// delete removes a value for a given key and returns the deleted value, or false
func (m *HashMapU64) deleteInternal(i uint64) {
	// set dib at bucketU64 i
	m.buckets[i].dib = 0
	// tombstone index and shift
	for {
		pi := i
		i = (i + 1) & m.mask
		if m.buckets[i].dib <= 1 {
			// im as free as a bird now!
			m.buckets[pi].entryU64 = *new(entryU64)
			m.buckets[pi] = *new(bucketU64)
			break
		}
		// shift
		m.buckets[pi] = m.buckets[i]
		m.buckets[pi].dib = m.buckets[pi].dib - 1
	}
	// decrement entryU64 count
	m.keys--
	// check and see if we need to resize
	if m.keys <= m.shrink && uint(len(m.buckets)) > m.size {
		// if it checks out, then resize down by 25%-ish
		m.resize(m.keys)
	}
}

// IteratorU64 is an iterator function type
type IteratorU64 func(key uint64, value uint64) bool

// Range takes an Iterator and ranges the HashMapU64 as long as long
// as the iterator function continues to be true. Range is not
// safe to perform an insert or remove operation while ranging!
func (m *HashMapU64) Range(it IteratorU64) {
	for i := 0; i < len(m.buckets); i++ {
		if m.buckets[i].dib < 1 {
			continue
		}
		if !it(m.buckets[i].key, m.buckets[i].val) {
			return
		}
	}
}

// GetHighestDIB returns the highest distance to initial bucketU64 value in the table
func (m *HashMapU64) GetHighestDIB() uint8 {
	var hdib uint8
	for i := 0; i < len(m.buckets); i++ {
		if m.buckets[i].dib > hdib {
			hdib = m.buckets[i].dib
		}
	}
	return hdib
}

// PercentFull returns the current load factor of the HashMapU64
func (m *HashMapU64) PercentFull() float64 {
	return float64(m.keys) / float64(len(m.buckets))
}

// Len returns the number of entries currently in the HashMapU64
func (m *HashMapU64) Len() int {
	return int(m.keys)
}

// Close closes and frees the current hashmap. Calling any method
// on the HashMapU64 after this will most likely result in a panic
func (m *HashMapU64) Close() {
	destroyMapU64(m)
}

// destroy does exactly what is sounds like it does
func destroyMapU64(m *HashMapU64) {
	m = nil
}
