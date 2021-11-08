package openaddr

import (
	"github.com/scottcagno/storage/pkg/hash/murmur3"
	"reflect"
	"unsafe"
)

// entryGP is a key value pair that is found in each bucketGP
type entryGP struct {
	key string
	val interface{}
}

// bucketGP represents a single slot in the HashMapGP table
type bucketGP struct {
	dib     uint8
	hashkey uint64
	entryGP
}

// checkHashAndKey checks if this bucketGP matches the specified hashkey and key
func (b *bucketGP) checkHashAndKey(hashkey uint64, key string) bool {
	return b.hashkey == hashkey && b.entryGP.key == key
}

// HashMapGP represents a closed hashing hashtable implementation
type HashMapGP struct {
	hash    hashFuncGP
	mask    uint64
	expand  uint
	shrink  uint
	keys    uint
	size    uint
	buckets []bucketGP
}

// defaultHashFunc is the default hashFunc used. This is here mainly as
// a convenience for the sharded hashmap to utilize
func defaultHashFuncGP(key string) uint64 {
	return murmur3.Sum64(*(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&key)),
		Len:  8,
		Cap:  8,
	})))
}

// hashFunc is a type definition for what a hash function should look like
type hashFuncGP func(key string) uint64

// NewHashMapGP returns a new HashMapGP instantiated with the specified size or
// the DefaultMapSize, whichever is larger
func NewHashMapGP(size uint) *HashMapGP {
	return newHashMapGP(size, defaultHashFuncGP)
}

// newHashMap is the internal variant of the previous function
// and is mainly used internally
func newHashMapGP(size uint, hash hashFuncGP) *HashMapGP {
	bukCnt := alignBucketCount(size)
	if hash == nil {
		hash = defaultHashFuncGP
	}
	m := &HashMapGP{
		hash:    hash,
		mask:    bukCnt - 1, // this minus one is extremely important for using a mask over modulo
		expand:  uint(float64(bukCnt) * DefaultLoadFactor),
		shrink:  uint(float64(bukCnt) * (1 - DefaultLoadFactor)),
		keys:    0,
		size:    size,
		buckets: make([]bucketGP, bukCnt),
	}
	return m
}

// resize grows or shrinks the HashMapGP by the newSize provided. It makes a
// new map with the new size, copies everything over, and then frees the old map
func (m *HashMapGP) resize(newSize uint) {
	newHM := newHashMapGP(newSize, m.hash)
	var buk bucketGP
	for i := 0; i < len(m.buckets); i++ {
		buk = m.buckets[i]
		if buk.dib > 0 {
			newHM.insertInternal(buk.hashkey, buk.entryGP.key, buk.entryGP.val)
		}
	}
	tsize := m.size
	*m = *newHM
	m.size = tsize
}

// Get returns a value for a given key, or returns false if none could be found
// Get can be considered the exported version of the lookup call
func (m *HashMapGP) Get(key string) (interface{}, bool) {
	return m.lookup(0, key)
}

// lookup returns a value for a given key, or returns false if none could be found
func (m *HashMapGP) lookup(hashkey uint64, key string) (interface{}, bool) {
	// check if map is empty
	if len(m.buckets) == 0 {
		// hopefully this should never really happen
		// do we really need to check this here?
		*m = *newHashMapGP(DefaultMapSize, m.hash)
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
			return m.buckets[i].entryGP.val, true
		}
		// keep on probing
		i = (i + 1) & m.mask
	}
}

// Set inserts a key value entryGP and returns the previous value or false
// Set can be considered the exported version of the insert call
func (m *HashMapGP) Set(key string, value interface{}) (interface{}, bool) {
	return m.insert(0, key, value)
}

// insert inserts a key value entryGP and returns the previous value, or false
func (m *HashMapGP) insert(hashkey uint64, key string, value interface{}) (interface{}, bool) {
	// check if map is empty
	if len(m.buckets) == 0 {
		// create a new map with default size
		*m = *newHashMapGP(DefaultMapSize, m.hash)
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
	// call the internal insert to insert the entryGP
	return m.insertInternal(hashkey, key, value)
}

// insertInternal inserts a key value entryGP and returns the previous value, or false
func (m *HashMapGP) insertInternal(hashkey uint64, key string, value interface{}) (interface{}, bool) {
	// create a new entryGP to insert
	newb := bucketGP{
		dib:     1,
		hashkey: hashkey,
		entryGP: entryGP{
			key: key,
			val: value,
		},
	}
	// mask the hashkey to get the initial index
	i := newb.hashkey & m.mask
	// search the position linearly
	for {
		// we found a spot, insert a new entryGP
		if m.buckets[i].dib == 0 {
			m.buckets[i] = newb
			m.keys++
			// no previous value to return, as this is a new entryGP
			return 0, false
		}
		// found existing entryGP, check hashes and keys
		if m.buckets[i].checkHashAndKey(newb.hashkey, newb.entryGP.key) {
			// hashes and keys are a match--update entryGP and return previous values
			oldval := m.buckets[i].entryGP.val
			m.buckets[i].val = newb.entryGP.val
			return oldval, true
		}
		// we did not find an empty slot or an existing matching entryGP
		// so check this entries dib against our new entryGP's dib
		if m.buckets[i].dib < newb.dib {
			// current position's dib is less than our new entryGP's, swap
			newb, m.buckets[i] = m.buckets[i], newb
		}
		// keep on probing until we find what we're looking for.
		// increase our search index by one as well as our new
		// entryGP's dib, then continue with the linear probe.
		i = (i + 1) & m.mask
		newb.dib = newb.dib + 1
	}
}

// Del removes a value for a given key and returns the deleted value, or false
// Del can be considered the exported version of the delete call
func (m *HashMapGP) Del(key string) (interface{}, bool) {
	return m.delete(0, key)
}

// delete removes a value for a given key and returns the deleted value, or false
func (m *HashMapGP) delete(hashkey uint64, key string) (interface{}, bool) {
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
		// found existing entryGP, check hashes and keys
		if m.buckets[i].checkHashAndKey(hashkey, key) {
			// hashes and keys are a match--delete entryGP and return previous values
			oldval := m.buckets[i].entryGP.val
			m.deleteInternal(i)
			return oldval, true
		}
		// keep on probing until we find what we're looking for.
		// increase our search index by one as well as our new
		// entryGP's dib, then continue with the linear probe.
		i = (i + 1) & m.mask
	}
}

// delete removes a value for a given key and returns the deleted value, or false
func (m *HashMapGP) deleteInternal(i uint64) {
	// set dib at bucketGP i
	m.buckets[i].dib = 0
	// tombstone index and shift
	for {
		pi := i
		i = (i + 1) & m.mask
		if m.buckets[i].dib <= 1 {
			// im as free as a bird now!
			m.buckets[pi].entryGP = *new(entryGP)
			m.buckets[pi] = *new(bucketGP)
			break
		}
		// shift
		m.buckets[pi] = m.buckets[i]
		m.buckets[pi].dib = m.buckets[pi].dib - 1
	}
	// decrement entryGP count
	m.keys--
	// check and see if we need to resize
	if m.keys <= m.shrink && uint(len(m.buckets)) > m.size {
		// if it checks out, then resize down by 25%-ish
		m.resize(m.keys)
	}
}

// IteratorGP is an iterator function type
type IteratorGP func(key string, value interface{}) bool

// Range takes an Iterator and ranges the HashMapGP as long as long
// as the iterator function continues to be true. Range is not
// safe to perform an insert or remove operation while ranging!
func (m *HashMapGP) Range(it IteratorGP) {
	for i := 0; i < len(m.buckets); i++ {
		if m.buckets[i].dib < 1 {
			continue
		}
		if !it(m.buckets[i].key, m.buckets[i].val) {
			return
		}
	}
}

// GetHighestDIB returns the highest distance to initial bucketGP value in the table
func (m *HashMapGP) GetHighestDIB() uint8 {
	var hdib uint8
	for i := 0; i < len(m.buckets); i++ {
		if m.buckets[i].dib > hdib {
			hdib = m.buckets[i].dib
		}
	}
	return hdib
}

// PercentFull returns the current load factor of the HashMapGP
func (m *HashMapGP) PercentFull() float64 {
	return float64(m.keys) / float64(len(m.buckets))
}

// Len returns the number of entries currently in the HashMapGP
func (m *HashMapGP) Len() int {
	return int(m.keys)
}

// Close closes and frees the current hashmap. Calling any method
// on the HashMapGP after this will most likely result in a panic
func (m *HashMapGP) Close() {
	destroyHashMapGP(m)
}

// destroy does exactly what is sounds like it does
func destroyHashMapGP(m *HashMapGP) {
	m = nil
}
