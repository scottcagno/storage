package openaddr

import "github.com/scottcagno/storage/pkg/hash/murmur3"

// entry is a key value pair that is found in each bucket
type entry struct {
	key string
	val []byte
}

// bucket represents a single slot in the HashMap table
type bucket struct {
	dib     uint8
	hashkey uint64
	entry
}

// checkHashAndKey checks if this bucket matches the specified hashkey and key
func (b *bucket) checkHashAndKey(hashkey uint64, key string) bool {
	return b.hashkey == hashkey && b.entry.key == key
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

// defaultHashFunc is the default hashFunc used. This is here mainly as
// a convenience for the sharded hashmap to utilize
func defaultHashFunc(key string) uint64 {
	return murmur3.Sum64([]byte(key))
}

// hashFunc is a type definition for what a hash function should look like
type hashFunc func(key string) uint64

// NewHashMap returns a new HashMap instantiated with the specified size or
// the DefaultMapSize, whichever is larger
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
		mask:    bukCnt - 1, // this minus one is extremely important for using a mask over modulo
		expand:  uint(float64(bukCnt) * DefaultLoadFactor),
		shrink:  uint(float64(bukCnt) * (1 - DefaultLoadFactor)),
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
		if buk.dib > 0 {
			newHM.insertInternal(buk.hashkey, buk.entry.key, buk.entry.val)
		}
	}
	tsize := m.size
	*m = *newHM
	m.size = tsize
}

// Get returns a value for a given key, or returns false if none could be found
// Get can be considered the exported version of the lookup call
func (m *HashMap) Get(key string) ([]byte, bool) {
	return m.lookup(0, key)
}

// lookup returns a value for a given key, or returns false if none could be found
func (m *HashMap) lookup(hashkey uint64, key string) ([]byte, bool) {
	// check if map is empty
	if len(m.buckets) == 0 {
		// hopefully this should never really happen
		// do we really need to check this here?
		*m = *newHashMap(DefaultMapSize, m.hash)
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
			return nil, false
		}
		// check for matching hashes and keys
		if m.buckets[i].checkHashAndKey(hashkey, key) {
			return m.buckets[i].entry.val, true
		}
		// keep on probing
		i = (i + 1) & m.mask
	}
}

// Set inserts a key value entry and returns the previous value or false
// Set can be considered the exported version of the insert call
func (m *HashMap) Set(key string, value []byte) ([]byte, bool) {
	return m.insert(0, key, value)
}

// insert inserts a key value entry and returns the previous value, or false
func (m *HashMap) insert(hashkey uint64, key string, value []byte) ([]byte, bool) {
	// check if map is empty
	if len(m.buckets) == 0 {
		// create a new map with default size
		*m = *newHashMap(DefaultMapSize, m.hash)
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
	// call the internal insert to insert the entry
	return m.insertInternal(hashkey, key, value)
}

// insertInternal inserts a key value entry and returns the previous value, or false
func (m *HashMap) insertInternal(hashkey uint64, key string, value []byte) ([]byte, bool) {
	// create a new entry to insert
	newb := bucket{
		dib:     1,
		hashkey: hashkey,
		entry: entry{
			key: key,
			val: value,
		},
	}
	// mask the hashkey to get the initial index
	i := newb.hashkey & m.mask
	// search the position linearly
	for {
		// we found a spot, insert a new entry
		if m.buckets[i].dib == 0 {
			m.buckets[i] = newb
			m.keys++
			// no previous value to return, as this is a new entry
			return nil, false
		}
		// found existing entry, check hashes and keys
		if m.buckets[i].checkHashAndKey(newb.hashkey, newb.entry.key) {
			// hashes and keys are a match--update entry and return previous values
			oldval := m.buckets[i].entry.val
			m.buckets[i].val = newb.entry.val
			return oldval, true
		}
		// we did not find an empty slot or an existing matching entry
		// so check this entries dib against our new entry's dib
		if m.buckets[i].dib < newb.dib {
			// current position's dib is less than our new entry's, swap
			newb, m.buckets[i] = m.buckets[i], newb
		}
		// keep on probing until we find what we're looking for.
		// increase our search index by one as well as our new
		// entry's dib, then continue with the linear probe.
		i = (i + 1) & m.mask
		newb.dib = newb.dib + 1
	}
}

// Del removes a value for a given key and returns the deleted value, or false
// Del can be considered the exported version of the delete call
func (m *HashMap) Del(key string) ([]byte, bool) {
	return m.delete(0, key)
}

// delete removes a value for a given key and returns the deleted value, or false
func (m *HashMap) delete(hashkey uint64, key string) ([]byte, bool) {
	// check if map is empty
	if len(m.buckets) == 0 {
		// nothing to see here folks
		return nil, false
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
			return nil, false
		}
		// found existing entry, check hashes and keys
		if m.buckets[i].checkHashAndKey(hashkey, key) {
			// hashes and keys are a match--delete entry and return previous values
			oldval := m.buckets[i].entry.val
			m.deleteInternal(i)
			return oldval, true
		}
		// keep on probing until we find what we're looking for.
		// increase our search index by one as well as our new
		// entry's dib, then continue with the linear probe.
		i = (i + 1) & m.mask
	}
}

// delete removes a value for a given key and returns the deleted value, or false
func (m *HashMap) deleteInternal(i uint64) {
	// set dib at bucket i
	m.buckets[i].dib = 0
	// tombstone index and shift
	for {
		pi := i
		i = (i + 1) & m.mask
		if m.buckets[i].dib <= 1 {
			// im as free as a bird now!
			m.buckets[pi].entry = *new(entry)
			m.buckets[pi] = *new(bucket)
			break
		}
		// shift
		m.buckets[pi] = m.buckets[i]
		m.buckets[pi].dib = m.buckets[pi].dib - 1
	}
	// decrement entry count
	m.keys--
	// check and see if we need to resize
	if m.keys <= m.shrink && uint(len(m.buckets)) > m.size {
		// if it checks out, then resize down by 25%-ish
		m.resize(m.keys)
	}
}

// Iterator is an iterator function type
type Iterator func(key string, value []byte) bool

// Range takes an Iterator and ranges the HashMap as long as long
// as the iterator function continues to be true. Range is not
// safe to perform an insert or remove operation while ranging!
func (m *HashMap) Range(it Iterator) {
	for i := 0; i < len(m.buckets); i++ {
		if m.buckets[i].dib < 1 {
			continue
		}
		if !it(m.buckets[i].key, m.buckets[i].val) {
			return
		}
	}
}

// GetHighestDIB returns the highest distance to initial bucket value in the table
func (m *HashMap) GetHighestDIB() uint8 {
	var hdib uint8
	for i := 0; i < len(m.buckets); i++ {
		if m.buckets[i].dib > hdib {
			hdib = m.buckets[i].dib
		}
	}
	return hdib
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
	destroyMap(m)
}

// destroy does exactly what is sounds like it does
func destroyMap(m *HashMap) {
	m = nil
}
