package cache

import (
	"fmt"
	"log"
	"sync"
)

// DefaultSize is the max size of the cache before
// the older items automatically get evicted
const DefaultSize = 256

// item is an item in the cache (doubly linked)
type item[K comparable, V any] struct {
	key        K
	value      V
	prev, next *item[K, V]
}

func (i *item[K, V]) String() string {
	ss := fmt.Sprintf("item[key=%v, value=%v, prev=%p, next=%p]", i.key, i.value, i.prev, i.next)
	return ss
}

// LRU is an LRU cache
type LRU[K comparable, V any] struct {
	size       int               // max num of items
	items      map[K]*item[K, V] // actives items
	head, tail *item[K, V]       // head and tail of list
	mu         sync.RWMutex
}

func NewLRU[K comparable, V any](size int) *LRU[K, V] {
	if size < 1 {
		size = DefaultSize
	}
	lru := &LRU[K, V]{
		size:  size,
		items: make(map[K]*item[K, V], size),
		head:  new(item[K, V]),
		tail:  new(item[K, V]),
	}
	lru.head.next = lru.tail
	lru.tail.prev = lru.head
	return lru
}

func (l *LRU[K, V]) init(size int) {
	if l.size < 1 {
		size = DefaultSize
	}
	l.size = size
	l.items = make(map[K]*item[K, V], size)
	l.head = new(item[K, V])
	l.tail = new(item[K, V])
	l.head.next = l.tail
	l.tail.prev = l.head
}

// evict pops, removes and returns (effectively evicting) an item
func (l *LRU[K, V]) evict() *item[K, V] {
	i := l.tail.prev
	l.pop(i)
	delete(l.items, i.key)
	return i
}

// pop, stack operation
func (l *LRU[K, V]) pop(i *item[K, V]) {
	i.prev.next = i.next
	i.next.prev = i.prev
}

// push, stack operation
func (l *LRU[K, V]) push(i *item[K, V]) {
	l.head.next.prev = i
	i.next = l.head.next
	i.prev = l.head
	l.head.next = i
}

// Resize sets the max size of the LRU cache and returns the evicted items. It will panic
// if the size is less than one item. I the value is less than the number of items in the
// cache, then items will be evicted.
func (l *LRU[K, V]) Resize(size int) (ekeys, evals []interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if size < 1 {
		log.Panicln("invalid size")
	}
	for size < len(l.items) {
		i := l.evict()
		ekeys, evals = append(ekeys, i.key), append(evals, i.value)
	}
	l.size = size
	return ekeys, evals
}

// Len returns the current length of the cache
func (l *LRU[K, V]) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.items)
}

// SetEvicted inserts or replaces a value for a given key.
// The item is returned if this operation causes an eviction.
func (l *LRU[K, V]) SetEvicted(key K, value V) (prev V, replaced bool, ekey K, eval V, evicted bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.items == nil {
		l.init(l.size)
	}
	i := l.items[key]
	if i == nil {
		if len(l.items) == l.size {
			i = l.evict()
			ekey, eval, evicted = i.key, i.value, true
		} else {
			i = new(item[K, V])
		}
		i.key, i.value = key, value
		l.push(i)
		l.items[key] = i
	} else {
		prev, replaced = i.value, true
		i.value = value
		if l.head.next != i {
			l.pop(i)
			l.push(i)
		}
	}
	return prev, replaced, ekey, eval, evicted
}

// Set inserts or replaces a value for the given key
func (l *LRU[K, V]) Set(key K, value V) (V, bool) {
	prev, replaced, _, _, _ := l.SetEvicted(key, value)
	return prev, replaced
}

// Get returns a value for the given key (if it exists)
func (l *LRU[K, V]) Get(key K) (V, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	i := l.items[key]
	if i == nil {
		return *new(V), false
	}
	if l.head.next != i {
		l.pop(i)
		l.push(i)
	}
	return i.value, true
}

// Del removes and value for the given key (if it exists)
func (l *LRU[K, V]) Del(key K) (V, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	i := l.items[key]
	if i == nil {
		return *new(V), false
	}
	delete(l.items, key)
	l.pop(i)
	return i.value, true
}

// Range iterates over all keys and values in the order of most
// recently used to least recently used items.
func (l *LRU[K, V]) Range(iter func(key K, value V) bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if head := l.head; head != nil {
		i := head.next
		for i != l.tail {
			if !iter(i.key, i.value) {
				return
			}
			i = i.next
		}
	}
}

// Reverse iterates over all keys and values in the order of least
// recently used to most recently used items.
func (l *LRU[K, V]) Reverse(iter func(key K, value V) bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if tail := l.tail; tail != nil {
		i := tail.prev
		for i != l.head {
			if !iter(i.key, i.value) {
				return
			}
			i = i.prev
		}
	}
}

func (l *LRU[K, V]) String() string {
	ss := fmt.Sprintf("lur:\n")
	ss += fmt.Sprintf("\tsize=%d\n", l.size)
	ss += fmt.Sprintf("\thead=%s\n", l.head)
	ss += fmt.Sprintf("\ttail=%s\n", l.tail)
	return ss
	//size       int
	//items      map[K]*item[K, V]
	//head *item[K, V]
	//tail *item[K, V]
}
