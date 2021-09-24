package chained

import (
	"fmt"
	"log"
	"math/bits"
	"runtime"
	"sync"
)

type shard struct {
	mu sync.RWMutex
	hm *HashMap // chained
}

type ShardedHashMap struct {
	mask   uint64
	hash   hashFunc
	shards []*shard
}

// NewShardedHashMap returns a new hashMap instantiated with the specified size or
// the defaultMapSize, whichever is larger
func NewShardedHashMap(size uint) *ShardedHashMap {
	return newShardedHashMap(size, defaultHashFunc)
}

func newShardedHashMap(size uint, fn hashFunc) *ShardedHashMap {
	shCount := alignShardCount(size)
	if fn == nil {
		fn = defaultHashFunc
	}
	shm := &ShardedHashMap{
		mask:   shCount - 1,
		hash:   fn,
		shards: make([]*shard, shCount),
	}
	hmSize := initialMapShardSize(uint16(shCount))
	log.Printf("new sharded hashmap with %d shards, each shard init with %d buckets\n", shCount, hmSize)
	for i := range shm.shards {
		shm.shards[i] = &shard{
			hm: newHashMap(hmSize, fn),
		}
	}
	return shm
}

func alignShardCount(size uint) uint64 {
	count := uint(16)
	for count < size {
		count *= 2
	}
	return uint64(count)
}

func initialMapShardSize(x uint16) uint {
	return uint(bits.Reverse16(x)) / 2
}

func (s *ShardedHashMap) getShard(key keyType) (uint64, uint64) {
	// calculate the hashkey value
	hashkey := s.hash(key)
	// mask the hashkey to get the initial index
	i := hashkey & s.mask
	return i, hashkey
}

func (s *ShardedHashMap) Put(key keyType, val valType) (valType, bool) {
	return s.insert(key, val)
}

func (s *ShardedHashMap) insert(key keyType, val valType) (valType, bool) {
	buk, hashkey := s.getShard(key)
	s.shards[buk].mu.Lock()
	pv, ok := s.shards[buk].hm.insert(hashkey, key, val)
	s.shards[buk].mu.Unlock()
	return pv, ok
}

func (s *ShardedHashMap) Get(key keyType) (valType, bool) {
	return s.lookup(key)
}

func (s *ShardedHashMap) lookup(key keyType) (valType, bool) {
	buk, hashkey := s.getShard(key)
	s.shards[buk].mu.RLock()
	pv, ok := s.shards[buk].hm.lookup(hashkey, key)
	s.shards[buk].mu.RUnlock()
	return pv, ok
}

func (s *ShardedHashMap) Del(key keyType) (valType, bool) {
	return s.delete(key)
}

func (s *ShardedHashMap) delete(key keyType) (valType, bool) {
	buk, hashkey := s.getShard(key)
	s.shards[buk].mu.Lock()
	pv, ok := s.shards[buk].hm.delete(hashkey, key)
	s.shards[buk].mu.Unlock()
	return pv, ok
}

func (s *ShardedHashMap) Len() int {
	var length int
	for i := range s.shards {
		s.shards[i].mu.Lock()
		length += s.shards[i].hm.Len()
		s.shards[i].mu.Unlock()
	}
	return length
}

func (s *ShardedHashMap) Range(it Iterator) {
	for i := range s.shards {
		s.shards[i].mu.Lock()
		s.shards[i].hm.Range(it)
		s.shards[i].mu.Unlock()
	}
}

func (s *ShardedHashMap) Stats() {
	for i := range s.shards {
		s.shards[i].mu.Lock()
		if pf := s.shards[i].hm.PercentFull(); pf > 0 {
			fmt.Printf("shard %d, fill percent: %.4f\n", i, pf)
		}
		s.shards[i].mu.Unlock()
	}
}

func (s *ShardedHashMap) Close() {
	for i := range s.shards {
		s.shards[i].mu.Lock()
		destroy(s.shards[i].hm)
		s.shards[i].mu.Unlock()
	}
	s.shards = nil
	runtime.GC()
}
