package openaddr

import (
	"encoding/binary"
	"fmt"
	"github.com/scottcagno/storage/pkg/bits"
	mathbits "math/bits"
	"runtime"
	"sync"
)

type shard struct {
	mu sync.RWMutex
	hm *HashMap // rhh
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
	//log.Printf("new sharded hashmap with %d shards, each shard init with %d buckets\n", shCount, hmSize)
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
	return uint(mathbits.Reverse16(x)) / 2
}

func (s *ShardedHashMap) getShard(key string) (uint64, uint64) {
	// calculate the hashkey value
	hashkey := s.hash(key)
	// mask the hashkey to get the initial index
	i := hashkey & s.mask
	return i, hashkey
}

func (s *ShardedHashMap) Add(key string, val []byte) ([]byte, bool) {
	if _, ok := s.lookup(key); ok {
		return nil, false // returns false if it didnt add
	}
	return s.insert(key, val)
}

func (s *ShardedHashMap) Set(key string, val []byte) ([]byte, bool) {
	return s.insert(key, val)
}

func (s *ShardedHashMap) SetBit(key string, idx uint, bit uint) bool {
	if bit != 0 && bit != 1 {
		return false
	}
	buk, hashkey := s.getShard(key)
	s.shards[buk].mu.Lock()
	ret, _ := s.shards[buk].hm.lookup(hashkey, key)
	if bit == 1 {
		bits.RawBytesSetBit(&ret, idx)
	}
	if bit == 0 {
		bits.RawBytesUnsetBit(&ret, idx)
	}
	_, _ = s.shards[buk].hm.insert(hashkey, key, ret)
	s.shards[buk].mu.Unlock()
	return true
}

func (s *ShardedHashMap) GetBit(key string, idx uint) (uint, bool) {
	buk, hashkey := s.getShard(key)
	s.shards[buk].mu.Lock()
	ret, ok := s.shards[buk].hm.lookup(hashkey, key)
	if ret == nil || !ok || idx > uint(len(ret)*8) {
		return 0, false
	}
	s.shards[buk].mu.Unlock()
	bit := bits.RawBytesGetBit(&ret, idx)
	return bit, bit != 0
}

func (s *ShardedHashMap) SetUint(key string, num uint64) (uint64, bool) {
	buk, hashkey := s.getShard(key)
	s.shards[buk].mu.Lock()
	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val, num)
	ret, ok := s.shards[buk].hm.insert(hashkey, key, val)
	if !ok {
		s.shards[buk].mu.Unlock()
		return 0, false
	}
	s.shards[buk].mu.Unlock()
	return binary.LittleEndian.Uint64(ret), true
}

func (s *ShardedHashMap) GetUint(key string) (uint64, bool) {
	buk, hashkey := s.getShard(key)
	s.shards[buk].mu.Lock()
	ret, ok := s.shards[buk].hm.lookup(hashkey, key)
	if !ok {
		s.shards[buk].mu.Unlock()
		return 0, false
	}
	s.shards[buk].mu.Unlock()
	return binary.LittleEndian.Uint64(ret), true
}

func (s *ShardedHashMap) insert(key string, val []byte) ([]byte, bool) {
	buk, hashkey := s.getShard(key)
	s.shards[buk].mu.Lock()
	pv, ok := s.shards[buk].hm.insert(hashkey, key, val)
	s.shards[buk].mu.Unlock()
	return pv, ok
}

func (s *ShardedHashMap) Get(key string) ([]byte, bool) {
	return s.lookup(key)
}

func (s *ShardedHashMap) lookup(key string) ([]byte, bool) {
	buk, hashkey := s.getShard(key)
	s.shards[buk].mu.RLock()
	pv, ok := s.shards[buk].hm.lookup(hashkey, key)
	s.shards[buk].mu.RUnlock()
	return pv, ok
}

func (s *ShardedHashMap) Del(key string) ([]byte, bool) {
	return s.delete(key)
}

func (s *ShardedHashMap) delete(key string) ([]byte, bool) {
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
		destroyMap(s.shards[i].hm)
		s.shards[i].mu.Unlock()
	}
	s.shards = nil
	runtime.GC()
}
