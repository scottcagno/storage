package openaddr

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/bits"
	"github.com/scottcagno/storage/pkg/util"
	"math/rand"
	"testing"
)

func Test_defaultHashFuncGP(t *testing.T) {
	set := make(map[uint64]string, len(words))
	var hash uint64
	var coll int
	for _, word := range words {
		hash = defaultHashFuncGP(word)
		if old, ok := set[hash]; !ok {
			set[hash] = word
		} else {
			coll++
			fmt.Printf(
				"collision: current word: %s, old word: %s, hash: %d\n", word, old, hash)
		}
	}
	fmt.Printf("encountered %d collisions comparing %d words\n", coll, len(words))
}

func Test_HashMapGP_Del(t *testing.T) {
	hm := NewHashMapGP(128)
	for i := 0; i < len(words); i++ {
		hm.Set(words[i], 0x69)
	}
	util.AssertExpected(t, 25, hm.Len())
	count := hm.Len()
	var stop = hm.Len()
	for i := 0; i < stop; i++ {
		ret, ok := hm.Del(words[i])
		util.AssertExpected(t, true, ok)
		util.AssertExpected(t, 0x69, ret)
		count--
	}
	util.AssertExpected(t, 0, count)
	hm.Close()
}

func Test_HashMapGP_Get(t *testing.T) {
	hm := NewHashMapGP(128)
	for i := 0; i < len(words); i++ {
		hm.Set(words[i], []byte{0x69})
	}
	util.AssertExpected(t, 25, hm.Len())
	var count int
	for i := 0; i < hm.Len(); i++ {
		ret, ok := hm.Get(words[i])
		util.AssertExpected(t, true, ok)
		util.AssertExpected(t, []byte{0x69}, ret)
		count++
	}
	util.AssertExpected(t, 25, count)
	hm.Close()
}

func Test_HashMapGP_Len(t *testing.T) {
	hm := NewHashMapGP(128)
	for i := 0; i < len(words); i++ {
		hm.Set(words[i], []byte{0x69})
	}
	util.AssertExpected(t, 25, hm.Len())
	hm.Close()
}

func Test_HashMapGP_PercentFull(t *testing.T) {
	hm := NewHashMapGP(0)
	for i := 0; i < len(words); i++ {
		hm.Set(words[i], []byte{0x69})
	}
	percent := fmt.Sprintf("%.2f", hm.PercentFull())
	util.AssertExpected(t, "0.78", percent)
	hm.Close()
}

func Test_HashMapGP_Set(t *testing.T) {
	hm := NewHashMapGP(128)
	for i := 0; i < len(words); i++ {
		hm.Set(words[i], []byte{0x69})
	}
	util.AssertExpected(t, 25, hm.Len())
	hm.Close()
}

func Test_HashMapGP_Range(t *testing.T) {
	hm := NewHashMapGP(128)
	for i := 0; i < len(words); i++ {
		hm.Set(words[i], 0x69)
	}
	util.AssertExpected(t, 25, hm.Len())
	var counted int
	hm.Range(func(key string, value interface{}) bool {
		if key != "" && value == 0x69 {
			counted++
			return true
		}
		return false
	})
	util.AssertExpected(t, 25, counted)
	hm.Close()
}

var resultGP interface{}

func BenchmarkHashMapGP_Set1(b *testing.B) {
	hm := NewHashMapGP(128)

	b.ResetTimer()
	b.ReportAllocs()

	var val []byte
	for n := 0; n < b.N; n++ {
		// try to get key/value "foo"
		v, ok := hm.Get("foo")
		val = v.([]byte)
		if !ok {
			// if it doesn't exist, then initialize it
			hm.Set("foo", make([]byte, 32))
		} else {
			// if it does exist, then pick a random number between
			// 0 and 256--this will be our bit we try and set
			ri := uint(rand.Intn(128))
			if ok := bits.RawBytesHasBit(&val, ri); !ok {
				// we check the bit to see if it's already set, and
				// then we go ahead and set it if it is not set
				bits.RawBytesSetBit(&val, ri)
			}
			// after this, we make sure to save the bitset back to the hashmap
			if n < 64 {
				fmt.Printf("addr: %p, %+v\n", val, val)
				//PrintBits(v)
			}
			hm.Set("foo", val)
		}
	}
	result = val
}

func BenchmarkHashMapGP_Set2(b *testing.B) {
	hm := NewHashMapGP(128)

	b.ResetTimer()
	b.ReportAllocs()

	var val []byte
	for n := 0; n < b.N; n++ {
		// try to get key/value "foo"
		v, ok := hm.Get("foo")
		val = v.([]byte)
		if !ok {
			// if it doesn't exist, then initialize it
			hm.Set("foo", make([]byte, 32))
		} else {
			val = append(val, []byte{byte(n >> 8)}...)
			// after this, we make sure to save the bitset back to the hashmap
			if n < 64 {
				fmt.Printf("addr: %p, %+v\n", val, val)
				//PrintBits(v)
			}
			hm.Set("foo", val)
		}
	}
	result = val
}
