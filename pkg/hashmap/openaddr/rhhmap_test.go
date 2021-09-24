package openaddr

import (
	"bytes"
	"fmt"
	"github.com/scottcagno/leviathan/pkg/bitset"
	"github.com/scottcagno/storage/pkg/util"
	"math/rand"
	"testing"
)

// 25 words
var words = []string{
	"reproducibility",
	"eruct",
	"acids",
	"flyspecks",
	"driveshafts",
	"volcanically",
	"discouraging",
	"acapnia",
	"phenazines",
	"hoarser",
	"abusing",
	"samara",
	"thromboses",
	"impolite",
	"drivennesses",
	"tenancy",
	"counterreaction",
	"kilted",
	"linty",
	"kistful",
	"biomarkers",
	"infusiblenesses",
	"capsulate",
	"reflowering",
	"heterophyllies",
}

func Test_defaultHashFunc(t *testing.T) {
	set := make(map[uint64]string, len(words))
	var hash uint64
	var coll int
	for _, word := range words {
		hash = defaultHashFunc(word)
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

func Test_HashMap_Del(t *testing.T) {
	hm := NewHashMap(128)
	for i := 0; i < len(words); i++ {
		hm.Set(words[i], []byte{0x69})
	}
	util.AssertExpected(t, 25, hm.Len())
	count := hm.Len()
	var stop = hm.Len()
	for i := 0; i < stop; i++ {
		ret, ok := hm.Del(words[i])
		util.AssertExpected(t, true, ok)
		util.AssertExpected(t, []byte{0x69}, ret)
		count--
	}
	util.AssertExpected(t, 0, count)
	hm.Close()
}

func Test_HashMap_Get(t *testing.T) {
	hm := NewHashMap(128)
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

func Test_HashMap_Len(t *testing.T) {
	hm := NewHashMap(128)
	for i := 0; i < len(words); i++ {
		hm.Set(words[i], []byte{0x69})
	}
	util.AssertExpected(t, 25, hm.Len())
	hm.Close()
}

func Test_HashMap_PercentFull(t *testing.T) {
	hm := NewHashMap(0)
	for i := 0; i < len(words); i++ {
		hm.Set(words[i], []byte{0x69})
	}
	percent := fmt.Sprintf("%.2f", hm.PercentFull())
	util.AssertExpected(t, "0.78", percent)
	hm.Close()
}

func Test_HashMap_Set(t *testing.T) {
	hm := NewHashMap(128)
	for i := 0; i < len(words); i++ {
		hm.Set(words[i], []byte{0x69})
	}
	util.AssertExpected(t, 25, hm.Len())
	hm.Close()
}

func Test_HashMap_Range(t *testing.T) {
	hm := NewHashMap(128)
	for i := 0; i < len(words); i++ {
		hm.Set(words[i], []byte{0x69})
	}
	util.AssertExpected(t, 25, hm.Len())
	var counted int
	hm.Range(func(key string, value []byte) bool {
		if key != "" && bytes.Equal(value, []byte{0x69}) {
			counted++
			return true
		}
		return false
	})
	util.AssertExpected(t, 25, counted)
	hm.Close()
}

var result interface{}

func BenchmarkHashMap_Set1(b *testing.B) {
	hm := NewHashMap(128)

	b.ResetTimer()
	b.ReportAllocs()

	var v []byte
	for n := 0; n < b.N; n++ {
		// try to get key/value "foo"
		v, ok := hm.Get("foo")
		if !ok {
			// if it doesn't exist, then initialize it
			hm.Set("foo", make([]byte, 32))
		} else {
			// if it does exist, then pick a random number between
			// 0 and 256--this will be our bit we try and set
			ri := uint(rand.Intn(128))
			if ok := bitset.RawBytesHasBit(&v, ri); !ok {
				// we check the bit to see if it's already set, and
				// then we go ahead and set it if it is not set
				bitset.RawBytesSetBit(&v, ri)
			}
			// after this, we make sure to save the bitset back to the hashmap
			if n < 64 {
				fmt.Printf("addr: %p, %+v\n", v, v)
				//PrintBits(v)
			}
			hm.Set("foo", v)
		}
	}
	result = v
}

func BenchmarkHashMap_Set2(b *testing.B) {
	hm := NewHashMap(128)

	b.ResetTimer()
	b.ReportAllocs()

	var v []byte
	for n := 0; n < b.N; n++ {
		// try to get key/value "foo"
		v, ok := hm.Get("foo")
		if !ok {
			// if it doesn't exist, then initialize it
			hm.Set("foo", make([]byte, 32))
		} else {
			v = append(v, []byte{byte(n >> 8)}...)
			// after this, we make sure to save the bitset back to the hashmap
			if n < 64 {
				fmt.Printf("addr: %p, %+v\n", v, v)
				//PrintBits(v)
			}
			hm.Set("foo", v)
		}
	}
	result = v
}
