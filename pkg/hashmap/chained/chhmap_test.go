package chained

import (
	"bytes"
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
	"strconv"
	"testing"
)

func TestNewHashMap(t *testing.T) {
	hm := NewHashMap(128)
	util.AssertExpected(t, 0, hm.Len())
	hm.Put("0", nil)
	util.AssertExpected(t, 1, hm.Len())
	for i := 1; i < 5; i++ {
		hm.Put(strconv.Itoa(i), nil)
	}
	util.AssertExpected(t, 5, hm.Len())
	hm.Close()
}

func Test_alignBucketCount(t *testing.T) {
	var count uint64
	util.AssertExpected(t, uint64(0), count)
	count = alignBucketCount(31)
	util.AssertExpected(t, uint64(32), count)
	count = alignBucketCount(12)
	util.AssertExpected(t, uint64(16), count)
}

func Test_bucket_delete(t *testing.T) {
	b := &bucket{
		hashkey: 1234567890,
		head:    nil,
	}

	b.insert("1", []byte("1"))
	b.insert("2", []byte("2"))
	b.insert("3", []byte("3"))
	b.insert("4", []byte("4"))
	b.insert("5", []byte("5"))

	var count int
	b.scan(func(key keyType, val valType) bool {
		if key != keyZeroType {
			count++
			return true
		}
		return false
	})
	util.AssertExpected(t, 5, count)

	val, ok := b.delete("1")
	util.AssertExpected(t, true, ok)
	util.AssertExpected(t, []byte("1"), val)

	val, ok = b.delete("2")
	util.AssertExpected(t, true, ok)
	util.AssertExpected(t, []byte("2"), val)

	val, ok = b.delete("3")
	util.AssertExpected(t, true, ok)
	util.AssertExpected(t, []byte("3"), val)

	count = 0
	b.scan(func(key keyType, val valType) bool {
		if key != keyZeroType {
			count++
			return true
		}
		return false
	})
	util.AssertExpected(t, 2, count)

	b = nil
}

func Test_bucket_insert(t *testing.T) {
	b := &bucket{
		hashkey: 1234567890,
		head:    nil,
	}
	val, ok := b.insert("1", []byte("1"))
	util.AssertExpected(t, false, ok)
	util.AssertExpected(t, []byte("1"), val)

	val, ok = b.insert("2", []byte("2"))
	util.AssertExpected(t, false, ok)
	util.AssertExpected(t, []byte("2"), val)

	val, ok = b.insert("3", []byte("3"))
	util.AssertExpected(t, false, ok)
	util.AssertExpected(t, []byte("3"), val)

	var count int
	b.scan(func(key keyType, val valType) bool {
		if key != keyZeroType {
			count++
			return true
		}
		return false
	})
	util.AssertExpected(t, 3, count)
	b = nil
}

func Test_bucket_scan(t *testing.T) {
	b := &bucket{
		hashkey: 1234567890,
		head:    nil,
	}

	b.insert("1", []byte("1"))
	b.insert("2", []byte("2"))
	b.insert("3", []byte("3"))
	b.insert("4", []byte("4"))
	b.insert("5", []byte("5"))

	var count int
	b.scan(func(key keyType, val valType) bool {
		if key != keyZeroType {
			count++
			return true
		}
		return false
	})
	util.AssertExpected(t, 5, count)
	b = nil
}

func Test_bucket_search(t *testing.T) {
	b := &bucket{
		hashkey: 1234567890,
		head:    nil,
	}

	b.insert("1", []byte("1"))
	b.insert("2", []byte("2"))
	b.insert("3", []byte("3"))
	b.insert("4", []byte("4"))
	b.insert("5", []byte("5"))

	val, ok := b.search("3")
	util.AssertExpected(t, true, ok)
	util.AssertExpected(t, []byte("3"), val)

	val, ok = b.search("1")
	util.AssertExpected(t, true, ok)
	util.AssertExpected(t, []byte("1"), val)

	val, ok = b.search("5")
	util.AssertExpected(t, true, ok)
	util.AssertExpected(t, []byte("5"), val)

	val, ok = b.search("2")
	util.AssertExpected(t, true, ok)
	util.AssertExpected(t, []byte("2"), val)

	val, ok = b.search("4")
	util.AssertExpected(t, true, ok)
	util.AssertExpected(t, []byte("4"), val)

	b = nil
}

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
		hm.Put(words[i], []byte{0x69})
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
		hm.Put(words[i], []byte{0x69})
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
		hm.Put(words[i], []byte{0x69})
	}
	util.AssertExpected(t, 25, hm.Len())
	hm.Close()
}

func Test_HashMap_PercentFull(t *testing.T) {
	hm := NewHashMap(0)
	for i := 0; i < len(words); i++ {
		hm.Put(words[i], []byte{0x69})
	}
	percent := fmt.Sprintf("%.2f", hm.PercentFull())
	util.AssertExpected(t, "0.78", percent)
	hm.Close()
}

func Test_HashMap_Put(t *testing.T) {
	hm := NewHashMap(128)
	for i := 0; i < len(words); i++ {
		hm.Put(words[i], []byte{0x69})
	}
	util.AssertExpected(t, 25, hm.Len())
	hm.Close()
}

func Test_HashMap_Range(t *testing.T) {
	hm := NewHashMap(128)
	for i := 0; i < len(words); i++ {
		hm.Put(words[i], []byte{0x69})
	}
	util.AssertExpected(t, 25, hm.Len())
	var counted int
	hm.Range(func(key keyType, value valType) bool {
		if key != "" && bytes.Equal(value, []byte{0x69}) {
			counted++
			return true
		}
		return false
	})
	util.AssertExpected(t, 25, counted)
	hm.Close()
}
