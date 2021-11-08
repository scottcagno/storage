package openaddr

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
	"testing"
)

func Test_HashMapU64_Del(t *testing.T) {
	hm := NewHashMapU64(128)
	for i := 0; i < len(words); i++ {
		hm.Set(uint64(i), 0x69)
	}
	util.AssertExpected(t, 25, hm.Len())
	count := hm.Len()
	var stop = hm.Len()
	for i := 0; i < stop; i++ {
		ret, ok := hm.Del(uint64(i))
		util.AssertExpected(t, true, ok)
		util.AssertExpected(t, 0x69, ret)
		count--
	}
	util.AssertExpected(t, 0, count)
	hm.Close()
}

func Test_HashMapU64_Get(t *testing.T) {
	hm := NewHashMapU64(128)
	for i := 0; i < len(words); i++ {
		hm.Set(uint64(i), 0x69)
	}
	util.AssertExpected(t, 25, hm.Len())
	var count int
	for i := 0; i < hm.Len(); i++ {
		ret, ok := hm.Get(uint64(i))
		util.AssertExpected(t, true, ok)
		util.AssertExpected(t, 0x69, ret)
		count++
	}
	util.AssertExpected(t, 25, count)
	hm.Close()
}

func Test_HashMapU64_Len(t *testing.T) {
	hm := NewHashMapU64(128)
	for i := 0; i < len(words); i++ {
		hm.Set(uint64(i), 0x69)
	}
	util.AssertExpected(t, 25, hm.Len())
	hm.Close()
}

func Test_HashMapU64_PercentFull(t *testing.T) {
	hm := NewHashMapU64(0)
	for i := 0; i < len(words); i++ {
		hm.Set(uint64(i), 0x69)
	}
	percent := fmt.Sprintf("%.2f", hm.PercentFull())
	util.AssertExpected(t, "0.78", percent)
	hm.Close()
}

func Test_HashMapU64_Set(t *testing.T) {
	hm := NewHashMapU64(128)
	for i := 0; i < len(words); i++ {
		hm.Set(uint64(i), 0x69)
	}
	util.AssertExpected(t, 25, hm.Len())
	hm.Close()
}

func Test_HashMapU64_Range(t *testing.T) {
	hm := NewHashMapU64(128)
	for i := 0; i < len(words); i++ {
		hm.Set(uint64(i), 0x69)
	}
	util.AssertExpected(t, 25, hm.Len())
	var counted int
	hm.Range(func(key uint64, value uint64) bool {
		if key != 0 && value == 0x69 {
			counted++
			return true
		}
		return false
	})
	util.AssertExpected(t, 25, counted)
	hm.Close()
}
