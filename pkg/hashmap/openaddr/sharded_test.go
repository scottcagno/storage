package openaddr

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
	"log"
	"strconv"
	"testing"
)

func TestShardedHashMap(t *testing.T) {
	count := 1000000
	hm := NewShardedHashMap(128)
	for i := 0; i < count; i++ {
		_, ok := hm.Set(strconv.Itoa(i), nil)
		if ok {
			t.Errorf("error: could not located value for key: %q\n", strconv.Itoa(i))
		}
	}
	if hm.Len() != count {
		t.Errorf("error: incorrect count of entries\n")
	}
	fmt.Printf("v2.hashmap containing %d entries is taking %d bytes (%.2f kb, %.2f mb)\n",
		count, util.Sizeof(hm), float64(util.Sizeof(hm)/1024), float64(util.Sizeof(hm)/1024/1024))
	for i := 0; i < count; i++ {
		_, ok := hm.Get(strconv.Itoa(i))
		if !ok {
			t.Errorf("error: could not located value for key: %q\n", strconv.Itoa(i))
		}
	}
	for i := 0; i < count; i++ {
		_, ok := hm.Del(strconv.Itoa(i))
		if !ok {
			t.Errorf("error: could not remove value for key: %q\n", strconv.Itoa(i))
		}
	}
	if hm.Len() != count-count {
		t.Errorf("error: incorrect count of entries\n")
	}
	hm.Close()
}

func TestShardedHashMap_SetAndGetBit(t *testing.T) {
	hm := NewShardedHashMap(128)
	hm.SetBit("mykey", 24, 1)
	hm.SetBit("mykey", 3, 1)
	hm.SetBit("mykey", 4, 1)
	b, ok := hm.GetBit("mykey", 3)
	fmt.Printf("hm.GetBit('mykey', 3)=%.32b (%v)\n", b, ok)
	hm.SetBit("mykey", 3, 0)
	b, ok = hm.GetBit("mykey", 3)
	fmt.Printf("hm.GetBit('mykey', 3)=%.32b (%v)\n", b, ok)
	hm.SetBit("mykey", 24, 1)
	b, ok = hm.GetBit("mykey", 24)
	fmt.Printf("hm.GetBit('mykey', 24)=%.32b (%v)\n", b, ok)
	hm.Close()
}

func TestShardedHashMap_SetAndGetUint(t *testing.T) {
	hm := NewShardedHashMap(128)

	hm.SetUint("counter", 1)
	n, ok := hm.GetUint("counter")
	fmt.Println(n, ok)

	n++
	hm.SetUint("counter", n)
	n, ok = hm.GetUint("counter")
	fmt.Println(n, ok)

	n += 8
	hm.SetUint("counter", n)
	n, ok = hm.GetUint("counter")
	fmt.Println(n, ok)

	hm.Close()
}

func PrintBits(b []byte) {
	//var res string = "16" // set this to the "bit resolution" you'd like to see
	var res = strconv.Itoa(8)
	log.Printf("%."+res+"b (%s bits)", b, res)
}
