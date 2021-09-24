package chained

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
	"strconv"
	"testing"
)

func TestShardedHashMap(t *testing.T) {
	count := 1000000
	hm := NewShardedHashMap(128)
	for i := 0; i < count; i++ {
		hm.Put(strconv.Itoa(i), nil)
	}
	if hm.Len() != count {
		t.Errorf("error: incorrect count of entries\n")
	}
	fmt.Printf("hashmap containing %d entries is taking %d bytes (%.2f kb, %.2f mb)\n",
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
