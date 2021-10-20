/*
 *
 *  * // Copyright (c) 2021 Scott Cagno. All rights reserved.
 *  * // The license can be found in the root of this project; see LICENSE.
 *
 */

package bloom

import (
	"bytes"
	_ "embed"
	"fmt"
	"github.com/scottcagno/storage/pkg/hashmap/openaddr"
	"github.com/scottcagno/storage/pkg/util"
	"log"
	"runtime"
	"strconv"
	"testing"
	"time"
)

var data = [11][]byte{
	[]byte("key-000000"),
	[]byte("Hendrix Avalos"),
	[]byte("Yasmin Mellor"),
	[]byte("Coco Mueller"),
	[]byte("Bodhi Jimenez"),
	[]byte("Seth Kinney"),
	[]byte("Carla Le"),
	[]byte("Kajus Spooner"),
	[]byte("Javier Barrera"),
	[]byte("Junaid O'Brien"),
	[]byte("Emma Guest"),
}

func TestBloomFilter(t *testing.T) {
	// test new filter
	bf := NewBloomFilter(1 << 12)
	fmt.Println(util.Sizeof(bf))

	// test adding data
	for i := 0; i < len(data); i++ {
		key := data[i]
		bf.Set(key)
	}

	// test checking data
	for i := 0; i < len(data); i++ {
		key := data[i]
		ok := bf.Has(key)
		if !ok {
			t.Errorf("error: expected=%v, got=%v\n", true, ok)
		}
	}
	for i := 0; i < len(data); i++ {
		key := []byte("key-" + strconv.Itoa(i))
		ok := bf.Has(key)
		if ok {
			t.Errorf("error: expected=%v, got=%v\n", false, ok)
		}
	}
}

func track(msg string) (string, time.Time) {
	return msg, time.Now()
}

func duration(msg string, start time.Time) {
	log.Printf("%v: %v\n", msg, time.Since(start))
}

//go:embed data.txt
var b []byte

func TestBloomFilterVsHashMap(t *testing.T) {
	words := bytes.Split(b, []byte{'\r', '\n'})
	bloomFilterTest(words...)
	hashMaptest(words...)
}

func bloomFilterTest(data ...[]byte) {
	bf := NewBloomFilter(16384)
	ts1 := time.Now()
	for i := 0; i < 10; i++ {
		for _, word := range data {
			key := fmt.Sprintf("%s-%d", word, i)
			bf.Set([]byte(key))
		}
	}
	ts2 := time.Since(ts1)
	size := util.Sizeof(bf)
	fmt.Printf(">> bloom filter size estimate %dB -> %dKB -> %dMB\n", size, size/1024, size/1024/1024)
	fmt.Println(ts2)
	bf = nil
	runtime.GC()
}

func hashMaptest(data ...[]byte) {
	hm := openaddr.NewHashMap(16384)
	ts1 := time.Now()
	for i := 0; i < 10; i++ {
		for _, word := range data {
			key := fmt.Sprintf("%s-%d", word, i)
			hm.Set(key, []byte(key))
		}
	}
	ts2 := time.Since(ts1)
	size := util.Sizeof(hm)
	fmt.Printf(">> hashmap size estimate %dB -> %dKB -> %dMB\n", size, size/1024, size/1024/1024)
	fmt.Println(ts2)
	hm = nil
	runtime.GC()
}
