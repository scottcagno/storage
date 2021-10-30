package index

import (
	"bytes"
	"github.com/scottcagno/storage/pkg/_junk/_index/bptree"
	"github.com/scottcagno/storage/pkg/_junk/_index/rbtree"
	"math/rand"
	"strconv"
	"testing"
)

const NumItems = 1000000

func RandomByteKey(length int) []byte {
	key := make([]byte, length)
	rand.Read(key)
	return key
}

func RandomStringKey(length int) string {
	return string(RandomByteKey(length))
}

func BenchmarkBPlusTree0_Set(b *testing.B) {
	k := make([][]byte, 0)
	for i := 0; i < NumItems; i++ {
		key := strconv.Itoa(rand.Intn(NumItems))
		k = append(k, []byte(key))
	}
	i, l := 0, len(k)

	b.ResetTimer()
	b.ReportAllocs()
	tr := bptree.NewBPTree()
	for n := 0; n < b.N; n++ {
		tr.Set(k[i], k[i])
		i++
		if i >= l {
			i = 0
		}
	}
	tr.Close()
}

func BenchmarkBPlusTree1_Set(b *testing.B) {
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = RandomByteKey(4)
	}
	b.ResetTimer()
	b.ReportAllocs()
	tr := bptree.NewBPTree()
	for i := 0; i < b.N; i++ {
		tr.Set(keys[i], keys[i])
	}
	tr.Close()
}

func BenchmarkBPlusTree2_Set(b *testing.B) {
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = RandomByteKey(4)
	}
	b.ResetTimer()
	b.ReportAllocs()
	tr := bptree.NewBPTree()
	for i := 0; i < b.N; i++ {
		tr.Set(keys[i], keys[i])
	}
	tr.Close()
}

type Arr []byte

func (this Arr) Compare(that _rbtree.Key) int {
	return bytes.Compare(this, that.(Arr))
}

func BenchmarkRBTree1_Set(b *testing.B) {
	keys := make([]Arr, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = RandomByteKey(4)
	}
	b.ResetTimer()
	b.ReportAllocs()
	tr := rbtree.NewRBTree()
	for i := 0; i < b.N; i++ {
		tr.Set(keys[i], keys[i])
	}
	tr.Close()
}

func BenchmarkRBTree2_Set(b *testing.B) {
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = RandomByteKey(4)
	}
	b.ResetTimer()
	b.ReportAllocs()
	tr := rbtree.NewRBTree()
	for i := 0; i < b.N; i++ {
		tr.Set(keys[i], keys[i])
	}
	tr.Close()
}

func BenchmarkMap_Set(b *testing.B) {
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = RandomStringKey(4)
	}
	b.ResetTimer()
	b.ReportAllocs()
	tr := make(map[string]interface{})
	for i := 0; i < b.N; i++ {
		tr[keys[i]] = keys[i]
	}
	tr = nil
}
