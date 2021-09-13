package index

import (
	"fmt"
	"github.com/scottcagno/data-structures/pkg/_reference/_rbtree"
	"github.com/scottcagno/data-structures/pkg/_reference/idx"
	"github.com/scottcagno/data-structures/pkg/treemap"
	"github.com/scottcagno/storage/pkg/index/bptree"
	"github.com/scottcagno/storage/pkg/index/rbtree"
	"log"
	"runtime"
	"strconv"
	"testing"
	"time"
)

const (
	MILLION              = 1000000
	HALF_MILLION         = MILLION / 2
	ONE_HUNDRED_THOUSAND = MILLION / 10
)

func clean() {
	log.Printf("\n>>> TAKING THE GARBAGE OUT, JUST A MOMENT PLEASE...\n")
	runtime.GC()
	time.Sleep(5 * time.Second)
}

func TestMem_BPlusTree1(t *testing.T) {

	fmt.Println("\n(B+Tree #1) [pkg/_reference/pbtree] TestInsertDeleteAndGet\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

	clean()

	mem1 := new(runtime.MemStats)
	runtime.ReadMemStats(mem1)

	t1 := time.Now()

	tr := bptree.NewTree()
	for i := 0; i < MILLION; i++ {
		key := []byte(strconv.Itoa(i))
		tr.Set(key, key)
	}

	t2 := time.Now()
	fmt.Printf("Insert time: %.5f sec\n", float64(t2.Sub(t1).Nanoseconds())/float64(time.Second.Nanoseconds()))

	count := 0
	for i := 0; i < MILLION+HALF_MILLION; i++ {
		key := []byte(strconv.Itoa(i))
		if v := tr.Get(key); v != nil {
			count++
		}
	}

	t3 := time.Now()
	fmt.Printf("Search time: %.5f sec with count %d\n", float64(t3.Sub(t2).Nanoseconds())/float64(time.Second.Nanoseconds()), count)

	log.Printf("get count: %d, tree count: %d\n", count, tr.Len())

	for i := 1; i < MILLION; i++ {
		key := strconv.Itoa(i)
		tr.Del([]byte(key))
	}

	t4 := time.Now()
	fmt.Printf("Delete time: %.5f sec\n", float64(t4.Sub(t3).Nanoseconds())/float64(time.Second.Nanoseconds()))

	mem2 := new(runtime.MemStats)
	runtime.ReadMemStats(mem2)
	if mem2.Alloc <= mem1.Alloc {
		fmt.Printf("Mem allocated: 0 MB\n")
	} else {
		fmt.Printf("Mem allocated: %3.3f MB\n", float64(mem2.Alloc-mem1.Alloc)/(1024*1024))
	}
}

func TestMem_BPlusTree2(t *testing.T) {

	fmt.Println("\n(B+Tree #2) [pkg/_reference/idx] TestInsertDeleteAndGet\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

	clean()

	mem1 := new(runtime.MemStats)
	runtime.ReadMemStats(mem1)

	t1 := time.Now()

	tr := idx.NewTree()
	for i := 0; i < MILLION; i++ {
		key := idx.Itob(int64(i))
		tr.Set(key, key)
	}

	t2 := time.Now()
	fmt.Printf("Insert time: %.5f sec\n", float64(t2.Sub(t1).Nanoseconds())/float64(time.Second.Nanoseconds()))

	count := 0
	for i := 0; i < MILLION+HALF_MILLION; i++ {
		key := idx.Itob(int64(i))
		if v := tr.Get(key); v != nil {
			count++
		}
	}

	t3 := time.Now()
	fmt.Printf("Search time: %.5f sec with count %d\n", float64(t3.Sub(t2).Nanoseconds())/float64(time.Second.Nanoseconds()), count)

	log.Printf("get count: %d, tree count: %d\n", count, tr.Count())

	for i := 1; i < MILLION; i++ {
		key := idx.Itob(int64(i))
		tr.Del(key)
	}

	t4 := time.Now()
	fmt.Printf("Delete time: %.5f sec\n", float64(t4.Sub(t3).Nanoseconds())/float64(time.Second.Nanoseconds()))

	mem2 := new(runtime.MemStats)
	runtime.ReadMemStats(mem2)
	if mem2.Alloc <= mem1.Alloc {
		fmt.Printf("Mem allocated: 0 MB\n")
	} else {
		fmt.Printf("Mem allocated: %3.3f MB\n", float64(mem2.Alloc-mem1.Alloc)/(1024*1024))
	}
}

func TestMem_BPlusTree3(t *testing.T) {

	fmt.Println("\n(B+Tree #3) [pkg/trees] TestInsertDeleteAndGet\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

	clean()

	mem1 := new(runtime.MemStats)
	runtime.ReadMemStats(mem1)

	t1 := time.Now()

	tr := bptree.NewBPTree()
	for i := 0; i < MILLION; i++ {
		key := idx.Itob(int64(i))
		tr.Set(key, key)
	}

	t2 := time.Now()
	fmt.Printf("Insert time: %.5f sec\n", float64(t2.Sub(t1).Nanoseconds())/float64(time.Second.Nanoseconds()))

	count := 0
	for i := 0; i < MILLION+HALF_MILLION; i++ {
		key := idx.Itob(int64(i))
		if v := tr.Get(key); v != nil {
			count++
		}
	}

	t3 := time.Now()
	fmt.Printf("Search time: %.5f sec with count %d\n", float64(t3.Sub(t2).Nanoseconds())/float64(time.Second.Nanoseconds()), count)

	log.Printf("get count: %d, tree count: %d\n", count, tr.Len())

	for i := 1; i < MILLION; i++ {
		key := idx.Itob(int64(i))
		tr.Del(key)
	}

	t4 := time.Now()
	fmt.Printf("Delete time: %.5f sec\n", float64(t4.Sub(t3).Nanoseconds())/float64(time.Second.Nanoseconds()))

	mem2 := new(runtime.MemStats)
	runtime.ReadMemStats(mem2)
	if mem2.Alloc <= mem1.Alloc {
		fmt.Printf("Mem allocated: 0 MB\n")
	} else {
		fmt.Printf("Mem allocated: %3.3f MB\n", float64(mem2.Alloc-mem1.Alloc)/(1024*1024))
	}
}

func TestMem_RBTree1(t *testing.T) {

	fmt.Println("\n(RBTree1) [pkg/_reference/_rbtree] TestInsertDeleteAndGet\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

	clean()

	mem1 := new(runtime.MemStats)
	runtime.ReadMemStats(mem1)

	t1 := time.Now()

	tr := _rbtree.NewTree()
	for i := 0; i < MILLION; i++ {
		key := Arr(idx.Itob(int64(i)))
		tr.Set(key, i)
	}

	t2 := time.Now()
	fmt.Printf("Insert time: %.5f sec\n", float64(t2.Sub(t1).Nanoseconds())/float64(time.Second.Nanoseconds()))

	count := 0
	for i := 0; i < MILLION+HALF_MILLION; i++ {
		key := Arr(idx.Itob(int64(i)))
		if v := tr.Get(key); v != nil && v == i {
			count++
		}
	}

	t3 := time.Now()
	fmt.Printf("Search time: %.5f sec with count %d\n", float64(t3.Sub(t2).Nanoseconds())/float64(time.Second.Nanoseconds()), count)

	log.Printf("get count: %d, tree count: %d\n", count, tr.Len())

	for i := 1; i < MILLION; i++ {
		key := Arr(idx.Itob(int64(i)))
		tr.Del(key)
	}

	t4 := time.Now()
	fmt.Printf("Delete time: %.5f sec\n", float64(t4.Sub(t3).Nanoseconds())/float64(time.Second.Nanoseconds()))

	mem2 := new(runtime.MemStats)
	runtime.ReadMemStats(mem2)
	if mem2.Alloc <= mem1.Alloc {
		fmt.Printf("Mem allocated: 0 MB\n")
	} else {
		fmt.Printf("Mem allocated: %3.3f MB\n", float64(mem2.Alloc-mem1.Alloc)/(1024*1024))
	}
}

func TestMem_RBTree2(t *testing.T) {

	fmt.Println("\n(RBTree2) [pkg/trees] TestInsertDeleteAndGet\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

	clean()

	mem1 := new(runtime.MemStats)
	runtime.ReadMemStats(mem1)

	t1 := time.Now()

	tr := rbtree.NewRBTree()
	for i := 0; i < MILLION; i++ {
		tr.Set(idx.Itob(int64(i)), idx.Itob(int64(i)))
	}

	t2 := time.Now()
	fmt.Printf("Insert time: %.5f sec\n", float64(t2.Sub(t1).Nanoseconds())/float64(time.Second.Nanoseconds()))

	count := 0
	for i := 0; i < MILLION+HALF_MILLION; i++ {
		if v := tr.Get(idx.Itob(int64(i))); v != nil {
			count++
		}
	}

	t3 := time.Now()
	fmt.Printf("Search time: %.5f sec with count %d\n", float64(t3.Sub(t2).Nanoseconds())/float64(time.Second.Nanoseconds()), count)

	log.Printf("get count: %d, tree count: %d\n", count, tr.Len())

	for i := 1; i < MILLION; i++ {
		tr.Del(idx.Itob(int64(i)))
	}

	t4 := time.Now()
	fmt.Printf("Delete time: %.5f sec\n", float64(t4.Sub(t3).Nanoseconds())/float64(time.Second.Nanoseconds()))

	mem2 := new(runtime.MemStats)
	runtime.ReadMemStats(mem2)
	if mem2.Alloc <= mem1.Alloc {
		fmt.Printf("Mem allocated: 0 MB\n")
	} else {
		fmt.Printf("Mem allocated: %3.3f MB\n", float64(mem2.Alloc-mem1.Alloc)/(1024*1024))
	}
}

func TestMem_RBTree3(t *testing.T) {

	fmt.Println("\n(RBTree3) [pkg/treemap] TestInsertDeleteAndGet\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

	clean()

	mem1 := new(runtime.MemStats)
	runtime.ReadMemStats(mem1)

	t1 := time.Now()

	tr := treemap.NewRBTree()
	for i := 0; i < MILLION; i++ {
		key := string(idx.Itob(int64(i)))
		tr.Put(treemap.Entry{Key: key, Value: key})
	}

	t2 := time.Now()
	fmt.Printf("Insert time: %.5f sec\n", float64(t2.Sub(t1).Nanoseconds())/float64(time.Second.Nanoseconds()))

	count := 0
	for i := 0; i < MILLION+HALF_MILLION; i++ {
		key := string(idx.Itob(int64(i)))
		if v, ok := tr.Get(treemap.Entry{Key: key}); ok && v != nil {
			count++
		}
	}

	t3 := time.Now()
	fmt.Printf("Search time: %.5f sec with count %d\n", float64(t3.Sub(t2).Nanoseconds())/float64(time.Second.Nanoseconds()), count)

	log.Printf("get count: %d, tree count: %d\n", count, tr.Len())

	for i := 1; i < MILLION; i++ {
		key := string(idx.Itob(int64(i)))
		tr.Del(treemap.Entry{Key: key})
	}

	t4 := time.Now()
	fmt.Printf("Delete time: %.5f sec\n", float64(t4.Sub(t3).Nanoseconds())/float64(time.Second.Nanoseconds()))

	mem2 := new(runtime.MemStats)
	runtime.ReadMemStats(mem2)
	if mem2.Alloc <= mem1.Alloc {
		fmt.Printf("Mem allocated: 0 MB\n")
	} else {
		fmt.Printf("Mem allocated: %3.3f MB\n", float64(mem2.Alloc-mem1.Alloc)/(1024*1024))
	}
}

func TestMem_Map1(t *testing.T) {

	fmt.Println("\n(Map1) TestInsertDeleteAndGetMap [map[string]interface{} 0 hint]\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

	clean()

	mem1 := new(runtime.MemStats)
	runtime.ReadMemStats(mem1)

	t1 := time.Now()

	tree := make(map[string]interface{})
	for i := 0; i < MILLION; i++ {
		key := strconv.Itoa(i)
		tree[key] = 10 + i
	}

	t2 := time.Now()
	fmt.Printf("Insert map time: %.5f sec\n", float64(t2.Sub(t1).Nanoseconds())/float64(time.Second.Nanoseconds()))

	count := 0
	for i := 0; i < MILLION+HALF_MILLION; i++ {
		key := strconv.Itoa(i)
		_, ok := tree[key]
		if ok {
			count++
		}
	}

	t3 := time.Now()
	fmt.Printf("Search map time: %.5f sec with count %d\n", float64(t3.Sub(t2).Nanoseconds())/float64(time.Second.Nanoseconds()), count)

	log.Printf("get count: %d, tree count: %d\n", count, len(tree))

	for i := 1; i < MILLION; i++ {
		key := strconv.Itoa(i)
		delete(tree, key)
	}

	t4 := time.Now()
	fmt.Printf("Delete map time: %.5f sec\n", float64(t4.Sub(t3).Nanoseconds())/float64(time.Second.Nanoseconds()))

	mem2 := new(runtime.MemStats)
	runtime.ReadMemStats(mem2)
	if mem2.Alloc <= mem1.Alloc {
		fmt.Printf("Mem map allocated: 0 MB\n")
	} else {
		fmt.Printf("Mem map allocated: %3.3f MB\n", float64(mem2.Alloc-mem1.Alloc)/(1024*1024))
	}
}

func TestMem_Map2(t *testing.T) {

	fmt.Println("\n(Map2) TestInsertDeleteAndGetMap [map[string]interface{} HALF_MILLION hint]\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

	clean()

	mem1 := new(runtime.MemStats)
	runtime.ReadMemStats(mem1)

	t1 := time.Now()

	tree := make(map[string]interface{}, HALF_MILLION)
	for i := 0; i < MILLION; i++ {
		key := strconv.Itoa(i)
		tree[key] = 10 + i
	}

	t2 := time.Now()
	fmt.Printf("Insert map time: %.5f sec\n", float64(t2.Sub(t1).Nanoseconds())/float64(time.Second.Nanoseconds()))

	count := 0
	for i := 0; i < MILLION+HALF_MILLION; i++ {
		key := strconv.Itoa(i)
		_, ok := tree[key]
		if ok {
			count++
		}
	}

	t3 := time.Now()
	fmt.Printf("Search map time: %.5f sec with count %d\n", float64(t3.Sub(t2).Nanoseconds())/float64(time.Second.Nanoseconds()), count)

	log.Printf("get count: %d, tree count: %d\n", count, len(tree))

	for i := 1; i < MILLION; i++ {
		key := strconv.Itoa(i)
		delete(tree, key)
	}

	t4 := time.Now()
	fmt.Printf("Delete map time: %.5f sec\n", float64(t4.Sub(t3).Nanoseconds())/float64(time.Second.Nanoseconds()))

	mem2 := new(runtime.MemStats)
	runtime.ReadMemStats(mem2)
	if mem2.Alloc <= mem1.Alloc {
		fmt.Printf("Mem map allocated: 0 MB\n")
	} else {
		fmt.Printf("Mem map allocated: %3.3f MB\n", float64(mem2.Alloc-mem1.Alloc)/(1024*1024))
	}
}
