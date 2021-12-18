package bio

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func doitNtimes(n int, fn func()) {
	for i := 0; i < n; i++ {
		time.Sleep(1 * time.Nanosecond)
		fn()
	}
}

// for benchmarks below
var result interface{}

func BenchmarkChunkSliceIter(b *testing.B) {
	var r [][]int
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		// always record the result of Fib to prevent
		// the compiler eliminating the function call
		set := rand.Intn(10)
		ChunkSliceIter(sampleDataInts[set], 16, func(p []int) int {
			// normally we would do stuff in here
			size := len(p)
			if size != chunkSize {
				return -1
			}
			return size
		})
	}
	// always store the result to a package level variable
	// so the compiler cannot eliminate the Benchmark itself.
	result = r
}

func BenchmarkChunkSliceV1(b *testing.B) {
	var r [][]int
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		// always record the result of Fib to prevent
		// the compiler eliminating the function call
		set := rand.Intn(10)
		r = ChunkSliceV1(sampleDataInts[set], 16)
	}
	// always store the result to a package level variable
	// so the compiler cannot eliminate the Benchmark itself.
	result = r
}

func BenchmarkChunkSliceV2(b *testing.B) {
	var r [][]int
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		// always record the result of Fib to prevent
		// the compiler eliminating the function call
		set := rand.Intn(10)
		r = ChunkSliceV2(sampleDataInts[set], 16)
	}
	// always store the result to a package level variable
	// so the compiler cannot eliminate the Benchmark itself.
	result = r
}

func TestNewWriter(t *testing.T) {
	b := new(bytes.Buffer)
	w := NewWriter(b)
	fmt.Println(Info(w, b))
}

func TestWriter_Write(t *testing.T) {
	b := new(bytes.Buffer)
	w := NewWriter(b)
	n, err := w.Write([]byte("entry 1: this is just a test. this is entry number one."))
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("wrote %d bytes.\n", n)
	fmt.Printf("%s", Info(w, b))
}

func TestWriter_WriteMultiple(t *testing.T) {
	b := new(bytes.Buffer)
	w := NewWriter(b)
	n, err := w.Write([]byte("entry 1: this is just a test. this is entry number one."))
	if err != nil {
		t.Error(err)
	}
	n, err = w.Write([]byte("entry 2: this is entry two"))
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("wrote %d bytes.\n", n)
	fmt.Printf("%s", Info(w, b))
}

func TestWriter_WriteOverflowChunk(t *testing.T) {
	b := new(bytes.Buffer)
	w := NewWriter(b)
	var data []byte
	for i := 0; i < blocksPerChunk; i++ {
		data = append(data, []byte(fmt.Sprintf("entry %d: this is just a test. Not sure how long it should be.", i))...)
	}
	n, err := w.Write(data)
	if err != nil {
		t.Logf("got: %v, expected: %v\n", err, err)
	} else {
		t.Error("did not get any error, but expected one")
	}
	fmt.Printf("wrote %d bytes.\n", n)
}
