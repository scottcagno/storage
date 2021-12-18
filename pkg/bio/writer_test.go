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
	fmt.Println(w.Info(b))
}

func TestWriter_WriteSpan(t *testing.T) {
	b := new(bytes.Buffer)
	w := NewWriter(b)
	n, err := w.Write([]byte("entry 1: this is just a test. this is entry number one."))
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("wrote %d bytes.\n", n)
	fmt.Printf("%s", w.Info(b))
}

func TestWriter_Write1(t *testing.T) {
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
	fmt.Printf("%s", w.Info(b))
}

func TestFoo(t *testing.T) {
	f := new(foo)
	f.write([]byte("entry 1: this is just a test. this is entry number 1"))
	fmt.Printf("%q\n", f.w)
}

const (
	fullSize = 32
	hedrSize = 6
	maxSize  = fullSize - hedrSize
)

type foo struct {
	w [fullSize * 2]byte
	n int
}

func (f *foo) write(b []byte) {
	var nn int
	off := maxSize
	for {
		nn += f.writeBlock(b[nn : nn+off])
		fmt.Println("wrote:", nn)
		if nn >= len(b) {
			break
		}
	}
}

func (f *foo) writeBlock(b []byte) int {
	fmt.Printf("(%d) writing: %q => ", f.n, f.w)
	n := copy(f.w[f.n:], []byte{0x07, 0x07, 0x07, 0x07, 0x07, 0x07})
	f.n += n
	n = copy(f.w[f.n:], b)
	if len(b) < maxSize {
		f.n += maxSize - len(b)
	}
	fmt.Printf("(%d) %q\n", f.n, f.w)
	return n
}
