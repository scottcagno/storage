package bw

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
	"testing"
)

var result interface{}

func BenchmarkNewWriter_WriteV1(b *testing.B) {

	var bb bytes.Buffer
	bb.Grow(4096)
	w := NewWriter(&bb)
	var x int
	var err error

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		x, err = w.Write(util.RandBytes(512))
		if err != nil {
			b.Error(err)
		}
		err = w.Flush()
		if err != nil {
			b.Error(err)
		}
	}
	// always store the result to a package level variable
	// so the compiler cannot eliminate the Benchmark itself.
	result = x
}

func BenchmarkNewWriter_WriteV2(b *testing.B) {

	var bb bytes.Buffer
	bb.Grow(4096)
	w := bufio.NewWriterSize(&bb, defaultBufSize)
	var x int
	var err error

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		x, err = w.Write(util.RandBytes(512))
		if err != nil {
			b.Error(err)
		}
		err = w.Flush()
		if err != nil {
			b.Error(err)
		}
	}
	// always store the result to a package level variable
	// so the compiler cannot eliminate the Benchmark itself.
	result = x
}

func TestNewWriter_WriteV1(t *testing.T) {

	var bb bytes.Buffer
	w := NewWriterSize(&bb, 256, 64)

	n, err := w.Write([]byte("this is a test, this is only a test. let's see if this ends up working or not.... hmmm"))
	if err != nil {
		t.Error(err)
	}
	err = w.Flush()
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("off=%d, data=%q (len=%d)\n", n, bb.Bytes(), len(bb.Bytes()))

	n, err = w.Write([]byte("and this is another test."))
	if err != nil {
		t.Error(err)
	}
	err = w.Flush()
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("off=%d, data=%q (len=%d)\n", n, bb.Bytes(), len(bb.Bytes()))
}
