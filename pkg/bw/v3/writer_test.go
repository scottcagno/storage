package v3

import (
	"bytes"
	"fmt"
	"testing"
)

var options = &Options{
	pageSize:  64,
	pageAlign: true,
	autoFlush: false,
}

var dataToWrite1 = []byte("this is a test; it seems to work great!")
var dataToWrite2 = []byte("this is a another test--it should overflow the first page. we'll see if it also works great!")

func TestDataWriter_Write(t *testing.T) {

	var bb bytes.Buffer
	var nn int
	dw := NewDataWriter(&bb, options)
	n, err := dw.Write(dataToWrite2)
	if err != nil {
		t.Error(err)
	}
	nn += n
	err = dw.Flush()
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("nn=%d, bb=%q\n", nn, bb.Bytes())
}

var result interface{}

func BenchmarkDataWriter_Write(b *testing.B) {

	var bb bytes.Buffer
	w := NewDataWriter(&bb, options)
	var x int
	var err error

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		x, err = w.Write(dataToWrite2)
		if err != nil {
			b.Error(err)
		}
		err = w.Flush()
		if err != nil {
			b.Error(err)
		}
		if x != 128 {
			b.Error("uhh, should have been 128")
		}
	}
	// always store the result to a package level variable
	// so the compiler cannot eliminate the Benchmark itself.
	result = x
}
