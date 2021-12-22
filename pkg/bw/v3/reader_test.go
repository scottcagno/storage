package v3

import (
	"bytes"
	"fmt"
	"testing"
)

func TestDataReader_Read(t *testing.T) {

	var bb bytes.Buffer
	var nn int

	dw := NewDataWriter(&bb, options)
	n, err := dw.Write(dataToWrite1)
	if err != nil {
		t.Error(err)
	}
	nn += n
	err = dw.Flush()
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("[WROTE] nn=%d, bb=%q\n", nn, bb.Bytes())

	dr := NewDataReader(&bb, options)
	got := make([]byte, 39)
	n, err = dr.Read(got)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("[READ] n=%d, got=%q\n", n, got)
}
