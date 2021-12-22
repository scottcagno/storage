package v2

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

func TestWriter_Write(t *testing.T) {

	var bb bytes.Buffer
	w := NewWriter(&bb, 64)

	n, err := w.Write([]byte("this is a test, this is only a test."))
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("off=%d, data=%q (len=%d)\n", n, bb.Bytes(), len(bb.Bytes()))

	n, err = w.Write([]byte("and this is another test."))
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("off=%d, data=%q (len=%d)\n", n, bb.Bytes(), len(bb.Bytes()))
}

func TestBufferedWriter(t *testing.T) {

	var bb bytes.Buffer

	bw := bufio.NewWriterSize(&bb, 36)

	n, err := bw.Write([]byte("this is a test, this is only a test."))
	if err != nil {
		t.Error(err)
	}

	//err = bw.Flush()
	//if err != nil {
	//	t.Error(err)
	//}

	n, err = bw.Write([]byte("and this is another test."))
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("off=%d, data=%q (len=%d)\n", n, bb.Bytes(), len(bb.Bytes()))
	fmt.Printf("off=%d, data=%q (len=%d)\n", n, bb.Bytes(), len(bb.Bytes()))

}
