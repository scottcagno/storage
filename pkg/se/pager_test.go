package se

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

var tpath = "se-pager-test/pagerfile.txt"

var records = [][]byte{
	[]byte("this record will fit in one (64b) page, not two"),
	[]byte("this record is a little bit longer than the first one--it should end up occupying two pages but not three"),
	[]byte("and this record is going to be a bit longer than the second one and ideally we should aim to fill three pages, but not four. this one should suffice."),
}

func TestNewPageWriter(t *testing.T) {

	// open file
	//f, err := openFile(tpath)
	//if err != nil {
	//	t.Error(err)
	//}
	//
	//// done forget to close file
	//defer f.Close()

	var bb bytes.Buffer

	pw := NewPageWriter(&bb)
	n, err := pw.Write(records[2])
	if err != nil {
		t.Error(err)
	}
	err = pw.Flush()
	if err != nil {
		t.Error(err)
	}
	time.Sleep(250 * time.Millisecond)
	fmt.Printf("wrote=%d, data=%q\n", n, bb.Bytes())

	pr := NewPageReader(&bb)
	buf := make([]byte, pageSize)
	n, err = pr.Read(buf)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("read=%d, data=%q\n", n, buf)
}
