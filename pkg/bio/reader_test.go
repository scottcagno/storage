package bio

import (
	"bytes"
	"fmt"
	"testing"
)

var (
	b *bytes.Buffer
	w *Writer
)

func setup(t *testing.T) {
	b = new(bytes.Buffer)
	w = NewWriter(b)
	n, err := w.Write([]byte("entry 1: this is just a test. this is entry number one."))
	if err != nil {
		t.Error(err)
	}
	n, err = w.Write([]byte("entry 2: this is entry two"))
	if err != nil {
		t.Error(err)
	}
	//fmt.Printf("wrote %d bytes.\n", n)
	//fmt.Printf("%s", Info(w, b))
	_ = n
}

func TestReader_NewReader(t *testing.T) {
	//setup(t)
	r := NewReader(new(bytes.Buffer))
	fmt.Printf("%s\n", r)
}

func TestReader_Read(t *testing.T) {
	setup(t)
	r := NewReader(b)
	buf := make([]byte, blockSize*4)
	_, err := r.Read(buf)
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < len(buf); i += blockSize {
		fmt.Printf("block[%d]\n\theader: %s\n\tdata: %q\n\n", i/blockSize,
			buf[i:i+headerSize], buf[i+headerSize:i+blockSize])
	}
}

func TestReader_ReadRecord(t *testing.T) {
	setup(t)
	r := NewReader(b)

	rec, err := r.readRecord()
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("record: %q\n", rec)
}
