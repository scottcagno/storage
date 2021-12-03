package main

import (
	"errors"
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmtree"
	"github.com/scottcagno/storage/pkg/util"
	"io"
	"log"
	"math"
	"os"
)

func main() {

	n := math.MaxUint16
	x := make([]int64, n)
	s := util.Sizeof(x)
	fmt.Printf("x := make([]int64, %d) is %dbytes, %dkb, %dmb\n", n, s, s/1000, s/1000/1000)

	bf, err := lsmtree.OpenBlockFile("cmd/blockfile/testing/test.txt")
	if err != nil {
		log.Panic(err)
	}

	// write...
	d1 := []byte("foo bar")
	off, err := bf.Write(d1)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("wrote data at offset %d\n", off)

	// write...
	d2 := []byte("baz nad")
	off, err = bf.Write(d2)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("wrote data at offset %d\n", off)

	// write...
	d3 := []byte("cool beans man")
	off, err = bf.Write(d3)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("wrote data at offset %d\n", off)

	// write...
	d4 := []byte("i really wonder what i can do to make this more idiomatic?")
	off, err = bf.Write(d4)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("wrote data at offset %d\n", off)

	// write...
	d5 := []byte("so far so good, i suppose")
	off, err = bf.Write(d5)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("wrote data at offset %d\n", off)

	// go back to beginning
	_, err = bf.Seek(0, io.SeekStart)
	if err != nil {
		log.Panic(err)
	}

	// read...
	d, err := bf.Read()
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data: %q\n", d)

	// read...
	d, err = bf.Read()
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data: %q\n", d)

	// read...
	d, err = bf.Read()
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data: %q\n", d)

	// read...
	d, err = bf.Read()
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data: %q\n", d)

	// read...
	d, err = bf.Read()
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data: %q\n", d)

	// read at...
	pos := int64(4096)
	d, err = bf.ReadAt(pos)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data at offset %d: %q\n", off, d)

	// read at...
	pos = int64(0)
	d, err = bf.ReadAt(pos)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data at offset %d: %q\n", off, d)

	// read at...
	pos = int64(12288)
	d, err = bf.ReadAt(pos)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data at offset %d: %q\n", off, d)

	// go back to beginning
	_, err = bf.Seek(0, io.SeekStart)
	if err != nil {
		log.Panic(err)
	}

	// test scan out
	err = bf.Scan(func(rd *lsmtree.Record) error {
		if rd != nil {
			fmt.Printf("record: %s\n", rd.String())
			return nil
		}
		return errors.New("something went wrong")
	})
	if err != nil {
		log.Panic(err)
	}

	// close
	err = bf.Close()
	if err != nil {
		log.Panic(err)
	}

	err = os.RemoveAll("cmd/blockfile/testing/")
	if err != nil {
		log.Panic(err)
	}
}

func write(w io.Writer, d []byte) {
	off, err := w.Write(d)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("wrote data at offset %d\n", off)
}

var setupDir = func() string {
	dir, err := os.MkdirTemp("dir", "tmp-*")
	if err != nil {
		log.Panic(err)
	}
	return dir
}

var cleanupDir = func(dir string) {
	err := os.RemoveAll(dir)
	if err != nil {
		log.Panic(err)
	}
}
