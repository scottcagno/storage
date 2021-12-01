package main

import (
	"errors"
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmtree"
	"io"
	"log"
	"os"
)

func main() {

	bf, err := lsmtree.OpenBlockFile("cmd/blockfile/testing/test.txt")
	if err != nil {
		log.Panic(err)
	}

	d1 := []byte("foo bar")
	write(bf, d1)

	d2 := []byte("baz nad")
	write(bf, d2)

	d3 := []byte("cool beans man")
	write(bf, d3)

	d4 := []byte("i really wonder what i can do to make this more idiomatic?")
	write(bf, d4)

	d5 := []byte("so far so good, i suppose")
	write(bf, d5)

	_, err = bf.Seek(0, io.SeekStart)
	if err != nil {
		log.Panic(err)
	}

	d, err := bf.Read()
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data: %q\n", d)

	d, err = bf.Read()
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data: %q\n", d)

	d, err = bf.Read()
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data: %q\n", d)

	d, err = bf.Read()
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data: %q\n", d)

	d, err = bf.Read()
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data: %q\n", d)

	off := int64(4096)
	d, err = bf.ReadAt(off)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data at offset %d: %q\n", off, d)

	off = int64(0)
	d, err = bf.ReadAt(off)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data at offset %d: %q\n", off, d)

	off = int64(12288)
	d, err = bf.ReadAt(off)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("read data at offset %d: %q\n", off, d)

	_, err = bf.Seek(0, io.SeekStart)
	if err != nil {
		log.Panic(err)
	}

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
