package main

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/common"
	"github.com/scottcagno/storage/pkg/wal"
	"io"
	"log"
)

func main() {

	// open write ahead log
	l, err := wal.Open("cmd/wal/testlog/logfile.txt")
	if err != nil {
		log.Panicf("open: %v", err)
	}

	l.PrintLoggerSegments()

	e, err := l.ReadAt(2)
	fmt.Printf("val=%q, err=%v\n", e, err)
	e, err = l.ReadAt(4)
	fmt.Printf("val=%q, err=%v\n", e, err)
	e, err = l.ReadAt(3)
	fmt.Printf("val=%q, err=%v\n", e, err)
	e, err = l.ReadAt(1)
	fmt.Printf("val=%q, err=%v\n", e, err)
	e, err = l.ReadAt(0)
	fmt.Printf("val=%q, err=%v\n", e, err)

	doWrite := false

	if doWrite {
		err = l.Write([]byte("#1 this is my first entry."))
		checkErr(err)
		err = l.Write([]byte("#2 this is my second entry."))
		checkErr(err)
		err = l.Write([]byte("#3 this is my third entry."))
		checkErr(err)
		err = l.Write([]byte("#4 this is my forth entry."))
		checkErr(err)
		err = l.Write([]byte("#5 this is my fifth entry."))
		checkErr(err)

	}

	doRead := true

	if doRead {

		err := l.Seek(0, io.SeekStart)
		checkErr(err)

		e1, err := l.Read()
		checkErrOrPrint(err, e1)
		e2, err := l.Read()
		checkErrOrPrint(err, e2)
		e3, err := l.Read()
		checkErrOrPrint(err, e3)
		e4, err := l.Read()
		checkErrOrPrint(err, e4)
		e5, err := l.Read()
		checkErrOrPrint(err, e5)

	}

	// close write ahead log
	l.Close()
}

func checkErr(err error) {
	if err != nil {
		log.Panicf("got error: %v\n", err)
	}
}

func checkErrOrPrint(err error, val []byte) {
	if err != nil {
		log.Panicf("got error: %v\n", err)
	}
	fmt.Printf("no error, val=%q\n", val)
}

func testing() {
	// open temp file
	fd := common.OpenTempFile(common.MkDir("sampledir"), "file1")
	defer fd.Close()

	// write data to file
	common.WriteData(fd, []byte(`this is a test. .`))

	// read data from file
	data := make([]byte, 64)
	common.ReadData(fd, data)
	fmt.Printf("got: %s\n", data)

	spans := wal.Segments(data, wal.DefaultFn)
	fmt.Println("spans")
	for i, s := range spans {
		fmt.Printf("i: %v, s: %v, data: %s\n", i, s, data[s.Start():s.End()])
	}
}
