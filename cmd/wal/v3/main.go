package main

import (
	"fmt"
	v3 "github.com/scottcagno/storage/pkg/wal/v3"
	"io"
	"log"
)

func main() {
	wal, err := v3.Open("cmd/wal/v3/testlog/")
	checkErr(err)

	fmt.Printf("WAL--\n%s\n", wal)

	doWrite := false
	if doWrite {
		err = wal.Write([]byte("#1 this is my first entry"))
		checkErr(err)

		err = wal.Write([]byte("#2 this is my second entry"))
		checkErr(err)

		err = wal.Write([]byte("#3 this is my third entry"))
		checkErr(err)
	}

	err = wal.Seek(8, io.SeekStart)
	checkErr(err)

	for {
		data, err := wal.Read()
		if err != nil {
			log.Printf("read: (%T) %+v", err, err)
			break
		}
		fmt.Printf("got data: %q\n", data)
	}

	en2, err := wal.ReadEntry(2)
	checkErr(err)
	fmt.Printf("entry %d: %q\n", 2, en2)

	en1, err := wal.ReadEntry(1)
	checkErr(err)
	fmt.Printf("entry %d: %q\n", 1, en1)

	fmt.Printf("WAL--\n%s\n", wal)

	err = wal.Close()
	checkErr(err)
}

func checkErr(err error) {
	if err != nil {
		log.Panicf("(%T) %v\n", err, err)
	}
}

func isEOF(err error) bool {
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return true
		}
		return false
	}
	return false
}
