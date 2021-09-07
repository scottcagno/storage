package main

import (
	"fmt"
	v5 "github.com/scottcagno/storage/pkg/wal/v5"
	"log"
)

var _ = v5.TEST

func main() {

	f, err := v5.Open("cmd/wal/v5/test/data.txt")
	checkErr(err)

	doWrite := true
	var idx2 int64
	if doWrite {
		idx1, err := f.Write([]byte("this is my first message"))
		checkErr(err)
		fmt.Printf("wrote entry #%d\n", idx1)

		idx2, err = f.Write([]byte("this is my second message"))
		checkErr(err)
		fmt.Printf("wrote entry #%d\n", idx2)

		idx3, err := f.Write([]byte("this is my third message"))
		checkErr(err)
		fmt.Printf("wrote entry #%d\n", idx3)
	}

	fmt.Println(f.Entries())

	for idx, entry := range f.Entries() {

		// test raw read at
		data1, err := f.ReadAt(int64(entry))
		checkErr(err)
		fmt.Printf("[1] reading data at offset %d: %q\n", entry, data1)

		// test "indexed" read at
		data2, err := f.ReadAtIndex(int64(idx))
		checkErr(err)
		fmt.Printf("[2] reading data at index %d: %q\n", idx, data2)

	}

	fmt.Println("LAST BUT NOT LEAST")
	data3, err := f.ReadAtIndex(3)
	checkErr(err)
	fmt.Printf("[3] reading data at index %d: %q\n", 3, data3)

}

func checkErr(err error) {
	if err != nil {
		log.Panicf("got error: %v\n", err)
	}
}
