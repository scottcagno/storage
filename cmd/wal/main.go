package main

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/common"
	"github.com/scottcagno/storage/pkg/wal"
	"os"
)

// main is the main function for the wal package
func main() {

	// open write-ahead log
	l, err := wal.Open("cmd/wal/data")
	common.ErrCheck(err)

	// defer close
	defer func(l *wal.Log) {
		path := l.Path()
		fmt.Printf("closing and cleaning up... ")
		err = l.Close()
		common.ErrCheck(err)
		err = os.RemoveAll(path)
		common.ErrCheck(err)
		fmt.Printf("done!\n")
	}(l)

	// init vars to store data
	indexes := make([]uint64, 0)

	fmt.Printf("writing...\n")

	// write data
	for i := 1; i < 64; i++ {
		n, err := l.Write([]byte(
			fmt.Sprintf("this is entry number %06d", i)))
		common.ErrCheck(err)
		indexes = append(indexes, n)
	}

	fmt.Printf("WRITE-AHEAD LOG\n%s\n", l)

	// read a few single entries
	for i := 0; i < l.Count()-1; i += l.Count() - 1/4 {
		idx := indexes[i]
		fmt.Printf("attempting to read data at index: %d... ", idx)
		data, err := l.Read(idx)
		common.ErrCheck(err)
		fmt.Printf("got: %q\n", data)
	}

	fmt.Printf("scanning...\n")

	// iterate all entries
	err = l.Scan(func(index uint64, data []byte) bool {
		if index >= 0 && data != nil {
			fmt.Printf("index: %06d, data: %q\n", index, data)
			return true
		}
		return false
	})

}
