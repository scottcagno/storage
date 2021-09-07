package main

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/file"
	"log"
)

var data = [][]byte{
	[]byte("This is one record."),
	[]byte("This is another record."),
	[]byte("Another record for the record."),
	[]byte("I am already running out of stuff to write."),
	[]byte("Roses are red, sometimes."),
	[]byte("Most balls are round."),
	[]byte("How about we turn that frown, the other way around."),
	[]byte("I wish I could type with my mind."),
	[]byte("Space and the cosmos and black holes are really cool."),
	[]byte("I wonder if we can all learn to seep like my father in law?"),
}

func main() {

	bf, err := file.Open("cmd/file/binfile/data.txt")
	checkErr(err)

	idx, err := bf.Write(data[0])
	checkErr(err)
	fmt.Printf("wrote data at index: %d\n", idx)

	fmt.Printf("last sequence number: %d\n", bf.LastSequence())

	res, err := bf.Read(idx)
	checkErr(err)
	fmt.Printf("read at: %d, result: %q\n", idx, res)

	for i := 0; i < 10; i++ {
		_, err := bf.Write(data[i])
		fmt.Printf("wrote data and did not record index\n")
		checkErr(err)
		fmt.Printf("last offset: %d\n", bf.LastOffset())
	}

	count := bf.Count()
	fmt.Printf("file entry count appears to be: %d\n", count)

	first, last := bf.First(), bf.Last()
	fmt.Printf("first entry index: %d, last entry index: %d\n", first, last)

	err = bf.Close()
	checkErr(err)
}

func checkErr(err error) {
	if err != nil {
		log.Panicf("got error: %v\n", err)
	}
}
