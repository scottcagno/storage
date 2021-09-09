package main

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/file/v1"
	"log"
	"time"
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

	bf, err := v1.Open("cmd/file/v1/binfile")
	checkErr(err)

	for _, s := range bf.GetSegments() {
		log.Printf("segment: %s", s)
		time.Sleep(250 * time.Millisecond)
	}

	idx, err := bf.Write(data[0])
	checkErr(err)
	latest, _ := bf.LatestIndex()
	fmt.Printf("wrote data at index: %d\n", idx)
	fmt.Printf("last sequence number: %d\n", latest)

	res, err := bf.Read(idx)
	checkErr(err)
	fmt.Printf("read at: %d, result: %q\n", idx, res)

	i := 0
	for {
		_, err = bf.Write(data[0])
		if err != nil {
			fmt.Println(">>> BREAKING...")
			break
		}
		//fmt.Printf(">>> wrote data and did not record index\n")
		//stime.Sleep(64 * time.Millisecond)
		i++
		if i > 9999 {
			fmt.Println(">> wrote batch of 10,000...")
			i = 0
		}
	}

	count := bf.EntryCount()
	fmt.Printf("file entry count appears to be: %d\n", count)

	first, err := bf.FirstIndex()
	checkErr(err)
	fmt.Printf("first entry index: %d\n", first)

	last, err := bf.LastIndex()
	checkErr(err)
	fmt.Printf("last entry index: %d\n", last)

	size := bf.Size()
	fmt.Printf("file size: %d B, %d KB, %d MB\n", size, size/1024, size/1024/1024)

	err = bf.Close()
	checkErr(err)
}

func checkErr(err error) {
	if err != nil {
		log.Panicf("got error: %v\n", err)
	}
}
