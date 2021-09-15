package main

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/binary"
	"log"
)

func main() {

	f, err := binary.OpenDataFile("cmd/binary/testing.txt")
	checkErr(err)

	var entries []int64

	for i := 0; i < 25; i++ {
		n, err := f.WriteEntry(makeEntry(uint64(i)))
		checkErr(err)
		entries = append(entries, n)
	}

	f.Range(func(e *binary.Entry) bool {
		if e == nil {
			return false
		}
		fmt.Printf("got entry: %s\n", e)
		return true
	})

	log.Printf("\n>> NOW, READING AT...<<\n")

	for _, eoff := range entries {
		fmt.Printf("reading entry at offset %d, ", eoff)
		e, err := f.ReadEntryAt(eoff)
		checkErr(err)
		if e == nil {
			continue
		}
		fmt.Printf("got entry: %s\n", e)
	}

	err = f.Close()
	checkErr(err)
}

func checkErr(err error) {
	if err != nil {
		log.Printf("got error: %q\n", err)
	}
}

func makeEntry(id uint64) *binary.Entry {
	return &binary.Entry{
		Id:    id,
		Key:   []byte(fmt.Sprintf("key-%06d", id)),
		Value: []byte(fmt.Sprintf("value-%06d", id)),
	}
}
