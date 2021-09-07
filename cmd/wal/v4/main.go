package main

import (
	v4 "github.com/scottcagno/storage/pkg/wal/v4"
	"log"
)

func main() {

	wal, err := v4.Open("cmd/wal/v4/data")
	checkErr(err)

	err = wal.Write([]byte("foo bar"))
	checkErr(err)

}

func checkErr(err error) {
	if err != nil {
		log.Panicf("got error: %v\n", err)
	}
}
