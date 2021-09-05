package main

import (
	"github.com/scottcagno/storage/pkg/common"
	v3 "github.com/scottcagno/storage/pkg/wal/v3"
	"log"
)

func main() {
	wal, err := v3.Open("cmd/wal/v3/testlog/")
	checkErr(err)

	err = wal.Close()
	checkErr(err)

	common.NewFileWatch("cmd/wal/v3/testlog/testing.txt")
}

func checkErr(err error) {
	if err != nil {
		log.Panicf("(%T) %v\n", err, err)
	}
}
