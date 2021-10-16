package main

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt"
	"strconv"
)

func main() {

	// open LSMTree
	db, err := lsmt.OpenLSMTree("cmd/lsmt/data")
	if err != nil {
		panic(err)
	}

	// write data
	err = db.Put("key-01", []byte("value-01"))
	if err != nil {
		panic(err)
	}

	// write data with "int" key
	err = db.Put(strconv.Itoa(2), []byte("value-02"))
	if err != nil {
		panic(err)
	}

	// read data (get first entry)
	val, err := db.Get("key-01")
	if err != nil {
		panic(err)
	}
	fmt.Printf("get(%q): %s\n", "key-01", val)

	// read data (from "int" key, aka the second entry)
	val, err = db.Get(strconv.Itoa(2))
	if err != nil {
		panic(err)
	}
	fmt.Printf("get(%d): %s\n", 2, val)

	// delete first entry
	err = db.Del("key-01")
	// try to find deleted entry
	val, err = db.Get("key-01")
	if err == nil {
		panic(err)
	}
	fmt.Printf("get(%q): %v\n", "key-01", val)

	// close LSMTree
	err = db.Close()
	if err != nil {
		panic(err)
	}

	// regarding key-spaces: I kinda figured we don't
	// really need them because of the following...

}
