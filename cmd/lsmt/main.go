package main

import (
	"encoding/json"
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt"
	"path/filepath"
	"strconv"
)

func main() {

	conf := &lsmt.LSMConfig{
		BaseDir:        "cmd/lsmt/data",
		FlushThreshold: -1,
		SyncOnWrite:    false,
	}

	// open LSMTree
	db, err := lsmt.OpenLSMTree(conf)
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

	// isolate key
	key := "key-01"

	// read data (get first entry)
	val, err := db.Get(key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("get(%q): %s\n", key, val)

	// read data (from "int" key, aka the second entry)
	val, err = db.Get(strconv.Itoa(2))
	if err != nil {
		panic(err)
	}
	fmt.Printf("get(%d): %s\n", 2, val)

	// delete first entry
	err = db.Del(key)
	fmt.Printf("del(%q) (error=%v)\n", key, err)

	// check the "has"
	ok := db.Has(key)
	fmt.Printf("has(%q)=%v\n", key, ok)

	// try to find deleted entry
	val, err = db.Get(key)
	if err == nil {
		panic(err)
	}
	fmt.Printf("get(%q): %v\n", key, val)

	// close LSMTree
	err = db.Close()
	if err != nil {
		panic(err)
	}

	// regarding key-spaces: I kinda figured we don't
	// really need them because of the following...
	usersKeyspace := filepath.Join("cmd/lsmt/keyspaces", "users")
	conf.BaseDir = usersKeyspace
	// open LSMTree (users keyspace)
	users, err := lsmt.OpenLSMTree(conf)
	if err != nil {
		panic(err)
	}

	ordersKeyspace := filepath.Join("cmd/lsmt/keyspaces", "orders")
	conf.BaseDir = ordersKeyspace
	// open LSMTree (orders keyspace)
	orders, err := lsmt.OpenLSMTree(conf)
	if err != nil {
		panic(err)
	}

	// add user 1
	user1 := User{Id: 1, Name: []string{"Scott", "Cagno"}, Age: 34, Active: true}
	data1, err := json.Marshal(user1)
	if err != nil {
		panic(err)
	}
	err = users.Put(strconv.Itoa(1), data1)
	if err != nil {
		panic(err)
	}

	// add order 1
	err = orders.Put("order-00001", []byte(`THIS IS MY ORDER`))
	if err != nil {
		panic(err)
	}

	// get user 1
	val, err = users.Get(strconv.Itoa(1))
	if err != nil {
		panic(err)
	}
	var user User
	err = json.Unmarshal(val, &user)
	if err != nil {
		panic(err)
	}
	fmt.Printf("got user1: (%T) %+v\n", user, user)

	// get order 1
	val, err = orders.Get("order-00001")
	if err != nil {
		panic(err)
	}
	fmt.Printf("got order: %q, %s\n", "order-00001", val)

	// close (users) LSMTree keyspace
	err = users.Close()
	if err != nil {
		panic(err)
	}

	// close (orders) LSMTree keyspace
	err = orders.Close()
	if err != nil {
		panic(err)
	}
}

type User struct {
	Id     int
	Name   []string
	Age    int
	Active bool
}
