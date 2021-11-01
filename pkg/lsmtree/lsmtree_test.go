package lsmtree

import (
	"log"
	"testing"
)

const thousand = 1000

// lsmtree options
var opt = &Options{
	BaseDir:      "lsmtree-testing",
	SyncOnWrite:  false,
	LoggingLevel: LevelOff,
}

func logAndCheckErr(msg string, err error, t *testing.T) {
	log.Println(msg)
	if err != nil {
		t.Fatalf("%s: %v\n", msg, err)
	}
}

func doNTimes(n int, fn func(i int)) {
	for i := 0; i < n; i++ {
		fn(i)
	}
}

func TestLSMTree_Put(t *testing.T) {

	// open
	db, err := OpenLSMTree(opt)
	logAndCheckErr("opening", err, t)

	// write
	doNTimes(1*thousand, func(i int) {
		// write entry
		err := db.Put(makeData("key", i), makeData("value", i))
		if err != nil {
			t.Fatalf("put: %v\n", err)
		}
	})

	// close
	err = db.Close()
	logAndCheckErr("closing", err, t)
}
