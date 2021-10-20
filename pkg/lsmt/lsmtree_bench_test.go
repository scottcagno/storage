package lsmt

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/sstable"
	"os"
	"testing"
)

func _makeKey(i int) string {
	return fmt.Sprintf("key-%06d", i)
}

func _makeVal(i int) []byte {
	return []byte(fmt.Sprintf("value-%08d", i))
}

func lsmTreeWrite(b *testing.B, db *LSMTree, count int) {

	// write Entries
	for i := 0; i < count; i++ {
		err := db.Put(makeKey(i), makeVal(i))
		if err != nil {
			b.Errorf("put: %v\n", err)
		}
	}
}

func lsmTreeRead(b *testing.B, db *LSMTree, count int) {

	// used to "catch" value
	var vv interface{}

	// read Entries
	for i := 0; i < count; i++ {
		v, err := db.Get(makeKey(i))
		if err != nil {
			if err == sstable.ErrSSTIndexNotFound {
				continue
			}
			b.Errorf("get: %v\n", err)
		}
		vv = v
		_ = vv
	}
}

func lsmTreeRemove(b *testing.B, db *LSMTree, count int) {

	// remove Entries
	for i := 0; i < count; i++ {
		err := db.Del(makeKey(i))
		if err != nil {
			b.Errorf("del: %v\n", err)
		}
	}
}

func setup(b *testing.B) *LSMTree {

	// open
	db, err := OpenLSMTree(conf)
	if err != nil {
		b.Errorf("open: %v\n", err)
	}

	// return db
	return db
}

func teardown(b *testing.B, db *LSMTree, shouldClean bool) {

	// close
	err := db.Close()
	if err != nil {
		b.Errorf("close: %v\n", err)
	}

	// check cleanup
	if shouldClean {
		err = os.RemoveAll(db.conf.BasePath)
		if err != nil {
			b.Fatalf("got error: %v\n", err)
		}
	}
}

func Bench_LSMTree_Write(b *testing.B, db *LSMTree, count int) {

	// reset measurements
	reset(b)

	// test write
	for i := 0; i < b.N; i++ {
		b.Run("lsmTreeWrite", func(b *testing.B) {
			lsmTreeWrite(b, db, count)
		})
	}
}

func Bench_LSMTree_Read(b *testing.B, db *LSMTree, count int) {

	// reset measurements
	reset(b)

	// test read
	for i := 0; i < b.N; i++ {
		b.Run("lsmTreeRead", func(b *testing.B) {
			lsmTreeRead(b, db, count)
		})
	}
}

func BenchmarkLSMTree(b *testing.B) {

	// count
	count := 10

	// setup (OPEN DB)
	db := setup(b)

	// writing
	Bench_LSMTree_Write(b, db, count)

	// reading
	Bench_LSMTree_Read(b, db, count)

	// teardown (CLOSE DB)
	teardown(b, db, false)
}

func reset(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
}
