package swal

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"os"
	"testing"
)

func makeEntry(i int) *binary.Entry {
	return &binary.Entry{
		Key:   []byte(fmt.Sprintf("key-%06d", i)),
		Value: []byte(fmt.Sprintf("value-%08d", i)),
	}
}

func walWrite(b *testing.B, wal *SWAL, count int) []int64 {

	// offsets
	var offsets []int64

	// write data
	for i := 0; i < count; i++ {
		n, err := wal.Write(makeEntry(i))
		if err != nil {
			b.Errorf("write: %v\n", err)
		}
		b.StopTimer()
		offsets = append(offsets, n)
		b.StartTimer()
	}
	b.StopTimer()
	// return offsets
	return offsets
}

func walRead(b *testing.B, wal *SWAL) {

	// used to "catch" value
	var vv interface{}

	// read data
	err := wal.Scan(func(e *binary.Entry) bool {
		vv = e
		_ = vv
		return e != nil
	})
	if err != nil {
		b.Errorf("read: %v\n", err)
	}
}

func setup(b *testing.B) *SWAL {

	// open
	wal, err := OpenWAL(conf)
	if err != nil {
		b.Errorf("open: %v\n", err)
	}

	// return wal
	return wal
}

func teardown(b *testing.B, wal *SWAL, shouldClean bool) {

	// close
	err := wal.Close()
	if err != nil {
		b.Errorf("close: %v\n", err)
	}

	// check cleanup
	if shouldClean {
		err = os.RemoveAll(wal.conf.BasePath)
		if err != nil {
			b.Fatalf("got error: %v\n", err)
		}
	}
}

func Bench_WAL_Write(b *testing.B, wal *SWAL, count int) {

	// reset measurements
	reset(b)

	// test write
	for i := 0; i < b.N; i++ {
		b.Run("lsmTreeWrite", func(b *testing.B) {
			walWrite(b, wal, count)
		})
	}
}

func Bench_WAL_Read(b *testing.B, wal *SWAL) {

	// reset measurements
	reset(b)

	// test read
	for i := 0; i < b.N; i++ {
		b.Run("lsmTreeRead", func(b *testing.B) {
			walRead(b, wal)
		})
	}
}

func BenchmarkWAL(b *testing.B) {

	// count
	count := 10

	// setup (OPEN DB)
	wal := setup(b)

	// writing
	Bench_WAL_Write(b, wal, count)

	// reading
	Bench_WAL_Read(b, wal)

	// teardown (CLOSE DB)
	teardown(b, wal, true)
}

func reset(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
}
