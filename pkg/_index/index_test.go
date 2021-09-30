package _index

import (
	"math/rand"
	"testing"
)

type entry1 struct {
	key   string
	value []byte
}

type ds1 struct {
	data map[string]entry1
}

func NewDS1() *ds1 {
	return &ds1{
		data: make(map[string]entry1, 0),
	}
}

func (d *ds1) Put(e1 entry1) {
	d.data[e1.key] = e1
}

type entry2 interface {
	Key() string
}

type rec struct {
	key   string
	value []byte
}

func (r rec) Key() string {
	return r.key
}

type ds2 struct {
	data map[string]entry2
}

func NewDS2() *ds2 {
	return &ds2{
		data: make(map[string]entry2, 0),
	}
}

func (d *ds2) Put(e2 entry2) {
	d.data[e2.Key()] = e2
}

func randByteKey(length int) []byte {
	key := make([]byte, length)
	rand.Read(key)
	return key
}

func randStringKey(length int) string {
	return string(randByteKey(length))
}

func BenchmarkTestDS1(b *testing.B) {
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = randByteKey(4)
	}
	b.ResetTimer()
	b.ReportAllocs()
	ds := NewDS1()
	for i := 0; i < b.N; i++ {
		e1 := entry1{string(keys[i]), keys[i]}
		ds.Put(e1)
	}
	ds = nil
}

func BenchmarkTestDS2(b *testing.B) {
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = randByteKey(4)
	}
	b.ResetTimer()
	b.ReportAllocs()
	ds := NewDS2()
	for i := 0; i < b.N; i++ {
		r := rec{string(keys[i]), keys[i]}
		ds.Put(r)
	}
	ds = nil
}
