package lsmtree

import (
	"bytes"
	"fmt"
)

type Entry struct {
	Key   []byte
	Value []byte
	CRC   uint32
}

type Batch struct {
	Entries []*Entry
}

func (b *Batch) String() string {
	var ss string
	for i := range b.Entries {
		ss += fmt.Sprintf("b.Entries[%d].key=%q, value=%q\n", i, b.Entries[i].Key, b.Entries[i].Value)
	}
	return ss
}

func NewBatch() *Batch {
	return &Batch{
		Entries: make([]*Entry, 0),
	}
}

func (b *Batch) Write(key string, value []byte) {
	b.Entries = append(b.Entries, &Entry{Key: []byte(key), Value: value})
}

func (b *Batch) WriteEntry(e *Entry) {
	b.Entries = append(b.Entries, e)
}

// Len [implementing sort interface]
func (b *Batch) Len() int {
	return len(b.Entries)
}

// Less [implementing sort interface]
func (b *Batch) Less(i, j int) bool {
	return bytes.Compare(b.Entries[i].Key, b.Entries[j].Key) == -1
}

// Swap [implementing sort interface]
func (b *Batch) Swap(i, j int) {
	b.Entries[i], b.Entries[j] = b.Entries[j], b.Entries[i]
}
