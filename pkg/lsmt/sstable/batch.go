package sstable

import (
	"bytes"
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
)

type Batch struct {
	data []*binary.Entry
}

func (b *Batch) String() string {
	var ss string
	for i := range b.data {
		ss += fmt.Sprintf("b.data[%d].key=%q, value=%q\n", i, b.data[i].Key, b.data[i].Value)
	}
	return ss
}

func NewBatch() *Batch {
	return &Batch{
		data: make([]*binary.Entry, 0),
	}
}

func (b *Batch) Write(key string, value []byte) {
	b.data = append(b.data, &binary.Entry{Key: []byte(key), Value: value})
}

func (b *Batch) WriteEntry(e *binary.Entry) {
	b.data = append(b.data, e)
}

// Len [implementing sort interface]
func (b *Batch) Len() int {
	return len(b.data)
}

// Less [implementing sort interface]
func (b *Batch) Less(i, j int) bool {
	return bytes.Compare(b.data[i].Key, b.data[j].Key) == -1
}

// Swap [implementing sort interface]
func (b *Batch) Swap(i, j int) {
	b.data[i], b.data[j] = b.data[j], b.data[i]
}
