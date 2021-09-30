package rbtree

import (
	"fmt"
	"strings"
)

// red-black tree entry with string key and int64 value
type rbStringInt64 struct {
	Key   string
	Value int64
}

func (r rbStringInt64) Compare(that RBEntry) int {
	return strings.Compare(r.Key, that.(rbStringInt64).Key)
}

func (r rbStringInt64) Size() int {
	return len(r.Key) + 8
}

func (r rbStringInt64) String() string {
	return fmt.Sprintf("entry.key=%q, entry.value=%d", r.Key, r.Value)
}

// red-black tree entry with string key and []byte value
type rbStringBytes struct {
	Key   string
	Value []byte
}

func (r rbStringBytes) Compare(that RBEntry) int {
	return strings.Compare(r.Key, that.(rbStringBytes).Key)
}

func (r rbStringBytes) Size() int {
	return len(r.Key) + len(r.Value)
}

func (r rbStringBytes) String() string {
	return fmt.Sprintf("entry.key=%q, entry.value=%q", r.Key, r.Value)
}

// red-black tree entry with int64 key
type rbInt64 struct {
	Key int64
}

func (r rbInt64) Compare(that RBEntry) int {
	if r.Key < that.(rbInt64).Key {
		return -1
	}
	if r.Key > that.(rbInt64).Key {
		return 1
	}
	return 0
}

func (r rbInt64) Size() int {
	return 8
}

func (r rbInt64) String() string {
	return fmt.Sprintf("entry=%d", r.Key)
}
