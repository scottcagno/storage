package rbtree

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"strconv"
)

func IntToKey(key int64) string {
	return "i" + strconv.FormatInt(key, 10)
}

func KeyToInt(key string) int64 {
	if len(key) != 11 || key[0] != 'i' {
		return -1
	}
	ikey, err := strconv.ParseInt(key[1:], 10, 0)
	if err != nil {
		return -1
	}
	return ikey
}

func IntToVal(val int64) []byte {
	buf := make([]byte, 1+binary.MaxVarintLen64)
	buf[0] = 'i'
	_ = binary.PutVarint(buf[1:], val)
	return buf
}

func ValToInt(val []byte) int64 {
	if len(val) != 11 || val[0] != 'i' {
		return -1
	}
	ival, n := binary.Varint(val[1:])
	if ival == 0 && n <= 0 {
		return -1
	}
	return ival
}

// HasInt tests and returns a boolean value if the
// provided key exists in the tree
func (t *rbTree) HasInt(key int64) bool {
	_, ok := t.getInternal(IntToKey(key))
	return ok
}

// AddInt adds the provided key and value only if it does not
// already exist in the tree. It returns false if the key and
// value was not able to be added, and true if it was added
// successfully
func (t *rbTree) AddInt(key int64, value int64) bool {
	_, ok := t.getInternal(IntToKey(key))
	if ok {
		// key already exists, so we are not adding
		return false
	}
	t.putInternal(IntToKey(key), IntToVal(value))
	return true
}

func (t *rbTree) PutInt(key int64, value int64) (int64, bool) {
	val, ok := t.putInternal(IntToKey(key), IntToVal(value))
	return ValToInt(val), ok
}

func (t *rbTree) GetInt(key int64) (int64, bool) {
	val, ok := t.getInternal(IntToKey(key))
	return ValToInt(val), ok
}

func (t *rbTree) DelInt(key int64) (int64, bool) {
	val, ok := t.delInternal(IntToKey(key))
	return ValToInt(val), ok
}

func (t *rbTree) ToList() (*list.List, error) {
	if t.count < 1 {
		return nil, fmt.Errorf("Error: there are not enough entrys in the tree\n")
	}
	li := list.New()
	t.ascend(t.root, t.min(t.root).entry, func(key string, value []byte) bool {
		li.PushBack(rbEntry{key: key, value: value})
		return true
	})
	return li, nil
}

func (t *rbTree) FromList(li *list.List) error {
	for e := li.Front(); e != nil; e = e.Next() {
		ent, ok := e.Value.(rbEntry)
		if !ok {
			return fmt.Errorf("Error: cannot add to tree, element (%T) "+
				"does not implement the rbEntry interface\n", ent.value)
		}
		t.putInternal(ent.key, ent.value)
	}
	return nil
}
