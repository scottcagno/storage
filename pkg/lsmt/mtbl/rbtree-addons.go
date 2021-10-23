package mtbl

import (
	"container/list"
	binaryStd "encoding/binary"
	"github.com/scottcagno/storage/pkg/lsmt/binary"

	"fmt"
	"strconv"
)

func IntToString(key int64) string {
	return "i" + strconv.FormatInt(key, 10)
}

func StringToInt(key string) int64 {
	if len(key) != 11 || key[0] != 'i' {
		return -1
	}
	ikey, err := strconv.ParseInt(key[1:], 10, 0)
	if err != nil {
		return -1
	}
	return ikey
}

func IntToBytes(val int64) []byte {
	buf := make([]byte, 1+binaryStd.MaxVarintLen64)
	buf[0] = 'i'
	_ = binaryStd.PutVarint(buf[1:], val)
	return buf
}

func BytesToInt(val []byte) int64 {
	if len(val) != 11 || val[0] != 'i' {
		return -1
	}
	ival, n := binaryStd.Varint(val[1:])
	if ival == 0 && n <= 0 {
		return -1
	}
	return ival
}

func (t *rbTree) ToList() (*list.List, error) {
	if t.count < 1 {
		return nil, fmt.Errorf("Error: there are not enough entrys in the tree\n")
	}
	li := list.New()
	t.ascend(t.root, t.min(t.root).entry, func(e *binary.Entry) bool {
		li.PushBack(e)
		return true
	})
	return li, nil
}

func (t *rbTree) FromList(li *list.List) error {
	for e := li.Front(); e != nil; e = e.Next() {
		ent, ok := e.Value.(*binary.Entry)
		if !ok {
			return fmt.Errorf("Error: cannot add to tree, element (%T) "+
				"does not implement the RBEntry interface\n", ent)
		}
		t.putInternal(ent)
	}
	return nil
}
