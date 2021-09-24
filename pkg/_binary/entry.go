package binary

import "fmt"

// Entry is a binary entry
type Entry struct {
	Id    int64
	Key   []byte
	Value []byte
}

// String is the string method for an *Entry
func (e *Entry) String() string {
	return fmt.Sprintf("entry.id=%d, entry.key=%q, entry.value=%q", e.Id, e.Key, e.Value)
}
