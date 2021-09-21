package binary

import "fmt"

// DataEntry is a key-value data entry
type DataEntry struct {
	Id    int64
	Key   []byte
	Value []byte
}

// String is the stringer method for a *DataEntry
func (de *DataEntry) String() string {
	return fmt.Sprintf("entry.id=%d, entry.key=%q, entry.value=%q", de.Id, de.Key, de.Value)
}
