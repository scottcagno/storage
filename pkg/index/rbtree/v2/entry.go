package v2

import "fmt"

type entry struct {
	key   string
	value []byte
}

func (e entry) String() string {
	return fmt.Sprintf("entry.key=%q, entry.value=%q\n", e.key, e.value)
}

var empty = *new(entry)

func isempty(e entry) bool {
	return e.key == ""
}

func compare(this, that entry) int {
	if len(this.key) < len(that.key) {
		return -1
	}
	if len(this.key) > len(that.key) {
		return +1
	}
	if this.key < that.key {
		return -1
	}
	if this.key > that.key {
		return 1
	}
	return 0
}
