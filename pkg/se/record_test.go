package se

import (
	"fmt"
	"testing"
)

func TestRecord_MakeRecord(t *testing.T) {

	r1 := makeRecord(1, []byte("this record will fit in one (64b) page, not two"))
	raw := r1.raw()
	fmt.Printf("%s\nRAW:%q (len=%d)\n\n", r1, raw, len(raw))

	r2 := makeRecord(2, []byte("this record is a little bit longer than the first one--it should end up occupying two pages but not three"))
	fmt.Printf("%s\n\n", r2)

	r3 := makeRecord(3, []byte("and this record is going to be a bit longer than the second one and ideally we should aim to fill three pages, but not four. this one should suffice."))
	fmt.Printf("%s\n\n", r3)
}
