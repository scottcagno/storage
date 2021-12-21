package se

import (
	"fmt"
	"testing"
)

var testingPath = "se-testing/dangfile.txt"

func TestEngine_Open(t *testing.T) {

	e, err := openEngine(testingPath)
	if err != nil {
		t.Error(err)
	}

	r1 := makeRecord(1, []byte("this record will fit in one (64b) page, not two"))
	_, err = e.write(r1)
	if err != nil {
		t.Error(err)
	}
	r2 := makeRecord(2, []byte("this record is a little bit longer than the first one--it should end up occupying two pages but not three"))
	_, err = e.write(r2)
	if err != nil {
		t.Error(err)
	}
	r3 := makeRecord(3, []byte("and this record is going to be a bit longer than the second one and ideally we should aim to fill three pages, but not four. this one should suffice."))
	_, err = e.write(r3)
	if err != nil {
		t.Error(err)
	}

	err = e.close()
	if err != nil {
		t.Error(err)
	}

	e, err = openEngine(testingPath)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("engine.index=%d\n", e.index)
	err = e.close()
	if err != nil {
		t.Error(err)
	}
}
