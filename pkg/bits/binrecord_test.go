package bits

import (
	"fmt"
	"testing"
)

func TestBinardRecord_IsSet(t *testing.T) {
	br := NewBinaryRecord(16)
	br.Set(2)
	br.Set(4)
	br.Set(6)
	AssertExpected(t, false, br.IsSet(1))
	AssertExpected(t, true, br.IsSet(2))
	AssertExpected(t, false, br.IsSet(3))
	AssertExpected(t, true, br.IsSet(4))
	AssertExpected(t, false, br.IsSet(5))
	AssertExpected(t, true, br.IsSet(6))
	AssertExpected(t, false, br.IsSet(128))
	br = nil
}

func TestBinardRecord_Len(t *testing.T) {
	br := NewBinaryRecord(16)
	AssertExpected(t, uint(16), br.Len())
	br = nil
}

func TestBinardRecord_Set(t *testing.T) {
	bs := NewBinaryRecord(16)
	bs.Set(2)
	bs.Set(4)
	bs.Set(6)
	AssertExpected(t, false, bs.IsSet(1))
	AssertExpected(t, true, bs.IsSet(2))
	AssertExpected(t, false, bs.IsSet(3))
	AssertExpected(t, true, bs.IsSet(4))
	AssertExpected(t, false, bs.IsSet(5))
	AssertExpected(t, true, bs.IsSet(6))
	bs = nil
}

func TestBinardRecord_String(t *testing.T) {
	br := NewBinaryRecord(16)
	br.Set(2)
	br.Set(4)
	br.Set(6)
	str := br.String()
	AssertExpected(t, true, str != "")
	fmt.Println(str)
	br = nil
}

func TestBinardRecord_Unset(t *testing.T) {
	br := NewBinaryRecord(16)
	br.Set(2)
	br.Set(4)
	br.Set(6)
	AssertExpected(t, false, br.IsSet(1))
	AssertExpected(t, true, br.IsSet(2))
	AssertExpected(t, false, br.IsSet(3))
	AssertExpected(t, true, br.IsSet(4))
	AssertExpected(t, false, br.IsSet(5))
	AssertExpected(t, true, br.IsSet(6))
	br.Unset(2)
	br.Unset(4)
	br.Unset(6)
	AssertExpected(t, false, br.IsSet(1))
	AssertExpected(t, false, br.IsSet(2))
	AssertExpected(t, false, br.IsSet(3))
	AssertExpected(t, false, br.IsSet(4))
	AssertExpected(t, false, br.IsSet(5))
	AssertExpected(t, false, br.IsSet(6))
	br = nil
}

func TestBinardRecord_Value(t *testing.T) {
	br := NewBinaryRecord(16)
	br.Set(1)
	v := br.Get(1)
	AssertExpected(t, uint(2), v)
	br = nil
}

func TestBinardRecord_resize(t *testing.T) {
	br := NewBinaryRecord(16)
	AssertExpected(t, uint(16), br.Len())
	br.Resize(32)
	AssertExpected(t, uint(32), br.Len())
	br = nil
}

func TestNewBinardRecord(t *testing.T) {
	br := NewBinaryRecord(16)
	AssertExpected(t, uint(16), br.Len())
	br = nil
}

func TestBinardRecordTestMany(t *testing.T) {
	br := NewBinaryRecord(16)
	for i := 0; i < 16; i++ {
		x := uint(i)
		AssertExpected(t, false, br.IsSet(x))
		br.Set(x)
		AssertExpected(t, true, br.IsSet(x))
		v := br.Get(x)
		AssertExpected(t, uint(1<<x), v)
		br.Unset(x)
		AssertExpected(t, false, br.IsSet(x))
	}
	br = nil
}
