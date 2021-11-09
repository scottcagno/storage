package bits

import (
	"fmt"
	"math"
	"testing"
)

const size = 1000000 // 1 million

func AssertExpected(t *testing.T, expected, got interface{}) {
	if expected != got { //|| !reflect.DeepEqual(expected, got) {
		t.Errorf("error, expected: %v, got: %v\n", expected, got)
	}
}

func TestBitSet_IsSet(t *testing.T) {
	bs := NewBitSet(16)
	bs.Set(2).Set(4).Set(6)
	AssertExpected(t, false, bs.IsSet(1))
	AssertExpected(t, true, bs.IsSet(2))
	AssertExpected(t, false, bs.IsSet(3))
	AssertExpected(t, true, bs.IsSet(4))
	AssertExpected(t, false, bs.IsSet(5))
	AssertExpected(t, true, bs.IsSet(6))
	AssertExpected(t, false, bs.IsSet(128))
	bs = nil
}

func TestBitSet_Len(t *testing.T) {
	bs := NewBitSet(16)
	AssertExpected(t, uint(16), bs.Len())
	bs = nil
}

func TestBitSet_Set(t *testing.T) {
	bs := NewBitSet(16)
	bs.Set(2).Set(4).Set(6)
	AssertExpected(t, false, bs.IsSet(1))
	AssertExpected(t, true, bs.IsSet(2))
	AssertExpected(t, false, bs.IsSet(3))
	AssertExpected(t, true, bs.IsSet(4))
	AssertExpected(t, false, bs.IsSet(5))
	AssertExpected(t, true, bs.IsSet(6))
	bs = nil
}

func TestBitSet_String(t *testing.T) {
	bs := NewBitSet(16)
	bs.Set(2).Set(4).Set(6)
	str := bs.String()
	AssertExpected(t, true, str != "")
	fmt.Println(str)
	bs = nil
}

func TestBitSet_Unset(t *testing.T) {
	bs := NewBitSet(16)
	bs.Set(2).Set(4).Set(6)
	AssertExpected(t, false, bs.IsSet(1))
	AssertExpected(t, true, bs.IsSet(2))
	AssertExpected(t, false, bs.IsSet(3))
	AssertExpected(t, true, bs.IsSet(4))
	AssertExpected(t, false, bs.IsSet(5))
	AssertExpected(t, true, bs.IsSet(6))
	bs.Unset(2).Unset(4).Unset(6)
	AssertExpected(t, false, bs.IsSet(1))
	AssertExpected(t, false, bs.IsSet(2))
	AssertExpected(t, false, bs.IsSet(3))
	AssertExpected(t, false, bs.IsSet(4))
	AssertExpected(t, false, bs.IsSet(5))
	AssertExpected(t, false, bs.IsSet(6))
	bs = nil
}

func TestBitSet_Value(t *testing.T) {
	bs := NewBitSet(16)
	bs.Set(1)
	v := bs.Value(1)
	AssertExpected(t, uint(2), v)
	bs = nil
}

func TestBitSet_resize(t *testing.T) {
	bs := NewBitSet(16)
	AssertExpected(t, uint(16), bs.Len())
	bs.resize(32)
	AssertExpected(t, uint(32), bs.Len())
	bs = nil
}

func TestNewBitSet(t *testing.T) {
	bs := NewBitSet(16)
	AssertExpected(t, uint(16), bs.Len())
	bs = nil
}

func Test_alignedSize(t *testing.T) {
	var size uint
	size = alignedSize(62)
	AssertExpected(t, uint(1), size)
	size = alignedSize(96)
	AssertExpected(t, uint(2), size)
}

func TestBitSetTestMany(t *testing.T) {
	bs := NewBitSet(16)
	for i := 0; i < 16; i++ {
		x := uint(i)
		AssertExpected(t, false, bs.IsSet(x))
		bs.Set(x)
		AssertExpected(t, true, bs.IsSet(x))
		v := bs.Value(x)
		AssertExpected(t, uint(1<<x), v)
		bs.Unset(x)
		AssertExpected(t, false, bs.IsSet(x))
	}
	bs = nil
}

func Benchmark_Log2_Version1(b *testing.B) {

	var result uint

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		result = log2Version1(uint(n))
	}

	_ = result
}
func Benchmark_Log2_Version2(b *testing.B) {

	var result uint

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		result = log2Version2(uint(n))
		_ = result
	}

	_ = result

}

func log2Version1(i uint) uint {
	v1 := func(i uint) uint {
		return uint(math.Log2(float64(i)))
	}
	//return v1(i)
	v := v1(i)
	if v < 0 {
		return 0
	}
	return v
}

func log2Version2(i uint) uint {
	v2 := func(i uint) uint {
		var n uint
		for ; i > 0; n++ {
			i >>= 1
		}
		return n - 1
	}
	//return v2(i)
	v := v2(i)
	if v < 0 {
		return 0
	}
	return v
}
