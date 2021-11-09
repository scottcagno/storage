package bits

import (
	"fmt"
	"strconv"
	"unsafe"
)

type sliceType = byte

var (
	wordSize     uint = uint(unsafe.Sizeof(sliceType(0)))
	log2WordSize uint = log2(wordSize)
)

func log2(i uint) uint {
	var n uint
	for ; i > 0; n++ {
		i >>= 1
	}
	return n - 1
}

func roundTo(value uint, roundTo uint) uint {
	return (value + (roundTo - 1)) &^ (roundTo - 1)
}

func checkResize(bs *[]byte, i uint) {
	if *bs == nil {
		*bs = make([]byte, 8)
		return
	}
	if i > uint(len(*bs)*8) {
		newbs := make([]byte, roundTo(i, 8))
		copy(newbs, *bs)
		*bs = newbs
	}
	return
}

func RawBytesHasBit(bs *[]byte, i uint) bool {
	checkResize(bs, i)
	//_ = (*bs)[i>>3]
	//
	// b.bits[i>>lg2ws]&(1<<(i&(ws-1))) != 0
	return (*bs)[i>>3]&(1<<(i&(7))) != 0
}

func RawBytesSetBit(bs *[]byte, i uint) {
	checkResize(bs, i)
	//_ = (*bs)[i>>3]
	//
	// b.bits[i>>lg2ws] |= 1 << (i & (ws - 1))
	(*bs)[i>>3] |= 1 << (i & (7))
}

func RawBytesGetBit(bs *[]byte, i uint) uint {
	checkResize(bs, i)
	//_ = (*bs)[i>>3]
	//
	// b.bits[i>>lg2ws] & (1 << (i & (ws - 1)))
	return uint((*bs)[i>>3] & (1 << (i & (7))))
}

func RawBytesUnsetBit(bs *[]byte, i uint) {
	checkResize(bs, i)
	//_ = (*bs)[i>>3]
	//
	// b.bits[i>>lg2ws] &^= 1 << (i & (ws - 1))
	(*bs)[i>>3] &^= 1 << (i & (7))
}

func RawBytesStringer(bs *[]byte) string {
	// print binary value of bitset
	//var res string = "16" // set this to the "bit resolution" you'd like to see
	var res = strconv.Itoa(len(*bs))
	return fmt.Sprintf("%."+res+"b (%s bits)", bs, res)

}
