package bitset

import (
	"fmt"
	"strconv"
)

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

func RawBytesSetBit(bs *[]byte, i uint) {
	checkResize(bs, i)
	//_ = (*bs)[i>>3]
	(*bs)[i>>3] |= 1 << (i & (7))
}

func RawBytesUnsetBit(bs *[]byte, i uint) {
	checkResize(bs, i)
	//_ = (*bs)[i>>3]
	(*bs)[i>>3] &^= 1 << (i & (7))
}

func RawBytesHasBit(bs *[]byte, i uint) bool {
	checkResize(bs, i)
	//_ = (*bs)[i>>3]
	return (*bs)[i>>3]&(1<<(i&(7))) != 0
}

func RawBytesGetBit(bs *[]byte, i uint) uint {
	checkResize(bs, i)
	//_ = (*bs)[i>>3]
	return uint((*bs)[i>>3] & (1 << (i & (7))))
}

func RawBytesStringer(bs *[]byte) string {
	// print binary value of bitset
	//var res string = "16" // set this to the "bit resolution" you'd like to see
	var res = strconv.Itoa(len(*bs))
	return fmt.Sprintf("%."+res+"b (%s bits)", bs, res)

}
