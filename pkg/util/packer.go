package util

func Pack2U32(dst *uint64, src1, src2 uint32) {
	*dst = uint64(src1) | uint64(src2)<<32
}

func Unpack2U32(dst *uint64) (uint32, uint32) {
	return uint32(*dst), uint32(*dst >> 32)
}
