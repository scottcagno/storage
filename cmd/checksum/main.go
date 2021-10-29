package main

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
	"hash/crc32"
	"hash/crc64"
	"log"
)

const size = 64

var (
	data08to16 = make([][]byte, size)
	data16to32 = make([][]byte, size)
	data32to64 = make([][]byte, size)
)

func init() {
	log.Println("Filling out random data")
	for i := 0; i < size; i++ {
		data08to16[i] = util.RandBytes(util.RandIntn(8, 16))
		data16to32[i] = util.RandBytes(util.RandIntn(16, 32))
		data32to64[i] = util.RandBytes(util.RandIntn(32, 64))
	}
}

func main() {

	fmt.Println("Comparing 8-16...")
	for i := 0; i < size; i++ {
		s32 := ChecksumCRC32(data08to16[i])
		s64 := ChecksumCRC64(data08to16[i])
		fmt.Printf("crc32: %s -> %d\ncrc64: %s -> %d\n", data08to16[i], s32, data08to16[i], s64)
	}
	fmt.Println()

	fmt.Println("Comparing 16-32...")
	for i := 0; i < size; i++ {
		s32 := ChecksumCRC32(data16to32[i])
		s64 := ChecksumCRC64(data16to32[i])
		fmt.Printf("crc32: %s -> %d\ncrc64: %s -> %d\n", data16to32[i], s32, data16to32[i], s64)
	}
	fmt.Println()

	fmt.Println("Comparing 32-64...")
	for i := 0; i < size; i++ {
		s32 := ChecksumCRC32(data32to64[i])
		s64 := ChecksumCRC64(data32to64[i])
		fmt.Printf("crc32: %s -> %d\ncrc64: %s -> %d\n", data32to64[i], s32, data32to64[i], s64)
	}
	fmt.Println()
}

func ChecksumCRC32(data []byte) uint32 {
	return crc32.Checksum(data, crc32.MakeTable(crc32.Koopman))
}

func ChecksumCRC64(data []byte) uint64 {
	return crc64.Checksum(data, crc64.MakeTable(crc64.ECMA))
}
