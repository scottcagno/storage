package util

import (
	"hash/crc32"
	"hash/crc64"
)

const defaultCRC32Poly = 0xeb31d82e // Koopman's polynomial

var defaultCRC32Table = crc32.MakeTable(defaultCRC32Poly)

func ChecksumCRC32(data []byte) uint32 {
	return crc32.Checksum(data, crc32.MakeTable(crc32.Koopman))
}

func ChecksumCRC64(data []byte) uint64 {
	return crc64.Checksum(data, crc64.MakeTable(crc64.ECMA))
}
