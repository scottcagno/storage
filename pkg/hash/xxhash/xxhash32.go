/*
 * // Copyright (c) 2021. Scott Cagno. All rights reserved.
 * // The license can be found in the root of this project; see LICENSE.
 */

package xxhash

import "hash"

const (
	prime32_1 = 2654435761
	prime32_2 = 2246822519
	prime32_3 = 3266489917
	prime32_4 = 668265263
	prime32_5 = 374761393
)

type xxHash32 struct {
	seed     uint32
	v1       uint32
	v2       uint32
	v3       uint32
	v4       uint32
	totalLen uint64
	buf      [16]byte
	bufused  int
}

// New returns a new Hash32 instance.
func NewHash32(seed uint32) hash.Hash32 {
	xxh := &xxHash32{seed: seed}
	xxh.Reset()
	return xxh
}

func Sum32(b []byte) uint32 {
	h := NewHash32(0xCAFE)
	h.Write(b)
	return h.Sum32()
}

// Sum appends the current hash to b and returns the resulting slice.
// It does not change the underlying hash state.
func (xxh xxHash32) Sum(b []byte) []byte {
	h32 := xxh.Sum32()
	return append(b, byte(h32), byte(h32>>8), byte(h32>>16), byte(h32>>24))
}

// Reset resets the Hash to its initial state.
func (xxh *xxHash32) Reset() {
	xxh.v1 = xxh.seed + prime32_1 + prime32_2
	xxh.v2 = xxh.seed + prime32_2
	xxh.v3 = xxh.seed
	xxh.v4 = xxh.seed - prime32_1
	xxh.totalLen = 0
	xxh.bufused = 0
}

// Size returns the number of bytes returned by Sum().
func (xxh *xxHash32) Size() int {
	return 4
}

// BlockSize gives the minimum number of bytes accepted by Write().
func (xxh *xxHash32) BlockSize() int {
	return 1
}

// Write adds input bytes to the Hash.
// It never returns an error.
func (xxh *xxHash32) Write(input []byte) (int, error) {
	n := len(input)
	m := xxh.bufused

	xxh.totalLen += uint64(n)

	r := len(xxh.buf) - m
	if n < r {
		copy(xxh.buf[m:], input)
		xxh.bufused += len(input)
		return n, nil
	}

	p := 0
	if m > 0 {
		// some data left from previous update
		copy(xxh.buf[xxh.bufused:], input[:r])
		xxh.bufused += len(input) - r

		// fast rotl(13)
		xxh.v1 = u32_rol13(xxh.v1+u32_u32(xxh.buf[:])*prime32_2) * prime32_1
		xxh.v2 = u32_rol13(xxh.v2+u32_u32(xxh.buf[4:])*prime32_2) * prime32_1
		xxh.v3 = u32_rol13(xxh.v3+u32_u32(xxh.buf[8:])*prime32_2) * prime32_1
		xxh.v4 = u32_rol13(xxh.v4+u32_u32(xxh.buf[12:])*prime32_2) * prime32_1
		p = r
		xxh.bufused = 0
	}

	// Causes compiler to work directly from registers instead of stack:
	v1, v2, v3, v4 := xxh.v1, xxh.v2, xxh.v3, xxh.v4
	for n := n - 16; p <= n; p += 16 {
		sub := input[p:][:16] //BCE hint for compiler
		v1 = u32_rol13(v1+u32_u32(sub[:])*prime32_2) * prime32_1
		v2 = u32_rol13(v2+u32_u32(sub[4:])*prime32_2) * prime32_1
		v3 = u32_rol13(v3+u32_u32(sub[8:])*prime32_2) * prime32_1
		v4 = u32_rol13(v4+u32_u32(sub[12:])*prime32_2) * prime32_1
	}
	xxh.v1, xxh.v2, xxh.v3, xxh.v4 = v1, v2, v3, v4

	copy(xxh.buf[xxh.bufused:], input[p:])
	xxh.bufused += len(input) - p

	return n, nil
}

// Sum32 returns the 32 bits Hash value.
func (xxh *xxHash32) Sum32() uint32 {
	h32 := uint32(xxh.totalLen)
	if xxh.totalLen >= 16 {
		h32 += u32_rol1(xxh.v1) + u32_rol7(xxh.v2) + u32_rol12(xxh.v3) + u32_rol18(xxh.v4)
	} else {
		h32 += xxh.seed + prime32_5
	}

	p := 0
	n := xxh.bufused
	for n := n - 4; p <= n; p += 4 {
		h32 += u32_u32(xxh.buf[p:p+4]) * prime32_3
		h32 = u32_rol17(h32) * prime32_4
	}
	for ; p < n; p++ {
		h32 += uint32(xxh.buf[p]) * prime32_5
		h32 = u32_rol11(h32) * prime32_1
	}

	h32 ^= h32 >> 15
	h32 *= prime32_2
	h32 ^= h32 >> 13
	h32 *= prime32_3
	h32 ^= h32 >> 16

	return h32
}

// Checksum returns the 32bits Hash value.
func Checksum32(input []byte, seed uint32) uint32 {
	n := len(input)
	h32 := uint32(n)

	if n < 16 {
		h32 += seed + prime32_5
	} else {
		v1 := seed + prime32_1 + prime32_2
		v2 := seed + prime32_2
		v3 := seed
		v4 := seed - prime32_1
		p := 0
		for n := n - 16; p <= n; p += 16 {
			sub := input[p:][:16] //BCE hint for compiler
			v1 = u32_rol13(v1+u32_u32(sub[:])*prime32_2) * prime32_1
			v2 = u32_rol13(v2+u32_u32(sub[4:])*prime32_2) * prime32_1
			v3 = u32_rol13(v3+u32_u32(sub[8:])*prime32_2) * prime32_1
			v4 = u32_rol13(v4+u32_u32(sub[12:])*prime32_2) * prime32_1
		}
		input = input[p:]
		n -= p
		h32 += u32_rol1(v1) + u32_rol7(v2) + u32_rol12(v3) + u32_rol18(v4)
	}

	p := 0
	for n := n - 4; p <= n; p += 4 {
		h32 += u32_u32(input[p:p+4]) * prime32_3
		h32 = u32_rol17(h32) * prime32_4
	}
	for p < n {
		h32 += uint32(input[p]) * prime32_5
		h32 = u32_rol11(h32) * prime32_1
		p++
	}

	h32 ^= h32 >> 15
	h32 *= prime32_2
	h32 ^= h32 >> 13
	h32 *= prime32_3
	h32 ^= h32 >> 16

	return h32
}

func u32_u32(buf []byte) uint32 {
	// go compiler recognizes this pattern and optimizes it on little endian platforms
	return uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24
}

func u32_rol1(u uint32) uint32 {
	return u<<1 | u>>31
}

func u32_rol7(u uint32) uint32 {
	return u<<7 | u>>25
}

func u32_rol11(u uint32) uint32 {
	return u<<11 | u>>21
}

func u32_rol12(u uint32) uint32 {
	return u<<12 | u>>20
}

func u32_rol13(u uint32) uint32 {
	return u<<13 | u>>19
}

func u32_rol17(u uint32) uint32 {
	return u<<17 | u>>15
}

func u32_rol18(u uint32) uint32 {
	return u<<18 | u>>14
}
