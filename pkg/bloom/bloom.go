/*
 *
 *  * // Copyright (c) 2021 Scott Cagno. All rights reserved.
 *  * // The license can be found in the root of this project; see LICENSE.
 *
 */

package bloom

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/bitset"
	"github.com/scottcagno/storage/pkg/hash/cityhash"
)

const (
	kp00 = uint64(1610612741)
	kp01 = uint64(402653189)
	kp02 = uint64(805306457)
	kp03 = uint64(201326611)
	kp04 = uint64(1728949133)
	kp05 = uint64(8543917829)
	kp06 = uint64(648679351)
	kp07 = uint64(9196230203)
)

// n|N = number of items in the filter
// p|P = probability of false positives
// m|M = total number of bits in the filter, ie. size
// k|K = number of hash functions

// BloomFilter is a basic bloom filter implementation
type BloomFilter struct {
	m    uint // m is the number of bits allocated for the filter
	k    uint // k is the number of hash functions for the filter
	n    uint // n is the number of items "in" the filter
	b    *bitset.BitSet
	mask uint64
}

// NewBloomFilter returns a new filter with m number of bits available and hints to use k hash functions
func NewBloomFilter(n uint) *BloomFilter {
	// using k=8 and maintaining a bitset m=n*24 provides a fairly
	// constant p=0.00004 (1 in 25,000) false positive ratio which
	// is probably acceptable in almost all cases I can think of
	return &BloomFilter{
		m:    n * 24,
		k:    8,
		b:    bitset.NewBitSet(n),
		mask: uint64(n - 1),
	}
}

// -> n = ceil(m / (-k / log(1 - exp(log(p) / k))))
// -> p = pow(1 - exp(-k / (m / n)), k)
// -> m = ceil((n * log(p)) / log(1 / pow(2, log(2))))
// -> k = round((m / n) * log(2))

func hashes(data []byte) [8]uint64 {
	h1, h2 := cityhash.Hash128WithSeed(data, kp00, kp01)
	h3, h4 := cityhash.Hash128WithSeed(data, kp02, kp03)
	h5, h6 := cityhash.Hash128WithSeed(data, kp04, kp05)
	h7, h8 := cityhash.Hash128WithSeed(data, kp06, kp07)
	return [8]uint64{h1, h2, h3, h4, h5, h6, h7, h8}
}

// mask returns the ith hashed location using the eight base hash values
func hashAndMask(h [8]uint64, i uint) uint64 {
	ii := uint64(i)
	return h[ii&1] + ii*h[2+(((ii+(ii&1))&7)>>1)]
}

// location returns the ith hashed location using the four base hash values
func (f *BloomFilter) hashAndMask(h [8]uint64, i uint) uint {
	return uint(hashAndMask(h, i) % uint64(f.m))
}

func info(data []byte, hashes [8]uint64, hashAndMask [8]uint) {
	fmt.Printf("data: %q\n", data)
	fmt.Printf("hashes:\n\t%v\n", hashes)
	fmt.Printf("set locations:\n")
	for i := 0; i < 8; i++ {
		fmt.Printf("\ti=%d, f.k=%d, l1=%d\n", i, 8, hashAndMask[i])
	}
}

// location returns the ith hashed location using the four base hash values
func location(h [8]uint64, i uint) uint64 {
	ii := uint64(i)
	return h[ii%2] + ii*h[2+(((ii+(ii%2))%8)/2)]
}

// location returns the ith hashed location using the four base hash values
func (f *BloomFilter) location(h [8]uint64, i uint) uint {
	return uint(location(h, i) % uint64(f.m))
}

func (f *BloomFilter) Set(data []byte) {
	h := hashes(data)
	for i := uint(0); i < f.k; i++ {
		f.b.Set(f.hashAndMask(h, i))
	}
}

// Has returns true if the data is in the BloomFilter, false otherwise.
// If true, the result might be a false positive. If false, the data
// is definitely not in the set.
func (f *BloomFilter) Has(data []byte) bool {
	h := hashes(data)
	for i := uint(0); i < f.k; i++ {
		if !f.b.IsSet(f.hashAndMask(h, i)) {
			return false
		}
	}
	return true
}

func split2(x uint64) []uint32 {
	return []uint32{
		uint32(x >> 0),
		uint32(x >> 32),
	}
}

func join2(a, b uint32) uint64 {
	return uint64(a) | uint64(b)<<32
}

func split4(x uint64) []uint16 {
	return []uint16{
		uint16(x >> 0),
		uint16(x >> 16),
		uint16(x >> 32),
		uint16(x >> 48),
	}
}

func join4(a, b, c, d int64) uint64 {
	return uint64(a) | uint64(b)<<16 | uint64(c)<<32 | uint64(d)<<48
}
