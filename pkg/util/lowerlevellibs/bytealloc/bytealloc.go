package bytealloc

import "github.com/scottcagno/storage/pkg/util/lowerlevellibs/bytealloc/rawalloc"

// An A provides chunk allocation of []byte, amortizing the overhead of each
// allocation. Because the underlying storage for the slices is shared, they
// should share a similar lifetime in order to avoid pinning large amounts of
// memory unnecessarily. The allocator itself is a []byte where cap() indicates
// the total amount of memory and len() is the amount already allocated. The
// size of the buffer to allocate from is grown exponentially when it runs out
// of room up to a maximum size (chunkAllocMaxSize).
type A []byte

const chunkAllocMinSize = 512
const chunkAllocMaxSize = 16384

func (a A) reserve(n int) A {
	allocSize := cap(a) * 2
	if allocSize < chunkAllocMinSize {
		allocSize = chunkAllocMinSize
	} else if allocSize > chunkAllocMaxSize {
		allocSize = chunkAllocMaxSize
	}
	if allocSize < n {
		allocSize = n
	}
	return rawalloc.New(0, allocSize)
}

// Alloc allocates a new chunk of memory with the specified length.
func (a A) Alloc(n int) (A, []byte) {
	if cap(a)-len(a) < n {
		a = a.reserve(n)
	}
	p := len(a)
	r := a[p : p+n : p+n]
	a = a[:p+n]
	return a, r
}

// Copy allocates a new chunk of memory, initializing it from src.
func (a A) Copy(src []byte) (A, []byte) {
	var alloc []byte
	a, alloc = a.Alloc(len(src))
	copy(alloc, src)
	return a, alloc
}
