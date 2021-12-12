package rawalloc

// New returns a new byte slice of the specified length and capacity where the
// backing memory is uninitialized. This differs from make([]byte) which
// guarantees that the backing memory for the slice is initialized to zero. Use
// carefully.
func New(len, cap int) []byte {
	ptr := mallocgc(uintptr(cap), nil, false)
	return (*[maxArrayLen]byte)(ptr)[:len:cap]
}
