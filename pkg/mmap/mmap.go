package mmap

import (
	"github.com/scottcagno/storage/pkg/mmap/segment"
	"io"
	"math"
)

// MaxInt is the maximum platform dependent signed integer.
const MaxInt = int(^uint(0) >> 1)

// Mode is a mapping mode.
type Mode int

const (
	// Share this mapping and allow the read-only access
	ModeReadOnly Mode = iota

	// Share this mapping
	// Updates to the mapping are visible to other processes
	// mapping the same region, and are carried through to the underlying file.
	// To precisely control when updates are carried through to the underlying file
	// requires the use of Mapping.Sync.
	ModeReadWrite

	// Create a private copy-on-write mapping.
	// Updates to the mapping are not visible to other processes
	// mapping the same region, and are not carried through to the underlying file.
	// It is unspecified whether changes made to the file are visible in the mapped region
	ModeWriteCopy
)

// Flag is a mapping flag
type Flag int

const (
	// Mapped memory pages may be executed
	FlagExecutable Flag = 1 << iota
)

// Mapping contains the cross-platform parts of a mapping.
type mapping struct {
	// writable specifies whether the mapped memory pages may be written.
	writable bool
	// executable specifies whether the mapped memory pages may be executed.
	executable bool
	// address specifies the pointer to the mapped memory.
	address uintptr
	// memory specifies the byte slice which wraps the mapped memory.
	memory []byte
	// off is the current offset
	off int64
	// segment specifies the lazily initialized data segment on top of the mapped memory.
	segment *segment.Segment
}

// Writable returns true if the mapped memory pages may be written.
func (m *mapping) Writable() bool {
	return m.writable
}

// Executable returns true if the mapped memory pages may be executed.
func (m *mapping) Executable() bool {
	return m.executable
}

// Address returns the pointer to the mapped memory.
func (m *mapping) Address() uintptr {
	return m.address
}

// Length returns the mapped memory length in bytes.
func (m *mapping) Length() uintptr {
	return uintptr(len(m.memory))
}

// Memory returns the byte slice which wraps the mapped memory.
func (m *mapping) Memory() []byte {
	return m.memory
}

// Segment returns the data segment on top of the mapped memory.
func (m *mapping) Segment() *segment.Segment {
	if m.segment == nil {
		m.segment = segment.New(0, m.memory)
	}
	return m.segment
}

// access checks given offset and length to match the available bounds
// and returns ErrOutOfBounds error at the access violation.
func (m *mapping) access(offset int64, length int) error {
	if offset < 0 || offset > math.MaxInt64-int64(length) || offset+int64(length) > int64(len(m.memory)) {
		return ErrOutOfBounds
	}
	return nil
}

// Read reads len(buf) bytes from the internal offset from the mapped memory.
// If the offset is out of the available bounds or there are not enough bytes to read
// the ErrOutOfBounds error will be returned. Otherwise len(buf) will be returned
// with no errors. Read implements the io.Reader interface.
func (m *mapping) Read(buf []byte) (int, error) {
	if m.memory == nil {
		return 0, ErrClosed
	}
	if err := m.access(m.off, len(buf)); err != nil {
		return 0, err
	}
	n := copy(buf, m.memory[m.off:])
	m.off += int64(n)
	return n, nil
}

// ReadAt reads len(buf) bytes at the given offset from start of the mapped memory from the mapped memory.
// If the given offset is out of the available bounds or there are not enough bytes to read
// the ErrOutOfBounds error will be returned. Otherwise len(buf) will be returned with no errors.
// ReadAt implements the io.ReaderAt interface.
func (m *mapping) ReadAt(buf []byte, offset int64) (int, error) {
	if m.memory == nil {
		return 0, ErrClosed
	}
	if err := m.access(offset, len(buf)); err != nil {
		return 0, err
	}
	return copy(buf, m.memory[offset:]), nil
}

// Write writes len(buf) bytes from the internal offset into the mapped memory.
// If the offset is out of the available bounds or there are not enough space to write all given bytes
// the ErrOutOfBounds error will be returned. Otherwise len(buf) will be returned with no errors.
// Write implements the io.Writer interface.
func (m *mapping) Write(buf []byte) (int, error) {
	if m.memory == nil {
		return 0, ErrClosed
	}
	if !m.writable {
		return 0, ErrReadOnly
	}
	if err := m.access(m.off, len(buf)); err != nil {
		return 0, err
	}
	n := copy(m.memory[m.off:], buf)
	m.off += int64(n)
	return n, nil
}

// WriteAt writes len(buf) bytes at the given offset from start of the mapped memory into the mapped memory.
// If the given offset is out of the available bounds or there are not enough space to write all given bytes
// the ErrOutOfBounds error will be returned. Otherwise len(buf) will be returned with no errors.
// WriteAt implements the io.WriterAt interface.
func (m *mapping) WriteAt(buf []byte, offset int64) (int, error) {
	if m.memory == nil {
		return 0, ErrClosed
	}
	if !m.writable {
		return 0, ErrReadOnly
	}
	if err := m.access(offset, len(buf)); err != nil {
		return 0, err
	}
	return copy(m.memory[offset:], buf), nil
}

// Seek sets the offset for the next Read or Write to offset, interpreted according to whence: SeekStart means
// relative to the start of the file, SeekCurrent means relative to the current offset, and SeekEnd means relative
// to the end. Seek returns the new offset relative to the start of the file and an error, if any.
// Seeking to an offset before the start of the file is an error. Seeking to any positive offset is legal, but the
// behavior of subsequent I/O operations on the underlying object is implementation-dependent.
func (m *mapping) Seek(offset int64, whence int) (int64, error) {
	if m.memory == nil {
		return 0, ErrClosed
	}
	if err := m.access(offset, 0); err != nil {
		return 0, err
	}
	switch whence {
	default:
		return 0, ErrSeekWhence
	case io.SeekStart:
		offset += 0
	case io.SeekCurrent:
		offset += m.off
	case io.SeekEnd:
		offset += int64(len(m.memory))
	}
	if offset < 0 {
		return 0, ErrSeekOffset
	}
	m.off = offset
	return offset - 0, nil
}
