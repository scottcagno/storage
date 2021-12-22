package bw

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

const (
	defaultBufSize = 512
	headerSize     = 16

	pageSize = 64
	pageMask = pageSize - headerSize

	kindGeneric = 0x01
)

// Writer implements buffering for an io.Writer object.
// If an error occurs writing to a Writer, no more data will be
// accepted and all subsequent writes, and Flush, will return the error.
// After all data has been written, the client should call the
// Flush method to guarantee all data has been forwarded to
// the underlying io.Writer.
type Writer struct {
	err   error
	buf   []byte
	n     int
	wr    io.Writer
	align int
}

// NewWriterSize returns a new Writer whose buffer has at least the specified
// size. If the argument io.Writer is already a Writer with large enough
// size, it returns the underlying Writer.
func NewWriterSize(w io.Writer, size int, align int) *Writer {
	if size <= 0 {
		size = defaultBufSize
	}
	return &Writer{
		buf:   make([]byte, size),
		wr:    w,
		align: align,
	}
}

// NewWriter returns a new Writer whose buffer has the default size.
func NewWriter(w io.Writer) *Writer {
	return NewWriterSize(w, defaultBufSize, 0)
}

// Size returns the size of the underlying buffer in bytes.
func (b *Writer) Size() int { return len(b.buf) }

// Reset discards any unflushed buffered data, clears any error, and
// resets b to write its output to w.
func (b *Writer) Reset(w io.Writer) {
	b.err = nil
	b.n = 0
	b.wr = w
}

// Flush writes any buffered data to the underlying io.Writer.
func (b *Writer) Flush() error {
	if b.err != nil {
		return b.err
	}
	if b.n == 0 {
		return nil
	}
	n, err := b.wr.Write(b.buf[0:b.n])
	if n < b.n && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if n > 0 && n < b.n {
			copy(b.buf[0:b.n-n], b.buf[n:b.n])
		}
		b.n -= n
		b.err = err
		return err
	}
	b.n = 0
	return nil
}

func (b *Writer) Available() int {
	return len(b.buf) - b.n
}

func (b *Writer) Buffered() int {
	return b.n
}

type header struct {
	kind  uint16
	crc32 uint32
	size  uint64
}

func (b *Writer) WriteHeader(buf []byte, hdr *header) (int, error) {
	if len(buf) < headerSize {
		return 0, io.ErrShortWrite
	}
	var n int
	binary.LittleEndian.PutUint16(buf[n:n+2], hdr.kind)
	n += 2
	binary.LittleEndian.PutUint32(buf[n:n+4], hdr.crc32)
	n += 4
	binary.LittleEndian.PutUint64(buf[n:n+8], hdr.size)
	n += 8
	return n, nil
}

func (b *Writer) Write(p []byte) (int, error) {
	nn := 0
	n, err := b.WriteHeader(b.buf[b.n:b.n+headerSize], &header{
		kind:  0xff,
		crc32: crc32.ChecksumIEEE(p),
		size:  uint64(len(p)),
	})
	if err != nil {
		return nn, err
	}
	b.n += n
	nn += n
	for len(p)+headerSize > b.Available() && b.err == nil {
		n = copy(b.buf[b.n:], p)
		b.n += n
		nn += n
		p = p[n:]
		b.Flush()
	}
	if b.err != nil {
		return nn, b.err
	}
	n = copy(b.buf[b.n:], p)
	b.n += n
	nn += n

	if b.align != 0 && b.n%b.align != 0 {
		b.n = (b.n + b.align - 1) &^ (b.align - 1)
	}
	return b.n / b.align, nil
}

func (b *Writer) WriteV2(p []byte) (int, error) {
	nn := 0
	for len(p) > b.Available() && b.err == nil {
		n := copy(b.buf[b.n:], p)
		b.n += n
		nn += n
		p = p[n:]
		b.Flush()
	}
	if b.err != nil {
		return nn, b.err
	}
	n := copy(b.buf[b.n:], p)
	b.n += n
	nn += n
	return nn, nil
}
