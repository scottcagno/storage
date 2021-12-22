package v2

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"unsafe"
)

const (
	blockSize  = 64
	headerSize = int(unsafe.Sizeof(header{}))

	uint16SZ = 2
	uint32SZ = 4
	uint64SZ = 8
)

type header struct {
	kind  uint16
	crc32 uint32
	size  uint64
}

func writeHeader(buf []byte, hdr *header) (int, error) {
	if len(buf) < headerSize {
		return 0, io.ErrShortWrite
	}
	var n int
	binary.LittleEndian.PutUint16(buf[n:n+uint16SZ], hdr.kind)
	n += uint16SZ
	binary.LittleEndian.PutUint32(buf[n:n+uint32SZ], hdr.crc32)
	n += uint32SZ
	binary.LittleEndian.PutUint64(buf[n:n+uint64SZ], hdr.size)
	n += uint64SZ
	return n, nil
}

type Writer struct {
	buf []byte
	n   int
	w   io.Writer
}

func NewWriter(w io.Writer, size int) *Writer {
	if size%blockSize != 0 {
		size = (size + blockSize - 1) &^ (blockSize - 1)
	}
	return &Writer{
		buf: make([]byte, size),
		n:   0,
		w:   w,
	}
}

var ErrDataOverflow = errors.New("data overflow--too big")

func (w *Writer) undoWrite(n int) {
	if n > 0 && n < w.n {
		copy(w.buf[0:w.n-n], w.buf[n:w.n])
	}
	w.n -= n
}

func (w *Writer) Write(p []byte) (int, error) {
	// error checking
	if len(p)+headerSize > len(w.buf[w.n:]) {
		return 0, ErrDataOverflow
	}
	var nn int

	// write header
	n, err := writeHeader(w.buf[w.n:], &header{
		kind:  0xff,
		crc32: crc32.ChecksumIEEE(p),
		size:  uint64(len(p)),
	})
	if err != nil {
		w.undoWrite(n)
		return n, err
	}

	// update offsets
	w.n += n
	nn += n

	// write data to buffer
	n = copy(w.buf[w.n:], p)
	w.n += n
	nn += n

	// take up some slack
	if w.n%blockSize != 0 {
		w.n = (w.n + blockSize - 1) &^ (blockSize - 1)
	}

	// persist
	x, err := w.w.Write(w.buf[:w.n])
	if err != nil {
		w.undoWrite(x)
		return x, err
	}

	// reset buffer
	w.n = 0
	return x, nil
}
