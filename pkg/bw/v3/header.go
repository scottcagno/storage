package v3

import (
	"encoding/binary"
	"io"
)

const (
	headerSize   = 16
	magicBytes   = 0xBABE
	kindActive   = 0xA1
	kindInactive = 0x00
)

var buf [headerSize]byte

func clear(b *[headerSize]byte) {
	*b = [headerSize]byte{}
}

type header struct {
	magic uint16
	kind  uint16
	crc32 uint32
	size  uint64
}

func (h *header) WriteTo(w io.Writer) (int64, error) {
	binary.LittleEndian.PutUint16(buf[0:2], h.magic) // byte offset: 0-2 (2 bytes)
	binary.LittleEndian.PutUint16(buf[2:4], h.kind)  // byte offset: 2-4 (2 bytes)
	binary.LittleEndian.PutUint32(buf[4:8], h.crc32) // byte offset: 4-8 (4 bytes)
	binary.LittleEndian.PutUint64(buf[8:16], h.size) // byte offset: 8-16 (8 bytes)
	n, err := w.Write(buf[:])
	if err != nil {
		return int64(n), err
	}
	clear(&buf) // reset buffer
	return int64(n), nil
}

func (h *header) ReadFrom(r io.Reader) (int64, error) {
	n, err := r.Read(buf[:])
	if err != nil {
		// reset buffer
		clear(&buf)
		return int64(n), err
	}
	h.magic = binary.LittleEndian.Uint16(buf[0:2]) // byte offset: 0-2 (2 bytes)
	h.kind = binary.LittleEndian.Uint16(buf[2:4])  // byte offset: 2-4 (2 bytes)
	h.crc32 = binary.LittleEndian.Uint32(buf[4:8]) // byte offset: 4-8 (4 bytes)
	h.size = binary.LittleEndian.Uint64(buf[8:16]) // byte offset: 8-16 (8 bytes)
	// reset buffer
	clear(&buf)
	return int64(n), nil
}
