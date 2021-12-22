package _se

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"unsafe"
)

const (
	headerSize = int(unsafe.Sizeof(header{}))
	magicIdent = 0xdeadbeef
)

type header struct {
	id     uint32 // 4 bytes
	crc    uint32 // 4 bytes
	magic  uint32 // 4 bytes
	pages  uint16 // 2 bytes
	length uint16 // 2 bytes
}

func (h *header) String() string {
	return fmt.Sprintf("header:\tid=%d, crc=%d, magic=%d, pages=%d, length=%d",
		h.id, h.crc, h.magic, h.pages, h.length)
}

type record struct {
	*header
	data    []byte
	padding int
}

func makeRecord(id uint32, data []byte) *record {
	alignedSize := align(len(data)+headerSize, pageSize-1)
	//fmt.Printf("alignedSize=%d, dataLen=%d, padding=%d\n", alignedSize, len(data), (alignedSize - (len(data) + headerSize)))
	return &record{
		header: &header{
			id:     id,
			crc:    crc32.ChecksumIEEE(data),
			magic:  magicIdent,
			pages:  uint16(alignedSize / pageSize),
			length: uint16(len(data)),
		},
		data:    data,
		padding: alignedSize - (len(data) + headerSize),
	}
}

func (r *record) write(ws io.WriteSeeker) (int, error) {
	var nn int
	buf := make([]byte, r.header.pages*pageSize)
	binary.LittleEndian.PutUint32(buf[nn:nn+4], r.header.id)
	nn += 4
	binary.LittleEndian.PutUint32(buf[nn:nn+4], r.header.crc)
	nn += 4
	binary.LittleEndian.PutUint32(buf[nn:nn+4], r.header.magic)
	nn += 4
	binary.LittleEndian.PutUint16(buf[nn:nn+2], r.header.pages)
	nn += 2
	binary.LittleEndian.PutUint16(buf[nn:nn+2], r.header.length)
	nn += 2
	copy(buf[nn:], r.data)
	n, err := ws.Write(buf)
	if err != nil {
		return nn, err
	}
	nn += n
	if r.padding > 0 {
		_, err = ws.Write(make([]byte, r.padding))
		if err != nil {
			return nn, err
		}
	}
	nn += r.padding
	return nn, nil
}

func (r *record) read(rs io.ReadSeeker) (int, error) {
	var nn int
	var buf [headerSize]byte
	n, err := rs.Read(buf[:])
	if err != nil {
		return n, err
	}
	h := new(header)
	h.id = binary.LittleEndian.Uint32(buf[nn : nn+4])
	nn += 4
	h.crc = binary.LittleEndian.Uint32(buf[nn : nn+4])
	nn += 4
	h.magic = binary.LittleEndian.Uint32(buf[nn : nn+4])
	nn += 4
	h.pages = binary.LittleEndian.Uint16(buf[nn : nn+2])
	nn += 2
	h.length = binary.LittleEndian.Uint16(buf[nn : nn+2])
	nn += 2
	r.header = h
	r.data = make([]byte, r.header.length)
	n, err = rs.Read(r.data)
	if err != nil {
		return nn, err
	}
	nn += n
	if r.padding > 0 {
		_, err = rs.Seek(int64(r.padding), io.SeekCurrent)
		if err != nil {
			return nn, err
		}
	}
	return nn, nil
}

func (r *record) raw() []byte {
	var n int
	buf := make([]byte, r.header.pages*pageSize)
	binary.LittleEndian.PutUint32(buf[n:n+4], r.header.id)
	n += 4
	binary.LittleEndian.PutUint32(buf[n:n+4], r.header.crc)
	n += 4
	binary.LittleEndian.PutUint32(buf[n:n+4], r.header.magic)
	n += 4
	binary.LittleEndian.PutUint16(buf[n:n+2], r.header.pages)
	n += 2
	binary.LittleEndian.PutUint16(buf[n:n+2], r.header.length)
	n += 2
	n += copy(buf[n:], r.data)
	return buf
}

func (r *record) String() string {
	ss := fmt.Sprintf("%s\n", r.header)
	ss += fmt.Sprintf("record:\tpadding=%d, data=%q", r.padding, r.data)
	return ss
}
