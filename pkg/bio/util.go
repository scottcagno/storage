package bio

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

func allocate(size int) []byte {
	return calloc(align(size, size-1))
}

func info(p *[]byte) {
	if p == nil {
		fmt.Println("ptr=nil, len=0, cap=0, data=nil")
	}
	fmt.Printf("ptr=%p, len=%d, cap=%d, data=%q\n", *p, len(*p), cap(*p), *p)
}

func longStr(s string, pre string, max int) string {
	var ss string
	for i := 0; i < len(s); i += max {
		j := i + max
		if j > len(s) {
			j = len(s)
		}
		fmtr := fmt.Sprintf("%s| %%-%ds |\n", pre, max)
		ss += fmt.Sprintf(fmtr, s[i:j])
	}
	return ss
}

func ChunkSliceIter(slice []int, chunkSize int, fn func(p []int) int) {
	for beg := 0; beg < len(slice); beg += chunkSize {
		end := beg + chunkSize
		// necessary check to avoid slicing beyond
		// slice capacity
		if end > len(slice) {
			end = len(slice)
		}
		n := fn(slice[beg:end])
		_ = n
	}
}

// this impl does not continuously modify the slice, and uses iteration
func ChunkSliceV1(slice []int, chunkSize int) [][]int {
	var chunks [][]int
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		// necessary check to avoid slicing beyond
		// slice capacity
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}
	return chunks
}

// this impl continuously modifies the slice and calls break eventually
func ChunkSliceV2(slice []int, chunkSize int) [][]int {
	var chunks [][]int
	for {
		if len(slice) == 0 {
			break
		}
		// necessary check to avoid slicing beyond
		// slice capacity
		if len(slice) < chunkSize {
			chunkSize = len(slice)
		}
		chunks = append(chunks, slice[0:chunkSize])
		slice = slice[chunkSize:]
	}
	return chunks
}

func calcBlocks(size int) int {
	size = align(size, blockMask)
	return size / (blockSize - headerSize)
}

func align(size, mask int) int {
	return (size + mask) &^ (mask)
}

func calloc(size int) []byte {
	return make([]byte, size, size)
}

func malloc(size int) []byte {
	return make([]byte, 0, size)
}

func clear(p *[]byte) (int, int) {
	*p = (*p)[:0]
	return len(*p), cap(*p)
}

func free(p *[]byte) {
	*p = nil
}

func decodeHeader(p []byte, h *header) (int, error) {
	if p == nil || len(p) != 6 {
		return -1, ErrInvalidSize
	}
	_ = p[5]
	h.status = p[0]
	h.kind = p[1]
	h.part = p[2]
	h.parts = p[3]
	h.length = uint16(p[4]) | uint16(p[5])<<8
	return len(p), nil
}

func encodeHeader(p []byte, h *header) (int, error) {
	if p == nil || len(p) != 6 {
		return -1, ErrInvalidSize
	}
	if h == nil {
		// encode "zero value" header
		h = new(header)
		h.status = statusEmpty
		h.kind = kindFull
		h.part = 1
		h.parts = 1
		h.length = 0
	}
	_ = p[5]
	p[0] = h.status
	p[1] = h.kind
	p[2] = h.part
	p[3] = h.parts
	p[4] = byte(h.length)
	p[5] = byte(h.length >> 8)
	return len(p), nil
}

func Info(w *Writer, b *bytes.Buffer) string {
	buf := b.Bytes()
	ss := fmt.Sprintf("writer:\n")
	ss += fmt.Sprintf("buffered=%d, available=%d\n", w.bw.Buffered(), w.bw.Available())
	for i := 0; i < b.Len(); i += blockSize {
		ss += fmt.Sprintf("\tblock[%.2d]\n", i/blockSize)
		hdr := new(header)
		decodeHeader(buf[i:i+headerSize], hdr)
		ss += fmt.Sprintf("\t\t%s\n", hdr)
		dat := buf[i+headerSize : i+blockSize]
		ss += fmt.Sprintf("\t\t%q\n", dat)
	}
	ss += fmt.Sprintf("\n---[ START HEXDUMP ]---\n%s\n---[ END HEXDUMP ]---\n", hex.Dump(buf))
	return ss
}
