package bio

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
)

type Writer struct {
	bw  *bufio.Writer // w is the underlying writer
	pad [blockSize]byte
}

// NewWriter returns a new writer whose buffer has
// an underlying size of chunkSize. A Writer writes
// fixed size blocks of data into fixed size chunks,
// also sometimes called spans.
func NewWriter(w io.Writer) *Writer {
	// if we get a bytes buffer as a writer
	// make sure we grow it, otherwise bad
	// things will happen
	if b, ok := w.(*bytes.Buffer); ok {
		if chunkSize > b.Cap() {
			b.Grow(chunkSize)
		}
	}
	// create and return a new writer
	return &Writer{
		bw: bufio.NewWriterSize(w, chunkSize),
	}
}

func (w *Writer) Write(p []byte) (int, error) {
	// perform error check
	if len(p) > maxDataPerChunk {
		return -1, ErrDataTooBig
	}
	var err error
	// get block count for writing
	part, parts := 1, divUp(len(p), maxDataPerBlock)
	// start writing blocks sequentially
	for i := 0; i < len(p); i += maxDataPerBlock {
		// setup j to be the slice ending boundary
		j := i + maxDataPerBlock
		// necessary check to avoid slicing beyond p's capacity
		if j > len(p) {
			j = len(p)
		}
		// write block (a slice of p, from i to j)
		_, err = w.writeBlock(p[i:j], part, parts)
		if err != nil {
			return -1, err
		}
		// increment parts (if need be)
		part++
	}
	// done writing, flush
	err = w.bw.Flush()
	if err != nil {
		return -1, err
	}
	// return
	return parts * blockSize, nil
}

func (w *Writer) writeBlock(p []byte, part, parts int) (int, error) {
	log.Printf("len(p)=%d\n", len(p))

	// create header
	h := &header{
		status: statusActive,
		kind:   getKind(part, parts),
		part:   uint8(part),
		parts:  uint8(parts),
		length: uint16(len(p)),
	}
	// write header
	_, err := h.WriteTo(w.bw)
	if err != nil {
		return -1, err
	}
	// write body
	n, err := w.bw.Write(p)
	if err != nil {
		return -1, err
	}
	// check to see if we need to pad
	if n < maxDataPerBlock {
		padding := maxDataPerBlock - n
		_, err = w.bw.Write(w.pad[:padding])
		if err != nil {
			return -1, err
		}
		log.Printf("[PADDING] %d bytes\n", padding)
	}
	// return exactly how much data was written into this block
	return n, nil
}

func (w *Writer) Info(b *bytes.Buffer) string {
	buf := b.Bytes()
	ss := fmt.Sprintf("writer:\n")
	ss += fmt.Sprintf("buffered=%d, available=%d\n", w.bw.Buffered(), w.bw.Available())
	for i := 0; i < len(buf); i += blockSize {
		ss += fmt.Sprintf("\tblock[%.2d]\n", i/blockSize)
		hdr := new(header)
		decodeHeader(buf[i:i+headerSize], hdr)
		ss += fmt.Sprintf("\t\t%s\n", hdr)
		dat := buf[i+headerSize : i+blockSize]
		//ss += fmt.Sprintf("\t\t%s\n", longStr(string(dat), "", blockSize))
		ss += fmt.Sprintf("\t\t%q\n", dat)
	}
	return ss
}
