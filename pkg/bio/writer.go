package bio

import (
	"fmt"
	"io"
	"log"
)

type Writer struct {
	err error
	buf []byte    // buf is a reserved buffer
	n   int       // n is the current offset in the buffer
	wr  io.Writer // w is the underlying writer
	bc  int       // block count
}

// NewWriter returns a new writer whose buffer has
// an underlying size of chunkSize. A Writer writes
// fixed size blocks of data into fixed size chunks,
// also sometimes called spans.
func NewWriter(w io.Writer) *Writer {
	bw := &Writer{
		wr:  w,
		buf: make([]byte, chunkSize, chunkSize),
	}
	bw.initBlocks()
	return bw
}

func (bw *Writer) initBlocks() {
	for n := 0; n < len(bw.buf); n += blockSize {
		_, err := encodeHeader(bw.buf[n:n+headerSize], nil)
		if err != nil {
			panic(err)
		}
		bw.bc++
	}
}

func getSliceBounds(p []byte, beg, end int) (int, int) {
	slice := p[beg:end]
	if len(slice) < maxDataPerBlock {
		return beg, len(slice)
	}
	return beg, maxDataPerBlock
}

func slice(p []byte, beg, end int) []byte {
	if beg < 0 {
		beg = 0
	}
	if end > len(p) {
		end = len(p)
	}
	return p[beg:end]
}

func (bw *Writer) Write1(p []byte) (int, error) {
	// get the base block count required
	blocks := divUp(len(p), maxDataPerBlock)
	if blocks > blocksPerChunk {
		return -1, ErrInvalidSize
	}
	var prev int
	prev = bw.n
	// write block, or blocks
	for block, off := 1, 0; block <= blocks; block++ {
		fmt.Printf("[BEFORE] bw.n=%d, off=%d\n", bw.n, off)
		// re-calc ending slice point
		data := slice(p, off, off+maxDataPerBlock)
		// write block and update the slice points
		n, err := bw.writeBlockPart(data, block, blocks)
		if err != nil {
			return -1, err
		}
		// update offset
		off += n
		fmt.Printf("[AFTER] bw.n=%d, off=%d\n", bw.n, off)
	}
	fmt.Printf("wrote %d blocks, previous offset=%d, current offset=%d\n", blocks, prev, bw.n)
	// flush block or blocks to disk
	err := bw.Flush()
	if err != nil {
		return -1, err
	}
	// return nil error
	return blocks, nil
}

func (bw *Writer) writeBlockPart(p []byte, part, parts int) (int, error) {
	log.Println("writing:", string(p))
	// check to make sure data is not too big
	if len(p) > maxDataPerBlock {
		return -1, ErrInvalidSize
	}
	// check to make sure we have room in the
	// current chunk to accommodate another block
	if bw.Available() < blockSize {
		// if not, flush and proceed
		err := bw.Flush()
		if err != nil {
			return -1, err
		}
	}
	// fill out header
	hdr := &header{
		status: statusActive,
		kind:   getKind(part, parts),
		part:   uint8(part),
		parts:  uint8(parts),
		length: uint16(len(p)),
	}
	// store local offset to track how much
	// data we write in this block
	var nn, wrote int
	nn = bw.n
	// encode header
	n, err := encodeHeader(bw.buf[nn:nn+headerSize], hdr)
	if err != nil {
		return -1, err
	}
	nn += n
	// write data
	n = copy(bw.buf[nn:], p)
	nn += n

	//// get the next offset alignment
	//noff := align(nn, blockMask)
	//// check if we need to pad out the block
	//if nn < noff {
	//	// we do, update local offset
	//	nn += noff - nn
	//}

	// store the actual data written (minus the header) so
	// we know where to pick up for the next write. we must
	// do this here before we proceed to pad out the block
	wrote = nn - headerSize

	// check to see if the block needs to be padded
	if diff := nn & blockMask; diff != 0 {
		// move offset to correct place for next write
		nn += blockSize - diff
	}

	// we should be good to go, lets update the writers
	// global offset now that we know everything is okay
	bw.n += nn
	// and return the ACTUAL data written, and a nil error
	return wrote, nil
}

func (bw *Writer) Write2(p []byte) (int, error) {
	// implement me...
	return 0, nil
}

func (bw *Writer) WriteSpan(p []byte) (int, error) {
	// check to make sure data is not too big
	if len(p) > maxDataPerChunk {
		return -1, ErrInvalidSize
	}
	// check to make sure we have room in the
	// current chunk to accommodate another block
	if bw.Available() < align(len(p), blockMask) {
		// if not, flush and proceed
		err := bw.Flush()
		if err != nil {
			return -1, err
		}
	}
	// check to make sure our buffer offset is
	// still aligned to a perfect block offset
	if bw.n&blockMask != 0 {
		return -1, ErrInvalidOffset
	}
	// check if write will fit in single block
	if len(p) <= maxDataPerBlock {
		log.Println("single block writer")
		// if it's good, then write the block
		n, err := bw.writeBlock(p, 1, 1)
		if err != nil {
			return n, err
		}
		err = bw.Flush()
		if err != nil {
			return n, err
		}
		// return data written and nil error
		return n, nil
	}
	// for later maybe?
	var nn int
	// calculate number of blocks
	blks := calcBlocks(len(p) + (len(p)/maxDataPerBlock)*headerSize)
	// otherwise, write a span of blocks
	for part, parts, off := 1, blks, 0; part <= parts; part++ {
		log.Printf("multi-block writer: part=%d, parts=%d, off=%d (n=%d)", part, parts, off, bw.n)
		// calculate offset
		//beg := (part - 1) * maxDataPerBlock
		// calculate end offset
		//end := part * maxDataPerBlock
		//if end > len(p) {
		//	end = len(p)
		//}
		// write block
		n, err := bw.writeBlock(p[off:off+maxDataPerBlock], part, parts)
		if err != nil {
			return 0, err
		}
		nn += n
		off += n - headerSize
	}
	// make sure we flush that data
	err := bw.Flush()
	if err != nil {
		return nn, err
	}
	// return data written and nil error
	return nn, nil
}

// writeBlock writes data to a block sized chunk. It's parent
// method is responsible for dividing up any data that is larger
// than what can fit in the block and supplying it with correct
// part and parts for the header
func (bw *Writer) writeBlock(p []byte, part, parts int) (int, error) {
	// check to make sure data is not too big
	if len(p) > maxDataPerBlock {
		return -1, ErrInvalidSize
	}
	// check to make sure we have room in the
	// current chunk to accommodate another block
	if bw.Available() < blockSize {
		// if not, flush and proceed
		err := bw.Flush()
		if err != nil {
			return -1, err
		}
	}
	// check to make sure our buffer offset is
	// still aligned to a perfect block offset
	if bw.n&blockMask != 0 {
		return -1, ErrInvalidOffset
	}
	// fill out header
	hdr := &header{
		status: statusActive,
		kind:   getKind(part, parts),
		part:   uint8(part),
		parts:  uint8(parts),
		length: uint16(len(p)),
	}
	// store current offset for later
	var nn int
	nn = bw.n
	// encode the header first
	n, err := encodeHeader(p[0:headerSize], hdr)
	if err != nil {
		return -1, err
	}
	nn += n
	// update buffer offset and write data to buffer
	n = copy(bw.buf[nn:], p[headerSize:])
	nn += n
	bw.n += nn
	// check to see if the block needs to be padded
	if diff := bw.n & blockMask; diff != 0 {
		// move offset to correct place for next write
		bw.n += blockSize - diff
	}
	// return bytes written, and a nil error
	return nn, nil

}

// Flush writes any buffered data to the underlying io.Writer
func (bw *Writer) Flush() error {
	if bw.err != nil {
		return bw.err
	}
	if bw.n == 0 {
		return nil
	}
	n, err := bw.wr.Write(bw.buf[0:bw.n])
	if n < bw.n && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if n > 0 && n < bw.n {
			copy(bw.buf[0:bw.n-n], bw.buf[n:bw.n])
		}
		bw.n -= n
		bw.err = err
		return err
	}
	bw.n = 0
	return nil
}

// Available returns how many bytes are unused in the buffer
func (bw *Writer) Available() int {
	return len(bw.buf) - bw.n
}

// Buffered returns the number of bytes that have been written into the buffer
func (bw *Writer) Buffered() int {
	return bw.n
}

func (bw *Writer) Info() string {
	ss := fmt.Sprintf("writer:\n")
	ss += fmt.Sprintf("n=%d, buffered=%d, available=%d\n", bw.n, bw.Buffered(), bw.Available())
	for i := 0; i < len(bw.buf); i += blockSize {
		ss += fmt.Sprintf("\tblock[%.2d]\n", i/blockSize)
		hdr := new(header)
		decodeHeader(bw.buf[i:i+headerSize], hdr)
		ss += fmt.Sprintf("\t\t%s\n", hdr)
		dat := bw.buf[i+headerSize : i+blockSize]
		//ss += fmt.Sprintf("\t\t%s\n", longStr(string(dat), "", blockSize))
		ss += fmt.Sprintf("\t\t%q\n", dat)
	}
	return ss
}
