package bio

import (
	"bufio"
	"fmt"
	"io"
)

// Reader is a bio reader that implements the
// io.Reader and io.ReaderAt interfaces
type Reader struct {
	br *bufio.Reader
}

// NewReader returns a new *Reader whose buffer has
// an underlying size of chunkSize. A Reader reads
// fixed size blocks of data into fixed size chunks,
// also sometimes called spans.
func NewReader(r io.Reader) *Reader {
	// create and return a new *Reader
	return &Reader{
		br: bufio.NewReaderSize(r, chunkSize),
	}
}

// ReadRecord read and returns the next record sequentially
func (r *Reader) ReadRecord() ([]byte, error) {
	// implement...
	return nil, nil
}

// ReadRecordAt reads and returns the record located at the provided offset
func (r *Reader) ReadRecordAt(offset int64) ([]byte, error) {
	// implement...
	return nil, nil
}

// Read reads data into p. It returns the number of bytes read
// into p. At EOF, the count will be zero and err will be io.EOF.
func (r *Reader) Read(p []byte) (int, error) {
	// perform error checking
	if p == nil {
		return -1, ErrDataIsEmpty
	}
	if len(p) > maxDataPerChunk {
		return -1, ErrSliceTooLarge
	}
	if len(p) < blockSize {
		return -1, ErrSliceTooSmall
	}
	// init error var for later
	var err error
	// start reading blocks sequentially
	for i := 0; i < len(p); i += blockSize {
		// setup j to be the slice ending boundary
		j := i + blockSize
		// necessary check to avoid slicing beyond p's capacity
		if j > len(p) {
			j = len(p)
		}
		// read block (a slice of p, from i to j)
		_, err = r.readBlock(p[i:j])
		if err != nil {
			return -1, err
		}
	}
	// return
	return 0, nil
}

func (r *Reader) readBlock(p []byte) (int, error) {
	// error check p
	if len(p) != blockSize {
		return -1, ErrInvalidSize
	}
	// read block
	n, err := r.br.Read(p)
	if err != nil {
		return -1, err
	}
	// return
	return n, nil
}

func (r *Reader) readRecord() ([]byte, error) {
	// init vars
	var parts uint8
	var length uint16
	// peek into header bytes, to find block count
	hdr, err := r.br.Peek(headerSize)
	if err != nil {
		return nil, err
	}
	// store block count for this record
	parts = hdr[3]
	// make slice large enough to hold record
	record := make([]byte, parts*maxDataPerBlock)
	// start the iteration
	for i := 0; i < int(parts); i++ {
		// peek into header bytes, to find block count
		hdr, err := r.br.Peek(headerSize)
		if err != nil {
			return nil, err
		}
		// get record length
		rlength := uint16(hdr[4]) | uint16(hdr[5])<<8
		// skip past header
		_, err = r.br.Discard(headerSize)
		if err != nil {
			return nil, err
		}
		// calculate offset
		off := i * blockSize
		// read into record and return
		_, err = r.br.Read(record[off : off+int(rlength)])
		if err != nil {
			return nil, err
		}
		// discard any padding
		if rlength != maxDataPerBlock {
			skip := maxDataPerBlock - int(rlength)
			_, err = r.br.Discard(skip)
			if err != nil {
				return nil, err
			}
		}
		// add to length
		length += rlength
	}
	// return record
	return record[:], nil
}

// ReadAt reads len(p) bytes into p starting at offset off in the
// underlying input source. It returns the number of bytes
// read (0 <= n <= len(p)) and any error encountered.
func (r *Reader) ReadAt(p []byte) (int, error) {

	// implement me
	return 0, nil
}

// String is *Reader's stringer method
func (r *Reader) String() string {
	ss := fmt.Sprintf("%#+v", r.br)
	return ss
}
