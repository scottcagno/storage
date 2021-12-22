package v3

import (
	"bufio"
	"errors"
	"hash/crc32"
	"io"
)

type DataReader struct {
	br   *bufio.Reader
	opts *Options
}

func NewDataReader(r io.Reader, opt *Options) *DataReader {
	if opt == nil {
		opt = defaultOptions
	}
	return &DataReader{
		br:   bufio.NewReaderSize(r, opt.pageSize),
		opts: opt,
	}
}

func (d *DataReader) readHeader(h *header) (int, error) {
	// read data into header
	n, err := h.ReadFrom(d.br)
	if err != nil {
		return int(n), err
	}
	return int(n), nil
}

var ErrBadChecksum = errors.New("bad checksum")

func (d *DataReader) Read(p []byte) (int, error) {
	// create header
	var h header
	// and read into header
	n, err := d.readHeader(&h)
	if err != nil {
		return n, err
	}
	var nn int
	nn += n
	// check len(p) to make sure it's large enough
	if len(p[:h.size]) < int(h.size) {
		return nn, ErrNotEnoughRoom
	}
	// it is, so let's read
	n, err = d.br.Read(p[:h.size])
	if err != nil {
		return n + nn, err
	}
	nn += n
	// check crc
	if h.crc32 != crc32.ChecksumIEEE(p[:h.size]) {
		return nn, ErrBadChecksum
	}
	// return bytes written
	return nn, nil
}
