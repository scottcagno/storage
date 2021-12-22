package v3

import (
	"bufio"
	"errors"
	"hash/crc32"
	"io"
	"log"
)

var emptyPage [4096]byte

type DataWriter struct {
	bw   *bufio.Writer
	opts *Options
}

func NewDataWriter(w io.Writer, opt *Options) *DataWriter {
	if opt == nil {
		opt = defaultOptions
	}
	return &DataWriter{
		bw:   bufio.NewWriterSize(w, opt.pageSize),
		opts: opt,
	}
}

func (d *DataWriter) writeHeader(p []byte) (int, error) {
	var kind uint16
	if len(p) == 0 || p == nil {
		kind = kindInactive
	}
	// create header
	h := &header{
		magic: magicBytes,
		kind:  kind,
		crc32: crc32.ChecksumIEEE(p),
		size:  uint64(len(p)),
	}
	// write header
	n, err := h.WriteTo(d.bw)
	if err != nil {
		return int(n), err
	}
	return int(n), nil
}

func (d *DataWriter) Write(p []byte) (int, error) {
	// write header
	n, err := d.writeHeader(p)
	if err != nil {
		return n, err
	}
	var nn int
	nn += n
	// write body
	n, err = d.bw.Write(p)
	if err != nil {
		return n + nn, err
	}
	nn += n
	// check align rules
	if d.opts.pageAlign {
		off := d.bw.Size() - d.bw.Buffered()
		//log.Printf("writing % x (len=%d)\n", emptyPage[0:off], len(emptyPage[0:off]))
		n, err = d.bw.Write(emptyPage[:off])
		if err != nil {
			return n + nn, err
		}
		nn += n
	}
	// return bytes written
	return nn, nil
}

var ErrNotEnoughRoom = errors.New("not enough room left in the buffer")

func (d *DataWriter) Align() (int, error) {
	// get current "offset"
	off := d.bw.Buffered()
	// get the "aligned offset"
	aln := ((off + (d.opts.pageSize - 1)) &^ (d.opts.pageSize - 1)) - off
	// check to see if the buffer
	// has enough room to take up
	// the "aligned offset" slack
	log.Printf(">>> off=%d, aln=%d, available=%d\n", off, aln, d.bw.Available())
	if aln > d.bw.Available() {
		return 0, ErrNotEnoughRoom
	}
	n, err := d.bw.Write(emptyPage[:off-aln])
	if err != nil {
		return n, err
	}
	return n, nil
}

func (d *DataWriter) Flush() error {
	return d.bw.Flush()
}
