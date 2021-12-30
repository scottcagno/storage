package v3

import (
	"bufio"
	"hash/crc32"
	"io"
)

type DataWriter struct {
	bw        *bufio.Writer
	opts      *Options
	emptyPage page
}

func NewDataWriter(w io.Writer, opt *Options) *DataWriter {
	checkOptions(opt)
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
		n, err = d.bw.Write(d.emptyPage.data[:off])
		if err != nil {
			return n + nn, err
		}
		nn += n
	}
	// check auto flush
	if d.opts.autoFlush {
		err = d.bw.Flush()
		if err != nil {
			return nn, err
		}
	}
	// return bytes written
	return nn, nil
}

func (d *DataWriter) Flush() error {
	return d.bw.Flush()
}
