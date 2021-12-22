package _se

import (
	"bufio"
	"errors"
	"io"
)

var page [pageSize]byte

var fakeHeader = [headerSize]byte{
	0xff, 0xff, 0xff,
}

type PageWriter struct {
	bw *bufio.Writer
}

func NewPageWriter(w io.Writer) *PageWriter {
	return &PageWriter{
		bw: bufio.NewWriterSize(w, pageSize),
	}
}

func (pw *PageWriter) Write(p []byte) (int, error) {
	var nn int
	// write header
	n, err := pw.bw.Write(fakeHeader[:])
	if err != nil {
		return -1, err
	}
	nn += n
	// write data
	n, err = pw.bw.Write(p)
	if err != nil {
		return -1, err
	}
	nn += n
	padding := pageSize - nn%pageSize
	// write padding
	if padding != 0 {
		n, err = pw.bw.Write(page[:padding])
		if err != nil {
			return -1, err
		}
		nn += n
	}
	return nn, nil
}

func (pw *PageWriter) Flush() error {
	err := pw.bw.Flush()
	if err != nil {
		return err
	}
	return nil
}

type PageReader struct {
	br *bufio.Reader
}

func NewPageReader(r io.Reader) *PageReader {
	return &PageReader{
		br: bufio.NewReaderSize(r, pageSize),
	}
}

var ErrBadSize = errors.New("bad size")

func (pr *PageReader) Read(p []byte) (int, error) {
	if len(p)%pageSize != 0 {
		return -1, ErrBadSize
	}
	var nn int
	// read header
	n, err := pr.br.Read(p[:headerSize])
	if err != nil {
		return -1, err
	}
	nn += n
	// decode and err check header
	//

	// read data
	n, err = pr.br.Read(p[nn:])
	if err != nil {
		return -1, err
	}
	nn += n
	return nn, nil
}
