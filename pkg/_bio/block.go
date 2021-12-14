package _bio

import (
	"errors"
	"io"
)

const (
	blockSize = 16
	chunkSize = 16 * blockSize
)

type block [blockSize]byte
type chunk [chunkSize]byte

var (
	ErrNilPointer = errors.New("nil pointer")
	ErrBadSize    = errors.New("bad size")
)

type blockReader struct {
	io.ReadWriteSeeker
}

func newBlockReader(writer io.ReadWriteSeeker) *blockReader {
	return &blockReader{}
}

func (br *blockReader) Read(p []byte) (int, error) {
	if p == nil {
		return -1, ErrNilPointer
	}
	if len(p) != blockSize {
		return -1, ErrBadSize
	}
	// ??
	return 0, nil
}

func (br *blockReader) Write(p []byte) (int, error) {
	return 0, nil
}

func (br *blockReader) Seek(off int64, whence int) (int64, error) {
	return 0, nil
}
