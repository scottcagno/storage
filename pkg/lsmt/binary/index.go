package binary

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Index is a binary entry index
type Index struct {
	Key    []byte
	Offset int64
}

// String is the stringer method for a *Index
func (di *Index) String() string {
	return fmt.Sprintf("index.key=%q, index.offset=%q", di.Key, di.Offset)
}

// EncodeIndex encodes and writes the provided entry index to w
func EncodeIndex(w io.WriteSeeker, e *Index) (int64, error) {
	// error check
	if e == nil {
		return -1, ErrBadEntry
	}
	// get the file pointer offset for the entry
	offset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	// make buffer
	buf := make([]byte, 18)
	// encode and write entry key length
	binary.LittleEndian.PutUint64(buf[0:8], uint64(len(e.Key)))
	_, err = w.Write(buf[0:8])
	if err != nil {
		return -1, err
	}
	// encode and write entry index data offset
	binary.PutVarint(buf[8:18], e.Offset)
	_, err = w.Write(buf[8:18])
	if err != nil {
		return -1, err
	}
	// write entry key
	_, err = w.Write(e.Key)
	if err != nil {
		return -1, err
	}
	return offset, nil
}

// DecodeIndex reads and decodes the provided entry index from r
func DecodeIndex(r io.Reader) (*Index, error) {
	// make buffer
	buf := make([]byte, 18)
	// read entry key length
	_, err := r.Read(buf[0:8])
	if err != nil {
		return nil, err
	}
	// read entry data offset
	_, err = r.Read(buf[8:18])
	if err != nil {
		return nil, err
	}
	// decode key length
	klen := binary.LittleEndian.Uint64(buf[0:8])
	// decode data offset
	off, _ := binary.Varint(buf[8:18])
	// make entry index
	e := &Index{
		Key:    make([]byte, klen),
		Offset: off,
	}
	// read key from data into entry key
	_, err = r.Read(e.Key)
	if err != nil {
		return nil, err
	}
	// return entry
	return e, nil
}

// DecodeIndexAt decodes the index at the provided offset using the provided reader
func DecodeIndexAt(r io.ReaderAt, offset int64) (*Index, error) {
	// make buffer
	buf := make([]byte, 18)
	// read entry key length
	n, err := r.ReadAt(buf[0:8], offset)
	if err != nil {
		return nil, err
	}
	// update offset
	offset += int64(n)
	// read entry data offset
	n, err = r.ReadAt(buf[8:18], offset)
	if err != nil {
		return nil, err
	}
	// update offset
	offset += int64(n)
	// decode key length
	klen := binary.LittleEndian.Uint64(buf[0:8])
	// decode data offset
	off, _ := binary.Varint(buf[8:18])
	// make entry index
	e := &Index{
		Key:    make([]byte, klen),
		Offset: off,
	}
	// read key from data into entry key
	n, err = r.ReadAt(e.Key, offset)
	if err != nil {
		return nil, err
	}
	// update offset
	offset += int64(n)
	// return entry
	return e, nil
}
