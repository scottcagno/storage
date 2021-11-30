package lsmtree

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
)

const (
	blockSize        = 4096
	recordHeaderSize = 16
	maxRecordSize    = blockSize * math.MaxUint16
)

const (
	statusFree    = uint16(asciiUnitSeparator >> 0 << 10)
	statusActive  = uint16(asciiUnitSeparator >> 1 << 10)
	statusDeleted = uint16(asciiUnitSeparator >> 2 << 10)
)

var (
	ErrNilPointer         = errors.New("got nil pointer")
	ErrBadOffsetAlignment = errors.New("bad offset; not correctly aligned")
	ErrOffsetOutOfBounds  = errors.New("bad offset; out of bounds")
	ErrDataTooLarge       = errors.New("data exceeds max record size")
)

type blockFile struct {
	path string   // path is the current filepath
	open bool     // open reports true if the file is open
	size int64    // size is the current size of the file
	fp   *os.File // fp is the file pointer
}

func openBlockFile(name string) (*blockFile, error) {
	// sanitize base path
	path, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	// sanitize any path separators
	path = filepath.ToSlash(path)
	// create any directories if they are not there
	err = os.MkdirAll(path, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// open file
	f, err := os.OpenFile(path, os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	// get file size
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	// setup new blockFile
	bf := &blockFile{
		path: path,
		open: true,
		size: fi.Size(),
		fp:   f,
	}
	// call init
	err = bf.init()
	if err != nil {
		return nil, err
	}
	// return blockFile
	return bf, nil
}

func (bf *blockFile) init() error {
	return nil
}

func (bf *blockFile) read(rd *recordData) (int64, error) {
	// read record data
	n, err := rd.read(bf.fp)
	if err != nil {
		return -1, err
	}
	return int64(n), nil
}

func (bf *blockFile) readRecordAt(rd *recordData, offset int64) (int64, error) {
	// read record data
	n, err := rd.readAt(bf.fp, offset)
	if err != nil {
		return -1, err
	}
	return int64(n), nil
}

func (bf *blockFile) writeRecord(rd *recordData) (int64, error) {
	// write record data
	n, err := rd.write(bf.fp)
	if err != nil {
		return -1, err
	}
	return n, nil
}

func (bf *blockFile) writeRecordAt(rd *recordData, offset int64) (int64, error) {
	// write record data
	n, err := rd.writeAt(bf.fp, offset)
	if err != nil {
		return -1, err
	}
	return n, nil
}

func align(size int64) int64 {
	if size > 0 {
		return ((size + 2) + blockSize - 1) &^ (blockSize - 1)
	}
	return blockSize
}

type recordHeader struct {
	status uint16 // max: 65535; status of the record
	blocks uint16 // max: 65535; blocks occupied by the record data
	length uint32 // max: 4294967295; length of the raw record data
	extra1 uint32 // max: 4294967295; extra1 currently unused
	extra2 uint32 // max: 4294967295; extra2 currently unused
}

func (rh *recordHeader) read(r io.Reader) (int, error) {
	// make buffer for record header
	buf := make([]byte, 16)
	// read in entire record header
	n, err := r.Read(buf)
	if err != nil {
		return n, err
	}
	// decode status
	rh.status = binary.LittleEndian.Uint16(buf[0:2])
	// decode blocks
	rh.blocks = binary.LittleEndian.Uint16(buf[2:4])
	// decode length
	rh.length = binary.LittleEndian.Uint32(buf[4:8])
	// decode extra1
	rh.extra1 = binary.LittleEndian.Uint32(buf[8:12])
	// decode extra2
	rh.extra2 = binary.LittleEndian.Uint32(buf[12:16])
	return n, nil
}

func (rh *recordHeader) readAt(r io.ReaderAt, offset int64) (int, error) {
	// make buffer for record header
	buf := make([]byte, 16)
	// read in entire record header
	n, err := r.ReadAt(buf, offset)
	if err != nil {
		return n, err
	}
	// decode status
	rh.status = binary.LittleEndian.Uint16(buf[0:2])
	// decode blocks
	rh.blocks = binary.LittleEndian.Uint16(buf[2:4])
	// decode length
	rh.length = binary.LittleEndian.Uint32(buf[4:8])
	// decode extra1
	rh.extra1 = binary.LittleEndian.Uint32(buf[8:12])
	// decode extra2
	rh.extra2 = binary.LittleEndian.Uint32(buf[12:16])
	return n, nil
}

func (rh *recordHeader) write(w io.Writer) (int, error) {
	// make buffer to encode record header into
	buf := make([]byte, 16)
	// encode status
	binary.LittleEndian.PutUint16(buf[0:2], rh.status)
	// encode blocks
	binary.LittleEndian.PutUint16(buf[2:4], rh.blocks)
	// encode length
	binary.LittleEndian.PutUint32(buf[4:8], rh.length)
	// encode extra1
	binary.LittleEndian.PutUint32(buf[8:12], rh.extra1)
	// encode extra2
	binary.LittleEndian.PutUint32(buf[12:16], rh.extra2)
	// write record header
	return w.Write(buf)
}

func (rh *recordHeader) writeAt(w io.WriterAt, offset int64) (int, error) {
	// make buffer to encode record header into
	buf := make([]byte, 16)
	// encode status
	binary.LittleEndian.PutUint16(buf[0:2], rh.status)
	// encode blocks
	binary.LittleEndian.PutUint16(buf[2:4], rh.blocks)
	// encode length
	binary.LittleEndian.PutUint32(buf[4:8], rh.length)
	// encode extra1
	binary.LittleEndian.PutUint32(buf[8:12], rh.extra1)
	// encode extra2
	binary.LittleEndian.PutUint32(buf[12:16], rh.extra2)
	// write record header at
	return w.WriteAt(buf, offset)
}

type recordData struct {
	*recordHeader        // record header
	data          []byte // raw record data
}

func makeRecord(d []byte) (*recordData, error) {
	// get aligned size
	size := align(recordHeaderSize + int64(len(d)) + 1)
	// error check
	if size > maxRecordSize {
		return nil, ErrDataTooLarge
	}
	// create record
	rd := &recordData{
		recordHeader: &recordHeader{
			status: statusActive,
			blocks: uint16(size / blockSize),
			length: uint32(len(d)),
			extra1: uint32(0),
			extra2: uint32(0),
		},
		data: append(d, asciiRecordSeparator),
	}
	// return record
	return rd, nil
}

func (rd *recordData) read(r io.Reader) (int, error) {
	// create record header
	rh := new(recordHeader)
	// read the record header
	n, err := rh.read(r)
	if err != nil {
		return -1, err
	}
	// init count
	count := n
	// fill out the record and allocate space to read in data
	rd.recordHeader = rh
	rd.data = make([]byte, rh.length)
	// read data into the record
	n, err = r.Read(rd.data)
	if err != nil {
		return -1, err
	}
	// update count
	count += n
	return count, nil
}

func (rd *recordData) readAt(r io.ReaderAt, offset int64) (int, error) {
	// create record header
	rh := new(recordHeader)
	// read the record header
	n, err := rh.readAt(r, offset)
	if err != nil {
		return -1, err
	}
	// update offset for next read
	offset += int64(n)
	// fill out the record and allocate space to read in data
	rd.recordHeader = rh
	rd.data = make([]byte, rh.length)
	// read data into the record
	n, err = r.ReadAt(rd.data, offset)
	if err != nil {
		return -1, err
	}
	// update offset to return
	offset += int64(n)
	return int(offset), nil
}

func (rd *recordData) write(w io.WriteSeeker) (int64, error) {
	// ensure record is not nil
	if rd == nil {
		return -1, ErrNilPointer
	}
	// get current offset (of the beginning of this record) to return
	offset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	// return err if offset is not block aligned
	if offset%blockSize != 0 {
		return -1, ErrBadOffsetAlignment
	}
	// write the record header
	_, err = rd.recordHeader.write(w)
	if err != nil {
		return -1, err
	}
	// write the record data
	_, err = w.Write(rd.data)
	if err != nil {
		return -1, err
	}
	// return record offset
	return offset, nil
}

func (rd *recordData) writeAt(w io.WriterAt, offset int64) (int64, error) {
	// ensure record is not nil
	if rd == nil {
		return -1, ErrNilPointer
	}
	// return err if offset is not block aligned
	if offset%blockSize != 0 {
		return -1, ErrBadOffsetAlignment
	}
	// write the record header
	n, err := rd.recordHeader.writeAt(w, offset)
	if err != nil {
		return -1, err
	}
	// update the offset for the next write
	offset += int64(n)
	// write the record data
	n, err = w.WriteAt(rd.data, offset)
	if err != nil {
		return -1, err
	}
	// update the offset
	offset += int64(n)
	// return record offset
	return offset, nil
}
