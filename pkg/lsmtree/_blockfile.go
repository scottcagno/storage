package lsmtree

import (
	"encoding/binary"
	"errors"
	"fmt"
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
	ErrBadOffsetAlignment = errors.New("bad offset; not correctly aligned")
	ErrOffsetOutOfBounds  = errors.New("bad offset; out of bounds")
	ErrDataTooLarge       = errors.New("data exceeds max record size")
	ErrScannerSkip        = errors.New("skip to next place with scanner/iterator")
)

type blockFile struct {
	path string   // path is the current filepath
	open bool     // open reports true if the file is open
	size int64    // size is the current size of the file
	fp   *os.File // fp is the file pointer
}

func OpenBlockFile(name string) (*blockFile, error) {
	return openBlockFile(name)
}

func openBlockFile(name string) (*blockFile, error) {
	// sanitize base path
	path, err := filepath.Abs(name)
	if err != nil {
		return nil, err
	}
	// sanitize any path separators
	path = filepath.ToSlash(path)
	// get dir
	dir, _ := filepath.Split(path)
	// create any directories if they are not there
	err = os.MkdirAll(dir, os.ModeDir)
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

func (bf *blockFile) Read() ([]byte, error) {
	// error check
	// allocate new record to read into
	rd := new(recordData)
	// read record
	_, err := bf.readRecord(rd)
	if err != nil {
		return nil, err
	}
	// return record data
	return rd.data, nil
}

func (bf *blockFile) ReadAt(off int64) ([]byte, error) {
	// error check
	// allocate new record to read into
	rd := new(recordData)
	// read record at
	_, err := bf.readRecordAt(rd, off)
	if err != nil {
		return nil, err
	}
	// return record data
	return rd.data, nil
}

func (bf *blockFile) Write(d []byte) (int, error) {
	// error check

	// make record
	rd, err := makeRecord(d)
	if err != nil {
		return -1, err
	}
	// get record offset
	off, err := currentOffset(bf.fp)
	if err != nil {
		return -1, err
	}
	// write record
	_, err = bf.writeRecord(rd)
	if err != nil {
		return -1, err
	}
	// return offset of record
	return int(off), nil
}

func (bf *blockFile) WriteAt(d []byte, off int64) (int, error) {
	// error check
	// make record
	rd, err := makeRecord(d)
	if err != nil {
		return -1, err
	}
	// write record
	_, err = bf.writeRecordAt(rd, off)
	if err != nil {
		return -1, err
	}
	// return bytes written
	return int(off), nil
}

func (bf *blockFile) Seek(offset int64, whence int) (int64, error) {
	off, err := bf.fp.Seek(offset, whence)
	if err != nil {
		return -1, err
	}
	return off, nil
}

type Record = recordData

func (bf *blockFile) Scan(fn func(rd *Record) error) error {
	for {
		// allocate new record to read into
		rd := new(recordData)
		// read record
		_, err := bf.readRecord(rd)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		err = fn(rd)
		if err != nil {
			if err == ErrScannerSkip {
				continue
			}
			return err
		}
	}
	return nil
}

func (bf *blockFile) Sync() error {
	err := bf.fp.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (bf *blockFile) Close() error {
	err := bf.fp.Sync()
	if err != nil {
		return err
	}
	err = bf.fp.Close()
	if err != nil {
		return err
	}
	return nil
}

func (bf *blockFile) readRecord(rd *recordData) (int, error) {
	// read record data
	n, err := rd.readData(bf.fp)
	if err != nil {
		return -1, err
	}
	// skip to the next alignment offset
	_, err = bf.fp.Seek(int64(rd.padding), io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	return n, nil
}

func (bf *blockFile) readRecordAt(rd *recordData, offset int64) (int, error) {
	// read record data
	n, err := rd.readDataAt(bf.fp, offset)
	if err != nil {
		return -1, err
	}
	return n, nil
}

func (bf *blockFile) writeRecord(rd *recordData) (int, error) {
	// write record data
	n, err := rd.writeData(bf.fp)
	if err != nil {
		return -1, err
	}
	// skip to the next alignment offset
	_, err = bf.fp.Seek(int64(rd.padding), io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	return n, nil
}

func (bf *blockFile) writeRecordAt(rd *recordData, offset int64) (int, error) {
	// write record data
	n, err := rd.writeDataAt(bf.fp, offset)
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

func getOffset(pos int, max int64) (int64, error) {
	// calculate the
	offset := int64(pos * blockSize)
	// return error if offset is not block aligned
	if offset%blockSize != 0 {
		return -1, ErrBadOffsetAlignment
	}
	// return error if offset is larger than max
	if offset > max {
		return -1, ErrOffsetOutOfBounds
	}
	// otherwise, return offset
	return offset, nil
}

func currentOffset(w io.Seeker) (int64, error) {
	// get current offset (of the beginning of this record) to return
	off, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	// return err if offset is not block aligned
	if off%blockSize != 0 {
		return -1, ErrBadOffsetAlignment
	}
	// return offset and a nil error
	return off, nil
}

type recordHeader struct {
	status  uint16 // max: 65535; status of the record
	blocks  uint16 // max: 65535; blocks occupied by the record data
	length  uint32 // max: 4294967295; length of the raw record data
	padding uint32 // max: 4294967295; padding is the extra unused bytes in the block
	magic   uint32 // max: 4294967295; magic currently unused
}

func (rh *recordHeader) readHeader(r io.Reader) (int, error) {
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
	// decode padding
	rh.padding = binary.LittleEndian.Uint32(buf[8:12])
	// decode magic
	rh.magic = binary.LittleEndian.Uint32(buf[12:16])
	return n, nil
}

func (rh *recordHeader) readHeaderAt(r io.ReaderAt, offset int64) (int, error) {
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
	// decode padding
	rh.padding = binary.LittleEndian.Uint32(buf[8:12])
	// decode extra2
	rh.magic = binary.LittleEndian.Uint32(buf[12:16])
	return n, nil
}

func (rh *recordHeader) writeHeader(w io.Writer) (int, error) {
	// make buffer to encode record header into
	buf := make([]byte, 16)
	// encode status
	binary.LittleEndian.PutUint16(buf[0:2], rh.status)
	// encode blocks
	binary.LittleEndian.PutUint16(buf[2:4], rh.blocks)
	// encode length
	binary.LittleEndian.PutUint32(buf[4:8], rh.length)
	// encode padding
	binary.LittleEndian.PutUint32(buf[8:12], rh.padding)
	// encode magic
	binary.LittleEndian.PutUint32(buf[12:16], rh.magic)
	// write record header
	return w.Write(buf)
}

func (rh *recordHeader) writeHeaderAt(w io.WriterAt, offset int64) (int, error) {
	// make buffer to encode record header into
	buf := make([]byte, 16)
	// encode status
	binary.LittleEndian.PutUint16(buf[0:2], rh.status)
	// encode blocks
	binary.LittleEndian.PutUint16(buf[2:4], rh.blocks)
	// encode length
	binary.LittleEndian.PutUint32(buf[4:8], rh.length)
	// encode padding
	binary.LittleEndian.PutUint32(buf[8:12], rh.padding)
	// encode magic
	binary.LittleEndian.PutUint32(buf[12:16], rh.magic)
	// write record header at
	return w.WriteAt(buf, offset)
}

type recordData struct {
	*recordHeader        // record header
	data          []byte // raw record data
}

func (rd *recordData) String() string {
	s := fmt.Sprintf("record:\n")
	s += fmt.Sprintf("\theader:\n")
	s += fmt.Sprintf("\t\tstatus: %d\n", rd.recordHeader.status)
	s += fmt.Sprintf("\t\tblocks: %d\n", rd.recordHeader.blocks)
	s += fmt.Sprintf("\t\tlength: %d\n", rd.recordHeader.length)
	s += fmt.Sprintf("\t\tpadding: %d\n", rd.recordHeader.padding)
	s += fmt.Sprintf("\t\tmagic: %d\n", rd.recordHeader.magic)
	s += fmt.Sprintf("\tdata: %s\n", rd.data)
	return s
}

func makeRecord(d []byte) (*recordData, error) {
	// calc "overhead"
	overhead := recordHeaderSize + int64(len(d))
	// get aligned size
	size := align(overhead)
	// error check
	if size > maxRecordSize {
		return nil, ErrDataTooLarge
	}
	// create record
	rd := &recordData{
		recordHeader: &recordHeader{
			status:  statusActive,
			blocks:  uint16(size / blockSize),
			length:  uint32(len(d)),
			padding: uint32(size - overhead),
			magic:   uint32(0),
		},
		data: d,
	}
	// return record
	return rd, nil
}

func (rd *recordData) readData(r io.Reader) (int, error) {
	// create record header
	rh := new(recordHeader)
	// read the record header
	n, err := rh.readHeader(r)
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

func (rd *recordData) readDataAt(r io.ReaderAt, offset int64) (int, error) {
	// create record header
	rh := new(recordHeader)
	// read the record header
	n, err := rh.readHeaderAt(r, offset)
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

func (rd *recordData) writeData(w io.Writer) (int, error) {
	// capture bytes written
	var wrote int
	// write the record header
	n, err := rd.recordHeader.writeHeader(w)
	if err != nil {
		return -1, err
	}
	// update wrote
	wrote += n
	// write the record data
	n, err = w.Write(rd.data)
	if err != nil {
		return -1, err
	}
	// update written
	wrote += n
	// return bytes written
	return wrote, nil
}

func (rd *recordData) writeDataAt(w io.WriterAt, offset int64) (int, error) {
	// capture bytes written
	var wrote int
	// write the record header
	n, err := rd.recordHeader.writeHeaderAt(w, offset)
	if err != nil {
		return -1, err
	}
	// update wrote and offset
	wrote += n
	offset += int64(n)
	// write the record data
	n, err = w.WriteAt(rd.data, offset)
	if err != nil {
		return -1, err
	}
	// update wrote
	wrote += n
	// return bytes written
	return wrote, nil
}
