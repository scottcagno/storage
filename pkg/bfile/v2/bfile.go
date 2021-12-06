package v2

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
	sectorSize = 512            // 512 B
	blockSize  = 8 * sectorSize //   4 KB
	chunkSize  = 8 * blockSize  //  32 KB
	extentSize = 8 * chunkSize  // 256 KB

	headerSize         = 12
	recordMaxSize      = blockSize * math.MaxUint16
	asciiUnitSeparator = 0x1F
	statusFree         = uint16(asciiUnitSeparator >> 0 << 10)
	statusActive       = uint16(asciiUnitSeparator >> 1 << 10)
	statusDeleted      = uint16(asciiUnitSeparator >> 2 << 10)
)

var (
	ErrBadOffsetAlignment = errors.New("bad offset; not correctly aligned")
	ErrOffsetOutOfBounds  = errors.New("bad offset; out of bounds")
	ErrDataTooLarge       = errors.New("data exceeds max record size")
	ErrFileClosed         = errors.New("file is closed")
	ErrScannerSkip        = errors.New("skip to next place with scanner/iterator")
)

// align block aligns the provided size
func align(size int64) int64 {
	if size > 0 {
		return ((size + 2) + blockSize - 1) &^ (blockSize - 1)
	}
	return blockSize
}

// at takes a block position and converts it to a block offset
func at(n int) int64 {
	return int64(n) * blockSize
}

// getOffset returns a block aligned offset for the provided position
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

/*
 * To save space, the header could always be adjusted as such:
 *
 * // header size would be reduced in half (12 down to 6 bytes)
 * type header struct {
 *     status  uint8 	//
 *     blocks  uint8 	// max block count of 255
 *     length  uint16 	// max length (aka record size) of 65535 bytes
 *     padding uint16	// max padding of 65535 bytes (also, padding could be removed)
 * }
 *
 */

// header represents a record header
type header struct {
	status  uint16 // status of the record
	blocks  uint16 // blocks used by the record
	length  uint32 // length of the record data
	padding uint32 // padding is the extra unused bytes in the block
}

// readHeader reads and fills out a header from the provided io.Reader
func (hdr *header) readHeader(r io.Reader) (int, error) {
	// make buffer for record header
	buf := make([]byte, headerSize)
	// read in entire record header
	n, err := r.Read(buf)
	if err != nil {
		return n, err
	}
	// decode status
	hdr.status = binary.LittleEndian.Uint16(buf[0:2])
	// decode blocks
	hdr.blocks = binary.LittleEndian.Uint16(buf[2:4])
	// decode length
	hdr.length = binary.LittleEndian.Uint32(buf[4:8])
	// decode padding
	hdr.padding = binary.LittleEndian.Uint32(buf[8:12])
	// return bytes read and nil
	return n, nil
}

// readHeaderAt reads and fills out a header from the provided io.ReaderAt
func (hdr *header) readHeaderAt(r io.ReaderAt, off int64) (int, error) {
	// make buffer for record header
	buf := make([]byte, headerSize)
	// read in entire record header
	n, err := r.ReadAt(buf, off)
	if err != nil {
		return n, err
	}
	// decode status
	hdr.status = binary.LittleEndian.Uint16(buf[0:2])
	// decode blocks
	hdr.blocks = binary.LittleEndian.Uint16(buf[2:4])
	// decode length
	hdr.length = binary.LittleEndian.Uint32(buf[4:8])
	// decode padding
	hdr.padding = binary.LittleEndian.Uint32(buf[8:12])
	// return bytes read and nil
	return n, nil
}

// writeHeader writes a header to the provided io.Writer
func (hdr *header) writeHeader(w io.Writer) (int, error) {
	// make buffer to encode record header into
	buf := make([]byte, headerSize)
	// encode status
	binary.LittleEndian.PutUint16(buf[0:2], hdr.status)
	// encode blocks
	binary.LittleEndian.PutUint16(buf[2:4], hdr.blocks)
	// encode length
	binary.LittleEndian.PutUint32(buf[4:8], hdr.length)
	// encode padding
	binary.LittleEndian.PutUint32(buf[8:12], hdr.padding)
	// write record header
	return w.Write(buf)
}

// writeHeaderAt writes a header to the provided io.WriterAt
func (hdr *header) writeHeaderAt(w io.WriterAt, offset int64) (int, error) {
	// make buffer to encode record header into
	buf := make([]byte, headerSize)
	// encode status
	binary.LittleEndian.PutUint16(buf[0:2], hdr.status)
	// encode blocks
	binary.LittleEndian.PutUint16(buf[2:4], hdr.blocks)
	// encode length
	binary.LittleEndian.PutUint32(buf[4:8], hdr.length)
	// encode padding
	binary.LittleEndian.PutUint32(buf[8:12], hdr.padding)
	// write record header at
	return w.WriteAt(buf, offset)
}

// record represents a data record on disk
type record struct {
	*header        // record header
	data    []byte // record data
}

// makeRecord creates and returns a new record with the provided data
func makeRecord(d []byte) (*record, error) {
	// calc "overhead"
	overhead := headerSize + int64(len(d))
	// get aligned size
	size := align(overhead)
	// error check
	if size > recordMaxSize {
		return nil, ErrDataTooLarge
	}
	// create record
	rd := &record{
		// create and fill record header
		header: &header{
			status:  statusActive,
			blocks:  uint16(size / blockSize),
			length:  uint32(len(d)),
			padding: uint32(size - overhead),
		},
		data: d,
	}
	// return record
	return rd, nil
}

// String is a stringer method for a record
func (rec *record) String() string {
	s := fmt.Sprintf("record:\n")
	s += fmt.Sprintf("\theader:\n")
	s += fmt.Sprintf("\t\tstatus: %d\n", rec.header.status)
	s += fmt.Sprintf("\t\tblocks: %d\n", rec.header.blocks)
	s += fmt.Sprintf("\t\tlength: %d\n", rec.header.length)
	s += fmt.Sprintf("\t\tpadding: %d\n", rec.header.padding)
	s += fmt.Sprintf("\tdata: %s\n", rec.data)
	return s
}

// readRecord reads and fills out a record from the provided io.Reader
func (rec *record) readRecord(r io.Reader) error {
	// create record header
	hdr := new(header)
	// read the record header
	_, err := hdr.readHeader(r)
	if err != nil {
		return err
	}
	// fill out the record and allocate space to read in data
	rec.header = hdr
	rec.data = make([]byte, hdr.length)
	// read data into the record
	_, err = r.Read(rec.data)
	if err != nil {
		return err
	}
	return nil
}

// readRecordAt reads and fills out a record from the provided io.ReaderAt
func (rec *record) readRecordAt(r io.ReaderAt, offset int64) error {
	// create record header
	hdr := new(header)
	// read the record header
	n, err := hdr.readHeaderAt(r, offset)
	if err != nil {
		return err
	}
	// fill out the record and allocate space to read in data
	rec.header = hdr
	rec.data = make([]byte, hdr.length)
	// read data into the record
	_, err = r.ReadAt(rec.data, offset+int64(n))
	if err != nil {
		return err
	}
	return nil
}

// writeRecord writes a record to the provided io.Writer
func (rec *record) writeRecord(w io.Writer) error {
	// write the record header
	_, err := rec.header.writeHeader(w)
	if err != nil {
		return err
	}
	// write the record data
	_, err = w.Write(rec.data)
	if err != nil {
		return err
	}
	// return nil error
	return nil
}

// writeRecordAt writes a record to the provided io.WriterAt
func (rec *record) writeRecordAt(w io.WriterAt, offset int64) error {
	// write the record header
	n, err := rec.header.writeHeaderAt(w, offset)
	if err != nil {
		return err
	}
	// write the record data
	_, err = w.WriteAt(rec.data, offset+int64(n))
	if err != nil {
		return err
	}
	// return nil error
	return nil
}

// bfile represents a block file
type bfile struct {
	path  string  // path is the path in which the file pointer is pointing to
	size  int64   // size reports the current file size
	open  bool    // open reports true if the file is open
	index []int64 // index is the record offset index
	fp    *os.File
}

// openBFile opens and returns a new or existing bfile
func openBFile(name string) (*bfile, error) {
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
	bf := &bfile{
		path:  path,
		size:  fi.Size(),
		open:  true,
		index: make([]int64, 0),
		fp:    f,
	}
	// call init
	err = bf.init()
	if err != nil {
		return nil, err
	}
	// return blockFile
	return bf, nil
}

// init initializes the bfile
func (bf *bfile) init() error {
	// init offset
	var offset int64
	// iterate and fill out
	for {
		// create record header
		hdr := new(header)
		// read the record header
		n, err := hdr.readHeaderAt(bf.fp, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// add record offset to index
		bf.index = append(bf.index, offset)
		// increment offset
		offset += int64(uint32(n) + hdr.length + hdr.padding)
		// keep on iterating till we reach the end of the file
	}
	// no errors to return
	return nil
}

// check performs basic error checking before a call to read, readAt, write or writeAt
func (bf *bfile) check(off int64) error {
	// check offset alignment
	if off != -1 && off%blockSize != 0 {
		return ErrBadOffsetAlignment
	}
	// check offset bounds
	if off != -1 && off > bf.size {
		return ErrOffsetOutOfBounds
	}
	// check to make sure file is open
	if !bf.open {
		return ErrFileClosed
	}
	return nil
}

// read attempts to read and return the data from the next record in the file.
// After a successful call to read, the file pointer is advanced ahead to the
// offset of the start of the next record. It returns an error if the call fails
// for any reason.
func (bf *bfile) read() ([]byte, error) {
	// error check
	err := bf.check(-1)
	if err != nil {
		return nil, err
	}
	// allocate new record to read into
	rec := new(record)
	// read record
	err = rec.readRecord(bf.fp)
	if err != nil {
		return nil, err
	}
	// skip to the next record offset
	_, err = bf.fp.Seek(int64(rec.padding), io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	// return record data
	return rec.data, nil
}

// readRaw attempts to read and return the actual record located in the file.
// After a successful call to read, the file pointer is advanced ahead to the
// offset of the start of the next record. It returns an error if the call fails
// for any reason.
func (bf *bfile) readRaw() (*record, error) {
	// error check
	err := bf.check(-1)
	if err != nil {
		return nil, err
	}
	// allocate new record to read into
	rec := new(record)
	// read record
	err = rec.readRecord(bf.fp)
	if err != nil {
		return nil, err
	}
	// skip to the next record offset
	_, err = bf.fp.Seek(int64(rec.padding), io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	// return record data
	return rec, nil
}

// readAt attempts to read and return the data from the next record in the file
// located at the provided offset. The file pointer is not advanced in a readAt
// call. It returns an error if the call fails for any reason.
func (bf *bfile) readAt(off int64) ([]byte, error) {
	// error check
	err := bf.check(off)
	if err != nil {
		return nil, err
	}
	// allocate new record to read into
	rec := new(record)
	// read record
	err = rec.readRecordAt(bf.fp, off)
	if err != nil {
		return nil, err
	}
	// return record data
	return rec.data, nil
}

// readAtIndex attempts to read and return the data from the next record in the file
// located at the provided index key. The file pointer is not advanced in a readAt
// call. It returns an error if the call fails for any reason.
func (bf *bfile) readAtIndex(i int) ([]byte, error) {
	// get offset from index
	off := bf.index[i]
	// error check
	err := bf.check(off)
	if err != nil {
		return nil, err
	}
	// allocate new record to read into
	rec := new(record)
	// read record
	err = rec.readRecordAt(bf.fp, off)
	if err != nil {
		return nil, err
	}
	// return record data
	return rec.data, nil
}

// write takes the provided data and creates a new record. It then attempts to
// write the record to disk. After a successful write the file pointer is advanced
// ahead to the offset of the start of the next record; it will also return the
// offset in the underlying file where the beginning of the record was written.
// It returns an error if the call fails for any reason.
func (bf *bfile) write(data []byte) (int64, error) {
	// error check
	err := bf.check(-1)
	if err != nil {
		return -1, err
	}
	// create record from provided data
	rec, err := makeRecord(data)
	if err != nil {
		return -1, err
	}
	// get offset to return later
	off, err := bf.fp.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	// write the record to disk
	err = rec.writeRecord(bf.fp)
	if err != nil {
		return -1, err
	}
	// advance the file pointer to the next alignment offset
	_, err = bf.fp.Seek(int64(rec.padding), io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	// add write to size
	bf.size += int64(rec.blocks) * blockSize
	// return offset where record was written
	return off, nil
}

// writeAt takes the provided data and creates a new record. It then attempts to
// write the record to disk using the provided offset. The function will not advance
// the file pointer, and it will not return the offset of where the record was
// written to in the underlying file (because it was provided.) It returns an error
// if the call fails for any reason.
func (bf *bfile) writeAt(data []byte, off int64) error {
	// error check
	err := bf.check(off)
	if err != nil {
		return err
	}
	// create record from provided data
	rec, err := makeRecord(data)
	if err != nil {
		return err
	}
	// write the record to disk
	err = rec.writeRecordAt(bf.fp, off)
	if err != nil {
		return err
	}
	return nil
}

// rewind moves the file pointer back to the beginning
func (bf *bfile) rewind() error {
	if !bf.open {
		return ErrFileClosed
	}
	_, err := bf.fp.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	return nil
}

// seek calls the seek operation of the underlying file pointer
func (bf *bfile) seek(offset int64, whence int) error {
	if !bf.open {
		return ErrFileClosed
	}
	_, err := bf.fp.Seek(offset, whence)
	if err != nil {
		return err
	}
	return nil
}

// count returns the offset index count (which should always match the record count
func (bf *bfile) count() int {
	return len(bf.index)
}

// sync calls Sync on the underlying *os.File
func (bf *bfile) sync() error {
	if !bf.open {
		return ErrFileClosed
	}
	err := bf.fp.Sync()
	if err != nil {
		return err
	}
	return nil
}

// close calls Sync and then Close on the underlying *os.File
func (bf *bfile) close() error {
	if !bf.open {
		return ErrFileClosed
	}
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
