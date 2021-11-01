package lsmtree

import (
	"encoding/binary"
	"io"
)

func readEntryHeader(r io.Reader, hdr *EntryHeader) (int, error) {
	// make header buffer to read data into
	buf := make([]byte, 16)
	// read the header from the underlying reader into the buffer
	n, err := r.Read(buf)
	if err != nil {
		return n, err
	}
	// decode key length
	hdr.klen = binary.LittleEndian.Uint32(buf[0:4])
	// decode value length
	hdr.vlen = binary.LittleEndian.Uint32(buf[4:8])
	// decode crc32 value
	hdr.crc = binary.LittleEndian.Uint32(buf[8:12])
	// skip the last 4 bytes (reserved for future use)
	//
	return n, nil
}

// readEntry reads the entry from the provided io.Reader
// and returns the entry or nil and an error
func readEntry(r io.Reader) (*Entry, error) {
	// make entry header
	hdr := new(EntryHeader)
	// reader entry header from r into EntryHeader
	_, err := readEntryHeader(r, hdr)
	if err != nil {
		return nil, err
	}
	// make entry to read key and value into
	e := &Entry{
		Key:   make([]byte, hdr.klen),
		Value: make([]byte, hdr.vlen),
		CRC:   hdr.crc,
	}
	// read key from data into entry key
	_, err = r.Read(e.Key)
	if err != nil {
		return nil, err
	}
	// read value key from data into entry value
	_, err = r.Read(e.Value)
	if err != nil {
		return nil, err
	}
	// make sure the crc checksum is valid
	crc := checksum(append(e.Key, e.Value...))
	if e.CRC != crc {
		return nil, ErrBadChecksum
	}
	// return entry
	return e, nil
}

func readEntryHeaderAt(r io.ReaderAt, offset int64, hdr *EntryHeader) (int, error) {
	// make header buffer to read data into
	buf := make([]byte, 16)
	// read the header from the underlying reader into the buffer
	n, err := r.ReadAt(buf, offset)
	if err != nil {
		return n, err
	}
	// decode key length
	hdr.klen = binary.LittleEndian.Uint32(buf[0:4])
	// decode value length
	hdr.vlen = binary.LittleEndian.Uint32(buf[4:8])
	// decode crc32 value
	hdr.crc = binary.LittleEndian.Uint32(buf[8:12])
	// skip the last 4 bytes (reserved for future use)
	//
	return n, nil
}

// readEntryAt reads the entry from the provided io.ReaderAt
// and returns the entry or nil and an error
func readEntryAt(r io.ReaderAt, offset int64) (*Entry, error) {
	// make entry header
	hdr := new(EntryHeader)
	// reader entry header from r into EntryHeader
	n, err := readEntryHeaderAt(r, offset, hdr)
	if err != nil {
		return nil, err
	}
	// update offset
	offset += int64(n)
	// make entry to read key and value into
	e := &Entry{
		Key:   make([]byte, hdr.klen),
		Value: make([]byte, hdr.vlen),
		CRC:   hdr.crc,
	}
	// read key from data into entry key
	n, err = r.ReadAt(e.Key, offset)
	if err != nil {
		return nil, err
	}
	// update offset
	offset += int64(n)
	// read value key from data into entry value
	n, err = r.ReadAt(e.Value, offset)
	if err != nil {
		return nil, err
	}
	// update offset
	offset += int64(n)
	// make sure the crc checksum is valid
	crc := checksum(append(e.Key, e.Value...))
	if e.CRC != crc {
		return nil, ErrBadChecksum
	}
	// return entry
	return e, nil
}

func writeEntryHeader(w io.Writer, hdr *EntryHeader) (int, error) {
	// make header buffer to write data into
	buf := make([]byte, 16)
	// encode key length into header
	binary.LittleEndian.PutUint32(buf[0:4], hdr.klen)
	// encode value length into header
	binary.LittleEndian.PutUint32(buf[4:8], hdr.vlen)
	// encode crc32 value into header
	binary.LittleEndian.PutUint32(buf[8:12], hdr.crc)
	// skip the last 4 bytes (reserved for future use)
	//
	// write the header to the underlying writer
	return w.Write(buf)
}

// writeEntry writes the provided entry to the provided io.Writer
func writeEntry(w io.WriteSeeker, e *Entry) (int64, error) {
	// error check
	if e == nil {
		return -1, ErrNilEntry
	}
	// get the file pointer offset for the entry
	offset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	// make entry header
	hdr := &EntryHeader{
		klen: uint32(len(e.Key)),
		vlen: uint32(len(e.Value)),
		crc:  e.CRC,
	}
	// write entry header
	_, err = writeEntryHeader(w, hdr)
	if err != nil {
		return -1, err
	}
	// write entry key
	_, err = w.Write(e.Key)
	if err != nil {
		return -1, err
	}
	// write entry value
	_, err = w.Write(e.Value)
	if err != nil {
		return -1, err
	}
	// return offset
	return offset, nil
}
