package binary

import (
	"encoding/binary"
	"io"
	"os"
)

// Reader provides a read-only file descriptor
type Reader struct {
	path string   // path of the file that is currently open
	fd   *os.File // underlying file to read from
	open bool     // is the file open
}

// OpenReader returns a *reader for the file at the provided path
func OpenReader(path string) (*Reader, error) {
	// open file at specified path
	fd, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	// return new reader
	return &Reader{
		path: path,
		fd:   fd,
		open: true,
	}, nil
}

// ReadFrom checks the given path and if it matches, simply returns
// the same reader, but if it is different it opens a new one recycling
// the same file descriptor. this allows you to read from multiple files
// fairly quickly and pain free.
func (r *Reader) ReadFrom(path string) (*Reader, error) {
	// if there is already a file opened
	if r.open {
		// and if that file has the same path, simply return r
		if r.path == path {
			return r, nil
		}
		// otherwise, a file is still opened at a different
		// location, so we must close it before we continue
		err := r.Close()
		if err != nil {
			return nil, err
		}
	}
	// open a file at a new path (if we're here then the file is closed)
	fd, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	r.path = path
	r.fd = fd
	r.open = true
	return r, nil
}

func DecodeEntry(r io.Reader) (*DataEntry, error) {
	// make buffer
	buf := make([]byte, 26)
	// read entry id
	_, err := r.Read(buf[0:10])
	if err != nil {
		return nil, err
	}
	// read entry key length
	_, err = r.Read(buf[10:18])
	if err != nil {
		return nil, err
	}
	// read entry value length
	_, err = r.Read(buf[18:26])
	if err != nil {
		return nil, err
	}
	// decode id
	id, _ := binary.Varint(buf[0:10])
	// decode key length
	klen := binary.LittleEndian.Uint64(buf[10:18])
	// decode value length
	vlen := binary.LittleEndian.Uint64(buf[18:26])
	// make entry to read data into
	e := &DataEntry{
		Id:    id,
		Key:   make([]byte, klen),
		Value: make([]byte, vlen),
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
	// return entry
	return e, nil
}

func DecodeEntryAt(r io.ReaderAt, offset int64) (*DataEntry, error) {
	// make buffer
	buf := make([]byte, 26)
	// read entry id
	n, err := r.ReadAt(buf[0:10], offset)
	if err != nil {
		return nil, err
	}
	// update offset
	offset += int64(n)
	// read entry key length
	n, err = r.ReadAt(buf[10:18], offset)
	if err != nil {
		return nil, err
	}
	// update offset
	offset += int64(n)
	// read entry value length
	n, err = r.ReadAt(buf[18:26], offset)
	if err != nil {
		return nil, err
	}
	// update offset for reading key data a bit below
	offset += int64(n)
	// decode id
	id, _ := binary.Varint(buf[0:10])
	// decode key length
	klen := binary.LittleEndian.Uint64(buf[10:18])
	// decode value length
	vlen := binary.LittleEndian.Uint64(buf[18:26])
	// make entry to read data into
	e := &DataEntry{
		Id:    id,
		Key:   make([]byte, klen),
		Value: make([]byte, vlen),
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
	// return entry
	return e, nil
}

// ReadEntryIndex reads the next encoded entry index, sequentially
func (r *Reader) ReadEntryIndex() (*EntryIndex, error) {
	// check to make sure file is open
	if !r.open {
		return nil, ErrFileClosed
	}
	// call decode entry
	return DecodeEntryIndex(r.fd)
}

// ReadEntryIndexAt reads the encoded entry index at the offset provided
func (r *Reader) ReadEntryIndexAt(offset int64) (*EntryIndex, error) {
	// check to make sure file is open
	if !r.open {
		return nil, ErrFileClosed
	}
	// call decode entry at
	return DecodeEntryIndexAt(r.fd, offset)
}

// ReadEntry reads the next encoded entry, sequentially
func (r *Reader) ReadEntry() (*DataEntry, error) {
	// check to make sure file is open
	if !r.open {
		return nil, ErrFileClosed
	}
	// call decode entry
	return DecodeEntry(r.fd)
}

// ReadEntryAt reads the encoded entry at the offset provided
func (r *Reader) ReadEntryAt(offset int64) (*DataEntry, error) {
	// check to make sure file is open
	if !r.open {
		return nil, ErrFileClosed
	}
	// call decode entry at
	return DecodeEntryAt(r.fd, offset)
}

// Offset returns the *Reader's current file pointer offset
func (r *Reader) Offset() (int64, error) {
	// check to make sure file is open
	if !r.open {
		return -1, ErrFileClosed
	}
	// return current offset
	return r.fd.Seek(0, io.SeekCurrent)
}

// Seek exposes io.Seeker
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	// check to make sure file is open
	if !r.open {
		return -1, ErrFileClosed
	}
	// seek to offset according to whence
	return r.fd.Seek(offset, whence)
}

// Close simply closes the *Reader
func (r *Reader) Close() error {
	// check to make sure file is not already closed
	if !r.open {
		return ErrFileClosed
	}
	// close the reader
	err := r.fd.Close()
	if err != nil {
		return err
	}
	r.open = false
	r.path = ""
	return nil
}
