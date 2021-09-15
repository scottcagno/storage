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

// ReadEntry reads the next encoded entry, sequentially
func (r *Reader) ReadEntry() (*Entry, error) {
	// make buffer
	buf := make([]byte, 24)
	// read entry id
	_, err := r.fd.Read(buf[0:8])
	if err != nil {
		return nil, err
	}
	// read entry key length
	_, err = r.fd.Read(buf[8:16])
	if err != nil {
		return nil, err
	}
	// read entry value length
	_, err = r.fd.Read(buf[16:24])
	if err != nil {
		return nil, err
	}
	// decode id
	id := binary.LittleEndian.Uint64(buf[0:8])
	// decode key length
	klen := binary.LittleEndian.Uint64(buf[8:16])
	// decode value length
	vlen := binary.LittleEndian.Uint64(buf[16:24])
	// make entry to read data into
	e := &Entry{
		Id:    id,
		Key:   make([]byte, klen),
		Value: make([]byte, vlen),
	}
	// read key from data into entry key
	_, err = r.fd.Read(e.Key)
	if err != nil {
		return nil, err
	}
	// read value key from data into entry value
	_, err = r.fd.Read(e.Value)
	if err != nil {
		return nil, err
	}
	// return entry
	return e, nil
}

// ReadEntryAt reads an encoded entry at the specified offset
func (r *Reader) ReadEntryAt(offset int64) (*Entry, error) {
	// make buffer
	buf := make([]byte, 24)
	// read entry id
	n, err := r.fd.ReadAt(buf[0:8], offset)
	if err != nil {
		return nil, err
	}
	// update offset
	offset += int64(n)
	// read entry key length
	n, err = r.fd.ReadAt(buf[8:16], offset)
	if err != nil {
		return nil, err
	}
	// update offset
	offset += int64(n)
	// read entry value length
	n, err = r.fd.ReadAt(buf[16:24], offset)
	if err != nil {
		return nil, err
	}
	// update offset for reading key data a bit below
	offset += int64(n)
	// decode id
	id := binary.LittleEndian.Uint64(buf[0:8])
	// decode key length
	klen := binary.LittleEndian.Uint64(buf[8:16])
	// decode value length
	vlen := binary.LittleEndian.Uint64(buf[16:24])
	// make entry to read data into
	e := &Entry{
		Id:    id,
		Key:   make([]byte, klen),
		Value: make([]byte, vlen),
	}
	// read key from data into entry key
	n, err = r.fd.ReadAt(e.Key, offset)
	if err != nil {
		return nil, err
	}
	// update offset
	offset += int64(n)
	// read value key from data into entry value
	_, err = r.fd.ReadAt(e.Value, offset)
	if err != nil {
		return nil, err
	}
	// return entry
	return e, nil
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
