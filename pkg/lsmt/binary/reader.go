package binary

import (
	"github.com/scottcagno/storage/pkg/lsmt"
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

// ReadIndex reads the next encoded index, sequentially
func (r *Reader) ReadIndex() (*Index, error) {
	// check to make sure file is open
	if !r.open {
		return nil, lsmt.ErrFileClosed
	}
	// call decode entry
	return DecodeIndex(r.fd)
}

// ReadIndexAt reads the encoded entry index at the offset provided
func (r *Reader) ReadIndexAt(offset int64) (*Index, error) {
	// check to make sure file is open
	if !r.open {
		return nil, lsmt.ErrFileClosed
	}
	// call decode entry at
	return DecodeIndexAt(r.fd, offset)
}

// ReadEntry reads the next encoded entry, sequentially
func (r *Reader) ReadEntry() (*Entry, error) {
	// check to make sure file is open
	if !r.open {
		return nil, lsmt.ErrFileClosed
	}
	// call decode entry
	return DecodeEntry(r.fd)
}

// ReadEntryAt reads the encoded entry at the offset provided
func (r *Reader) ReadEntryAt(offset int64) (*Entry, error) {
	// check to make sure file is open
	if !r.open {
		return nil, lsmt.ErrFileClosed
	}
	// call decode entry at
	return DecodeEntryAt(r.fd, offset)
}

// Offset returns the *Reader's current file pointer offset
func (r *Reader) Offset() (int64, error) {
	// check to make sure file is open
	if !r.open {
		return -1, lsmt.ErrFileClosed
	}
	// return current offset
	return r.fd.Seek(0, io.SeekCurrent)
}

// Seek exposes io.Seeker
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	// check to make sure file is open
	if !r.open {
		return -1, lsmt.ErrFileClosed
	}
	// seek to offset according to whence
	return r.fd.Seek(offset, whence)
}

// Close simply closes the *Reader
func (r *Reader) Close() error {
	// check to make sure file is not already closed
	if !r.open {
		return lsmt.ErrFileClosed
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
