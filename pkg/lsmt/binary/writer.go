package binary

import (
	"io"
	"os"
)

// Writer provides a write-only file descriptor
type Writer struct {
	path   string   // path of the file that is currently open
	fd     *os.File // underlying file to write to
	doSync bool     // should sync on every call (default: true)
	open   bool     // is the file open
}

// OpenWriter returns a *writer for the file at the provided path
func OpenWriter(path string) (*Writer, error) {
	// open file at specified path
	fd, err := os.OpenFile(path, os.O_WRONLY, 0666)
	if err != nil {
		return nil, err
	}
	// seek to the end of the current file to continue appending data
	_, err = fd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	// return new writer
	return &Writer{
		path:   path,
		fd:     fd,
		doSync: true,
		open:   true,
	}, nil
}

func OpenWriterWithSync(path string, doSync bool) (*Writer, error) {
	w, err := OpenWriter(path)
	if err != nil {
		return nil, err
	}
	w.doSync = doSync
	return w, nil
}

func (w *Writer) SetSyncOnWrite(ok bool) {
	w.doSync = ok
}

// WriteIndex writes the provided entry index to disk
func (w *Writer) WriteIndex(e *Index) (int64, error) {
	// call encode entry
	offset, err := EncodeIndex(w.fd, e)
	if err != nil {
		return -1, err
	}
	// make sure we call sync!!
	if w.doSync {
		err = w.fd.Sync()
		if err != nil {
			return -1, err
		}
	}
	return offset, err
}

// WriteEntry writes the provided entry to disk
func (w *Writer) WriteEntry(e *Entry) (int64, error) {
	// call encode entry
	offset, err := EncodeEntry(w.fd, e)
	if err != nil {
		return -1, err
	}
	// make sure we call sync!!
	if w.doSync {
		err = w.fd.Sync()
		if err != nil {
			return -1, err
		}
	}
	return offset, err
}

// Offset returns the *Writer's current file pointer offset
func (w *Writer) Offset() (int64, error) {
	// check to make sure file is not closed
	if !w.open {
		return -1, ErrFileClosed
	}
	// return current offset using seek
	return w.fd.Seek(0, io.SeekCurrent)
}

// Offset returns the *ReadWriteSeeker's current file pointer offset
func Offset(rw io.ReadWriteSeeker) (int64, error) {
	// check to make sure file is not closed
	// return current offset using seek
	return rw.Seek(0, io.SeekCurrent)
}

// Close syncs and closes the *writer
func (w *Writer) Close() error {
	// ensure file is not closed
	if !w.open {
		return ErrFileClosed
	}
	// flush any cached or buffered data to the drive
	err := w.fd.Sync()
	if err != nil {
		return err
	}
	// close writer
	err = w.fd.Close()
	if err != nil {
		return err
	}
	w.open = false
	w.path = ""
	return nil
}

// Sync syncs
func (w *Writer) Sync() error {
	// flush buffer to disk
	err := w.fd.Sync()
	if err != nil {
		return err
	}
	return nil
}
