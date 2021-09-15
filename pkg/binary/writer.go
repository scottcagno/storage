package binary

import (
	"encoding/binary"
	"io"
	"os"
)

// Writer provides a write-only file descriptor
type Writer struct {
	path string   // path of the file that is currently open
	fd   *os.File // underlying file to write to
	open bool     // is the file open
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
		path: path,
		fd:   fd,
		open: true,
	}, nil
}

// WriteEntry writes the provided entry to disk
func (w *Writer) WriteEntry(e *Entry) (int64, error) {
	// error check
	if e == nil {
		return -1, ErrBadEntry
	}
	// get the file pointer offset for the entry
	offset, err := w.Offset()
	if err != nil {
		return -1, err
	}
	// make buffer
	buf := make([]byte, 24)
	// encode and write entry id
	binary.LittleEndian.PutUint64(buf[0:8], e.Id)
	_, err = w.fd.Write(buf[0:8])
	if err != nil {
		return -1, err
	}
	// encode and write entry key length
	binary.LittleEndian.PutUint64(buf[8:16], uint64(len(e.Key)))
	_, err = w.fd.Write(buf[8:16])
	if err != nil {
		return -1, err
	}
	// encode and write entry value length
	binary.LittleEndian.PutUint64(buf[16:24], uint64(len(e.Value)))
	_, err = w.fd.Write(buf[16:24])
	if err != nil {
		return -1, err
	}
	// write entry key
	_, err = w.fd.Write(e.Key)
	if err != nil {
		return -1, err
	}
	// write entry value
	_, err = w.fd.Write(e.Value)
	if err != nil {
		return -1, err
	}
	// perform a sync and force flush to disk
	err = w.fd.Sync()
	if err != nil {
		return -1, err
	}
	return offset, nil
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
