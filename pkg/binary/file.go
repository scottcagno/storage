package binary

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrFileClosed = errors.New("error: file is closed")
	ErrBadEntry   = errors.New("error: bad entry")
)

// Entry is a binary entry
type Entry struct {
	Id    uint64
	Key   []byte
	Value []byte
}

// String is the string method for an *Entry
func (e *Entry) String() string {
	return fmt.Sprintf("entry.id=%d, entry.key=%q, entry.value=%q", e.Id, e.Key, e.Value)
}

// Touch cleans and initializes the path, files and folders
func Touch(path string) (string, error) {
	// get absolute path
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	// convert any slashes
	path = filepath.ToSlash(path)
	// check to see if the path exists
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		// create dirs if they need creating
		dir, file := filepath.Split(path)
		err = os.MkdirAll(dir, os.ModeDir)
		if err != nil {
			return "", err
		}
		// create and files if the need creating
		fd, err := os.Create(dir + file)
		if err != nil {
			return "", err
		}
		// close, because we are just touching them
		err = fd.Close()
		if err != nil {
			return "", err
		}
	}
	// return sanitized path (creating any files or folders)
	return path, nil
}

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

// DataFile is is a syncronized binary reader and writer
type DataFile struct {
	sync.RWMutex
	r *Reader
	w *Writer
}

// OpenDataFile opens and returns a new datafile
func OpenDataFile(path string) (*DataFile, error) {
	// create and sanitize the path
	path, err := Touch(path)
	if err != nil {
		return nil, err
	}
	// open a new reader
	r, err := OpenReader(path)
	if err != nil {
		return nil, err
	}
	// open a new writer
	w, err := OpenWriter(path)
	if err != nil {
		return nil, err
	}
	// init data file and return
	return &DataFile{
		r: r,
		w: w,
	}, nil
}

// WriteEntry writes an entry in an append-only fashion
func (d *DataFile) WriteEntry(e *Entry) (int64, error) {
	// lock
	d.Lock()
	defer d.Unlock()
	// write entry (sequentially, append-only)
	return d.w.WriteEntry(e)
}

// ReadEntry attempts to read and return the next entry sequentially
func (d *DataFile) ReadEntry() (*Entry, error) {
	// read lock
	d.RLock()
	defer d.RUnlock()
	// read next entry sequentially
	return d.r.ReadEntry()
}

// ReadEntryAt attempts to read and return an entry at the specified offset
func (d *DataFile) ReadEntryAt(offset int64) (*Entry, error) {
	// lock
	d.RLock()
	defer d.RUnlock()
	// read entry at specified offset
	return d.r.ReadEntryAt(offset)
}

// Range iterates the entries as long as the provided boolean function is true
func (d *DataFile) Range(iter func(e *Entry) bool) error {
	// lock
	d.Lock()
	defer d.Unlock()
	// grab the reader's offset, so we can set it back later
	offset, err := d.r.Offset()
	if err != nil {
		return err
	}
	// go to the beginning of the file
	_, err = d.r.fd.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	// start loop
	for {
		// read entry, and check for EOF
		e, err := d.r.ReadEntry()
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// entry is good, lets pass it to our boolean function
		if !iter(e) {
			continue // if iter(e) returns false, continue to next entry
		}
	}
	// we are done reading all the entries (hopefully), so
	// we seek back to where we were at the start of this function
	_, err = d.r.fd.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	return nil
}

// Close closes the DataFile
func (d *DataFile) Close() error {
	// lock
	d.Lock()
	defer d.Unlock()
	// close reader
	err := d.r.Close()
	if err != nil {
		return err
	}
	// close writer
	err = d.w.Close()
	if err != nil {
		return err
	}
	return nil
}
