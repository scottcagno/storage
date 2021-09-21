package binary

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrFileClosed = errors.New("error: file is closed")
	ErrBadEntry   = errors.New("error: bad entry")
)

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
func (d *DataFile) WriteEntry(de *DataEntry) (int64, error) {
	// lock
	d.Lock()
	defer d.Unlock()
	// write entry (sequentially, append-only)
	return d.w.WriteEntry(de)
}

// WriteEntryIndex writes an entry in an append-only fashion
func (d *DataFile) WriteEntryIndex(ei *EntryIndex) (int64, error) {
	// lock
	d.Lock()
	defer d.Unlock()
	// write entry (sequentially, append-only)
	return d.w.WriteEntryIndex(ei)
}

// ReadEntry attempts to read and return the next entry sequentially
func (d *DataFile) ReadEntry() (*DataEntry, error) {
	// read lock
	d.RLock()
	defer d.RUnlock()
	// read next entry sequentially
	return d.r.ReadEntry()
}

// ReadEntryIndex attempts to read and return the next entry sequentially
func (d *DataFile) ReadEntryIndex() (*EntryIndex, error) {
	// read lock
	d.RLock()
	defer d.RUnlock()
	// read next entry sequentially
	return d.r.ReadEntryIndex()
}

// ReadEntryAt attempts to read and return an entry at the specified offset
func (d *DataFile) ReadEntryAt(offset int64) (*DataEntry, error) {
	// lock
	d.RLock()
	defer d.RUnlock()
	// read entry at specified offset
	return d.r.ReadEntryAt(offset)
}

// ReadEntryIndexAt attempts to read and return an entry at the specified offset
func (d *DataFile) ReadEntryIndexAt(offset int64) (*EntryIndex, error) {
	// lock
	d.RLock()
	defer d.RUnlock()
	// read entry at specified offset
	return d.r.ReadEntryIndexAt(offset)
}

// Range iterates the entries as long as the provided boolean function is true
func (d *DataFile) Range(iter func(de *DataEntry) bool) error {
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
		de, err := d.r.ReadEntry()
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// entry is good, lets pass it to our boolean function
		if !iter(de) {
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

// Offset is a helper function that returns the current
// offset of the provided reader or writer
func Offset(rw io.ReadWriteSeeker) (int64, error) {
	return rw.Seek(0, io.SeekCurrent)
}
