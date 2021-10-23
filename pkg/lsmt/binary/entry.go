package binary

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Entry is a key-value data entry
type Entry struct {
	Key   []byte
	Value []byte
}

// String is the stringer method for a *Entry
func (de *Entry) String() string {
	return fmt.Sprintf("entry.key=%q, entry.value=%q", de.Key, de.Value)
}

// Size returns the approxamite size of the entry in bytes
func (de *Entry) Size() int {
	return len(de.Key) + len(de.Value) + 24

}

// EncodeEntry writes the provided entry to the writer provided
func EncodeEntry(w io.WriteSeeker, e *Entry) (int64, error) {
	// error check
	if e == nil {
		return -1, ErrBadEntry
	}
	// get the file pointer offset for the entry
	offset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	// make buffer
	buf := make([]byte, 16)
	// encode and write entry key length
	binary.LittleEndian.PutUint64(buf[0:8], uint64(len(e.Key)))
	_, err = w.Write(buf[0:8])
	if err != nil {
		return -1, err
	}
	// encode and write entry value length
	binary.LittleEndian.PutUint64(buf[8:16], uint64(len(e.Value)))
	_, err = w.Write(buf[8:16])
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
	return offset, nil
}

// DecodeEntry encodes the next entry from the reader provided
func DecodeEntry(r io.Reader) (*Entry, error) {
	// make buffer
	buf := make([]byte, 16)
	// read entry key length
	_, err := r.Read(buf[0:8])
	if err != nil {
		return nil, err
	}
	// read entry value length
	_, err = r.Read(buf[8:16])
	if err != nil {
		return nil, err
	}
	// decode key length
	klen := binary.LittleEndian.Uint64(buf[0:8])
	// decode value length
	vlen := binary.LittleEndian.Uint64(buf[8:16])
	// make entry to read data into
	e := &Entry{
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

// DecodeEntryAt decodes the entry from the reader provided at the offset provided
func DecodeEntryAt(r io.ReaderAt, offset int64) (*Entry, error) {
	// make buffer
	buf := make([]byte, 16)
	// read entry key length
	n, err := r.ReadAt(buf[0:8], offset)
	if err != nil {
		return nil, err
	}
	// update offset
	offset += int64(n)
	// read entry value length
	n, err = r.ReadAt(buf[8:16], offset)
	if err != nil {
		return nil, err
	}
	// update offset for reading key data a bit below
	offset += int64(n)
	// decode key length
	klen := binary.LittleEndian.Uint64(buf[0:8])
	// decode value length
	vlen := binary.LittleEndian.Uint64(buf[8:16])
	// make entry to read data into
	e := &Entry{
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
