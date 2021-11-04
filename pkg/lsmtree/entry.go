package lsmtree

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"unsafe"
)

// Index is a binary entry index
type Index struct {
	Key    []byte
	Offset int64
}

// String is the stringer method for a *Index
func (i *Index) String() string {
	return fmt.Sprintf("index.key=%q, index.offset=%d", i.Key, i.Offset)
}

// Tombstone is a marker for an entry that has been deleted
var Tombstone = []byte{0xDE, 0xAD, 0xBE, 0xEF}

func makeTombstone() []byte {
	data := make([]byte, 4)
	copy(data, Tombstone)
	return data
}

// EntryHeader is mainly used for serialization
type EntryHeader struct {
	klen uint32
	vlen uint32
	crc  uint32
}

func (e *Entry) getEntryHeader() *EntryHeader {
	return &EntryHeader{
		klen: uint32(len(e.Key)),
		vlen: uint32(len(e.Value)),
		crc:  e.CRC,
	}
}

// Entry represents a single entry or record
type Entry struct {
	Key   []byte
	Value []byte
	CRC   uint32
}

func (e *Entry) hasTombstone() bool {
	return bytes.Equal(e.Value, Tombstone)
}

// String is the stringer method for an *Entry
func (e *Entry) String() string {
	return fmt.Sprintf("entry.Key=%q, entry.Value=%q, entry.CRC=%d",
		e.Key, e.Value, e.CRC)
}

// Size returns the size in bytes for an entry
func (e *Entry) Size() int64 {
	ks := int(unsafe.Sizeof(e.Key)) + len(e.Key)
	vs := int(unsafe.Sizeof(e.Value)) + len(e.Value)
	cs := unsafe.Sizeof(e.CRC)
	return int64(ks) + int64(vs) + int64(cs)
}

// Batch is a set of entries
type Batch struct {
	Entries []*Entry
	size    int64
}

// NewBatch instantiates a new batch of entries
func NewBatch() *Batch {
	return &Batch{
		Entries: make([]*Entry, 0),
	}
}

// Size returns the batch size in bytes
func (b *Batch) Size() int64 {
	return b.size
}

// Write writes a new entry to the batch
func (b *Batch) Write(key, value []byte) error {
	return b.writeEntry(&Entry{Key: key, Value: value})
}

// writeEntry is the internal write implementation
func (b *Batch) writeEntry(e *Entry) error {
	// make checksum for entry
	e.CRC = checksum(append(e.Key, e.Value...))
	// check entry
	err := checkEntry(e)
	if err != nil {
		return err
	}
	// add size
	b.size += e.Size()
	// write entry to batch
	b.Entries = append(b.Entries, e)
	// check size
	if b.size >= defaultFlushThreshold {
		// if batch has met or exceeded flush threshold
		return ErrFlushThreshold
	}
	return nil
}

// Discard just vaporizes a batch
func (b *Batch) Discard() {
	for i := range b.Entries {
		b.Entries[i].Key = nil
		b.Entries[i].Value = nil
	}
	b.Entries = nil
	b.size = -1
	b = nil
}

// Len [implementing sort interface]
func (b *Batch) Len() int {
	return len(b.Entries)
}

// Less [implementing sort interface]
func (b *Batch) Less(i, j int) bool {
	return bytes.Compare(b.Entries[i].Key, b.Entries[j].Key) == -1
}

// Swap [implementing sort interface]
func (b *Batch) Swap(i, j int) {
	b.Entries[i], b.Entries[j] = b.Entries[j], b.Entries[i]
}

// checksum is the checksum calculator used with an entry
func checksum(data []byte) uint32 {
	return crc32.Checksum(data, crc32.MakeTable(crc32.Koopman))
}

// checkEntry ensures the entry does not violate the max key and value config
func checkEntry(e *Entry) error {
	// init err
	var err error
	// key checks
	err = checkKey(e)
	if err != nil {
		return err
	}
	// value checks
	err = checkValue(e)
	if err != nil {
		return err
	}
	return nil
}

// checkKey checks the entry key size is okay
func checkKey(e *Entry) error {
	if e.Key == nil || len(e.Key) < minKeySizeAllowed {
		return ErrBadKey
	}
	if int64(len(e.Key)) > maxKeySizeAllowed {
		return ErrKeyTooLarge
	}
	return nil
}

// checkValue checks the entry value size is okay
func checkValue(e *Entry) error {
	if e.Value == nil || len(e.Value) < minValueSizeAllowed {
		return ErrBadValue
	}
	if int64(len(e.Value)) > maxValueSizeAllowed {
		return ErrValueTooLarge
	}
	return nil
}

// checkCRC verifies the crc32 checksum is correct
func checkCRC(e *Entry, crc uint32) error {
	if e.CRC != crc {
		return ErrBadChecksum
	}
	return nil
}
