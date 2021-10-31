package lsmtree

import (
	"bytes"
	"hash/crc32"
)

// Tombstone is a marker for an entry that has been deleted
var Tombstone = []byte(nil)

// Entry represents a single entry or record
type Entry struct {
	Key   []byte
	Value []byte
	CRC   uint32
}

// Batch is a set of entries
type Batch struct {
	Entries []*Entry
}

// NewBatch instantiates a new batch of entries
func NewBatch() *Batch {
	return &Batch{
		Entries: make([]*Entry, 0),
	}
}

// Write writes a new entry to the batch
func (b *Batch) Write(key, value []byte) {
	b.writeEntry(&Entry{Key: key, Value: value})
}

// writeEntry is the internal write implementation
func (b *Batch) writeEntry(e *Entry) {
	e.CRC = checksum(append(e.Key, e.Value...))
	b.Entries = append(b.Entries, e)
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
func checkEntry(e *Entry, keyMax, valMax int64) error {
	// init err
	var err error
	// key checks
	err = checkKey(e, keyMax)
	if err != nil {
		return err
	}
	// value checks
	err = checkValue(e, valMax)
	if err != nil {
		return err
	}
	return nil
}

// checkKey checks the entry key size is okay
func checkKey(e *Entry, max int64) error {
	if e.Key == nil || len(e.Key) < minKeySizeAllowed {
		return ErrBadKey
	}
	if int64(len(e.Key)) > max {
		return ErrKeyTooLarge
	}
	return nil
}

// checkValue checks the entry value size is okay
func checkValue(e *Entry, max int64) error {
	if e.Value == nil || len(e.Value) < minValueSizeAllowed {
		return ErrBadValue
	}
	if int64(len(e.Value)) > max {
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
