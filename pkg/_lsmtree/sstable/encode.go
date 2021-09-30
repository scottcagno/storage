package sstable

import (
	"encoding/binary"
	"io"
)

func EncodeDataEntry(w io.WriteSeeker, ent *sstDataEntry) (int64, error) {
	// get offset of where this entry is located
	offset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	// make buffer for encoding
	buf := make([]byte, 16)
	// encode key length
	binary.LittleEndian.PutUint64(buf[0:8], uint64(len(ent.key)))
	// encode value length
	binary.LittleEndian.PutUint64(buf[8:16], uint64(len(ent.value)))
	// write key and value length
	_, err = w.Write(buf)
	if err != nil {
		return -1, err
	}
	// write key data
	_, err = w.Write([]byte(ent.key))
	if err != nil {
		return -1, err
	}
	// write value data
	_, err = w.Write(ent.value)
	if err != nil {
		return -1, err
	}
	// return offset of entry
	return offset, nil
}

func EncodeIndexEntry(w io.WriteSeeker, idx *sstIndexEntry) (int64, error) {
	// get offset of where this entry is located
	offset, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}
	// make buffer for encoding
	buf := make([]byte, 16)
	// encode key length
	binary.LittleEndian.PutUint64(buf[0:8], uint64(len(idx.key)))
	// encode offset value
	binary.LittleEndian.PutUint64(buf[8:16], uint64(idx.offset))
	// write key length and offset value
	_, err = w.Write(buf)
	if err != nil {
		return -1, err
	}
	// write key data
	_, err = w.Write([]byte(idx.key))
	if err != nil {
		return -1, err
	}
	// return offset of entry
	return offset, nil
}
