package sstable

import (
	"encoding/binary"
	"io"
)

func DecodeDataEntry(r io.Reader) (*sstDataEntry, error) {
	// make buffer for decoding
	buf := make([]byte, 16)
	// read key length
	_, err := r.Read(buf[0:8])
	if err != nil {
		return nil, err
	}
	// read val length
	_, err = r.Read(buf[8:16])
	if err != nil {
		return nil, err
	}
	// decode key length
	klen := binary.LittleEndian.Uint64(buf[0:8])
	// decode val length
	vlen := binary.LittleEndian.Uint64(buf[8:16])
	// make buffer to load the key and value into
	data := make([]byte, klen+vlen)
	// read key and value
	_, err = r.Read(data)
	if err != nil {
		return nil, err
	}
	// fill out sstDataEntry
	ent := &sstDataEntry{
		key:   string(data[0:klen]),
		value: data[klen : klen+vlen],
	}
	// return
	return ent, nil
}

func DecodeIndexEntry(r io.Reader) (*sstIndexEntry, error) {
	// make buffer for decoding
	buf := make([]byte, 16)
	// read key length
	_, err := r.Read(buf[0:8])
	if err != nil {
		return nil, err
	}
	// read data offset
	_, err = r.Read(buf[8:16])
	if err != nil {
		return nil, err
	}
	// decode key length
	keyLength := binary.LittleEndian.Uint64(buf[0:8])
	// decode data offset
	dataOffset := binary.LittleEndian.Uint64(buf[8:16])
	// make buffer to load the key into
	key := make([]byte, keyLength)
	// read key
	_, err = r.Read(key)
	if err != nil {
		return nil, err
	}
	// fill out sstIndexEntry
	idx := &sstIndexEntry{
		key:    string(key),
		offset: int64(dataOffset),
	}
	return idx, nil
}
