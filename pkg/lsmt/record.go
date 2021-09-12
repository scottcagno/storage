package lsmt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"time"
)

const (
	statusEmpty    = 0x00
	statusNotEmpty = 0x01
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func getBuf() *bytes.Buffer {
	return bufPool.Get().(*bytes.Buffer)
}

func putBuf(b *bytes.Buffer) {
	b.Reset()
	bufPool.Put(b)
}

type record struct {
	Typ   uint8     // 1 byte
	Ts    time.Time // 14 bytes
	Key   []byte
	Value []byte
}

func encodeRecord(r *record) ([]byte, error) {
	buf := getBuf()
	// encode type
	err := binary.Write(buf, binary.LittleEndian, r.Typ)
	if err != nil {
		return nil, err
	}
	// get time
	btim, err := r.Ts.MarshalBinary()
	if err != nil {
		return nil, err
	}
	// encode key encode time
	err = binary.Write(buf, binary.LittleEndian, btim)
	if err != nil {
		return nil, err
	}
	// encode key length
	err = binary.Write(buf, binary.LittleEndian, len(r.Key))
	if err != nil {
		return nil, err
	}
	// encode value length
	err = binary.Write(buf, binary.LittleEndian, len(r.Value))
	if err != nil {
		return nil, err
	}
	// write key
	err = binary.Write(buf, binary.LittleEndian, r.Key)
	if err != nil {
		return nil, err
	}
	// write value
	err = binary.Write(buf, binary.LittleEndian, r.Value)
	if err != nil {
		return nil, err
	}
	// finalize the binary record and return
	brecord := make([]byte, buf.Len())
	brecord = buf.Bytes()
	// don't forget to "recycle"
	putBuf(buf)
	return brecord, nil
}

var (
	ErrGotEmptyKeyValue = errors.New("error: empty key and/or value")
	ErrBadKeyLength     = errors.New("error: bad key length")
	ErrBadValueLength   = errors.New("error: bad value length")
)

func EncodeRecord(key []byte, value []byte) ([]byte, error) {
	if key == nil || value == nil {
		return nil, ErrGotEmptyKeyValue
	}
	if len(key) > 65535 || len(key) == 0 {
		return nil, ErrBadKeyLength
	}
	if len(value) > 4294967295 || len(value) == 0 {
		return nil, ErrBadValueLength
	}
	r := &record{
		Typ:   statusNotEmpty,
		Ts:    time.Now(),
		Key:   key,
		Value: value,
	}
	return encodeRecord(r)
}

func DecodeRecord(d []byte) (*record, error) {
	typ := d[0]
	var timestamp time.Time
	err := timestamp.UnmarshalBinary(d[1:16])
	if err != nil {
		return nil, err
	}
	keylen := binary.LittleEndian.Uint16(d[16:18])
	vallen := binary.LittleEndian.Uint32(d[18:22])
	offset := 22
	record := &record{
		Typ:   typ,
		Ts:    timestamp,
		Key:   d[offset : offset+int(keylen)],
		Value: d[offset+int(keylen) : offset+int(keylen)+int(vallen)],
	}
	return record, nil
}
