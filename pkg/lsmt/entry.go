package lsmt

import (
	"encoding/binary"
	"time"
)

type Entry struct {
	Type      uint8
	Timestamp time.Time
	Key       string
	Value     []byte
}

func (e *Entry) MarshalBinary() ([]byte, error) {
	data := make([]byte, 32+len(e.Key)+len(e.Value))
	data[0] = e.Type
	ts, err := e.Timestamp.MarshalBinary()
	if err != nil {
		return nil, err
	}
	copy(data[1:16], ts)
	binary.LittleEndian.PutUint64(data[16:24], uint64(len(e.Key)))
	binary.LittleEndian.PutUint64(data[24:32], uint64(len(e.Value)))
	copy(data[32:], e.Key)
	copy(data[32+len(e.Key):], e.Value)
	return data, nil
}

func (e *Entry) UnmarshalBinary(data []byte) error {
	_ = data[31]
	e.Type = data[0]
	var ts time.Time
	err := ts.UnmarshalBinary(data[1:16])
	if err != nil {
		return err
	}
	e.Timestamp = ts
	keylen := binary.LittleEndian.Uint64(data[16:24])
	vallen := binary.LittleEndian.Uint64(data[24:32])
	copy([]byte(e.Key), data[32:32+keylen])
	copy(e.Value, data[32+keylen:32+keylen+vallen])
	return nil
}
