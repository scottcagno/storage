package main

import (
	"encoding/json"
	"io"
	"os"
)

type RecordFn func(at io.Reader) (json.RawMessage, error)

func MakeIndex(path string, fn RecordFn) error {
	r, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	w, err := os.OpenFile(path, os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	for {
		rec, err := fn(r)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		if !json.Valid(rec) {
			return ErrIncomplete
		}
		_, err = w.Write(rec)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeRecordIndex(w io.Writer, rec json.RawMessage) error {
	// do something here
	return nil
}

type foo struct {
	k int    `json:"k"`
	v string `json:"v"`
}

func (f *foo) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		K int    `json:"k"`
		V string `json:"v"`
	}{
		K: f.k,
		V: f.v,
	})
}

func (f *foo) UnmarshalJSON(data []byte) error {
	type fooer struct {
		K int    `json:"k"`
		V string `json:"v"`
	}
	var ff fooer
	err := json.Unmarshal(data, &ff)
	if err != nil {
		return err
	}
	f.k = ff.K
	f.v = ff.V
	return nil
}
