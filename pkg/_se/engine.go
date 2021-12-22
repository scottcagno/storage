package _se

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	pageSize = 64
)

func align(size, mask int) int {
	return (size + mask) &^ (mask)
}

type span struct {
	start int
	end   int
}

type engine struct {
	fp    *os.File
	size  int64
	index []int64
}

func openFile(path string) (*os.File, error) {
	// sanitize base path
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	// sanitize any path separators
	path = filepath.ToSlash(path)
	// get dir
	dir, _ := filepath.Split(path)
	// create any directories if they are not there
	err = os.MkdirAll(dir, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// open file
	f, err := os.OpenFile(path, os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	return f, err
}

func openEngine(path string) (*engine, error) {
	// open file
	f, err := openFile(path)
	if err != nil {
		return nil, err
	}
	// get file size
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	// init engine
	e := &engine{
		fp:    f,
		size:  fi.Size(),
		index: make([]int64, 0, 64),
	}
	// setup engine
	err = e.setup()
	if err != nil {
		return nil, err
	}
	// return engine
	return e, nil
}

func (e *engine) setup() error {
	if e.size < 1 {
		return nil
	}
	var i int
	for {
		off, err := e.fp.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		rec, err := e.read()
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		fmt.Printf("%s\n\n", rec)
		if rec.header.magic != magicIdent {
			continue
		}
		e.index = append(e.index, off)
		i++
		fmt.Println(i)
	}
	return nil
}

func (e *engine) write(rec *record) (int, error) {
	n, err := rec.write(e.fp)
	if err != nil {
		return n, err
	}
	return n, nil
}

func (e *engine) writeAt(rec *record, page int) (int, error) {
	return 0, nil
}

func (e *engine) read() (*record, error) {
	r := new(record)
	_, err := r.read(e.fp)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (e *engine) readAt(data []byte, offset int64) (int, error) {

	return 0, nil
}

func (e *engine) seek(off int64, whence int) (int64, error) {
	return 0, nil
}

func (e *engine) flush() error {
	err := e.fp.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (e *engine) close() error {
	err := e.fp.Sync()
	if err != nil {
		return err
	}
	err = e.fp.Close()
	if err != nil {
		return err
	}
	return nil
}
