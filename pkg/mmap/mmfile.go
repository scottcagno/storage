package mmap

import (
	"os"
	"path/filepath"
	"reflect"
	"unsafe"
)

// OpenMappedFile prepares a file, calls the initializer if file was just created
// and returns a new mapping of the prepared file into the memory.
func OpenFileMapping(name string, perm os.FileMode, size uintptr, flags Flag, init func(mf *Mapping) error) (*Mapping, error) {
	mf, created, err := func() (*Mapping, bool, error) {
		created := false
		if _, err := os.Stat(name); err != nil && os.IsNotExist(err) {
			created = true
		}
		f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, perm)
		if err != nil {
			return nil, false, err
		}
		defer func() {
			if f != nil {
				_ = f.Close()
			}
		}()
		onFailure := func() {
			_ = f.Close()
			f = nil
			if created {
				_ = os.Remove(name)
			}
		}
		if err := f.Truncate(int64(size)); err != nil {
			onFailure()
			return nil, false, err
		}
		mf, err := Open(f.Fd(), 0, size, ModeReadWrite, flags)
		if err != nil {
			onFailure()
			return nil, false, err
		}
		return mf, created, nil
	}()
	if err != nil {
		return nil, err
	}
	if created && init != nil {
		if err := init(mf); err != nil {
			_ = mf.Close()
			_ = os.Remove(name)
			return nil, err
		}
	}
	return mf, nil
}

func OpenFile(path string) (*os.File, error) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		dir, file := filepath.Split(path)
		err = os.MkdirAll(dir, os.ModeDir)
		if err != nil {
			return nil, err
		}
		fd, err := os.Create(dir + file)
		if err != nil {
			return nil, err
		}
		err = fd.Close()
		if err != nil {
			return fd, err
		}
	}
	fd, err := os.OpenFile(path, os.O_RDWR, os.ModeSticky)
	if err != nil {
		return nil, err
	}
	return fd, nil
}

// UnsafeBytesToString converts bytes to string saving allocations
func UnsafeBytesToString(bytes []byte) string {
	sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))

	return *(*string)(unsafe.Pointer(&reflect.StringHeader{
		Data: sliceHeader.Data,
		Len:  sliceHeader.Len,
	}))
}

// UnsafeStringToBytes converts bytes to string saving allocations by re-using
func UnsafeStringToBytes(s string) []byte {
	stringHeader := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: stringHeader.Data,
		Len:  stringHeader.Len,
		Cap:  stringHeader.Len,
	}))
}
