package lsmtree

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func initBasePath(base string) (string, error) {
	path, err := filepath.Abs(base)
	if err != nil {
		return "", err
	}
	// sanitize any path separators
	path = filepath.ToSlash(path)
	// create any directories if they are not there
	err = os.MkdirAll(path, os.ModeDir)
	if err != nil {
		return "", err
	}
	// return "sanitized" path
	return path, nil
}

func GetTempFileForTesting(t *testing.T, fn func(file *os.File)) {
	fd, err := os.CreateTemp(t.TempDir(), "tmp-file-*.txt")
	if err != nil {
		t.Fatalf("create and open temp dir and file: %v\n", err)
	}
	defer func(fd *os.File) {
		err := fd.Close()
		if err != nil {
			t.Fatalf("defferred close of file: %v\n", err)
		}
	}(fd)
	fn(fd)
}

func FileIsOpen(fd *os.File) bool {
	if fd == nil {
		return false
	}
	_, err := fd.Seek(0, io.SeekCurrent)
	if err != nil {
		if errors.Is(err, os.ErrClosed) {
			return false
		}
		return false
	}
	return true
}
