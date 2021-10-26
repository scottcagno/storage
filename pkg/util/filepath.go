package util

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// GetFilepath returns the absolute filepath from where it's called. It returns the
// result as a split string, first the root directory up until the current file and
// returns the two in this order: dir, file
func GetFilepath() (string, string) {
	// Caller reports file and line number information about function invocations on
	// the calling goroutine's stack. The argument skip is the number of stack frames
	// to ascend, with 0 identifying the caller of Caller.
	_, filename, _, ok := runtime.Caller(1)
	if !ok {
		return "", ""
	}
	// clean and split the filepath
	return filepath.Split(filepath.Clean(filename))
}

func CreateBaseDir(base string) error {
	base, err := filepath.Abs(base)
	if err != nil {
		return err
	}
	base = filepath.ToSlash(base)
	err = os.MkdirAll(base, os.ModeDir)
	if err != nil {
		return err
	}
	return nil
}

func UpdateBaseDir(base string) (string, error) {
	base, err := filepath.Abs(base)
	if err != nil {
		return "", err
	}
	base = filepath.ToSlash(base)
	files, err := os.ReadDir(base)
	if err != nil {
		return "", err
	}
	base = filepath.Join(base, fmt.Sprintf("%06d", len(files)))
	_, err = os.Stat(base)
	if os.IsExist(err) {
		return "", nil
	}
	err = os.MkdirAll(base, os.ModeDir)
	if err != nil {
		return "", err
	}
	return base, err
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}
