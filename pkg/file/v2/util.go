package v2

import (
	"fmt"
	"os"
	"path/filepath"
)

// clean sanitizes a given path
func clean(path string) string {
	return ""
}

// name formats and returns a log name based on an index
func fileName(index uint64) string {
	return fmt.Sprintf("%s%020d%s", logPrefix, index, logSuffix)
}

func openFile(path string) (*os.File, error) {
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
	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666) // os.ModeSticky
	if err != nil {
		return nil, err
	}
	return fd, nil
}
