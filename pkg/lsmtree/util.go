package lsmtree

import (
	"os"
	"path/filepath"
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
