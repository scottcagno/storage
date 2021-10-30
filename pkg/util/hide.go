//go:build !windows
// +build !windows

package util

import (
	"os"
	"path/filepath"
	"strings"
)

func HideFile(filename string) error {
	if !strings.HasPrefix(filepath.Base(filename), ".") {
		err := os.Rename(filename, "."+filename)
		if err != nil {
			return err
		}
	}
	return nil
}
