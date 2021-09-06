// +build !windows

package common

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
