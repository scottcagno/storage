package web

import (
	"github.com/scottcagno/storage/pkg/util"
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

func SanitizePath(path string) string {
	abs, _ := util.GetFilepath()
	return filepath.Join(abs, path)
}
