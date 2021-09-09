package v2

import "fmt"

// clean sanitizes a given path
func clean(path string) string {
	return ""
}

// name formats and returns a log name based on an index
func fileName(index uint64) string {
	return fmt.Sprintf("%s%020d%s", logPrefix, index, logSuffix)
}
