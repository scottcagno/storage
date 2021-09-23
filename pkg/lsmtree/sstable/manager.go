package sstable

import (
	"log"
	"path/filepath"
)

type SSManager struct {
	base string // base is the base path of the db
}

func MergeSSTables(base string, ssi1, ssi2 int64) error {
	// make sure we are working with absolute paths
	base, err := filepath.Abs(base)
	if err != nil {
		return err
	}
	// sanitize any path separators
	base = filepath.ToSlash(base)
	// get full file path for ssi1
	path1 := filepath.Join(base, IndexFileNameFromIndex(ssi1))
	// get full file path for ssi2
	path2 := filepath.Join(base, IndexFileNameFromIndex(ssi2))
	// start merging...
	log.SetPrefix("[INFO] ")
	log.Printf("Loading: %q, and %q to prepare for merge.\n", filepath.Base(path1), filepath.Base(path2))
	return nil
}
