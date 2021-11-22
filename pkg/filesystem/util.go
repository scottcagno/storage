package filesystem

import (
	"io/fs"
	"io/ioutil"
	"path/filepath"
)

func List(dir string, fn func(f fs.FileInfo) error) error {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, f := range files {
		err = fn(f)
		if err != nil {
			if err == fs.SkipDir {
				continue
			}
			return nil
		}
	}
	return nil
}

func Walk(dir string, fn func(f fs.FileInfo) error) error {
	err := filepath.Walk(dir,
		func(path string, f fs.FileInfo, err error) error {
			if err != nil {
				return err
			}
			err = fn(f)
			if err != nil {
				if err == fs.SkipDir {
					return fs.SkipDir
				}
				return err
			}
			return nil
		})
	if err != nil {
		return err
	}
	return nil
}
