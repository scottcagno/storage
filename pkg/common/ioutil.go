package common

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

func OpenTempFile(dir, file string) *os.File {
	f, err := ioutil.TempFile(dir, file)
	if err != nil {
		log.Panicf("opening temp file: %v", err)
		return nil
	}
	return f
}

func MkDir(name string) string {
	d, err := ioutil.TempDir("", name)
	if err != nil {
		log.Panicf("creating temp dir: %v", err)
	}
	return d
}

func WriteData(fd *os.File, data []byte) {
	fd.Seek(0, 0)
	if _, err := fd.Write(data); err != nil {
		log.Panicf("writing: %v", err)
	}
}

func ReadData(fd *os.File, data []byte) {
	fd.Seek(0, 0)
	if _, err := fd.Read(data); err != nil {
		log.Panicf("reading: %v", err)
	}
}

func CreateFile(path string) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		dir, file := filepath.Split(path)
		err = os.MkdirAll(dir, os.ModeDir)
		if err != nil {
			return err
		}
		fd, err := os.Create(dir + file)
		if err != nil {
			return err
		}
		err = fd.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func CreateFileSize(path string, size int64) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		dir, file := filepath.Split(path)
		err = os.MkdirAll(dir, os.ModeDir)
		if err != nil {
			return err
		}
		fd, err := os.Create(dir + file)
		if err != nil {
			return err
		}
		err = fd.Truncate(size)
		if err != nil {
			return err
		}
		err = fd.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func TruncateFile(fd *os.File, size int64) error {
	err := fd.Truncate(size)
	if err != nil {
		return err
	}
	return nil
}

func CleanPath(path string) (string, error) {
	return filepath.Abs(path)
}

func OpenFile(path string) (*os.File, error) {
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
	fd, err := os.OpenFile(path, os.O_RDWR, 0666) // os.ModeSticky
	if err != nil {
		return nil, err
	}
	return fd, nil
}

func ListDir(path string) error {
	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, file := range files {
		fmt.Println(file.Name())
	}
	return nil
}

func WalkDir(path string) error {
	root := path
	fileSystem := os.DirFS(root)
	fn := func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return nil
	}
	return fs.WalkDir(fileSystem, ".", fn)
}
