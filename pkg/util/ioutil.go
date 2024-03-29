package util

import (
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"
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

func CleanPath(path string) (string, string) {
	path, err := filepath.Abs(path)
	if err != nil {
		log.Panicf("cleaning path: %v\n", err)
	}
	return filepath.Split(filepath.ToSlash(path))
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

func WatchFile(path string) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	initialStat, err := os.Stat(path)
	if err != nil {
		return err
	}
	for {
		stat, err := os.Stat(path)
		if err != nil {
			return err
		}
		if stat.Size() != initialStat.Size() || stat.ModTime() != initialStat.ModTime() {
			break
		}
		time.Sleep(3 * time.Second) // poll rate
	}
	return nil
}

func NewFileWatch(path string) {
	done := make(chan bool)
	go func(done chan bool) {
		defer func() {
			done <- true
		}()
		err := WatchFile(path)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("File has been changed")
	}(done)
	<-done
}

func FilePointerOffset(fd *os.File) int64 {
	pos, err := fd.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Printf("error with file pointer offset: %v\n", err)
		return -1
	}
	return pos
}
