package common

import (
	"io/ioutil"
	"log"
	"os"
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
