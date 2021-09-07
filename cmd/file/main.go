package main

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/common"
	"log"
	"os"
	"path/filepath"
)

func CleaningPath(path string) {
	npath, err := common.CleanPath(path)
	if err != nil {
		panic(err)
	}
	fmt.Printf("cleaning path: %q -> %q\n", path, npath)
	npath = filepath.ToSlash(npath)
	fmt.Printf("cleaning path: %q -> %q\n", path, npath)
}

func main() {
	p := `foo/bar/foo.txt`

	CleaningPath(p)
	return

	f, err := common.OpenFile("cmd/file/test.txt")
	checkErr(err)
	defer f.Close()

	printFPOffset(f)

	doWrite := false
	if doWrite {
		fmt.Printf("writing data... (fp: %d)\n", common.FilePointerOffset(f))
		for i := 0; i < 10; i++ {
			printFPOffset(f)
			_, err := f.Write(getData(i))
			checkErr(err)
		}
	}

	fmt.Println(">> done writing data.")
	printFPOffset(f)

	buf1 := getData(3)
	_, err = f.WriteAt(buf1, 27)
	checkErr(err)
	fmt.Printf("WRITE AT(%d): %q", 27, buf1)
	printFPOffset(f)

	buf2 := make([]byte, 27)
	_, err = f.ReadAt(buf2, 27)
	checkErr(err)
	fmt.Printf("READ AT(%d): %q", 81, buf2)
	printFPOffset(f)

	doRead := true
	if doRead {
		fmt.Printf("reading data... (fp: %d)\n", common.FilePointerOffset(f))
		for i := 0; i < 10; i++ {
			printFPOffset(f)
			data := make([]byte, 27)
			_, err := f.Read(data)
			checkErr(err)
			fmt.Printf("read: %q", data)
		}
	}

}

func printFPOffset(f *os.File) {
	off := common.FilePointerOffset(f)
	fmt.Printf("fp offset: %d\n", off)
}

func getData(n int) []byte {
	return []byte(fmt.Sprintf("#%d this is entry number %d!\n", n, n))
}

func checkErr(err error) {
	if err != nil {
		log.Panicf("got error: %v\n", err)
	}
}
