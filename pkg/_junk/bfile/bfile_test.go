package bfile

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
	"io"
	"os"
	"strings"
	"testing"
	"unsafe"
)

const count = 25

func Test_bfile_headerSize(t *testing.T) {
	fmt.Printf("header{}, size=%d\n", unsafe.Sizeof(header{}))
}

func Test_bfile_write(t *testing.T) {

	// init path
	path := "testing/test-001.txt"

	// open
	f := setup(path, t)

	// read all
	doReadAll := true
	if doReadAll {
		//readAll(f, t)
		//readAllRaw(f, t)
		readAllAtIndex(f, t)
	}

	// write
	doWrite := false
	if doWrite {
		// init offsets
		var offsets []int64

		// write
		for i := 0; i < count; i++ {
			data := []byte(fmt.Sprintf("record-%.8d data lives here", i))
			// write data
			off, err := f.write(data)
			checkErr(err, t)
			// add offsets to offset list
			offsets = append(offsets, off)
		}

		// print offsets
		for i := range offsets {
			fmt.Printf("record %d, offset=%d\n", i, offsets[i])
		}

		// rewind
		err := f.rewind()
		checkErr(err, t)
	}

	// read all
	if doReadAll {
		readAll(f, t)
	}

	// read at
	doReadAt := false
	if doReadAt {
		blocks := []int{0, 3, 7, 5, 11, 13, 21, 17}
		for i := range blocks {
			readAt(f, blocks[i], t)
		}
	}

	// close
	teardown(f, true, t)

}

func readAllAtIndex(f *bfile, t *testing.T) {
	// read all records
	for i := 0; i < f.count(); i++ {
		data, err := f.readAtIndex(i)
		if err != nil {
			if err == io.EOF {
				break
			}
			checkErr(err, t)
		}
		fmt.Printf("readAtIndex[%d]: %q\n", i, data)
	}
}

func readAllRaw(f *bfile, t *testing.T) {
	// read all records
	for {
		rec, err := f.readRaw()
		if err != nil {
			if err == io.EOF {
				break
			}
			checkErr(err, t)
		}
		fmt.Printf("%s\n", rec)
	}
}

func readAll(f *bfile, t *testing.T) {
	// read all
	for {
		data, err := f.read()
		if err != nil {
			if err == io.EOF {
				break
			}
			checkErr(err, t)
		}
		fmt.Printf("read data: %q\n", data)
	}
}

func readAt(f *bfile, block int, t *testing.T) {
	// read at
	fmt.Printf("reading at block %d (offset: %d): ", block, at(block))
	data, err := f.readAt(at(block))
	fmt.Printf("data=%q\n", data)
	checkErr(err, t)
}

func Test_getWords(t *testing.T) {
	for i := 0; i < 15; i++ {
		n := util.RandIntn(1, 5)
		w := getWords(n)
		fmt.Printf("getting %d words: %q\n", n, w)
	}
}

var words = strings.Fields(util.WaltWhitmanText)

func getWords(n int) []byte {
	if n < 1 {
		n = 1
	}
	at := util.RandIntn(0, len(words)-n)
	if n > 1 {
		return []byte(strings.Join(words[at:at+n], " "))
	}
	return []byte(words[at])
}

func setup(path string, t *testing.T) *bfile {
	// open file
	f, err := openBFile(path)
	checkErr(err, t)
	// return file
	return f
}

func teardown(f *bfile, clean bool, t *testing.T) {
	// get path
	path := f.path

	// close file
	err := f.close()
	checkErr(err, t)

	// clean up
	if clean {
		os.RemoveAll(path)
	}
}

func checkErr(err error, t *testing.T) {
	if err != nil {
		t.Errorf("error >>> %+v\n", err)
	}
}
