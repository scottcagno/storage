package datafile

import (
	"errors"
	"fmt"
	"log"
	"math/bits"
	"os"
	"path/filepath"
	"strconv"
)

type kind uint8

const (
	blockSize   = 4096
	chunkSize   = 16 * blockSize
	extentSize  = 16 * chunkSize
	segmentSize = 16 * extentSize

	blockSizeMask   = blockSize - 1
	chunkSizeMask   = chunkSize - 1
	extentSizeMask  = extentSize - 1
	segmentSizeMask = segmentSize - 1

	fullBlock   = kind(1)
	firstBlock  = kind(2)
	middleBlock = kind(3)
	lastBlock   = kind(4)
)

var (
	ErrOutOfBounds = errors.New("out of bounds")
)

// align aligns sizes
func align(n int, mask int) int {
	return ((n + 2) + mask) &^ mask
}

// malloc allocates heap memory and copies d into it
func malloc(d []byte, sizeMask int) ([]byte, error) {
	if len(d) > sizeMask {
		return nil, ErrOutOfBounds
	}
	heap := make([]byte, align(0, sizeMask))
	copy(heap[0:], d)
	return heap, nil
}

// free releases memory to be cleaned up
func free(d *[]byte) {
	*d = nil
}

func wordSize(t interface{}) int {
	switch t.(type) {
	case uint8:
		return bits.OnesCount(uint(^uint8(0)))
	case uint16:
		return bits.OnesCount(uint(^uint16(0)))
	case uint32:
		return bits.OnesCount(uint(^uint32(0)))
	case uint64:
		return bits.OnesCount(uint(^uint64(0)))
	}
	return -1
}

type bitsetU16 uint16

func (bs *bitsetU16) has(i uint) bool {
	return *bs&(1<<(i&(15))) != 0
}

func (bs *bitsetU16) set(i uint) {
	*bs |= 1 << (i & (15))
}

func (bs *bitsetU16) unset(i uint) {
	*bs &^= 1 << (i & (15))
}

func (bs bitsetU16) String() string {
	// print binary value of bitset
	//var res string = "16" // set this to the "bit resolution" you'd like to see
	var res = strconv.Itoa(16)
	return fmt.Sprintf("%."+res+"b (%s bits)", bs, res)
}

// header represents a block header
type header struct {
	stat uint8  // stat is the block status
	kind uint8  // kind is the type of block
	used uint16 // used is the length of the data
	free uint16 // free is the free bytes at the end
}

// block is a contiguous set of bytes 4kb in size
type block struct {
	*header
	data []byte
}

// Write is the write method for a block
func (b *block) Write(d []byte) (int, error) {
	// allocate
	data, err := malloc(d, blockSizeMask)
	if err != nil {
		return -1, err
	}
	b.data = data
	return -1, nil
}

// chunk is a contiguous set of 16 blocks
type chunk struct {
	free bitsetU16 // bitmap of free blocks in chunk
	data [chunkSize]byte
}

// Write is the write method for a chunk
func (b *chunk) Write(d []byte) (int, error) {
	// placeholder
	return -1, nil
}

// extent is a contiguous set of 16 chunks
type extent struct {
	free bitsetU16 // bitmap of free blocks in extent
	data [extentSize]byte
}

// Write is the write method for an extent
func (b *extent) Write(d []byte) (int, error) {
	// placeholder
	return -1, nil
}

// segment is a contiguous set of 16 extents
type segment struct {
	free bitsetU16 // bitmap of free extents
	data [segmentSize]byte
}

// Write is the write method for a segment
func (b *segment) Write(d []byte) (int, error) {
	// placeholder
	return -1, nil
}

// datafile is a file containing one or more segments
type datafile struct {
	fp   *os.File
	data []*segment
}

func openDataFile(path string) (*datafile, error) {
	// sanitize path
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	// split path
	dir, name := filepath.Split(filepath.ToSlash(path))
	// init file and dirs
	var fp *os.File
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		// create dir
		err = os.MkdirAll(dir, os.ModeDir)
		if err != nil {
			return nil, err
		}
		// create file
		fp, err = os.Create(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		// truncate to size
		err = fp.Truncate(segmentSize)
		if err != nil {
			return nil, err
		}
		// close file
		err = fp.Close()
		if err != nil {
			return nil, err
		}
	}
	// open existing file
	fp, err = os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	// create data file
	df := &datafile{
		fp:   fp,
		data: make([]*segment, 0),
	}
	// call load
	err = df.load()
	if err != nil {
		return nil, err
	}
	// return data file
	return df, nil
}

func (df *datafile) load() error {
	return nil
}

func (df *datafile) Sync() error {
	err := df.fp.Sync()
	if err != nil {
		return err
	}
	err = df.fp.Close()
	if err != nil {
		return err
	}
	return nil
}

func (df *datafile) Close() error {
	err := df.fp.Close()
	if err != nil {
		return err
	}
	return nil
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
