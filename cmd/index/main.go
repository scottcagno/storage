package main

import (
	"fmt"
	"io/fs"
	"log"
	"path/filepath"
	"strings"
)

var paths []string

func main() {

	for i := 0; i < 3; i++ {
		for j := 0; j < 5; j++ {
			paths = append(paths, fmt.Sprintf("cmd/index/data/%.3d/data-%.3d.dat", i, j))
		}
	}

	doWrite := false
	if doWrite {
		for i := range paths {
			mdb, err := OpenMockDB(paths[i])
			if err != nil {
				log.Panic("open", err)
			}
			batch, err := makeAndWriteBatch(1, 256)
			if err != nil {
				log.Panic("make and write batch", err)
			}
			err = mdb.PutBatch(batch)
			if err != nil {
				log.Panic("put batch", err)
			}
			err = mdb.Close()
			if err != nil {
				log.Panic("close", err)
			}
		}
	}

	doRead := true
	doScan := false
	if doRead {
		filepath.Walk("cmd/index/data", func(path string, fi fs.FileInfo, err error) error {
			if !fi.IsDir() && strings.HasSuffix(fi.Name(), ".dat") {
				path = filepath.ToSlash(path)
				fmt.Printf("opening: %q, and counting records...\n", path)
				mdb, err := OpenMockDB(path)
				if err != nil {
					return err
				}
				count := mdb.Count()
				if doScan {
					mdb.Scan(func(me *mockEntry) error {
						if me == nil {
							return IterStop
						}
						fmt.Printf("read record: %s\n", me)
						return nil
					})
				}
				err = mdb.Close()
				if err != nil {
					return err
				}
				fmt.Printf("found %d, records.\n", count)
			}
			return nil
		})
	}
}

func makeKey(i int) string {
	return fmt.Sprintf("key-%.4d", i)
}

func makeVal(i int) string {
	return fmt.Sprintf("value-%.16d", i*3)
}

func makeAndWriteBatch(n1, n2 int) (*Batch, error) {
	batch := NewBatch()
	for i := n1; i < n2; i++ {
		err := batch.Write(makeKey(i), makeVal(i))
		if err != nil {
			return nil, err
		}
	}
	return batch, nil
}
