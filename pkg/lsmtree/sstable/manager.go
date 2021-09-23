package sstable

import (
	"fmt"
	"os"
	"path/filepath"
)

// https: //play.golang.org/p/jRpPRa4Q4Nh

func MergeSSTables(base string, i1, i2 int64) error {
	// load indexes
	ssi1, err := OpenSSIndex(base, i1)
	if err != nil {
		return err
	}
	ssi2, err := OpenSSIndex(base, i2)
	if err != nil {
		return err
	}
	// sanitize any path separators
	base = filepath.ToSlash(base)
	// create new index file path
	path := filepath.Join(base, DataFileNameFromIndex(i2+1))
	// create new file
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	// defer tmp file close
	defer fd.Close()
	// pass tables to the merge writer
	err = mergeWriter(fd, ssi1, ssi2)
	if err != nil {
		return err
	}
	return nil
}

func mergeWriter(fd *os.File, ssi1 *SSIndex, ssi2 *SSIndex) error {

	i, j := 0, 0
	n1, n2 := ssi1.Len(), ssi2.Len()

	for i < n1 && j < n2 {
		if ssi1.data[i].key < ssi2.data[j].key {
			// if not common print smaller
			_, err := fmt.Fprintf(fd, "%q,", ssi1.data[i].key)
			if err != nil {
				return err
			}
			i++
			continue
		}
		if ssi2.data[j].key <= ssi1.data[i].key {
			// if not common print smaller
			_, err := fmt.Fprintf(fd, "%q,", ssi2.data[j].key)
			if err != nil {
				return err
			}
			if ssi2.data[j].key == ssi1.data[i].key {
				i++
			}
			j++
			continue
		}
	}

	// print remaining
	for i < n1 {
		_, err := fmt.Fprintf(fd, "%q,", ssi1.data[i].key)
		if err != nil {
			return err
		}
		i++
	}

	// print remaining
	for j < n2 {
		_, err := fmt.Fprintf(fd, "%q,", ssi2.data[j].key)
		if err != nil {
			return err
		}
		j++
	}

	// return error free
	return nil
}
