package sstable

import (
	"io"
	"log"
)

// https: //play.golang.org/p/jRpPRa4Q4Nh

func MergeSSTables(base string, i1, i2 int64) error {
	// load indexes
	sst1, err := OpenSSTable(base, i1)
	if err != nil {
		return err
	}
	sst2, err := OpenSSTable(base, i2)
	if err != nil {
		return err
	}
	log.Printf(">>> DEBUG 1")
	// make batch to write data to
	batch := NewBatch()
	// pass tables to the merge writer
	err = mergeWriter(sst1, sst2, batch)
	if err != nil {
		return err
	}
	log.Printf(">>> DEBUG 2")
	// close table 1
	err = sst1.Close()
	if err != nil {
		return err
	}
	// close table 2
	err = sst2.Close()
	if err != nil {
		return err
	}
	log.Printf(">>> DEBUG 3")
	// open new sstable to write to
	sst3, err := CreateSSTable(base, i2+1)
	//fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	log.Printf(">>> DEBUG 4")
	// write batch to table
	err = sst3.WriteBatch(batch)
	log.Printf(">>> DEBUG 5")
	// flush and close sstable
	err = sst3.Close()
	if err != nil {
		return err
	}
	log.Printf(">>> DEBUG 6")
	return nil
}

func mergeWriter(sst1, sst2 *SSTable, batch *Batch) error {

	i, j := 0, 0
	n1, n2 := sst1.index.Len(), sst2.index.Len()

	log.Printf(">>> DEBUG 1.1")

	var err error
	var de *sstDataEntry
	for i < n1 && j < n2 {
		if sst1.index.data[i].key < sst2.index.data[j].key {
			// read entry from sst1
			de, err = sst1.ReadEntryAt(sst1.index.data[i].offset)
			if err != nil {
				return err
			}
			// write entry to sst3 batch
			batch.WriteDataEntry(de)
			log.Printf(">>> DEBUG 1.2")
			i++
			continue
		}
		if sst2.index.data[j].key <= sst1.index.data[i].key {
			// read entry from sst2
			de, err = sst1.ReadEntryAt(sst2.index.data[j].offset)
			if err != nil {
				return err
			}
			// write entry to sst3 batch
			batch.WriteDataEntry(de)
			log.Printf(">>> DEBUG 1.3")
			if sst2.index.data[j].key == sst1.index.data[i].key {
				i++
			}
			j++
			continue
		}
	}

	if err == io.EOF {
		return nil
	}

	// print remaining
	for i < n1 {
		// read entry from sst1
		de, err = sst1.ReadEntryAt(sst1.index.data[i].offset)
		if err != nil {
			return err
		}
		// write entry to sst3 batch
		batch.WriteDataEntry(de)
		log.Printf(">>> DEBUG 1.4")
		i++
	}

	// print remaining
	for j < n2 {
		// read entry from sst2
		de, err = sst1.ReadEntryAt(sst2.index.data[j].offset)
		if err != nil {
			return err
		}
		// write entry to sst3 batch
		batch.WriteDataEntry(de)
		log.Printf(">>> DEBUG 1.5")
		j++
	}

	// return error free
	return nil
}
