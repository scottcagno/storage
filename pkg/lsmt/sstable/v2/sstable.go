package v2

import (
	"errors"
	"fmt"
	"github.com/scottcagno/storage/pkg/binary"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SSTableBasePath = "data"
	SSTablePrefix   = "sst-"
	SSTableSuffix   = ".db"
)

var (
	ErrReaderClosed = errors.New("error: reader is not open")
	ErrWriterClosed = errors.New("error: writer is not open")
)

// makeFileName returns a file name using the provided timestamp.
// If t is nil, it will create a new name using time.Now()
func makeFileName() string {
	//t := time.Now()
	//tf := t.Format("2006-01-03_15:04:05:000000")
	//return fmt.Sprintf("%s%s%s", LogPrefix, time.RFC3339Nano, LogSuffix)
	return fmt.Sprintf("%s%d%s", SSTablePrefix, time.Now().UnixMicro(), SSTableSuffix)
}

// cleanPath sanitizes path provided
func cleanPath(path string) (string, error) {
	// sanitize base path
	base, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.ToSlash(base), nil
}

func makeSSTableFileNames(base string, seq int64) (string, string) {
	sstIndexPath := fmt.Sprintf("%s%08d-index%s",
		SSTablePrefix, seq, SSTableSuffix)
	sstDataPath := fmt.Sprintf("%s%08d-data%s",
		SSTablePrefix, seq, SSTableSuffix)
	return filepath.Join(base, sstIndexPath), filepath.Join(base, sstDataPath)
}

func getSeqFromFileName(fileName string) (int64, error) {
	n := len(SSTablePrefix)
	return strconv.ParseInt(fileName[n:n+8], 10, 64)
}

// SSTable is a sorted strings table
type SSTable struct {
	lock      sync.RWMutex
	sequence  int64          // sequence is the sequence index of this sstable
	indexPath string         // indexPath is the full path of the sst key index
	dataPath  string         // dataPath is the full path to the sst raw data
	ir        *binary.Reader // ir is a binary file reader for this table index
	dr        *binary.Reader // dr is a binary file reader for this table data
	iw        *binary.Writer // iw is a binary file writer for this table index
	dw        *binary.Writer // dw is a binary file writer for this table data
}

func CreateNewSSTable(sequence int64) (*SSTable, error) {
	// sanitize base path
	base, err := cleanPath(SSTableBasePath)
	if err != nil {
		return nil, err
	}
	// check to see if path exists
	_, err = os.Stat(base)
	if os.IsNotExist(err) {
		// create dirs if they don't exist
		err = os.MkdirAll(base, os.ModeDir)
		if err != nil {
			return nil, err
		}
	}
	// create new sstable index file
	sstIndexPath := filepath.Join(base, fmt.Sprintf("%s%08d-index%s",
		SSTablePrefix, sequence, SSTableSuffix))
	fd, err := os.Create(sstIndexPath)
	if err != nil {
		return nil, err
	}
	err = fd.Close()
	if err != nil {
		return nil, err
	}
	// create new sstable data file
	sstDataPath := filepath.Join(base, fmt.Sprintf("%s%08d-data%s",
		SSTablePrefix, sequence, SSTableSuffix))
	fd, err = os.Create(sstDataPath)
	if err != nil {
		return nil, err
	}
	err = fd.Close()
	if err != nil {
		return nil, err
	}
	// open index file reader
	ir, err := binary.OpenReader(sstIndexPath)
	if err != nil {
		return nil, err
	}
	// open index file writer
	iw, err := binary.OpenWriter(sstIndexPath)
	if err != nil {
		return nil, err
	}
	// open data file reader
	dr, err := binary.OpenReader(sstDataPath)
	if err != nil {
		return nil, err
	}
	// open data file writer
	dw, err := binary.OpenWriter(sstDataPath)
	if err != nil {
		return nil, err
	}
	// create new sstable instance
	sst := &SSTable{
		sequence:  sequence,
		indexPath: sstIndexPath,
		dataPath:  sstDataPath,
		ir:        ir,
		dr:        dr,
		iw:        iw,
		dw:        dw,
	}
	// return new sstable
	return sst, nil
}

func (s *SSTable) WriteEntryAndIndex(key string, value []byte) error {
	if s.iw == nil || s.dw == nil {
		return ErrWriterClosed
	}
	// write entry
	offset, err := s.dw.WriteEntry(&binary.Entry{Key: []byte(key), Value: value})
	if err != nil {
		return err
	}
	// write key data index to index file
	_, err = s.iw.WriteEntryIndex(&binary.EntryIndex{Key: []byte(key), Offset: offset})
	if err != nil {
		return err
	}
	return nil
}

func MakeSSTableOLD() (*SSTable, error) {
	// sanitize base path
	base, err := cleanPath(SSTableBasePath)
	if err != nil {
		return nil, err
	}
	// for later
	var sequence int64
	// check to see if path exists
	_, err = os.Stat(base)
	if os.IsNotExist(err) {
		// create dirs if they don't exist
		err = os.MkdirAll(base, os.ModeDir)
		if err != nil {
			return nil, err
		}
		// empty, so set sequence to 1
		sequence = 1
	}
	// not empty, so read sstable sequence index
	if sequence == 0 {
		// get the last sstable sequence if there is one
		files, err := os.ReadDir(base)
		if err != nil {
			return nil, err
		}
		// iterate entries
		for _, file := range files {
			// skip any non sstable files
			if file.IsDir() ||
				!strings.HasPrefix(SSTablePrefix, file.Name()) ||
				!strings.HasSuffix(SSTableSuffix, file.Name()) {
				// if it's not an sstable, then skip
				continue
			}
			// otherwise, attempt to get sequence index from file name
			index, err := getSeqFromFileName(file.Name())
			if err != nil || index == 0 {
				continue
			}
			// if we successfully got one...
			if index > sequence {
				// ... and it is larger than the current
				// sequence index, then update sequence
				sequence = index
			}
		}
	}
	// create new sstable file names for index, and data
	sstIndexPath, sstDataPath := makeSSTableFileNames(base, sequence)
	// create index file
	fd, err := os.Create(sstIndexPath)
	if err != nil {
		return nil, err
	}
	err = fd.Close()
	if err != nil {
		return nil, err
	}
	// create data file
	fd, err = os.Create(sstDataPath)
	if err != nil {
		return nil, err
	}
	err = fd.Close()
	if err != nil {
		return nil, err
	}
	// open index file reader
	ir, err := binary.OpenReader(sstIndexPath)
	if err != nil {
		return nil, err
	}
	// open index file writer
	iw, err := binary.OpenWriter(sstIndexPath)
	if err != nil {
		return nil, err
	}
	// open data file reader
	dr, err := binary.OpenReader(sstDataPath)
	if err != nil {
		return nil, err
	}
	// open data file writer
	dw, err := binary.OpenWriter(sstDataPath)
	if err != nil {
		return nil, err
	}
	// create new sstable instance
	sst := &SSTable{
		sequence:  sequence,
		indexPath: sstIndexPath,
		dataPath:  sstDataPath,
		ir:        ir,
		dr:        dr,
		iw:        iw,
		dw:        dw,
	}
	// TODO: make load and offset some of the work above into load function

	// return new sstable
	return sst, nil
}

// CreateSSTable creates and returns a new sstable for writing
func CreateSSTable(base string) (*SSTable, error) {
	// sanitize base path
	base, err := cleanPath(base)
	if err != nil {
		return nil, err
	}
	// create dirs if they don't exist
	err = os.MkdirAll(base, os.ModeDir)
	if err != nil {
		return nil, err
	}
	// create new sstable file
	path := filepath.Join(base, makeFileName())
	fd, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	err = fd.Close()
	if err != nil {
		return nil, err
	}
	// create new sstable file names for index, and data
	sstIndexPath, sstDataPath := makeSSTableFileNames(base, 0)
	// open index file reader
	ir, err := binary.OpenReader(sstIndexPath)
	if err != nil {
		return nil, err
	}
	// open index file writer
	iw, err := binary.OpenWriter(sstIndexPath)
	if err != nil {
		return nil, err
	}
	// open data file reader
	dr, err := binary.OpenReader(sstDataPath)
	if err != nil {
		return nil, err
	}
	// open data file writer
	dw, err := binary.OpenWriter(sstDataPath)
	if err != nil {
		return nil, err
	}
	// return new sstable
	return &SSTable{
		sequence:  0,
		indexPath: sstIndexPath,
		dataPath:  sstDataPath,
		ir:        ir,
		dr:        dr,
		iw:        iw,
		dw:        dw,
	}, nil
}

// OpenSSTable returns a new sstable in read only mode, if it exists
func OpenSSTable(path string) (*SSTable, error) {
	// check to make sure table is there
	_, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	// create new sstable file names for index, and data
	sstIndexPath, sstDataPath := makeSSTableFileNames(path, 0)
	// open index file reader
	ir, err := binary.OpenReader(sstIndexPath)
	if err != nil {
		return nil, err
	}
	// open index file writer
	iw, err := binary.OpenWriter(sstIndexPath)
	if err != nil {
		return nil, err
	}
	// open data file reader
	dr, err := binary.OpenReader(sstDataPath)
	if err != nil {
		return nil, err
	}
	// open data file writer
	dw, err := binary.OpenWriter(sstDataPath)
	if err != nil {
		return nil, err
	}
	// return sstable
	return &SSTable{
		sequence:  0,
		indexPath: sstIndexPath,
		dataPath:  sstDataPath,
		ir:        ir,
		dr:        dr,
		iw:        iw,
		dw:        dw,
	}, nil
}

// DataPath returns full path of table
func (s *SSTable) DataPath() string {
	return s.dataPath
}

// IndexPath returns full path of table
func (s *SSTable) IndexPath() string {
	return s.dataPath
}

// Read reads the next single entry from the sstable file sequentially
func (s *SSTable) Read() (*binary.Entry, error) {
	// error check
	if s.ir == nil {
		return nil, ErrReaderClosed
	}
	// lock
	s.lock.RLock()
	defer s.lock.RUnlock()
	// read next entry
	e, err := s.ir.ReadEntry()
	if err != nil {
		return nil, err
	}
	return e, nil
}

// ReadAt reads a single entry from the sstable file at the provided offset
func (s *SSTable) ReadAt(offset int64) (*binary.Entry, error) {
	// error check
	if s.ir == nil {
		return nil, ErrReaderClosed
	}
	// lock
	s.lock.RLock()
	defer s.lock.RUnlock()
	// read entry
	e, err := s.ir.ReadEntryAt(offset)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// Write writes a single entry to the sstable file (sequentially)
func (s *SSTable) Write(key string, value []byte) error {
	// error check
	if s.dw == nil {
		return ErrWriterClosed
	}
	// lock
	s.lock.Lock()
	defer s.lock.Unlock()
	// get offset to add to entry
	offset, err := s.dw.Offset()
	if err != nil {
		return err
	}
	// create entry
	e := &binary.Entry{
		Id:    offset,
		Key:   []byte(key),
		Value: value,
	}
	// write entry
	_, err = s.dw.WriteEntry(e)
	if err != nil {
		return err
	}
	return nil
}

// Close closes the sstable and files (it makes sure to sync first)
func (s *SSTable) Close() error {
	if s.dr != nil {
		err := s.dr.Close()
		if err != nil {
			return err
		}
	}
	if s.dw != nil {
		// call close
		err := s.dw.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
