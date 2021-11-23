package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	maxKeySize = math.MaxUint8
	maxValSize = math.MaxUint16
)

var (
	ErrIncomplete = errors.New("key or value is incomplete or missing")
	ErrTooLarge   = errors.New(
		fmt.Sprintf("key or value exceeded max size allowed (maxKey=%d, maxVal=%d)", maxKeySize, maxValSize))
	ErrNotFound = errors.New("entry not found")

	IterSkip = errors.New("skipping this item")
	IterStop = errors.New("breaking out and stopping iteration")
)

type mockEntry struct {
	k string
	v string
}

func (me *mockEntry) check() error {
	if me.k == "" || me.v == "" {
		return ErrIncomplete
	}
	if len(me.k) > maxKeySize || len(me.v) > maxValSize {
		return ErrTooLarge
	}
	return nil
}

func (me *mockEntry) String() string {
	return fmt.Sprintf("{%q:%q}", me.k, me.v)
}

type mockIndex struct {
	mdat map[string]int64 // mapped keys
	odat []string         // ordered keys
}

func makeMockIndex() *mockIndex {
	return &mockIndex{
		mdat: make(map[string]int64),
		odat: make([]string, 0),
	}
}

func (mi *mockIndex) put(k string, off int64) {
	// check if key is in map
	if _, ok := mi.mdat[k]; !ok {
		// add to ordered set if not in map
		mi.odat = append(mi.odat, k)
		if !sort.StringsAreSorted(mi.odat) {
			sort.Strings(mi.odat)
		}
	}
	// upsert into map
	mi.mdat[k] = off
}

func (mi *mockIndex) get(k string) (int64, error) {
	// check if key is in map
	off, ok := mi.mdat[k]
	if !ok {
		// if not, return -1
		return -1, ErrNotFound
	}
	return off, nil
}

func (mi *mockIndex) del(k string) {
	// check if key is in map
	if _, ok := mi.mdat[k]; !ok {
		// if not, just return
		return
	}
	// otherwise, delete from map
	delete(mi.mdat, k)
	// and delete from ordered set
	deleteFromSlice(&mi.odat, k)
}

func (mi *mockIndex) scan(fn func(k string, off int64) error) {
	for _, key := range mi.odat {
		err := fn(key, mi.mdat[key])
		if err != nil {
			if err == IterSkip {
				continue
			}
			break
		}
	}
}

// deleteFromSliceV1 expects a sorted slice!!! It will FAIL HARD if it doesn't get one
func deleteFromSlice(a *[]string, s string) {
	i := binarySearch(*a, s)
	if (*a)[i] != s {
		return // s was not found
	}
	*a = deleteAtI(*a, i)
}

// binarySearch expects a sorted slice!!! It will FAIL HARD if it doesn't get one
func binarySearch(a []string, x string) int {
	i, j := 0, len(a)
	for i < j {
		h := int(uint(i+j) >> 1)
		if !(a[h] >= x) {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

func deleteAtI(a []string, i int) []string {
	if i < len(a)-1 {
		copy(a[i:], a[i+1:])
	}
	a[len(a)-1] = ""
	a = a[:len(a)-1]
	return a
}

type mockDB struct {
	lock  sync.RWMutex
	f     *os.File
	index *mockIndex
}

func OpenMockDB(base string) (*mockDB, error) {
	dir, file := cleanPath(base)
	f, err := openFile(filepath.Join(dir, file))
	if err != nil {
		return nil, err
	}
	mdb := &mockDB{
		f: f,
	}
	err = mdb.loadIndex()
	if err != nil {
		return nil, err
	}
	return mdb, err
}

func (mdb *mockDB) loadIndex() error {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()
	if mdb.index == nil {
		mdb.index = makeMockIndex()
	}
	for {
		// get offset of entry
		off, err := offset(mdb.f)
		if err != nil {
			return err
		}
		// read entry at offset
		me, err := read(mdb.f)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		// read entry successfully, add to index
		mdb.index.put(me.k, off)
	}
	return nil
}

func (mdb *mockDB) Scan(fn func(me *mockEntry) error) {
	mdb.index.scan(func(k string, off int64) error {
		me, err := readAt(mdb.f, off)
		if err != nil {
			return err
		}
		err = fn(me)
		if err != nil {
			return err
		}
		return nil
	})
}

func (mdb *mockDB) Get(k string) (string, error) {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()
	off, err := mdb.index.get(k)
	if err != nil {
		return "", err
	}
	me, err := readAt(mdb.f, off)
	if err != nil {
		return "", err
	}
	return me.v, nil
}

func (mdb *mockDB) Put(k string, v string) error {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()
	off, err := write(mdb.f, &mockEntry{k, v})
	if err != nil {
		return err
	}
	mdb.index.put(k, off)
	return nil
}

var tombstone = &mockEntry{"_TS", "_TS"}

func (mdb *mockDB) Del(k string) error {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()
	_, err := write(mdb.f, tombstone)
	if err != nil {
		return err
	}
	mdb.index.del(k)
	return nil
}

func (mdb *mockDB) PutBatch(b *Batch) error {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()
	sort.Stable(b)
	for _, me := range b.entries {
		off, err := write(mdb.f, me)
		if err != nil {
			return err
		}
		mdb.index.put(me.k, off)
	}
	return nil
}

func (mdb *mockDB) Sync() error {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()
	err := mdb.f.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (mdb *mockDB) Close() error {
	mdb.lock.Lock()
	defer mdb.lock.Unlock()
	err := mdb.f.Sync()
	if err != nil {
		return err
	}
	err = mdb.f.Close()
	if err != nil {
		return err
	}
	return nil
}

func (mdb *mockDB) Count() int {
	return len(mdb.index.odat)
}

type Batch struct {
	entries []*mockEntry
}

func NewBatch() *Batch {
	return &Batch{
		entries: make([]*mockEntry, 0),
	}
}

func (b *Batch) Write(k string, v string) error {
	me := &mockEntry{k: k, v: v}
	err := me.check()
	if err != nil {
		return err
	}
	b.entries = append(b.entries, me)
	return nil
}

func (b *Batch) Len() int {
	return len(b.entries)
}

func (b *Batch) Less(i, j int) bool {
	return b.entries[i].k < b.entries[j].k
}

func (b *Batch) Swap(i, j int) {
	b.entries[i], b.entries[j] = b.entries[j], b.entries[i]
}

func getUint16(b []byte) uint16 {
	_ = b[1] // early bounds check
	return uint16(b[0]) | uint16(b[1])<<8
}

func putUint16(b []byte, v uint16) {
	_ = b[1] // early bounds check
	b[0] = byte(v)
	b[1] = byte(v >> 8)
}

func read(r io.Reader) (*mockEntry, error) {
	// make entry header
	hdr := make([]byte, 3)
	_, err := r.Read(hdr)
	if err != nil {
		return nil, err
	}
	// get key and value length
	klen := hdr[0]
	vlen := getUint16(hdr[1:])
	// read key
	k := make([]byte, klen)
	_, err = r.Read(k)
	if err != nil {
		return nil, err
	}
	// read value
	v := make([]byte, vlen)
	_, err = r.Read(v)
	if err != nil {
		return nil, err
	}
	// make and return entry
	me := &mockEntry{
		k: string(k),
		v: string(v),
	}
	return me, nil
}

func readAt(r io.ReaderAt, off int64) (*mockEntry, error) {
	// make entry header
	hdr := make([]byte, 3)
	n, err := r.ReadAt(hdr, off)
	if err != nil {
		return nil, err
	}
	// update offset
	off += int64(n)
	// get key and value length
	klen := hdr[0]
	vlen := getUint16(hdr[1:])
	// read key
	k := make([]byte, klen)
	n, err = r.ReadAt(k, off)
	if err != nil {
		return nil, err
	}
	// update offset
	off += int64(n)
	// read value
	v := make([]byte, vlen)
	_, err = r.ReadAt(v, off)
	if err != nil {
		return nil, err
	}
	// update offset
	off += int64(n)
	// make and return entry
	me := &mockEntry{
		k: string(k),
		v: string(v),
	}
	return me, nil
}

func write(w io.WriteSeeker, me *mockEntry) (int64, error) {
	// err check entry
	err := me.check()
	if err != nil {
		return -1, err
	}
	// get the file pointer offset for the entry
	off, err := offset(w)
	if err != nil {
		return -1, err
	}
	// make and encode entry header
	hdr := make([]byte, 3)
	hdr[0] = uint8(len(me.k))
	putUint16(hdr[1:], uint16(len(me.v)))
	// write entry header
	_, err = w.Write(hdr)
	if err != nil {
		return -1, err
	}
	// write entry key and value
	_, err = w.Write([]byte(me.k + me.v))
	if err != nil {
		return -1, err
	}
	// return offset
	return off, nil
}

func offset(ws io.WriteSeeker) (int64, error) {
	// get and return current offset
	return ws.Seek(0, io.SeekCurrent)
}

func cleanPath(path string) (string, string) {
	path, err := filepath.Abs(path)
	if err != nil {
		log.Panicf("cleaning path: %v\n", err)
	}
	return filepath.Split(filepath.ToSlash(path))
}

func openFile(path string) (*os.File, error) {
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
