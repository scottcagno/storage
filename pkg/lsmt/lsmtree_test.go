package lsmt

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	binary2 "github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/util"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func makeKey(i int) string {
	//return fmt.Sprintf("key-%06d", i)
	hexa := strconv.FormatInt(int64(i), 16)
	return fmt.Sprintf("%s%06s", "key-", hexa)
}

func makeVal(i int) []byte {
	return []byte(fmt.Sprintf("value-%08d", i))
}
func makeCustomKey(format string, i int) string {
	return fmt.Sprintf(format, i)
}

func makeCustomVal(i int, suffix string) []byte {
	return []byte(fmt.Sprintf("value-%08d-%s", i, suffix))
}

func logit(s string) {
	log.SetPrefix("[INFO] ")
	log.Printf("%s\n", s)
}

var conf = &LSMConfig{
	BaseDir:         "lsm-testing",
	FlushThreshold:  -1,
	SyncOnWrite:     false,
	BloomFilterSize: 1 << 16,
}

func TestLSMTLogging(t *testing.T) {

	level := LevelInfo
	l := NewLogger(level)
	fmt.Println(LevelText(level))
	l.Debug("foo")
	l.Debug("foo with args: %d\n", 4)
	l.Info("foo")
	l.Info("foo with args: %d\n", 4)
	l.Warn("foo")
	l.Warn("foo with args: %d\n", 4)
	l.Error("foo")
	l.Error("foo with args: %d\n", 4)
	for i := LevelOff; i < LevelFatal; i++ {

	}

}

func TestLSMTreeKeyOverride(t *testing.T) {

	db, err := OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	err = db.Put("Hi!", []byte("Hello world, LSMTree!"))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	err = db.Put("Does it override key?", []byte("No!"))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	err = db.Put("Does it override key?", []byte("Yes, absolutely! The key has been overridden."))
	if err != nil {
		panic(fmt.Errorf("failed to put: %w", err))
	}

	err = db.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	db, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	key := "Hi!"
	val, err := db.Get(key)
	if err != nil {
		panic(fmt.Errorf("failed to get value: %w", err))
	}
	fmt.Printf("get(%q)=%q\n", key, val)

	key = "Does it override key?"
	val, err = db.Get(key)
	if err != nil {
		panic(fmt.Errorf("failed to get value: %w", err))
	}
	fmt.Printf("get(%q)=%q\n", key, val)

	err = db.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// Expected output:
	// Hello world, LSMTree!
	// Yes, absolutely! The key has been overridden.
}

var (
	ErrKeyRequired   = errors.New("")
	ErrValueRequired = errors.New("")
)

func TestPrintMaxSizes(t *testing.T) {

	getSize := func(s string, size uint) string {
		out := fmt.Sprintf("%s: %d B", s, size)
		if size >= 1<<10 {
			out += fmt.Sprintf(", %d KB", size/1000)
		}
		if size >= 1<<20 {
			out += fmt.Sprintf(", %d MB", size/1000/1000)
		}
		if size >= 1<<30 {
			out += fmt.Sprintf(", %d GB", size/1000/1000/1000)
		}
		if size >= 1<<40 {
			out += fmt.Sprintf(", %d TB", size/1000/1000/1000/1000)
		}
		if size >= 1<<50 {
			out += fmt.Sprintf(", %d PB", size/1000/1000/1000/1000/1000)
		}
		return out
	}

	fmt.Println(util.Sizeof(len([]byte{})))
	var s0 string
	fmt.Println(util.Sizeof(s0))

	var s1 string = "xxxxxxxx"
	fmt.Println(util.Sizeof(s1))

	fmt.Printf("%s\n", getSize("MaxUint8", math.MaxUint8))
	fmt.Printf("%s\n", getSize("MaxUint16", math.MaxUint16))
	fmt.Printf("%s\n", getSize("MaxUint32", math.MaxUint32))
	fmt.Printf("%s\n", getSize("MaxUint64", math.MaxUint64))

}

func TestLSMTreeLogger(t *testing.T) {

	db, err := OpenLSMTree(conf)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	err = db.Put("jkldafdsa", nil)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
}

func TestPutForErrors(t *testing.T) {

	origPath := conf.BaseDir
	tempPath := filepath.Join(conf.BaseDir, "put-for-errors")
	conf.BaseDir = tempPath

	defer func() {
		if err := os.RemoveAll(conf.BaseDir); err != nil {
			panic(fmt.Errorf("failed to remove %s: %w", conf.BaseDir, err))
		}
		conf.BaseDir = origPath
	}()

	logit("opening")
	db, err := OpenLSMTree(conf)
	if err != nil {
		panic(fmt.Errorf("failed to open LSM tree %s: %w", conf.BaseDir, err))
	}

	logit("checking put empty key")
	err = db.Put("", []byte("some value"))
	if err != nil {
		t.Errorf("empty key: %v\n", err)
	}

	logit("checking put nil value")
	err = db.Put("some key", nil)
	if err != nil {
		t.Errorf("nil val: %v\n", err)
	}

	logit("checking put empty value")
	err = db.Put("some key", []byte{})
	if err != nil {
		t.Errorf("empty val slice: %v\n", err)
	}

	logit("checking put large key (65,536 bytes)")
	var largeKey [65536]byte
	err = db.Put(string(largeKey[:]), []byte("some value"))
	if err != nil {
		t.Errorf("large key: %v\n", err)
	}

	logit("checking put large value (4,294,967,296 bytes)")
	var largeValue [4294967296]byte
	err = db.Put("some key", largeValue[:])
	if err != nil {
		t.Errorf("large val: %v\n", err)
	}

	logit("close")
	err = db.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}
}

func TestOpenAndCloseNoWrite(t *testing.T) {

	db, err := OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	err = db.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	db, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	err = db.Close()
	if err != nil {
		t.Errorf("open: %v\n", err)
	}
}

func TestLSMTreeReadEmptyDir(t *testing.T) {
	sanitize := func(base string) (string, error) {
		base, err := filepath.Abs(base)
		if err != nil {
			return "", err
		}
		base = filepath.ToSlash(base)
		return base, nil
	}
	base, err := sanitize("testing-empty-dir")
	if err != nil {
		t.Errorf("sanitize: %v\n", err)
	}
	_, err = os.ReadDir(base)
	if err != nil {
		t.Errorf("read dir: %T %v\n", err, err)
	}
}

func testSSTableBehavior(t *testing.T) {

	origPath := conf.BaseDir
	tempPath := filepath.Join(conf.BaseDir, "sstables")
	conf.BaseDir = tempPath

	// open lsm tree
	logit("opening lsm tree")
	lsm, err := OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// note: write data until ss-table flush is triggered
	// note: close tree, and re-open and "delete" one of
	// note: the records that is now on disk. check and
	// note: make sure that it takes the "deleted" record
	// note: over the one on disk.

	// write data
	logit("writing data")
	ts1 := time.Now()
	for i := 0; i < 1024; i++ {
		err = lsm.Put(makeKey(i), makeCustomVal(i, lgVal))
		if err != nil {
			t.Errorf("error type: %T, put: %v\n", err, err)
		}
	}
	ts2 := time.Now()
	fmt.Println(util.FormatTime("writing entries", ts1, ts2))

	// get lsm-tree status
	logit("getting lsm-tree stats")
	st, err := lsm.Stats()
	if err != nil {
		t.Errorf("stats [text]: %v\n", err)
	}
	dat, err := st.JSON()
	if err != nil {
		t.Errorf("stats [json]: %v\n", err)
	}
	fmt.Printf("stats:\n%s\n\njson:\n%s\n", st, dat)

	// close
	logit("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open lsm tree
	logit("opening lsm tree")
	lsm, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}
	util.DEBUG(">>>>>>>>> has 500: %v\n", lsm.Has(makeKey(500)))

	// delete record(s)
	logit("deleting record(s) [500,501,502,503 and 505]")
	err = lsm.Del(makeKey(500))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}
	err = lsm.Del(makeKey(501))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}
	err = lsm.Del(makeKey(502))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}
	err = lsm.Del(makeKey(503))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}
	err = lsm.Del(makeKey(505))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}

	// close
	logit("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open lsm tree
	logit("opening lsm tree")
	lsm, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// checking for records
	logit("checking for records [475-512]")
	for i := 475; i < 512; i++ {
		key := makeKey(i)
		ok := lsm.Has(key)
		log.Printf("[record: %d] has(%s): %v\n", i, key, ok)
	}

	// close
	logit("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	conf.BaseDir = origPath
}

func testLSMTreeHasAndBatches(t *testing.T) {

	origPath := conf.BaseDir
	tempPath := filepath.Join(conf.BaseDir, "batches")
	conf.BaseDir = tempPath

	logit(fmt.Sprintf("bloom fileter size: %d\n", conf.BloomFilterSize))

	// open lsm tree
	logit("opening lsm tree")
	lsm, err := OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	count := 32500

	// batch some entries
	logit("batching some entries")
	ts1 := time.Now()
	batch := binary2.NewBatch()
	for i := 0; i < count; i++ {
		batch.Write(makeKey(i), makeVal(i))
	}
	ts2 := time.Now()
	fmt.Println(util.FormatTime("batching entries", ts1, ts2))

	//// write batch
	//logit("write batch")
	//ts1 = time.Now()
	//err = lsm.PutBatch(batch)
	//if err != nil {
	//	t.Errorf("put batch: %v\n", err)
	//}
	//ts2 = time.Now()
	//fmt.Println(util.FormatTime("writing batch", ts1, ts2))

	//// manual sync
	//logit("manual sync")
	//err = lsm.Sync()
	//if err != nil {
	//	t.Errorf("manual sync: %v\n", err)
	//}

	// close
	logit("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open lsm tree
	logit("opening lsm tree")
	lsm, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// check has
	logit("checking has")
	for i := range batch.Entries {
		if i%500 == 0 {
			logit(fmt.Sprintf("checking entry: %d", i))
			// get entry
			entry := batch.Entries[i]
			// check for valid key
			if ok := lsm.Has(string(entry.Key)); !ok {
				t.Errorf("has(%q) should be true, got: %v\n", entry.Key, ok)
			}
			// check invalid key also
			invalid := makeCustomKey("%d-poopoo", i)
			if ok := lsm.Has(invalid); ok {
				t.Errorf("has(%q) should be false, got: %v\n", invalid, ok)
			}
		} else {
			// skip some entries
			continue
		}
	}

	//// check get batch
	//logit("checkin get batch")
	//var keys []string
	//for i := range batch.Entries {
	//	if i%500 == 0 {
	//		keys = append(keys, string(batch.Entries[i].Key))
	//	} else {
	//		continue
	//	}
	//}
	//_, err = lsm.GetBatch(keys...)
	//if err != nil && err != ErrNotFound {
	//	t.Errorf("getbatch: %v\n", err)
	//}

	// add a few more records just to ensure the segment file is working properly
	err = lsm.Put("foo-1", []byte("bar-1"))
	if err != nil {
		t.Errorf("putting: %v\n", err)
	}

	err = lsm.Put("key-big-1", []byte(lgVal))
	if err != nil {
		t.Errorf("putting: %v\n", err)
	}

	err = lsm.Put("foo-2", []byte("bar-2"))
	if err != nil {
		t.Errorf("putting: %v\n", err)
	}

	err = lsm.Put("key-big-2", []byte(lgVal))
	if err != nil {
		t.Errorf("putting: %v\n", err)
	}

	err = lsm.Put("foo-3", []byte("bar-3"))
	if err != nil {
		t.Errorf("putting: %v\n", err)
	}

	v, err := lsm.Get("key-big-2")
	if err != nil {
		t.Errorf("getting: %v\n", err)
	}
	if v == nil || !bytes.Equal(v, []byte(lgVal)) {
		t.Errorf("getting val, expected lgVal but got: %v\n", v)
	}

	// close
	logit("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	conf.BaseDir = origPath
}

func TestLSMTree_Search_vs_LinearSearch(t *testing.T) {

	count := 50000

	// open lsm tree
	logit("opening lsm tree")
	lsm, err := OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// write Entries
	logit("writing data")
	for i := 0; i < count; i++ {
		err := lsm.Put(makeKey(i), makeVal(i))
		if err != nil {
			t.Errorf("put: %v\n", err)
		}
	}

	// close
	logit("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open lsm tree
	logit("opening lsm tree")
	lsm, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// reading entries linear scanner
	logit("reading entries [scanner]")
	err = lsm.Scan(0, func(e *binary2.Entry) bool {
		if e.Key != nil && e.Value != nil {
			fmt.Printf("--> %s\n", e)
			return true
		}
		return false
	})
	if err != nil {
		t.Errorf("scanning: %v\n", err)
	}

	// close
	logit("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open lsm tree
	logit("opening lsm tree")
	lsm, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// reading entries search
	logit("reading entries [search]")
	for i := 0; i < count; i += 1000 {
		ts1 := time.Now()
		k := makeKey(i)
		v, err := lsm.Get(k)
		if err != nil || v == nil {
			t.Errorf("reading: %v\n", err)
		}
		ts2 := time.Now()
		fmt.Println(util.FormatTime("reading entries [search]", ts1, ts2))
		if i%1000 == 0 {
			fmt.Printf("get(%q) -> %q\n", k, v)
		}
	}

	// close
	logit("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open lsm tree
	logit("opening lsm tree")
	lsm, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// reading entries linear search
	logit("reading entries [linear search]")
	for i := 0; i < count; i += 1000 {
		ts1 := time.Now()
		k := makeKey(i)
		v, err := lsm.Get2(k)
		if err != nil || v == nil {
			t.Errorf("reading: %v\n", err)
		}
		ts2 := time.Now()
		fmt.Println(util.FormatTime("reading entries [linear search]", ts1, ts2))
		if i%1000 == 0 {
			fmt.Printf("get(%q) -> %q\n", k, v)
		}
	}

	// close
	logit("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}
}

func TestLSMTree(t *testing.T) {

	// sstable tests
	testSSTableBehavior(t)

	// test has and batching
	testLSMTreeHasAndBatches(t)

	max := 100000
	for i := 10; i <= max; i *= 10 {
		log.Printf("running tests with count: %d\n", i)
		testingLSMTreeN(i, t)
		runtime.GC()
		time.Sleep(3)
	}
	doClean := false
	if doClean {
		err := os.RemoveAll(conf.BaseDir)
		if err != nil {
			t.Errorf("remove: %s, err: %v\n", conf.BaseDir, err)
		}
	}
}

func testingLSMTreeN(count int, t *testing.T) {

	strt := 0
	stop := strt + count

	origPath := conf.BaseDir
	tempPath := filepath.Join(conf.BaseDir, strconv.Itoa(count))
	conf.BaseDir = tempPath

	n, err := ReadLastSequenceNumber(conf.BaseDir)
	if n > 0 && err == nil {
		strt = int(n)
		stop = strt + count
	}
	util.DEBUG("start: %d, stop: %d, count: %d\n", strt, stop, count)

	// open lsm tree
	logit("opening lsm tree")
	lsm, err := OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// write Entries
	logit("writing data")
	ts1 := time.Now()
	for i := strt; i < stop; i++ {
		err := lsm.Put(makeKey(i), makeVal(i))
		if err != nil {
			t.Errorf("put: %v\n", err)
		}
	}
	ts2 := time.Now()
	fmt.Println(util.FormatTime("writing entries", ts1, ts2))

	// close
	logit("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open
	logit("opening lsm tree")
	lsm, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	doPrintAllReads := false
	doPrintSomeReads := true

	// read Entries
	logit("reading data")
	ts1 = time.Now()
	var step int
	if count >= 100 {
		step = count / 100
	} else {
		step = 1
	}
	for i := strt; i < stop; i += step {
		v, err := lsm.Get(makeKey(i))
		if err != nil && err == ErrNotFound {
			// skip, we don't care if it's not found
			continue
		}
		if err != nil {
			t.Errorf("get: %v\n", err)
		}
		if doPrintAllReads {
			fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
		} else if doPrintSomeReads {
			if i%step == 0 {
				fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
			}
		}
	}
	ts2 = time.Now()
	fmt.Println(util.FormatTime("reading Entries", ts1, ts2))

	doDelete := true
	if doDelete {
		// remove Entries
		logit("removing data (only odds)")
		ts1 = time.Now()
		for i := strt; i < stop; i++ {
			if i%2 != 0 {
				key := makeKey(i)
				err = lsm.Del(key)
				if err != nil {
					t.Errorf("del: %v\n", err)
				}
			}
		}
		ts2 = time.Now()
		fmt.Println(util.FormatTime("removing Entries", ts1, ts2))
	}

	// close
	logit("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open
	logit("opening lsm tree")
	lsm, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// read Entries
	logit("reading data")
	ts1 = time.Now()
	for i := strt; i < stop; i += step {
		v, err := lsm.Get(makeKey(i))
		if err != nil && err == ErrNotFound {
			// skip, we don't care if it's not found
			continue
		}
		if err != nil {
			t.Errorf("get: %v\n", err)
		}
		if doPrintAllReads {
			fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
		} else if doPrintSomeReads {
			if i%step == 0 {
				fmt.Printf("get(%q) -> %q\n", makeKey(i), v)
			}
		}
	}
	ts2 = time.Now()
	fmt.Println(util.FormatTime("reading data", ts1, ts2))

	_ = WriteLastSequenceNumber(int64(stop-1), conf.BaseDir)

	//
	err = lsm.sstm.CompactAllSSTables()
	if err != nil {
		t.Errorf("lsm.compact error: %s\n", err)
	}

	// close
	logit("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	conf.BaseDir = origPath

}

func TestLSMTree_Put(t *testing.T) {
}

func TestLSMTree_Get(t *testing.T) {

}

func TestLSMTree_Del(t *testing.T) {

}

func TestLSMTree_Close(t *testing.T) {

}

func WriteLastSequenceNumber(n int64, base string) error {
	dat := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(dat, n)
	file := filepath.Join(base, "last-seq.dat")
	err := os.WriteFile(file, dat, 0666)
	if err != nil {
		return err
	}
	return nil
}

func ReadLastSequenceNumber(base string) (int64, error) {
	file := filepath.Join(base, "last-seq.dat")
	dat, err := os.ReadFile(file)
	if err != nil {
		return -1, err
	}
	err = os.RemoveAll(file)
	n, _ := binary.Varint(dat)
	return n, nil
}

var smVal = `Praesent efficitur, ante eget eleifend scelerisque, neque erat malesuada neque, vel euismod 
dui leo a nisl. Donec a eleifend dui. Maecenas necleo odio. In maximus convallis ligula eget sodales.`

var mdVal = `Quisque bibendum tellus ac odio dictum vulputate. Sed imperdiet enim eget tortor vehicula, 
nec vehicula erat lacinia. Praesent et bibendum turpis. Mauris ac blandit nulla, ac dignissim 
quam. Ut ut est placerat quam suscipit sodales a quis lacus. Praesent hendrerit mattis diam et 
sodales. In a augue sit amet odio iaculis tempus sed a erat. Donec quis nisi tellus. Nam hendrerit 
purus ligula, id bibendum metus pulvinar sed. Nulla eu neque lobortis, porta elit quis, luctus 
purus. Vestibulum et ultrices nulla. Curabitur sagittis, sem sed elementum aliquam, dui mauris 
interdum libero, ullamcorper convallis urna tortor ornare metus. Integer non nibh id diam accumsan 
tincidunt. Quisque sed felis aliquet, luctus dolor vitae, porta nibh. Vestibulum ac est mollis, 
sodales erat et, pharetra nibh. Maecenas porta diam in elit venenatis, sed bibendum orci 
feugiat. Suspendisse diam enim, dictum quis magna sed, aliquet porta turpis. Etiam scelerisque 
aliquam neque, vel iaculis nibh laoreet ac. Sed placerat, arcu eu feugiat ullamcorper, massa 
justo aliquet lorem, id imperdiet neque ipsum id diam. Vestibulum semper felis urna, sit amet 
volutpat est porttitor nec. Phasellus lacinia volutpat orci, id eleifend ipsum semper non. 
`

var lgVal = `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent libero turpis, aliquam quis 
consequat ac, volutpat et arcu. Nullam varius, ligula eu venenatis dignissim, lectus ligula 
ullamcorper odio, in rhoncus nisi nisl congue sem. In hac habitasse platea dictumst. Donec 
sem est, rutrum ut libero nec, placerat vehicula neque. Nulla mollis dictum nunc, ut viverra 
ex. Nam ac lacus at quam rhoncus finibus. Praesent efficitur, ante eget eleifend scelerisque, 
neque erat malesuada neque, vel euismod dui leo a nisl. Donec a eleifend dui. Maecenas nec 
leo odio. In maximus convallis ligula eget sodales. Nullam a mi hendrerit, finibus dolor eu, 
pellentesque ligula. Proin ultricies vitae neque sit amet tempus. Sed a purus enim. Maecenas 
maximus placerat risus, at commodo libero consectetur sed. Nullam pulvinar lobortis augue in 
pulvinar. Aliquam erat volutpat. Vestibulum eget felis egestas, sollicitudin sem eu, venenatis 
metus. Nam ac eros vel sem suscipit facilisis in ut ligula. Nulla porta eros eu arcu efficitur 
molestie. Proin tristique eget quam quis ullamcorper. Integer pretium tellus non sapien euismod, 
et ultrices leo placerat. Suspendisse potenti. Aenean pulvinar pretium diam, lobortis pretium 
sapien congue quis. Fusce tempor, diam id commodo maximus, mi turpis rhoncus orci, ut blandit 
ipsum turpis congue dolor. Aenean lobortis, turpis nec dignissim pulvinar, sem massa bibendum 
lorem, ut scelerisque nibh odio sed odio. Sed sed nulla lectus. Donec vitae ipsum dolor. Donec 
eu gravida lectus. In tempor ultrices malesuada. Cras sodales in lacus et volutpat. Vivamus 
nibh ante, egestas vitae faucibus id, consectetur at augue. Pellentesque habitant morbi tristique 
senectus et netus et malesuada fames ac turpis egestas. Pellentesque quis velit non quam convallis 
molestie sit amet sit amet metus. Aenean eget sapien nisl. Lorem ipsum dolor sit amet, consectetur 
adipiscing elit. Donec maximus nisi in nunc pellentesque imperdiet. Aliquam erat volutpat. 
Quisque bibendum tellus ac odio dictum vulputate. Sed imperdiet enim eget tortor vehicula, nec 
vehicula erat lacinia. Praesent et bibendum turpis. Mauris ac blandit nulla, ac dignissim quam. 
Ut ut est placerat quam suscipit sodales a quis lacus. Praesent hendrerit mattis diam et sodales. 
In a augue sit amet odio iaculis tempus sed a erat. Donec quis nisi tellus. Nam hendrerit purus 
ligula, id bibendum metus pulvinar sed. Nulla eu neque lobortis, porta elit quis, luctus purus. 
Vestibulum et ultrices nulla. Curabitur sagittis, sem sed elementum aliquam, dui mauris interdum 
libero, ullamcorper convallis urna tortor ornare metus. Integer non nibh id diam accumsan 
tincidunt. Quisque sed felis aliquet, luctus dolor vitae, porta nibh. Vestibulum ac est mollis, 
sodales erat et, pharetra nibh. Maecenas porta diam in elit venenatis, sed bibendum orci 
feugiat. Suspendisse diam enim, dictum quis magna sed, aliquet porta turpis. Etiam scelerisque 
aliquam neque, vel iaculis nibh laoreet ac. Sed placerat, arcu eu feugiat ullamcorper, massa 
justo aliquet lorem, id imperdiet neque ipsum id diam. Vestibulum semper felis urna, sit amet 
volutpat est porttitor nec. Phasellus lacinia volutpat orci, id eleifend ipsum semper non. 
Pellentesque quis velit non quam convallis molestie sit amet sit amet metus. Aenean eget sapien 
nisl. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec maximus nisi in nunc 
pellentesque imperdiet. Aliquam erat volutpat. Quisque bibendum tellus ac odio dictum vulputate. 
Sed imperdiet enim eget tortor vehicula, nec vehicula erat lacinia. Praesent et bibendum turpis. 
Mauris ac blandit nulla, ac dignissim quam. Ut ut est placerat quam suscipit sodales a quis 
lacus. Praesent hendrerit mattis diam et sodales. In a augue sit amet odio iaculis tempus sed 
a erat. Donec quis nisi tellus. Nam hendrerit purus ligula, id bibendum metus pulvinar sed. 
Nulla eu neque lobortis, porta elit quis, luctus purus. Vestibulum et ultrices nulla.`
