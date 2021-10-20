package lsmt

import (
	"encoding/binary"
	"fmt"
	binary2 "github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/util"
	"hash/fnv"
	"log"
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

func logger(s string) {
	log.SetPrefix("[INFO] ")
	log.Printf("%s\n", s)
}

var conf = &LSMConfig{
	BasePath:              "lsm-testing",
	FlushThreshold:        -1,
	SyncOnWrite:           false,
	CompactAndMergeOnOpen: false,
	BloomFilterSize:       1 << 16,
}

func TestOpenAndCloseNoWrite(t *testing.T) {

	db, err := OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	err = db.Close()
	if err != nil {
		t.Errorf("open: %v\n", err)
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

	origPath := conf.BasePath
	tempPath := filepath.Join(conf.BasePath, "sstables")
	conf.BasePath = tempPath

	// open lsm tree
	logger("opening lsm tree")
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
	logger("writing data")
	ts1 := time.Now()
	for i := 0; i < 1024; i++ {
		err := lsm.Put(makeKey(i), makeCustomVal(i, smVal))
		if err != nil {
			t.Errorf("put: %v\n", err)
		}
	}
	ts2 := time.Now()
	fmt.Println(util.FormatTime("writing entries", ts1, ts2))

	doSync := true
	if doSync {
		// manual sync
		logger("manual sync")
		err = lsm.Sync()
		if err != nil {
			t.Errorf("manual sync: %v\n", err)
		}
	}

	// close
	logger("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	conf.BasePath = origPath

}

func testLSMTreeHasAndBatches(t *testing.T) {

	origPath := conf.BasePath
	tempPath := filepath.Join(conf.BasePath, "batches")
	conf.BasePath = tempPath

	logger(fmt.Sprintf("bloom fileter size: %d\n", conf.BloomFilterSize))

	// open lsm tree
	logger("opening lsm tree")
	lsm, err := OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	count := 32500

	// batch some entries
	logger("batching some entries")
	ts1 := time.Now()
	batch := binary2.NewBatch()
	for i := 0; i < count; i++ {
		batch.Write(makeKey(i), makeVal(i))
	}
	ts2 := time.Now()
	fmt.Println(util.FormatTime("batching entries", ts1, ts2))

	// write batch
	logger("write batch")
	ts1 = time.Now()
	err = lsm.PutBatch(batch)
	if err != nil {
		t.Errorf("put batch: %v\n", err)
	}
	ts2 = time.Now()
	fmt.Println(util.FormatTime("writing batch", ts1, ts2))

	// manual sync
	logger("manual sync")
	err = lsm.Sync()
	if err != nil {
		t.Errorf("manual sync: %v\n", err)
	}

	// close
	logger("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open lsm tree
	logger("opening lsm tree")
	lsm, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// check has
	logger("checking has")
	for i := range batch.Entries {
		if i%500 == 0 {
			logger(fmt.Sprintf("checking entry: %d", i))
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

	mid := batch.Len() / 2
	b1 := batch.Entries[:mid]
	b2 := batch.Entries[mid:]

	// check get batch 1
	logger("checkin get batch 1")
	var keys1 []string
	for _, e := range b1 {
		keys1 = append(keys1, string(e.Key))
	}
	_, err = lsm.GetBatch(keys1...)
	if err != nil {
		t.Errorf("(1) getbatch: %v\n", err)
	}

	// check get batch 2
	logger("checkin get batch 2")
	var keys2 []string
	for _, e := range b2 {
		keys2 = append(keys2, string(e.Key))
	}
	_, err = lsm.GetBatch(keys2...)
	if err != nil {
		t.Errorf("(2) getbatch: %v\n", err)
	}

	// close
	logger("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	conf.BasePath = origPath
}

func TestLSMTree(t *testing.T) {

	// sstable tests
	//testSSTableBehavior(t)

	// test has and batching
	testLSMTreeHasAndBatches(t)
	return

	max := 100000
	for i := 10; i <= max; i *= 10 {
		log.Printf("running tests with count: %d\n", i)
		testingLSMTreeN(i, t)
		runtime.GC()
		time.Sleep(3)
	}
	doClean := false
	if doClean {
		err := os.RemoveAll(conf.BasePath)
		if err != nil {
			t.Errorf("remove: %s, err: %v\n", conf.BasePath, err)
		}
	}
}

func testingLSMTreeN(count int, t *testing.T) {

	strt := 0
	stop := strt + count

	origPath := conf.BasePath
	tempPath := filepath.Join(conf.BasePath, strconv.Itoa(count))
	conf.BasePath = tempPath

	n, err := ReadLastSequenceNumber(conf.BasePath)
	if n > 0 && err == nil {
		strt = int(n)
		stop = strt + count
	}
	util.DEBUG("start: %d, stop: %d, count: %d\n", strt, stop, count)

	// open lsm tree
	logger("opening lsm tree")
	lsm, err := OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// write Entries
	logger("writing data")
	ts1 := time.Now()
	for i := strt; i < stop; i++ {
		err := lsm.Put(makeKey(i), makeVal(i))
		if err != nil {
			t.Errorf("put: %v\n", err)
		}
	}
	ts2 := time.Now()
	fmt.Println(util.FormatTime("writing entries", ts1, ts2))

	err = lsm.Sync()
	if err != nil {
		t.Errorf(">>> syncing: %v\n", err)
	}

	// close
	logger("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open
	logger("opening lsm tree")
	lsm, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	doPrintAllReads := false
	doPrintSomeReads := true

	// read Entries
	logger("reading data")
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
		logger("removing data (only odds)")
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
	logger("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open
	logger("opening lsm tree")
	lsm, err = OpenLSMTree(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// read Entries
	logger("reading data")
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

	_ = WriteLastSequenceNumber(int64(stop-1), conf.BasePath)

	//
	err = lsm.sstm.CompactAllSSTables()
	if err != nil {
		t.Errorf("lsm.compact error: %s\n", err)
	}

	// close
	logger("closing lsm tree")
	err = lsm.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	conf.BasePath = origPath

}

func TestLSMTree_Put(t *testing.T) {
	fnv.New32()
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
