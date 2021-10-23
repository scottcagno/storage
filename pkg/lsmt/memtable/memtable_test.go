package memtable

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"github.com/scottcagno/storage/pkg/lsmt/trees/rbtree"
	"github.com/scottcagno/storage/pkg/util"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

var conf = &MemtableConfig{
	BasePath:       "memtable-testing",
	FlushThreshold: -1,
	SyncOnWrite:    false,
}

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

type Memtables struct {
	//mtc    [2]*MemtableConfig
	base   string
	mts    [2]*Memtable
	active int
}

func OpenMemtables(base string) (*Memtables, error) {
	mt, err := OpenMemtable(&MemtableConfig{
		BasePath: filepath.Join(base, strconv.Itoa(0)),
	})
	if err != nil {
		return nil, err
	}
	mts := &Memtables{
		base:   base,
		mts:    [2]*Memtable{mt, nil},
		active: 0,
	}
	return mts, nil
}

func (mts *Memtables) Close() error {
	err := mts.mts[mts.active].Close()
	if err != nil {
		return err
	}
	return nil
}

func (mts *Memtables) Has(k string) bool {
	return mts.mts[mts.active].Has(k)
}

func (mts *Memtables) Put(e *binary.Entry) error {
	return mts.mts[mts.active].Put(e)
}

func (mts *Memtables) Get(k string) (*binary.Entry, error) {
	return mts.mts[mts.active].Get(k)
}

func (mts *Memtables) Del(k string) error {
	return mts.mts[mts.active].Del(k)
}

func (mts *Memtables) CheckCycle(err error) error {
	if err != nil && err == ErrFlushThreshold {
		var swap int
		if mts.active == 0 {
			swap = 1
		}
		if mts.active == 1 {
			swap = 0
		}
		// open a new mem-table (soon to be the active one)
		mts.mts[swap], err = OpenMemtable(&MemtableConfig{
			BasePath: filepath.Join(mts.base, strconv.Itoa(swap)),
		})
		if err != nil {
			return err
		}
		// swap the tables
		old := mts.active
		mts.active = swap
		// close the active mem-table (it will be inactive momentarily)
		err = mts.mts[old].Close()
		if err != nil {
			return err
		}
		// wipe the active mem-table (it will be inactive momentarily)
		err = os.RemoveAll(filepath.Join(mts.base, strconv.Itoa(old)))
		if err != nil {
			return err
		}
		mts.mts[old] = nil
	}
	return nil
}

func Test_REPLICATE_MTS(t *testing.T) {

	origPath := conf.BasePath
	tempPath := filepath.Join(conf.BasePath, "attempting-to-replicate")
	conf.BasePath = tempPath

	// open mem-table
	logger("opening mem-table")
	mts, err := OpenMemtables(conf.BasePath)
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
	for i := 0; i < 512; i++ {
		e := &binary.Entry{[]byte(makeKey(i)), makeCustomVal(i, lgVal)}
		err = mts.Put(e)
		if err != nil && err != ErrFlushThreshold {
			t.Errorf("error type: %T, put: %v\n", err, err)
		}
		if err == ErrFlushThreshold {
			logger("checking cycle...")
			err = mts.CheckCycle(err)
			//logger("simulating ss-table dump")
			//err = mt.Reset()
			//if err != nil {
			//	logger("Ohhhh, I think I've isolated something...")
			//	time.Sleep(15 * time.Second)
			//}
			if err != nil {
				t.Errorf("cycle: %v\n", err)
			}
		}
	}
	ts2 := time.Now()
	fmt.Println(util.FormatTime("writing entries", ts1, ts2))

	// close
	logger("closing mem-table")
	err = mts.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open mem-table
	logger("opening mem-table")
	mts, err = OpenMemtables(conf.BasePath)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}
	util.DEBUG(">>>>>>>>> has 500: %v\n", mts.Has(makeKey(500)))

	// delete record(s)
	logger("deleting record(s) [500,501,502,503 and 505]")
	err = mts.Del(makeKey(500))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}
	err = mts.Del(makeKey(501))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}
	err = mts.Del(makeKey(502))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}
	err = mts.Del(makeKey(503))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}
	err = mts.Del(makeKey(505))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}

	// close
	logger("closing mem-table")
	err = mts.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open mem-table
	logger("opening mem-table")
	mts, err = OpenMemtables(conf.BasePath)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// checking for records
	logger("checking for records [475-512]")
	for i := 475; i < 512; i++ {
		key := makeKey(i)
		ok := mts.Has(key)
		log.Printf("[record: %d] has(%s): %v\n", i, key, ok)
	}

	// close
	logger("closing mem-table")
	err = mts.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	conf.BasePath = origPath
}

func Test_REPLICATE(t *testing.T) {

	origPath := conf.BasePath
	tempPath := filepath.Join(conf.BasePath, "attempting-to-replicate")
	conf.BasePath = tempPath

	// open mem-table
	logger("opening mem-table")
	mt, err := OpenMemtable(conf)
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
	for i := 0; i < 512; i++ {
		e := &binary.Entry{[]byte(makeKey(i)), makeCustomVal(i, lgVal)}
		err = mt.Put(e)
		if err != nil && err != ErrFlushThreshold {
			t.Errorf("error type: %T, put: %v\n", err, err)
		}
		if err == ErrFlushThreshold {
			logger("simulating ss-table dump")
			err = mt.Reset()
			if err != nil {
				logger("Ohhhh, I think I've isolated something...")
				//time.Sleep(15 * time.Second)
			}
			if err != nil {
				t.Errorf("reset: %v\n", err)
			}
		}
	}
	ts2 := time.Now()
	fmt.Println(util.FormatTime("writing entries", ts1, ts2))

	// close
	logger("closing mem-table")
	err = mt.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open mem-table
	logger("opening mem-table")
	mt, err = OpenMemtable(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}
	util.DEBUG(">>>>>>>>> has 500: %v\n", mt.Has(makeKey(500)))

	// delete record(s)
	logger("deleting record(s) [500,501,502,503 and 505]")
	err = mt.Del(makeKey(500))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}
	err = mt.Del(makeKey(501))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}
	err = mt.Del(makeKey(502))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}
	err = mt.Del(makeKey(503))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}
	err = mt.Del(makeKey(505))
	if err != nil {
		t.Errorf("delete: %v\n", err)
	}

	// close
	logger("closing mem-table")
	err = mt.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	// open mem-table
	logger("opening mem-table")
	mt, err = OpenMemtable(conf)
	if err != nil {
		t.Errorf("open: %v\n", err)
	}

	// checking for records
	logger("checking for records [475-512]")
	for i := 475; i < 512; i++ {
		key := makeKey(i)
		ok := mt.Has(key)
		log.Printf("[record: %d] has(%s): %v\n", i, key, ok)
	}

	// close
	logger("closing mem-table")
	err = mt.Close()
	if err != nil {
		t.Errorf("close: %v\n", err)
	}

	conf.BasePath = origPath
}

func TestMemtable_Reset(t *testing.T) {

	path := conf.BasePath

	// open mem-table
	memt, err := OpenMemtable(conf)
	HandleErr(t, "opening", err)

	// create new batch
	batch := binary.NewBatch()

	// write some data to batch
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i+1)
		val := fmt.Sprintf("my-value-%06d", i+1)
		batch.WriteEntry(&binary.Entry{Key: []byte(key), Value: []byte(val)})
		HandleErr(t, "writing", err)
	}

	// write batch data to memt
	for i := range batch.Entries {
		e := batch.Entries[i]
		err = memt.Put(e)
		HandleErr(t, "writing", err)
	}

	// attempt reset
	err = memt.Reset()
	HandleErr(t, "reset", err)

	// write some more data
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key-%04d", i+1)
		val := fmt.Sprintf("my-value-%06d", i+1)
		err = memt.Put(&binary.Entry{Key: []byte(key), Value: []byte(val)})
		HandleErr(t, "writing", err)
	}

	// close mem-table
	err = memt.Close()
	HandleErr(t, "closing", err)

	// clean up (maybe)
	doClean := false
	if doClean {
		err = os.RemoveAll(path)
		HandleErr(t, "remove all", err)
	}
}

func TestMemtable_All(t *testing.T) {

	path := conf.BasePath

	// open mem-table
	memt, err := OpenMemtable(conf)
	HandleErr(t, "opening", err)

	// if there is data, read it and exit
	if memt.Len() > 0 {
		memt.Scan(func(me rbtree.RBEntry) bool {
			fmt.Printf("%s\n", me.(memtableEntry))
			return true
		})
		goto close
	}

	// otherwise, write some data
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i+1)
		val := fmt.Sprintf("my-value-%06d", i+1)
		err = memt.Put(&binary.Entry{Key: []byte(key), Value: []byte(val)})
		HandleErr(t, "writing", err)
	}

close:
	// close mem-table
	err = memt.Close()
	HandleErr(t, "closing", err)

	// clean up (maybe)
	doClean := false
	if doClean {
		err = os.RemoveAll(path)
		HandleErr(t, "remove all", err)
	}
}

func HandleErr(t *testing.T, str string, err error) {
	if err != nil {
		t.Fatalf("%s: %v\n", str, err)
	}
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
