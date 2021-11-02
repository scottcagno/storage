package lsmtree

import (
	"fmt"
	"log"
	"testing"
)

const thousand = 1000

// lsmtree options
var opt = &Options{
	BaseDir:      "lsmtree-testing",
	SyncOnWrite:  false,
	LoggingLevel: LevelOff,
}

func logAndCheckErr(msg string, err error, t *testing.T) {
	log.Println(msg)
	if err != nil {
		t.Fatalf("%s: %v\n", msg, err)
	}
}

func doNTimes(n int, fn func(i int)) {
	for i := 0; i < n; i++ {
		fn(i)
	}
}

func TestLSMTree_Put(t *testing.T) {

	// open
	db, err := OpenLSMTree(opt)
	logAndCheckErr("opening", err, t)

	// write
	doNTimes(1*thousand, func(i int) {
		// write entry
		err := db.Put(makeData("key", i), []byte(mdVal))
		if err != nil {
			t.Fatalf("put: %v\n", err)
		}
	})

	// close
	err = db.Close()
	logAndCheckErr("closing", err, t)
}

func TestLSMTree_PutBatch(t *testing.T) {

	// open
	db, err := OpenLSMTree(opt)
	logAndCheckErr("opening", err, t)

	// make new batch
	batch := NewBatch()

	// write
	doNTimes(1*thousand, func(i int) {
		// write entry to batch
		err := batch.Write(makeData("key", i), []byte(smVal))
		if err != nil {
			t.Fatalf("batch write: %v\n", err)
		}
	})

	// write batch
	err = db.PutBatch(batch)
	logAndCheckErr("put batch", err, t)

	// close
	err = db.Close()
	logAndCheckErr("closing", err, t)

}

func TestLSMTree_Get(t *testing.T) {

	// open
	db, err := OpenLSMTree(opt)
	logAndCheckErr("opening", err, t)

	// read
	doNTimes(1*thousand, func(i int) {
		// get entry at i
		k := makeData("key", i)
		v, err := db.Get(k)
		if err != nil {
			t.Fatalf("get(%q): %v\n", k, err)
		}
		fmt.Printf("got(%q)->%q\n", k, v)
	})

	// close
	err = db.Close()
	logAndCheckErr("closing", err, t)
}

func TestLSMTree_GetBatch(t *testing.T) {

	// open
	db, err := OpenLSMTree(opt)
	logAndCheckErr("opening", err, t)

	// make keys
	var keys [][]byte
	doNTimes(1*thousand, func(i int) {
		// get entry at i
		k := makeData("key", i)
		keys = append(keys, k)
	})
	// read using get batch
	batch, err := db.GetBatch(keys...)
	logAndCheckErr("read using get batch", err, t)
	for i := range batch.Entries {
		fmt.Printf("%s\n", batch.Entries[i])
	}

	// close
	err = db.Close()
	logAndCheckErr("closing", err, t)
}

func TestLSMTreeKeyOverride(t *testing.T) {

	db, err := OpenLSMTree(opt)
	logAndCheckErr("opening", err, t)

	err = db.Put([]byte("Hi!"), []byte("Hello world, LSMTree!"))
	logAndCheckErr("put (1st)", err, t)

	err = db.Put([]byte("Does it override key?"), []byte("No!"))
	logAndCheckErr("put (2nd)", err, t)

	err = db.Put([]byte("Does it override key?"), []byte("Yes, absolutely! The key has been overridden."))
	logAndCheckErr("put (2nd override)", err, t)

	err = db.Close()
	logAndCheckErr("closing", err, t)

	db, err = OpenLSMTree(opt)
	logAndCheckErr("opening", err, t)

	key := []byte("Hi!")
	val, err := db.Get(key)
	logAndCheckErr("get (1st)", err, t)
	fmt.Printf("get(%q)=%q\n", key, val)

	key = []byte("Does it override key?")
	val, err = db.Get(key)
	logAndCheckErr("get (2nd)", err, t)
	fmt.Printf("get(%q)=%q\n", key, val)

	err = db.Close()
	logAndCheckErr("closing", err, t)

	// Expected output:
	// Hello world, LSMTree!
	// Yes, absolutely! The key has been overridden.
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
