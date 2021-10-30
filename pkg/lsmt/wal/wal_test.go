package wal

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/lsmt/binary"
	"os"
	"testing"
	"time"
)

var conf = &WALConfig{
	BasePath:    "wal-testing",
	MaxFileSize: -1,
	SyncOnWrite: false,
}

func TestOpenAndCloseNoWrite(t *testing.T) {
	// open
	wal, err := OpenWAL(conf)
	if err != nil {
		t.Fatalf("opening: %v\n", err)
	}
	// close
	err = wal.Close()
	if err != nil {
		t.Fatalf("closing: %v\n", err)
	}
	// open
	wal, err = OpenWAL(conf)
	if err != nil {
		t.Fatalf("opening: %v\n", err)
	}
	// close
	err = wal.Close()
	if err != nil {
		t.Fatalf("closing: %v\n", err)
	}
}

func TestWAL(t *testing.T) {
	//
	// open log
	wal, err := OpenWAL(conf)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// get path for cleanup
	path := wal.GetConfig().BasePath
	//
	// do some writing
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i+1)
		val := fmt.Sprintf("my-value-%06d-%s", i+1, lgVal)
		_, err := wal.Write(&binary.Entry{Key: []byte(key), Value: []byte(val)})
		if err != nil {
			t.Fatalf("error writing: %v\n", err)
		}
	}
	//
	// do some reading
	err = wal.Scan(func(e *binary.Entry) bool {
		fmt.Printf("%s\n", e)
		return true
	})
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// clean up
	doClean := false
	if doClean {
		err = os.RemoveAll(path)
		if err != nil {
			t.Fatalf("got error: %v\n", err)
		}
	}
}

func TestLog_Reset(t *testing.T) {
	//
	// open log
	wal, err := OpenWAL(conf)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// do some writing
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i+1)
		val := fmt.Sprintf("my-value-%06d-%s", i+1, lgVal)
		_, err := wal.Write(&binary.Entry{Key: []byte(key), Value: []byte(val)})
		if err != nil {
			t.Fatalf("error writing: %v\n", err)
		}
	}
	//
	// do some reading
	err = wal.Scan(func(e *binary.Entry) bool {
		fmt.Printf("%s\n", e)
		return true
	})
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}

	fmt.Printf("chillin for a few....")
	time.Sleep(3 * time.Second)

	err = wal.CloseAndRemove()
	if err != nil {
		t.Fatalf("close and remove: %v\n", err)
	}

}

func TestLog_TruncateFront(t *testing.T) {

	//
	// open log
	wal, err := OpenWAL(conf)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// get path for cleanup
	path := wal.GetConfig().BasePath
	//
	// do some writing
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%04d", i+1)
		val := fmt.Sprintf("my-value-%06d", i+1)
		_, err := wal.Write(&binary.Entry{Key: []byte(key), Value: []byte(val)})
		if err != nil {
			t.Fatalf("error writing: %v\n", err)
		}
	}
	//
	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// open log
	wal, err = OpenWAL(conf)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// print segment info
	fmt.Printf("--- PRINTING SEGMENT INFO ---\n")
	for _, s := range wal.segments {
		fmt.Printf("%s\n", s)
	}
	//
	// print dir structure
	files, err := os.ReadDir(path)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	for _, file := range files {
		fmt.Printf("segment: %s\n", file.Name())
	}
	//
	// test truncate front
	err = wal.TruncateFront(256)
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	fmt.Printf("--- PRINTING SEGMENT INFO ---\n")
	for _, s := range wal.segments {
		fmt.Printf("%s\n", s)
	}
	//
	// close log
	err = wal.Close()
	if err != nil {
		t.Fatalf("got error: %v\n", err)
	}
	//
	// clean up
	doClean := false
	if doClean {
		err = os.RemoveAll(path)
		if err != nil {
			t.Fatalf("got error: %v\n", err)
		}
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
