package lsmtree

//wasnt' really sure where you wanted me to put this, I hope here is ok
//I just copy/pasted from playground, I'll be cleaning this up then

import (
	"fmt"
	"sync"
)

//this is a placeholder for an actual directory
type myDir struct {
	files []*myFile
}

//placeholder for an actual file
type myFile struct {
	data []*myEntry
}

//just going to search int for now
type myEntry struct {
	value int
}

//This is just a quick search function I whipped up for an example, it would
//be replaced by whatever search function you're currently using for the sstable.
//Once I get this into my IDE I'll implement something that actually works with files.
func search(f *myFile, i int, c chan *myEntry, wg *sync.WaitGroup) {
	for _, v := range f.data {
		if v.value == i {
			c <- v
			wg.Done()
			return
		}
	}
	wg.Done()
	return
}

//I'm not sure what the input parameter will be, I'm assuming we'll be searching a
//directory, but for now I'm just building very simply
func channelSearch(d *myDir, i int) (*myEntry, error) {
	//this is the channel we'll be receiving data on
	c := make(chan *myEntry)
	//the wait group will tell us when all the concurrent search functions are over
	wg := &sync.WaitGroup{}

	//turns out making this concurrent was a bad idea, because waitgroup
	//I guess we can make it concurrent again if there's a way to count
	//how many files we need to search, then just add that number directly
	//to the wait group. But I really feel like blocking here is slowing us down
	for _, f := range d.files {
		wg.Add(1)
		go search(f, i, c, wg)
	}

	//close c if all searches finish without returning a value
	go func() {
		wg.Wait()
		close(c)
		return
	}()

	//listen on channel c. This syntax is a little awkward, since I'm not
	//running it concurrently, but I'm also not blocking on this function.
	//I should figure out a way to make this a go func(){}() without having
	//to create another channel, thus defeating the purpose of running this
	//loop concurrently
	for {
		v, ok := <-c
		if ok != true { //if we've closed the channel
			break
		}
		return v, nil
	}

	//I should really put the wg blocking operation here, just gotta wrap my head around the logistics first

	return &myEntry{}, fmt.Errorf("Not Found")

}

//linear search to benchmark beside
func linearSearch(d *myDir, i int) (int, error) {
	for _, f := range d.files {
		for _, e := range f.data {
			if e.value == i {
				return i, nil
			}
		}
	}
	return -1, fmt.Errorf("Not found")
}
