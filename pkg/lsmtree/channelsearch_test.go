package lsmtree

/*Not that, for the time being,
this is really more of a proof of
concept than an actual test*/

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestChannelSearch(t *testing.T) {

	rand.Seed(time.Now().UnixNano())

	dir := &myDir{
		files: make([]*myFile, 500),
	}

	nums := make(map[int]bool)

	fmt.Println("Making \"files\"")
	for i := 0; i < 500; i++ {
		f := &myFile{
			data: make([]*myEntry, 1000),
		}
		for j := 0; j < 1000; j++ {
			for {
				num := rand.Int()
				if _, ok := nums[num]; ok != true {
					f.data[j] = &myEntry{
						value: num,
					}
					break
				}
			}
		}
		dir.files[i] = f
	}

	var totalLinear time.Duration
	var totalChannel time.Duration

	totalFound := 0

	for i := 0; i < 20; i++ {
		//try linear search for benchmark
		num := rand.Int()
		t := time.Now()
		_, err := linearSearch(dir, num)
		elapsed := time.Since(t)
		totalLinear += elapsed
		if err != nil {
			fmt.Printf("Linear search for %v: %v. [%v] elapsed\n", num, err, elapsed)
		} else {
			totalFound++
			fmt.Printf("Linear search for %v: data found! [%v] elapsed\n", num, elapsed)
		}

		//now go for channel
		t = time.Now()
		_, err = channelSearch(dir, num)
		elapsed = time.Since(t)
		totalChannel += elapsed
		if err != nil {
			fmt.Printf("Channel search for %v: %v. [%v] elapsed\n", num, err, elapsed)
		} else {
			fmt.Printf("Channel search for %v: data found! [%v] elapsed\n", num, elapsed)
		}
	}

	fmt.Printf("%v out of 20 searched items found!\n", totalFound)
	fmt.Printf("Total linear search time: %v\n", totalLinear)
	fmt.Printf("Total channel search time: %v\n", totalChannel)

}
