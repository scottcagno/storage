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

	for i := 0; i < 500; i++ {
		f := &myFile{
			data: make([]*myEntry, 1000),
		}
		for j := 0; j < 1000; j++ {
			for {
				num := rand.Intn(750000)
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
	totalCFound := 0

	for i := 0; i < 1000; i++ {
		num := rand.Intn(750000)
		//try linear search for benchmark
		t := time.Now()
		_, err := linearSearch(dir, num)
		elapsed := time.Since(t)
		totalLinear += elapsed
		if err != nil {
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
		} else {
			totalCFound++
			fmt.Printf("Channel search for %v: data found! [%v] elapsed\n", num, elapsed)
		}
	}

	fmt.Printf("Linear Search: %v out of 20000 searched items found!\n", totalFound)
	fmt.Printf("Channel Search: %v out of 20000 searched items found!\n", totalCFound)
	fmt.Printf("Total linear search time: %v\n", totalLinear)
	fmt.Printf("Total channel search time: %v\n", totalChannel)

}
