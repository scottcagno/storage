package lsmtree

/*Not that, for the time being,
this is really more of a proof of
concept than an actual test*/

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestChannelSearch(t *testing.T) {

	rand.Seed(time.Now().UnixNano())

	dir := &myDir{
		files: make([]*myFile, 1000),
	}

	nums := make(map[int]bool)

	for i := 0; i < 1000; i++ {
		f := &myFile{
			data: make([]*myEntry, 10000),
		}
		for j := 0; j < 10000; j++ {
			for {
				num := rand.Intn(20000000)
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

	var combinedLinear time.Duration
	var totalLinear time.Duration
	var combinedChannel time.Duration
	var totalChannel time.Duration

	totalFound := 0
	totalCFound := 0

	myNums := make([]int, 1000)

	for i := 0; i < 1000; i++ {
		num := rand.Intn(750000)
		myNums[i] = num
	}

	var wg = &sync.WaitGroup{}

	wg.Add(2)

	go func() {
		//try linear search for benchmark
		start := time.Now()
		for _, num := range myNums {
			t := time.Now()
			_, err := linearSearch(dir, num)
			elapsed := time.Since(t)
			combinedLinear += elapsed
			if err != nil {
			} else {
				totalFound++
				fmt.Printf("Linear search for %v: data found! [%v] elapsed\n", num, elapsed)
			}
		}
		totalLinear = time.Since(start)
		wg.Done()
	}()

	go func() {
		//now go for channel
		start := time.Now()
		for _, num := range myNums {
			t := time.Now()
			_, err := channelSearch(dir, num)
			elapsed := time.Since(t)
			combinedChannel += elapsed
			if err != nil {
			} else {
				totalCFound++
				fmt.Printf("Channel search for %v: data found! [%v] elapsed\n", num, elapsed)
			}
		}
		totalChannel = time.Since(start)
		wg.Done()
	}()

	wg.Wait()

	fmt.Println("1,000 files and 10,000,000 entries searched")
	fmt.Printf("Linear Search: %v out of 1000 searched items found!\n", totalFound)
	fmt.Printf("Channel Search: %v out of 1000 searched items found!\n", totalCFound)
	fmt.Printf("Total linear search time: [combined]%v, [total] %v\n", combinedLinear, totalLinear)
	fmt.Printf("Total channel search time: [combined]%v, [total]%v\n", combinedChannel, totalChannel)

}
