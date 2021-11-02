package lsmtree

/*Not that, for the time being,
this is really more of a proof of
concept than an actual test*/

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestChannelSearch_TempFileExampleThingy(t *testing.T) {

	// write some data starting at n and going till p
	exampleWriteData := func(file *os.File, n, p int) *os.File {
		fmt.Printf("writing data from lines %d to %d\n", n, p)
		for i := n; i < p; i++ {
			// notice the '\n' and the end of the line
			line := fmt.Sprintf("%d: this is line number %d\n", i, i)
			_, err := file.WriteString(line)
			if err != nil {
				t.Fatalf("write: %v\n", err)
			}
		}
		err := file.Sync()
		if err != nil {
			t.Fatalf("sync: %v\n", err)
		}
		return file
	}

	// get the current location of the file pointer
	exampleGetFPOffset := func(file *os.File) int64 {
		// by calling seek offset=0, at the current position
		// we get the current offset of the file pointer
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			t.Fatalf("seek: %v\n", err)
		}
		return offset
	}

	// rewind the file pointer
	exampleRewind := func(file *os.File) {
		// A file works like a tape, or record. The file pointer moves as
		// we write or read data. So we have to go back to the beginning.
		_, err := file.Seek(0, io.SeekStart) // seek to pos 0, from the start of the file
		if err != nil {
			t.Fatalf("seek: %v\n", err)
		}
	}

	// read data and return data by line in a 2d array
	exampleReadData := func(file *os.File) [][]byte {
		// normally, this entire function would be
		// done another way, but for now this is how
		// im going to show you. I'll also show you
		// an easier read and write example elsewhere
		var lines [][]byte
		data := make([]byte, 1)
		var buffer []byte
		for {
			// read data size slice of data
			_, err := file.Read(data)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				t.Fatalf("reading: %v\n", err)
			}
			// add the current read to the buffer
			buffer = append(buffer, data...)
			// change value below to see the buffer
			printBuffer := false
			if printBuffer {
				fmt.Printf(">>>> buffer=%q\n", buffer)
			}
			// let's see if we can spot a '\n' anywhere
			n := bytes.IndexByte(buffer, '\n')
			if n == -1 {
				// keep reading
				continue
			}
			// found end of line, add to lines
			lines = append(lines, buffer[:n])
			// reset the buffer
			buffer = nil
		}
		return lines
	}

	// this requires you to use the function signature that
	// matches this -> func(file *os.File) { ... }
	myActualTestFunctionGoesHere := func(file *os.File) {
		// I have an open file I can work with in this closure,
		// and I also don't have to worry about closing or
		// removing it when I am done. For example:

		fmt.Printf("current file pointer offset: %d\n", exampleGetFPOffset(file))

		fmt.Println("Let's write some data!")
		exampleWriteData(file, 0, 100) // write lines 0-100
		fmt.Printf("current file pointer offset: %d\n", exampleGetFPOffset(file))

		fmt.Println("Now, just like our old tapes, we must rewind!")
		exampleRewind(file)
		fmt.Printf("current file pointer offset: %d\n", exampleGetFPOffset(file))

		fmt.Println("Now let's read the data we wrote!")
		lines := exampleReadData(file)
		fmt.Printf("current file pointer offset: %d\n", exampleGetFPOffset(file))

		fmt.Println("Now let's print out the data!!")
		for i := range lines {
			fmt.Printf("%s\n", lines[i])
		}
	}

	// this is how you run it
	GetTempFileForTesting(t, myActualTestFunctionGoesHere)

	myEncodingAndDecodingFuncGoesHere := func(file *os.File) {

		// take data, encode it for easier reading
		encodeData := func(data []byte) []byte {
			// get the length of the data passed in
			size := len(data)
			// make a buffer large enough to hold a uint32
			buf := make([]byte, binary.MaxVarintLen32)
			// encode the length into the buffer--we are
			// basically just converting len(data) to a
			// byte slice, so we can write it to a file.
			binary.LittleEndian.PutUint32(buf, uint32(size))
			// now, lets append the "length" to the start
			// front of the slice of data...
			buf = append(buf, data...)
			return buf
		}

		// write some data starting at n and going till p
		exampleWriteEncodedData := func(file *os.File, n, p int) *os.File {
			fmt.Printf("writing data from lines %d to %d\n", n, p)
			for i := n; i < p; i++ {
				// notice the '\n' and the end of the line
				line := encodeData([]byte(fmt.Sprintf("%d: this is ENCODED line number %d", i, i)))
				_, err := file.Write(line)
				if err != nil {
					t.Fatalf("write: %v\n", err)
				}
			}
			err := file.Sync()
			if err != nil {
				t.Fatalf("sync: %v\n", err)
			}
			return file
		}

		checkEOF := func(err error) bool {
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					return true
				}
				t.Fatalf("reading: %v\n", err)
			}
			return false
		}

		// read data and return data by line in a 2d array
		exampleReadEncodedData := func(file *os.File) [][]byte {
			var lines [][]byte
			// see how simply encoding the data length makes this
			// soooooo much easier?
			for {
				// make a new size buffer
				sizebuf := make([]byte, binary.MaxVarintLen32)
				// read data size slice of data
				_, err := file.Read(sizebuf)
				if checkEOF(err) {
					break
				}
				// decode the length of the message to be decoded
				size := binary.LittleEndian.Uint32(sizebuf)
				// make a new buffer of the correct size of the message
				data := make([]byte, size)
				// read data size slice of data
				_, err = file.Read(data)
				if checkEOF(err) {
					break
				}
				// add the read data to the lines
				lines = append(lines, data)
			}
			return lines
		}

		fmt.Printf("current file pointer offset: %d\n", exampleGetFPOffset(file))

		fmt.Println("Let's write some ENCODED data!")
		exampleWriteEncodedData(file, 0, 100) // write lines 0-100
		fmt.Printf("current file pointer offset: %d\n", exampleGetFPOffset(file))

		fmt.Println("Now, just like our old tapes, we must rewind!")
		exampleRewind(file)
		fmt.Printf("current file pointer offset: %d\n", exampleGetFPOffset(file))

		fmt.Println("Now let's read the ENCODED data we wrote!")
		lines := exampleReadEncodedData(file)
		fmt.Printf("current file pointer offset: %d\n", exampleGetFPOffset(file))

		fmt.Println("Now let's print out the data!!")
		for i := range lines {
			fmt.Printf("%s\n", lines[i])
		}
	}

	// this is another test that requires a file, and
	// im going to put it in the same place
	GetTempFileForTesting(t, myEncodingAndDecodingFuncGoesHere)
}

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
