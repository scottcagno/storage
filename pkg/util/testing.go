package util

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"text/tabwriter"
)

func Trace() string {
	pc := make([]uintptr, 10) // at least 1 entry needed
	runtime.Callers(3, pc)
	f := runtime.FuncForPC(pc[0])
	file, line := f.FileLine(pc[0])
	sfile := strings.Split(file, "/")
	sname := strings.Split(f.Name(), "/")
	return fmt.Sprintf("[%s:%d %s]", sfile[len(sfile)-1], line, sname[len(sname)-1])
}

func BtoKB(b uint64) uint64 {
	return b / 1024
}

func BtoMB(b uint64) uint64 {
	return b / 1024 / 1024
}

func BtoGB(b uint64) uint64 {
	return b / 1024 / 1024 / 1024
}

func PrintStats(mem runtime.MemStats) {
	runtime.ReadMemStats(&mem)
	fmt.Printf("\t[MEASURMENT]\t[BYTES]\t\t[KB]\t\t[MB]\t[GC=%d]\n", mem.NumGC)
	fmt.Printf("\tmem.Alloc:\t\t%d\t%d\t\t%d\n", mem.Alloc, BtoKB(mem.Alloc), BtoMB(mem.Alloc))
	fmt.Printf("\tmem.TotalAlloc:\t%d\t%d\t\t%d\n", mem.TotalAlloc, BtoKB(mem.TotalAlloc), BtoMB(mem.TotalAlloc))
	fmt.Printf("\tmem.HeapAlloc:\t%d\t%d\t\t%d\n", mem.HeapAlloc, BtoKB(mem.HeapAlloc), BtoMB(mem.HeapAlloc))
	fmt.Printf("\t-----\n\n")
}

func PrintStatsTab(mem runtime.MemStats) {
	runtime.ReadMemStats(&mem)
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 5, 4, 4, ' ', tabwriter.AlignRight)
	fmt.Fprintln(w, "Alloc\tTotalAlloc\tHeapAlloc\tNumGC\t")
	fmt.Fprintf(w, "%v\t%v\t%v\t%v\t\n", mem.Alloc, mem.TotalAlloc, mem.HeapAlloc, mem.NumGC)
	fmt.Fprintln(w, "-----\t-----\t-----\t-----\t")
	w.Flush()
}

func DEBUG(format string, v ...interface{}) {
	log.Printf(Trace()+" "+format, v...)
}

func AssertExpected(t *testing.T, expected, got interface{}) bool {
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("error, expected: %v, got: %v\n", expected, got)
		return false
	}
	return true
}

func AssertLen(t *testing.T, expected, got interface{}) bool {
	return AssertExpected(t, expected, got)
}

func AssertEqual(t *testing.T, expected, got interface{}) bool {
	return AssertExpected(t, expected, got)
}

func AssertTrue(t *testing.T, got interface{}) bool {
	return AssertExpected(t, true, got)
}

func AssertError(t *testing.T, got interface{}) bool {
	return AssertExpected(t, got, got)
}

func AssertNoError(t *testing.T, got interface{}) bool {
	return AssertExpected(t, nil, got)
}

func AssertNil(t *testing.T, got interface{}) bool {
	return AssertExpected(t, nil, got)
}

func AssertNotNil(t *testing.T, got interface{}) bool {
	return got != nil
}

func GetListOfRandomWordsHttp(num int) []string {
	host := "https://random-word-api.herokuapp.com"
	var api string
	if num == -1 {
		api = "/all"
	} else {
		api = "/word?number=" + strconv.Itoa(num)
	}
	resp, err := http.Get(host + api)
	if err != nil {
		log.Panic(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Panic(err)
	}
	var resplist []string
	err = json.Unmarshal(body, &resplist)
	if err != nil {
		log.Panic(err)
	}
	return resplist
}
