package main

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/filesystem"
	"log"
	"os"
	"path/filepath"
	"regexp"
)

func main() {
	// roughly equivalent to: grep --color=never -En "(maxKeySizeAllowed|commitLog)" pkg/lsmtree/*.go
	filesystem.Grep(`(maxKeySizeAllowed|commitLog)`, "pkg/lsmtree/*.go")
}

func testWalk(pattern, path string) {
	// compile regex pattern
	reg := regexp.MustCompile(pattern)
	// "clean" path
	path = filepath.ToSlash(path)
	// start walking
	err := filepath.Walk(path,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			// check for match
			foundMatch := reg.MatchString(path)
			if foundMatch {
				fmt.Fprintf(os.Stdout, "path: %q, size: %d\n", path, info.Size())
			}
			return nil
		})
	if err != nil {
		log.Println(err)
	}
}

func runCat() {

}

func runLS() {

}

func runGrep() {

}
