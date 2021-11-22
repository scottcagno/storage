package main

import (
	"bytes"
	"fmt"
	"github.com/scottcagno/storage/pkg/filesystem"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func main() {

	fmt.Printf("Running List... [skip printing directories, and hidden files]\n")
	if err := filesystem.List(".", func(f fs.FileInfo) error {
		// only print files, not directories
		if f.IsDir() {
			return fs.SkipDir
		}
		// only print files that are not "hidden"
		if !strings.HasPrefix(f.Name(), ".") {
			fmt.Printf("%s\n", f.Name())
		}
		return nil
	}); err != nil {
		log.Printf("List: %s\n", err)
	}

	fmt.Printf("\n\nRunning Walk... [only list contents of the base files and lsmtree]\n")
	if err := filesystem.Walk(".", func(f fs.FileInfo) error {
		// only walk a few dirs, skipping all others
		if f.IsDir() && f.Name() != "." && f.Name() != "pkg" && f.Name() != "lsmtree" {
			return fs.SkipDir
		}
		fmt.Printf("%s (dir=%v)\n", f.Name(), f.IsDir())
		return nil
	}); err != nil {
		log.Printf("List: %s\n", err)
	}

	//fmt.Printf("Running cat...\n")
	//runCat()
	//
	//fmt.Printf("\rRunning ls...\n")
	//runLS()
	//
	//fmt.Printf("\rRunning grep...\n")
	//runGrep()
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
	br := bytes.NewReader([]byte(`this is a test, this is only a test.\nplease and thank you\n`))
	filesystem.Cat(os.Stdout, br)
}

func runLS() {
	filesystem.LS("pkg/lsmtree")
}

func runGrep() {
	// roughly equivalent to: grep --color=never -En "(maxKeySizeAllowed|commitLog)" pkg/lsmtree/*.go
	filesystem.Grep(`(maxKeySizeAllowed|commitLog)`, "pkg/lsmtree/*.go")
}
