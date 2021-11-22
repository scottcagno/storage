package filesystem

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

func GrepWriter(w io.Writer, pattern string, path string) {
	grep(w, pattern, path)
}

func Grep(pattern string, path string) {
	grep(os.Stdout, pattern, path)
}

func grep(w io.Writer, pattern string, path string) {
	// "error check"
	if w == nil {
		w = os.Stdout
	}
	// compile regex pattern
	reg, err := regexp.Compile(pattern)
	if err != nil {
		return
	}
	// "clean" path
	dir, file := filepath.Split(filepath.ToSlash(path))
	// start walking
	err = filepath.Walk(dir,
		func(lpath string, info os.FileInfo, err error) error {
			// clean local path
			lpath = filepath.ToSlash(lpath)
			// handle path error
			if err != nil {
				fmt.Fprintf(os.Stderr, "prevent panic by handling failure accessing a path %q: %v\n", lpath, err)
				return err
			}
			// check for local file path match
			fileMatches, _ := filepath.Match(file, lpath)
			if !info.IsDir() && fileMatches {
				// do search
				err = search(w, lpath, reg)
				if err != nil {
					return err
				}
			}
			return nil
		})
	// check for errors
	if err != nil {
		fmt.Fprintf(os.Stderr, "walk: %T, %+v, %s", err, err, err)
	}
}

func search(w io.Writer, path string, reg *regexp.Regexp) error {
	// open file
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	// get scanner and start scanning
	sc, ln := bufio.NewScanner(fd), 1
	for sc.Scan() {
		// check for match in file
		foundMatch := reg.Match(sc.Bytes())
		if foundMatch {
			fmt.Fprintf(w, "\r%s:%d:%s\n", fd.Name(), ln, sc.Bytes())
			//break
		}
		ln++
	}
	// close file
	err = fd.Close()
	if err != nil {
		return err
	}
	// check scan errors
	if err := sc.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scanner: %T %s\n", err, err)
		return err
	}
	return nil
}
