package filesystem

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var showAll = flag.Bool("a", false, "flag to output all files")
var showLong = flag.Bool("l", false, "flag to output long format")
var sortModTime = flag.Bool("t", false, "flag to sort output by modification time")
var sortSize = flag.Bool("s", false, "flag to sort output by size")

func LSCli() {
	// parse flags
	flag.Parse()
	// if no file was given, copy stdin to stdout
	if flag.NArg() == 0 {
		ls(".")
		return
	}
	// otherwise, a file was given, so use that as input
	ls(flag.Arg(0))
}

func LS(dir string) {
	if dir == "" {
		dir = "."
	}
	ls(dir)
}

const doNotSkip bool = false

func ls(dir string) {
	// skip function
	skip := func(de fs.DirEntry) bool {
		return doNotSkip
	}
	// setup listing
	var listing []fs.DirEntry
	// walk dir path
	err := filepath.WalkDir(dir, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			fmt.Fprintf(os.Stderr, "prevent panic by handling failure accessing a path %q: %v\n", path, err)
			return err
		}
		if skip(de) {
			return fs.SkipDir
		}
		if canPrint(de) {
			listing = append(listing, de)
			//fmt.Fprintf(os.Stdout, "%s", fmtEntry(de))
		}
		return nil
	})
	// check for errors
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
	}
	// finish
	printListing(listing)
	if !*showLong {
		fmt.Println()
	}
}

func canPrint(de fs.DirEntry) bool {
	if *showAll {
		return true
	}
	return !strings.HasPrefix(de.Name(), ".")
}

func fmtEntry(de fs.DirEntry) string {
	if *showLong {
		return fmt.Sprintf("%s\n", de.Name())
	}
	return fmt.Sprintf("%s  ", de.Name())
}

func printListing(listing []fs.DirEntry) {
	if *sortSize {
		By(size).Sort(listing)
	}
	if *sortModTime {
		By(modtime).Sort(listing)
	}
	for _, de := range listing {
		fmt.Fprintf(os.Stdout, "%s", fmtEntry(de))
	}
}

var size = func(d1, d2 fs.DirEntry) bool {
	d1i, err := d1.Info()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
	}
	d2i, err := d2.Info()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
	}
	return d1i.Size() > d2i.Size()
}

var modtime = func(d1, d2 fs.DirEntry) bool {
	d1i, err := d1.Info()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
	}
	d2i, err := d2.Info()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
	}
	return d1i.ModTime().UnixNano() > d2i.ModTime().UnixNano()
}

type dirEntrySorter struct {
	entries []fs.DirEntry
	by      func(e1, e2 fs.DirEntry) bool
}

func (des *dirEntrySorter) Len() int {
	return len(des.entries)
}

func (des *dirEntrySorter) Swap(i, j int) {
	des.entries[i], des.entries[j] = des.entries[j], des.entries[i]
}

func (des *dirEntrySorter) Less(i, j int) bool {
	return des.by(des.entries[i], des.entries[j])
}

type By func(e1, e2 fs.DirEntry) bool

func (by By) Sort(entries []fs.DirEntry) {
	des := &dirEntrySorter{
		entries: entries,
		by:      by,
	}
	sort.Sort(des)
}
