package filesystem

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

var showLines = flag.Bool("n", false, "flag to output the line numbers")

func CatCLI() {
	// parse flags
	flag.Parse()
	// if no file was given, copy stdin to stdout
	if flag.NArg() == 0 {
		cat(os.Stdout, os.Stdin)
		return
	}
	// otherwise, a file was given, so open and use as input
	fd, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	// defer close
	defer func(fd *os.File) {
		err := fd.Close()
		if err != nil {
			log.Panic(err)
		}
	}(fd)
	cat(os.Stdout, fd)
}

func Cat(w io.Writer, r io.Reader) {
	show, ok := os.LookupEnv("SHOW_LINES")
	if ok {
		show = strings.ToLower(show)
		if show == "1" || show == "t" || show == "true" {
			*showLines = true
		}
	}
	cat(w, r)
}

func cat(w io.Writer, r io.Reader) {
	if !*showLines {
		_, err := io.Copy(w, r)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s", err)
		}
		return
	}
	sc, ln := bufio.NewScanner(r), 1
	for sc.Scan() {
		fmt.Fprintf(w, "\r%6d  %s\n", ln, sc.Bytes())
		ln++
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
	}
}
