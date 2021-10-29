package main

import (
	"fmt"
	"github.com/scottcagno/storage/pkg/util"
	"os"
	"path/filepath"
	"time"
)

func main() {

	// Setup our Ctrl+C handler
	util.ShutdownHook(func() {
		// remove all files
		DeleteFiles()
	})

	// Run our program... We create a file to clean up then sleep
	CreateFile()
	for {
		fmt.Println("- Sleeping")
		time.Sleep(10 * time.Second)
	}
}

const FileNameExample = "cmd/signals/files/go-example.txt"

// DeleteFiles is used to simulate a 'clean up' function to run on shutdown. Because
// it's just an example it doesn't have any error handling.
func DeleteFiles() {
	fmt.Println("- Run Clean Up - Delete Our Example File")
	_ = os.Remove(FileNameExample)
	fmt.Println("- Good bye!")
}

// CreateFile creates a file so that we have something to clean up when we close our program.
func CreateFile() {
	fmt.Println("- Create Our Example File")
	_ = os.MkdirAll(filepath.Dir(FileNameExample), os.ModeDir)
	file, _ := os.Create(FileNameExample)
	defer file.Close()
}
