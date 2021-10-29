package util

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func HandleSignalInterrupt(msg string, args ...interface{}) {
	log.Printf(msg, args...)
	log.Println("Please press ctrl+c to exit.")
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		os.Exit(1)
	}()
}

func HandleSigInt(fn func()) {
	log.Println("Please press ctrl+c to exit.")
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fn()
		os.Exit(1)
	}()
}

func ShutdownHook(fn func()) {
	log.Println("Please press ctrl+c to exit.")
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
	go func() {
		sig := <-c
		log.Printf("Received signal: %q (%d)\n", sig, sig)
		if fn != nil {
			fn()
		}
		os.Exit(1)
	}()
}
