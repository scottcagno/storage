package util

import (
	"fmt"
	"log"
	"time"
)

/*
	usage:

	func foo() {
		defer TimeThis(Msg("foo"))
		// code to measure
	}

*/

func Msg(msg string) (string, time.Time) {
	return msg, time.Now()
}

func TimeThis(msg string, start time.Time) {
	log.Printf("%v: %v\n", msg, time.Since(start))
}

func FormatTime(msg string, t1, t2 time.Time) string {
	return fmt.Sprintf("%s: %0.6f sec\n",
		msg, // the message to print
		float64(t2.Sub(t1).Nanoseconds())/float64(time.Second.Nanoseconds()), // the seconds
	)
}
