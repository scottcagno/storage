package common

import "log"

func ErrCheck(err error) {
	if err != nil {
		log.Panicf("error: [%T] %q\n", err, err)
	}
}
