package logging

import (
	"io"
	"log"
	"os"
)

func NewStdOutLogger(out io.Writer) *log.Logger {
	return log.New(out, "[INFO] ", log.Ldate|log.Ltime|log.Lshortfile|log.Lmsgprefix)
}

func NewStdErrLogger(out io.Writer) *log.Logger {
	return log.New(out, "[ERROR] ", log.Ldate|log.Ltime|log.Lshortfile|log.Lmsgprefix)
}

func NewLogger(out, err io.Writer) (*log.Logger, *log.Logger) {
	return NewStdOutLogger(out), NewStdErrLogger(err)
}

func NewDefaultLogger() (*log.Logger, *log.Logger) {
	return NewLogger(os.Stdout, os.Stderr)
}
