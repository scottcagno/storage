package logging

import (
	"fmt"
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

type logLevel = int

const (
	LevelDebug logLevel = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
	LevelOff
)

func LevelText(level logLevel) string {
	switch level {
	case LevelDebug:
		return "Level=Debug"
	case LevelInfo:
		return "Level=Info"
	case LevelWarn:
		return "Level=Warn"
	case LevelError:
		return "Level=Error"
	case LevelFatal:
		return "Level=Fatal"
	case LevelOff:
		return "Level=Off"
	default:
		return "Level=Unknown"
	}
}

type LevelLogger struct {
	*log.Logger
	level logLevel
}

func NewLevelLogger(level logLevel) *LevelLogger {
	return &LevelLogger{
		Logger: log.New(os.Stderr, "", log.LstdFlags),
		level:  level,
	}
}

func (l *LevelLogger) Debug(s string, a ...interface{}) {
	if l.level > LevelDebug {
		return
	}
	ls := fmt.Sprintf("| DEBUG | %s", s)
	if a == nil || len(a) == 0 {
		l.Println(ls)
		return
	}
	l.Printf(ls, a...)
}

func (l *LevelLogger) Info(s string, a ...interface{}) {
	if l.level > LevelInfo {
		return
	}
	ls := fmt.Sprintf("|  INFO | %s", s)
	if a == nil || len(a) == 0 {
		l.Println(ls)
		return
	}
	l.Printf(ls, a...)
}

func (l *LevelLogger) Warn(s string, a ...interface{}) {
	if l.level > LevelWarn {
		return
	}
	ls := fmt.Sprintf("|  WARN | %s", s)
	if a == nil || len(a) == 0 {
		l.Println(ls)
		return
	}
	l.Printf(ls, a...)
}

func (l *LevelLogger) Error(s string, a ...interface{}) {
	if l.level > LevelError {
		return
	}
	ls := fmt.Sprintf("| ERROR | %s", s)
	if a == nil || len(a) == 0 {
		l.Println(ls)
		return
	}
	l.Printf(ls, a...)
}

func (l *LevelLogger) Fatal(s string, a ...interface{}) {
	if l.level > LevelFatal {
		return
	}
	ls := fmt.Sprintf("| FATAL | %s", s)
	if a == nil || len(a) == 0 {
		l.Fatalln(ls)
	}
	l.Fatalf(ls, a...)
}
