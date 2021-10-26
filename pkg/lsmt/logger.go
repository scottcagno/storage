package lsmt

import (
	"fmt"
	"log"
	"os"
)

const (
	LevelOff logLevel = iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

func LevelText(level logLevel) string {
	switch level {
	case LevelOff:
		return "Level=Off"
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
	default:
		return "Level=Unknown"
	}
}

type logLevel int

type Logger struct {
	*log.Logger
	level logLevel
}

func NewLogger(level logLevel) *Logger {
	return &Logger{
		Logger: log.New(os.Stderr, "", log.LstdFlags),
		level:  level,
	}
}

func (l *Logger) Debug(s string, a ...interface{}) {
	if l.level < LevelDebug {
		return
	}
	ls := fmt.Sprintf("| DEBUG | %s", s)
	if a == nil || len(a) == 0 {
		l.Println(ls)
		return
	}
	l.Printf(ls, a...)
}

func (l *Logger) Info(s string, a ...interface{}) {
	if l.level < LevelInfo {
		return
	}
	ls := fmt.Sprintf("|  INFO | %s", s)
	if a == nil || len(a) == 0 {
		l.Println(ls)
		return
	}
	l.Printf(ls, a...)
}

func (l *Logger) Warn(s string, a ...interface{}) {
	if l.level < LevelWarn {
		return
	}
	ls := fmt.Sprintf("|  WARN | %s", s)
	if a == nil || len(a) == 0 {
		l.Println(ls)
		return
	}
	l.Printf(ls, a...)
}

func (l *Logger) Error(s string, a ...interface{}) {
	if l.level < LevelError {
		return
	}
	ls := fmt.Sprintf("| ERROR | %s", s)
	if a == nil || len(a) == 0 {
		l.Println(ls)
		return
	}
	l.Printf(ls, a...)
}

func (l *Logger) Fatal(s string, a ...interface{}) {
	if l.level < LevelFatal {
		return
	}
	ls := fmt.Sprintf("| FATAL | %s", s)
	if a == nil || len(a) == 0 {
		l.Fatalln(ls)
	}
	l.Fatalf(ls, a...)
}
