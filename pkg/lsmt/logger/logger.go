package logger

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

const (
	White = iota
	Black = iota + 30
	Red
	Green
	Yellow
	Blue
	Purple
	Cyan
	Grey
)

const (
	levelDefault = iota
	levelTrace
	levelDebug
	levelInfo
	levelWarn
	levelError
	levelFatal
	levelPanic
)

const (
	color = iota
	prefix
)

var colors = map[int]string{
	White:  "\033[0m",  // \033[0m
	Black:  "\033[30m", // \033[30m
	Red:    "\033[31m", // \033[31m
	Green:  "\033[32m", // \033[32m
	Yellow: "\033[33m", // \033[33m
	Blue:   "\033[34m", // \033[34m
	Purple: "\033[35m", // \033[35m
	Cyan:   "\033[36m", // \033[36m
	Grey:   "\033[37m", // \033[37m
}

var levels = map[int][2]string{
	levelTrace:   {colors[Grey], "TRCE"},
	levelDebug:   {colors[Grey], "DBUG"},
	levelInfo:    {colors[Blue], "INFO"},
	levelWarn:    {colors[Yellow], "WARN"},
	levelError:   {colors[Red], "EROR"},
	levelFatal:   {colors[Red], "FATL"},
	levelPanic:   {colors[Red], "PANC"},
	levelDefault: {colors[White], "NORM"},
}

// date or time format ref -> f1, f2 := "2006/01/02:15:04:05Z07", "2006-01-02 15:04:05Z07"
// t := time.Now()
// s1 := t.Format(f1)

var DefaultLogger = NewLogger()

type Logger struct {
	lock      sync.Mutex    // sync
	log       *log.Logger   // actual logger
	buf       *bytes.Buffer // buffer
	printFunc bool
	printFile bool
	dep       int // call depth
}

func NewLogger() *Logger {
	_ = log.Ldate | log.Ltime | log.Lshortfile | log.Lmsgprefix
	l := &Logger{
		log: log.New(os.Stderr, "", log.LstdFlags),
		buf: new(bytes.Buffer),
		dep: 5,
	}
	return l
}

func (l *Logger) logInternal(level, depth int, format string, args ...interface{}) {
	l.lock.Lock()
	defer l.lock.Unlock()
	levelInfo, ok := levels[level]
	if !ok {
		levelInfo = levels[levelDefault]
	}
	l.buf.Reset()
	l.buf.WriteString("| ")
	l.buf.WriteString(levelInfo[color])
	l.buf.WriteString(levelInfo[prefix])
	l.buf.WriteString(colors[White])
	l.buf.WriteString(" | ")
	if l.printFunc || l.printFile {
		if level == levelFatal {
			depth += 1
		}
		if level != levelPanic {
			fn, file := trace(depth)
			if l.printFunc {
				l.buf.WriteByte('[')
				l.buf.WriteString(strings.Split(fn, ".")[1])
				l.buf.WriteByte(']')
			}
			if l.printFunc && l.printFile {
				l.buf.WriteByte(' ')
			}
			if l.printFile {
				l.buf.WriteString(file)
			}
			l.buf.WriteString(" - ")
		}
	}
	l.buf.WriteString(format)
	if args == nil || len(args) == 0 {
		l.log.Print(l.buf.String())
		return
	}
	l.log.Printf(l.buf.String(), args...)
}

func (l *Logger) SetPrefix(prefix string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.log.SetPrefix(prefix)
}

func (l *Logger) SetPrintFunc(ok bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.printFunc = ok
}

func (l *Logger) SetPrintFile(ok bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.printFile = ok
}

func (l *Logger) SetCallDepth(depth int) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.dep = depth
}

func (l *Logger) Trace(message string) {
	l.logInternal(levelTrace, l.dep, message)
}

func (l *Logger) Tracef(format string, args ...interface{}) {
	l.logInternal(levelTrace, l.dep, format, args...)
}

func (l *Logger) Debug(message string) {
	l.logInternal(levelDebug, l.dep, message)
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.logInternal(levelDebug, l.dep, format, args...)
}

func (l *Logger) Info(message string) {
	l.logInternal(levelInfo, l.dep, message)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.logInternal(levelInfo, l.dep, format, args...)
}

func (l *Logger) Warn(message string) {
	l.logInternal(levelWarn, l.dep, message)
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	l.logInternal(levelWarn, l.dep, format, args...)
}

func (l *Logger) Error(message string) {
	l.logInternal(levelError, l.dep, message)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.logInternal(levelError, l.dep, format, args...)
}

func (l *Logger) Fatal(message string) {
	l.logInternal(levelFatal, l.dep, message)
	os.Exit(1)
}

func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.logInternal(levelFatal, l.dep, format, args...)
	os.Exit(1)
}

func (l *Logger) Panic(message string) {
	l.logInternal(levelPanic, l.dep, message)
	panic(message)
}

func (l *Logger) Panicf(format string, args ...interface{}) {
	l.logInternal(levelPanic, l.dep, format, args...)
	panic(fmt.Sprintf(format, args...))
}

func (l *Logger) Print(message string) {
	l.logInternal(levelDefault, l.dep, message)
}

func (l *Logger) Printf(format string, args ...interface{}) {
	l.logInternal(levelDefault, l.dep, format, args...)
}

func trace0(calldepth int) (string, string) {
	//pc, file, line, ok := runtime.Caller(calldepth)
	pc, file, line, _ := runtime.Caller(calldepth)
	fn := runtime.FuncForPC(pc)
	//funcName := filepath.Base(fn.Name())
	//fileName := filepath.Base(file)
	//return fmt.Sprintf("%s %s:%d", funcName, fileName, line)
	return filepath.Base(fn.Name()), fmt.Sprintf("%s:%d", filepath.Base(file), line)
}

func trace(calldepth int) (string, string) {
	pc := make([]uintptr, 10) // at least 1 entry needed
	runtime.Callers(calldepth, pc)
	fn := runtime.FuncForPC(pc[0])
	file, line := fn.FileLine(pc[0])
	//sfile := strings.Split(file, "/")
	//sname := strings.Split(f.Name(), "/")
	return filepath.Base(fn.Name()), fmt.Sprintf("%s:%d", filepath.Base(file), line)
}
