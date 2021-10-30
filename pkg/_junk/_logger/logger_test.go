package logger

import (
	"log"
	"os"
	"testing"
)

var multiLine = `this is going to be a multi line log. It will show
you what will happen when the logger
encounters a multi level log message.
Here you go!`

func TestStdLog(t *testing.T) {
	l := log.New(os.Stderr, "", log.LstdFlags)
	l.Printf("this is my log: %s\n", "logging something")

	l.Printf("this is my log: %s\n", "logging something")
}

func TestPrintColors(t *testing.T) {

	l := NewLogger()

	l.Info(multiLine)

	l.Trace("this is a *trace* log\n\tthis is still the same trace log...")

	l.Debug("this is a *debug* log")

	l.Info("this is a *info* log")

	l.Info("turning on the func printer")

	l.SetPrintFunc(true)

	l.Print("this is a *default* log")

	l.Warn("this is a *warn* log")

	l.Info("turning on the file printer")

	l.SetPrintFile(true)

	l.Warn("there we go! is it working?")

	l.Error("this is a *error* log")

	l.Info("turning off the func printer")

	l.SetPrintFunc(false)

	l.Info("and we're done.")

	l.Info("so is this working too?")

	l.Error("hmmm i really hope so")

	l.Warn("otherwise, i am not sure what I will do!")

	recover()

}
