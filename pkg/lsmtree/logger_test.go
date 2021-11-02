package lsmtree

import (
	"fmt"
	"testing"
)

func TestLogger(t *testing.T) {

	for i := LevelDebug; i < LevelOff; i++ {
		level := logLevel(i)
		l := newLogger(level)
		fmt.Println(LevelText(level))
		l.Debug("foo")
		l.Debug("foo with args: %d\n", 4)
		l.Info("foo")
		l.Info("foo with args: %d\n", 4)
		l.Warn("foo")
		l.Warn("foo with args: %d\n", 4)
		l.Error("foo")
		l.Error("foo with args: %d\n", 4)
	}

}
