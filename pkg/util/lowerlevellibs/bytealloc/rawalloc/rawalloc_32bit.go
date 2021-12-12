//go:build 386 || amd64p32 || arm || armbe || mips || mipsle || mips64p32 || mips64p32le || ppc || sparc
// +build 386 amd64p32 arm armbe mips mipsle mips64p32 mips64p32le ppc sparc

package rawalloc

const (
	maxArrayLen = 1<<31 - 1
)
