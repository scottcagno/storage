package lsmtree

import (
	"log"
	"os"
	"testing"
)

func Test_align(t *testing.T) {
	type args struct {
		size int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{name: "size-0", args: args{0}, want: blockSize},
		{name: "size-1250", args: args{1250}, want: blockSize},
		{name: "size-2500", args: args{2500}, want: blockSize},
		{name: "size-4000", args: args{4000}, want: blockSize},
		{name: "size-4100", args: args{4100}, want: blockSize * 2},
		{name: "size-6500", args: args{6500}, want: blockSize * 2},
		{name: "size-7200", args: args{7200}, want: blockSize * 2},
		{name: "size-8100", args: args{8100}, want: blockSize * 2},
		{name: "size-8200", args: args{8200}, want: blockSize * 3},
		{name: "size-9900", args: args{9900}, want: blockSize * 3},
		{name: "size-10957", args: args{10957}, want: blockSize * 3},
		{name: "size-12200", args: args{12200}, want: blockSize * 3},
		{name: "size-12300", args: args{12300}, want: blockSize * 4},
		{name: "size-16300", args: args{16300}, want: blockSize * 4},
		{name: "size-16400", args: args{16400}, want: blockSize * 5},
		{name: "size-20400", args: args{20400}, want: blockSize * 5},
		{name: "size-20500", args: args{20500}, want: blockSize * 6},
		{name: "size-24500", args: args{24500}, want: blockSize * 6},
		{name: "size-24600", args: args{24600}, want: blockSize * 7},
		{name: "size-28600", args: args{28600}, want: blockSize * 7},
		{name: "size-28700", args: args{28700}, want: blockSize * 8},
		{name: "size-32700", args: args{32700}, want: blockSize * 8},
		{name: "size-32800", args: args{32800}, want: blockSize * 9},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := align(tt.args.size); got != tt.want {
				t.Errorf("align() = %v, want %v", got, tt.want)
			}
		})
	}
}

var setupDir = func() string {
	dir, err := os.MkdirTemp("dir", "tmp-*")
	if err != nil {
		log.Panic(err)
	}
	return dir
}

var cleanupDir = func(dir string) {
	err := os.RemoveAll(dir)
	if err != nil {
		log.Panic(err)
	}
}
