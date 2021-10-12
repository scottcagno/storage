package web

import (
	"bytes"
	"sync"
)

var (
	initBufferPool sync.Once
	bufferPool     *BufferPool
)

type BufferPool struct {
	pool sync.Pool
}

func newBufferPool() *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

// OpenBufferPool returns a single instance of the buffer pool
// utilizing sync.Once which gives us a no-brainier implementation
// of the classic singleton pattern
func OpenBufferPool() *BufferPool {
	initBufferPool.Do(func() {
		bufferPool = newBufferPool()
	})
	return bufferPool
}

func (bp *BufferPool) Get() *bytes.Buffer {
	return bp.pool.Get().(*bytes.Buffer)
}

func (bp *BufferPool) Put(buf *bytes.Buffer) {
	buf.Reset()
	bp.pool.Put(buf)
}
