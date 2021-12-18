package bio

import "errors"

var (
	ErrInvalidSize   = errors.New("bio: invalid size")
	ErrInvalidOffset = errors.New("bio: invalid offset")
	ErrSliceTooLarge = errors.New("bio: slice is too large")
	ErrSliceTooSmall = errors.New("bio: slice is too small")
	ErrDataIsEmpty   = errors.New("bio: data is empty or nil")
)

const (
	blockSize       = 32
	headerSize      = 6
	maxDataPerBlock = blockSize - headerSize
)

const (
	blocksPerChunk  = 16
	chunkSize       = blocksPerChunk * blockSize
	maxDataPerChunk = blocksPerChunk * maxDataPerBlock
)

const (
	chunksPerSegment = 15
	segmentSize      = chunksPerSegment * chunkSize
)

const (
	blockMask  = blockSize - 1
	headerMask = headerSize - 1
	chunkMask  = chunkSize - 1
)

func divUp(dividend, divisor int) int {
	// divide
	res := dividend / divisor
	// divided evenly
	if (dividend % divisor) == 0 {
		return res
	}
	// rounded down
	if (divisor ^ dividend) >= 0 {
		return res + 1
	}
	return res
}
