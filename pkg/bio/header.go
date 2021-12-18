package bio

import (
	"fmt"
	"io"
	"sync"
)

const (
	statusEmpty   = 0
	statusActive  = 1
	statusDeleted = 2
	statusOther   = 3
)

const (
	kindFull    = 1
	kindBeg     = 2
	kindMid     = 3
	kindEnd     = 4
	kindUnknown = 5
)

type headerBuff struct {
	hdr *header
	buf []byte
}

func (hb *headerBuff) clear() {
	hb.hdr.status = 0
	hb.hdr.kind = 0
	hb.hdr.part = 0
	hb.hdr.parts = 0
	hb.hdr.length = 0
	hb.buf[0] = 0
	hb.buf[1] = 0
	hb.buf[2] = 0
	hb.buf[3] = 0
	hb.buf[4] = 0
	hb.buf[5] = 0
}

var headerPool = sync.Pool{
	New: func() interface{} {
		return &headerBuff{
			hdr: new(header),
			buf: make([]byte, headerSize),
		}
	},
}

func getHdr() *headerBuff {
	hb := headerPool.Get().(*headerBuff)
	hb.clear()
	return hb
}

func putHdr(hb *headerBuff) {
	headerPool.Put(hb)
}

// header represents a block header
type header struct {
	status uint8
	kind   uint8
	part   uint8
	parts  uint8
	length uint16
}

// ReadFrom implements the ReaderFrom interface for header
func (h *header) ReadFrom(r io.Reader) (int64, error) {
	p := make([]byte, headerSize)
	n, err := r.Read(p)
	h.status = p[0]
	h.kind = p[1]
	h.part = p[2]
	h.parts = p[3]
	h.length = uint16(p[4]) | uint16(p[5])<<8
	if err != nil {
		return -1, err
	}
	fmt.Printf(">>> header >> ReadFrom > (%d) %s\n", n, h)
	return int64(n), err
}

// WriteTo implements the WriterTo interface for header
func (h *header) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte{
		h.status,
		h.kind,
		h.part,
		h.parts,
		byte(h.length),
		byte(h.length >> 8),
	})
	if err != nil {
		return -1, err
	}
	return int64(n), nil
}

// String is header's stringer method
func (h *header) String() string {
	return fmt.Sprintf("status=%d, kind=%d, part=%d, parts=%d, length=%d",
		h.status, h.kind, h.part, h.parts, h.length)
}

// getKind is a helper function that will
// return a kind type based on the configuration
// of the part and parts provided
func getKind(part, parts int) uint8 {
	if parts == 1 {
		return kindFull
	}
	if part == 1 {
		return kindBeg
	}
	if part > 1 && part < parts {
		return kindMid
	}
	if part == parts {
		return kindEnd
	}
	return kindUnknown
}
