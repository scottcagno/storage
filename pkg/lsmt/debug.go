package lsmt

type DebugEntry struct {
	Key string
	Val []byte
	CRC uint32
}

func NewDebugEntry(k string, v []byte) *DebugEntry {
	return &DebugEntry{
		Key: k,
		Val: v,
		CRC: CalcCRC(append([]byte(k), v...)),
	}
}

func (de *DebugEntry) IsOK(entry *DebugEntry) bool {
	return de.CRC == entry.CRC
}
