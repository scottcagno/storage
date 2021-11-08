package bitset

type BinaryRecord struct {
	data []byte
}

func NewBinaryRecord(hint uint) *BinaryRecord {
	br := &BinaryRecord{
		data: nil,
	}
	checkResize(&br.data, hint)
	return br
}

func (br *BinaryRecord) Set(i uint) {
	RawBytesSetBit(&br.data, i)
}

func (br *BinaryRecord) Unset(i uint) {
	RawBytesUnsetBit(&br.data, i)
}

func (br *BinaryRecord) IsSet(i uint) bool {
	return RawBytesHasBit(&br.data, i)
}

func (br *BinaryRecord) Get(i uint) uint {
	return RawBytesGetBit(&br.data, i)
}

func (br *BinaryRecord) Len() uint {
	return uint(len(br.data) * 8)
}

func (br *BinaryRecord) String() string {
	return RawBytesStringer(&br.data)
}
