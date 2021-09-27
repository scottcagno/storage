package sstable

type SSTManager struct {
	base string
}

func Open(base string) (*SSTManager, error) {
	sstm := &SSTManager{
		base: base,
	}
	return sstm, nil
}
