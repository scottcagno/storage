package sstable

const (
	filePrefix      = "sst-"
	dataFileSuffix  = ".dat"
	indexFileSuffix = ".idx"
)

type SSTManager struct {
	base string
}

func Open(base string) (*SSTManager, error) {
	sstm := &SSTManager{
		base: base,
	}
	return sstm, nil
}

func (sstm *SSTManager) Get(k string) ([]byte, error) {
	return nil, nil
}

func (sstm *SSTManager) Close() error {
	// TODO: implement
	return nil
}
