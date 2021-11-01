package lsmtree

type ssTableBloom struct {
}

type ssTableIndex struct {
	*ssTableBloom
}

type ssTable struct {
	*ssTableIndex
}

type ssTableManager struct {
	baseDir string
}

func openSSTableManager(base string) (*ssTableManager, error) {
	sstm := &ssTableManager{
		baseDir: base,
	}
	return sstm, nil
}

func (sstm *ssTableManager) get(e *Entry) (*Entry, error) {
	return nil, nil
}

func (sstm *ssTableManager) flushToSSTable(memt *memTable) error {
	return nil
}
