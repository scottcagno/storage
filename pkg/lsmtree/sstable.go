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
}

func openSSTableManager() (*ssTableManager, error) {
	return nil, nil
}

func (sstm *ssTableManager) get(e *Entry) (*Entry, error) {
	return nil, nil
}
