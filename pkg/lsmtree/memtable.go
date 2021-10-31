package lsmtree

type memTable struct {
	table     *rbTree
	threshold int64
}

func openMemTable(threshold int64) (*memTable, error) {
	mt := &memTable{
		table:     newRBTree(),
		threshold: threshold,
	}
	return mt, nil
}

func (mt *memTable) get(e *Entry) (*Entry, error) {
	ent, found := mt.table.Get(e)
	if !found {
		return nil, ErrNotFound
	}
	if ent.hasTombstone() {
		return nil, ErrFoundTombstone
	}
	return ent, nil
}

func (mt *memTable) put(e *Entry) error {
	_, needFlush := mt.table.UpsertAndCheckIfFull(e, mt.threshold)
	if needFlush {
		return ErrFlushThreshold
	}
	return nil
}
