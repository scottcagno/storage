package lsmtree

type memTable struct {
	table     *rbTree
	threshold int
}

func openMemTable(threshold int) (*memTable, error) {
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
