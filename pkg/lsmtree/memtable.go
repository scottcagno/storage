package lsmtree

type MemTable struct {
	table     *rbTree
	threshold int
}

func OpenMemTable(threshold int) (*MemTable, error) {
	return nil, nil
}
