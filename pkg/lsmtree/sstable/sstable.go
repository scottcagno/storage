package sstable

type SSTable struct {
	base string // base is the base path of the db
}

func makeSSTable(base string) (*SSTable, error) {
	return &SSTable{
		base: base,
	}, nil
}

func openSSTable(base string) (*SSTable, error) {
	return &SSTable{
		base: base,
	}, nil
}
