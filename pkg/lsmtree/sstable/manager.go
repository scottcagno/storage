package sstable

type SSManager struct {
	base string // base is the base path of the db
}

func Open(base string) (*SSManager, error) {
	return &SSManager{
		base: base,
	}, nil
}
