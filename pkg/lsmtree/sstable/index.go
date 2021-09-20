package sstable

type SSIndex struct {
	base string // base is the base path of the db
}

func openSSIndex(base string) (*SSIndex, error) {
	return &SSIndex{
		base: base,
	}, nil
}
