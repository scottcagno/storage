package memtable

type Memtable struct {
	base string // base is the base path of the db
}

func Open(base string) (*Memtable, error) {
	return &Memtable{
		base: base,
	}, nil
}
