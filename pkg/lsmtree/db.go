package lsmtree

import (
	"github.com/scottcagno/storage/pkg/lsmtree/memtable"
	"github.com/scottcagno/storage/pkg/lsmtree/sstable"
)

type DB struct {
	base string // base is the base path of the db
	mem  *memtable.Memtable
	sst  *sstable.SSManager
}
