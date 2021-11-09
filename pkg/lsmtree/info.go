package lsmtree

import "github.com/scottcagno/storage/pkg/util"

type Info struct {
	waclSize      int64
	memtCount     int64
	memtSize      int64
	sstmSSTCount  int64
	sstmIndexSize int64
}

func (lsm *LSMTree) GetInfo() *Info {
	lsm.lock.Lock()
	defer lsm.lock.Unlock()

	return &Info{
		waclSize:      lsm.wacl.size(),
		memtCount:     int64(lsm.memt.countOfEntries()),
		memtSize:      lsm.memt.size,
		sstmSSTCount:  int64(lsm.sstm.sstcount),
		sstmIndexSize: int64(util.Sizeof(lsm.sstm.index)),
	}
}

func SizeInKB(size int64) int64 {
	return size / 1000
}

func SizeInMB(size int64) int64 {
	return size / 1000 / 1000
}
