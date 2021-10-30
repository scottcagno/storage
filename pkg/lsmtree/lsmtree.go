package lsmtree

import "sync"

type LSMTree struct {
	lock   sync.RWMutex
	opt    *Options
	logDir string
	sstDir string
	wacl   *CommitLog
	memt   *MemTable
}

func OpenLSMTree(options *Options) (*LSMTree, error) {
	return nil, nil
}

func (lsm *LSMTree) Put(k, v []byte) error {
	return nil
}

func (lsm *LSMTree) putEntry(e *Entry) error {
	return nil
}

func (lsm *LSMTree) Get(k []byte) ([]byte, error) {
	return nil, nil
}

func (lsm *LSMTree) getEntry(k []byte) (*Entry, error) {
	return nil, nil
}

func (lsm *LSMTree) Del(k []byte) error {
	return nil
}

func (lsm *LSMTree) delEntry(k []byte) error {
	return nil
}

func (lsm *LSMTree) PutBatch(b *Batch) error {
	return nil
}

func (lsm *LSMTree) GetBatch(keys ...[]byte) (*Batch, error) {
	return nil, nil
}

func (lsm *LSMTree) Sync() error {
	return nil
}

func (lsm *LSMTree) Close() error {
	return nil
}
