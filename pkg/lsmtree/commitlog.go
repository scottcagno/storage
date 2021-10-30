package lsmtree

import "io"

type CommitLog struct {
	baseDir     string
	syncOnWrite bool
}

func OpenCommitLog(base string, syncOnWrite bool) (*CommitLog, error) {
	return nil, nil
}

func (c *CommitLog) Read(r io.Reader) (int, error) {
	return -1, nil
}

func (c *CommitLog) Write(w io.Writer) (int, error) {
	return -1, nil
}

func (c *CommitLog) CloseAndRemove() error {
	return nil
}

func (c *CommitLog) Sync() error {
	return nil
}

func (c *CommitLog) Close() error {
	return nil
}
