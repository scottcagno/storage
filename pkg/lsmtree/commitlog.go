package lsmtree

import "io"

type commitLog struct {
	baseDir     string
	syncOnWrite bool
}

func openCommitLog(base string, syncOnWrite bool) (*commitLog, error) {
	return nil, nil
}

func (c *commitLog) Read(r io.Reader) (int, error) {
	return -1, nil
}

func (c *commitLog) Write(w io.Writer) (int, error) {
	return -1, nil
}

func (c *commitLog) CloseAndRemove() error {
	return nil
}

func (c *commitLog) Sync() error {
	return nil
}

func (c *commitLog) Close() error {
	return nil
}
