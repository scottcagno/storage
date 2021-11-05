package lsmtree

import "testing"

func TestCreateSSAndIndexTables(t *testing.T) {

	// make batch
	memt := newRBTree()
	for i := 0; i < 50000; i++ {
		e := &Entry{
			Key:   makeData("key", i),
			Value: []byte(mdVal),
		}
		_, _ = memt.putEntry(e)
	}

	// create ss-table and ss-table-index
	err := createSSAndIndexTables("ss-table-testing", memt)
	if err != nil {
		t.Fatalf("create ss-table and ss-table-index: %v\n", err)
	}
}
