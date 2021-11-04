package lsmtree

import "testing"

func TestCreateSSAndIndexTables(t *testing.T) {

	// make batch
	batch := NewBatch()
	for i := 0; i < 50000; i++ {
		err := batch.Write(makeData("key", i), []byte(mdVal))
		if err != nil {
			t.Fatalf("writing batch: %v\n", err)
		}
	}

	// create ss-table and ss-table-index
	err := createSSAndIndexTables("ss-table-testing", getLevelFromSize(batch.Size()), batch)
	if err != nil {
		t.Fatalf("create ss-table and ss-table-index: %v\n", err)
	}
}
