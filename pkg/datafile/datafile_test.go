package datafile

import "testing"

func TestOpenFile(t *testing.T) {

	// open data file
	df, err := openDataFile("testing/testfile.txt")
	if err != nil {
		t.Error(err)
	}

	// close data file
	err = df.Close()
	if err != nil {
		t.Error(err)
	}
}
