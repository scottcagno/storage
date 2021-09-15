package v2

// entry contains the metadata for a single entry within the file segment
type entry struct {
	index  uint64 // index is the "id" of this entry
	offset uint64 // offset is the actual offset of this entry in the segment file
}

// segment contains the metadata for the file segment
type segment struct {
	path      string  // path is the full path to this segment file
	index     uint64  // starting index of the segment
	entries   []entry // entries is an index of the entries in the segment
	remaining uint64  // remaining is the bytes left after max file size minus entry data
}
