package bio

type block struct {
	hdr     *header
	data    []byte
	padding int
}
