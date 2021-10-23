package lsmt

import (
	"encoding/json"
	"fmt"
	"strings"
)

type LSMTreeStats struct {
	Config    *LSMConfig `json:"config,omitempty"`
	MtEntries int        `json:"mt_entries,omitempty"`
	MtSize    int64      `json:"mt_size,omitempty"`
	BfEntries int        `json:"bf_entries,omitempty"`
	BfSize    int64      `json:"bf_size,omitempty"`
}

func (s *LSMTreeStats) String() string {
	var ss []string
	ss = append(ss, fmt.Sprintf("%T", s))
	ss = append(ss, fmt.Sprintf("\tConfig: %v", s.Config))
	ss = append(ss, fmt.Sprintf("\tMtEntries: %v", s.MtEntries))
	ss = append(ss, fmt.Sprintf("\tMtSize: %v", s.MtSize))
	ss = append(ss, fmt.Sprintf("\tBfEntries: %v", s.BfEntries))
	ss = append(ss, fmt.Sprintf("\tBfSize: %v", s.BfSize))
	return strings.Join(ss, "\n")
}

func (s *LSMTreeStats) JSON() (string, error) {
	dat, err := json.MarshalIndent(s, "", "\t")
	if err != nil {
		return "", err
	}
	return string(dat), nil
}
