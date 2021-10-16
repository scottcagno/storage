package lsmt

import "regexp"

func Search(pattern string, input []byte) (bool, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false, err
	}
	return re.Match(input), nil
}
