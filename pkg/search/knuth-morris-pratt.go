package search

// KnuthMorrisPratt algorithm is oftentimes only the best performing when it's used on shorter texts or
// if you are pre-computing the search tables beforehand. Otherwise, Boyer-Moore (and even Rabin-Karp) will
// beat it almost out most of the time.
type KnuthMorrisPratt struct{}

func NewKnuthMorrisPratt() *KnuthMorrisPratt {
	return new(KnuthMorrisPratt)
}

func (kmp *KnuthMorrisPratt) String() string {
	return "KNUTH-MORRIS-PRATT"
}

func (kmp *KnuthMorrisPratt) FindIndex(text, pattern []byte) int {
	if text == nil || pattern == nil {
		return -1
	}
	return knuthMorrisPrattFinder(text, pattern)
}

func (kmp *KnuthMorrisPratt) FindIndexString(text, pattern string) int {
	return knuthMorrisPrattFinderString(text, pattern)
}

func knuthMorrisPrattFinder(text, pattern []byte) int {
	nn := kmpFinder(text, pattern)
	if len(nn) > 0 {
		return nn[0]
	}
	return -1
}

func knuthMorrisPrattFinderString(text, pattern string) int {
	nn := kmpFinderString(text, pattern)
	if len(nn) > 0 {
		return nn[0]
	}
	return -1
}

var patternSize = 86

func kmpFinder(s, sub []byte) []int {
	next := preKMP(sub)
	i, j := 0, 0

	m, n := len(sub), len(s)

	x, y := sub, s
	var ret []int

	//got zero target or want, just return empty result
	if m == 0 || n == 0 {
		return ret
	}

	//want string bigger than target string
	if n < m {
		return ret
	}

	for j < n {
		for i > -1 && x[i] != y[j] {
			i = next[i]
		}
		i++
		j++

		//fmt.Println(i, j)
		if i >= m {
			ret = append(ret, j-i)
			//fmt.Println("find:", j, i)
			i = next[i]
		}
	}

	return ret
}

func preMP(x []byte) []int {
	var i, j int
	length := len(x) - 1
	//var mpNext [patternSize]int
	mpNext := make([]int, len(x)+1)
	i = 0
	j = -1
	mpNext[0] = -1

	for i < length {
		for j > -1 && x[i] != x[j] {
			j = mpNext[j]
		}
		i++
		j++
		mpNext[i] = j
	}
	return mpNext
}

func preKMP(x []byte) []int {
	var i, j int
	length := len(x) - 1
	//var kmpNext [patternSize]int
	kmpNext := make([]int, len(x)+1)
	i = 0
	j = -1
	kmpNext[0] = -1

	for i < length {
		for j > -1 && x[i] != x[j] {
			j = kmpNext[j]
		}

		i++
		j++

		if x[i] == x[j] {
			kmpNext[i] = kmpNext[j]
		} else {
			kmpNext[i] = j
		}
	}
	return kmpNext
}

func kmpFinderString(s, sub string) []int {
	next := preKMPString(sub)
	i, j := 0, 0

	m, n := len(sub), len(s)

	x, y := []byte(sub), []byte(s)
	var ret []int

	//got zero target or want, just return empty result
	if m == 0 || n == 0 {
		return ret
	}

	//want string bigger than target string
	if n < m {
		return ret
	}

	for j < n {
		for i > -1 && x[i] != y[j] {
			i = next[i]
		}
		i++
		j++

		//fmt.Println(i, j)
		if i >= m {
			ret = append(ret, j-i)
			//fmt.Println("find:", j, i)
			i = next[i]
		}
	}

	return ret
}

func preMPString(x string) []int {
	var i, j int
	length := len(x) - 1
	//var mpNext [patternSize]int
	mpNext := make([]int, len(x)+1)
	i = 0
	j = -1
	mpNext[0] = -1

	for i < length {
		for j > -1 && x[i] != x[j] {
			j = mpNext[j]
		}
		i++
		j++
		mpNext[i] = j
	}
	return mpNext
}

func preKMPString(x string) []int {
	var i, j int
	length := len(x) - 1
	//var kmpNext [patternSize]int
	kmpNext := make([]int, len(x)+1)
	i = 0
	j = -1
	kmpNext[0] = -1

	for i < length {
		for j > -1 && x[i] != x[j] {
			j = kmpNext[j]
		}

		i++
		j++

		if x[i] == x[j] {
			kmpNext[i] = kmpNext[j]
		} else {
			kmpNext[i] = j
		}
	}
	return kmpNext
}
