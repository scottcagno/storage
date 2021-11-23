package main

import (
	_ "embed"
	"fmt"
	"github.com/scottcagno/storage/pkg/search"
	"time"
)

var (
	pattern = []string{
		`I do not say these things for a dollar or to fill up the time while I wait for a boat`,
		`pocketless`,
		`baz_DOES_NOT_EXIST`,
		`There is that in me—I do not know what it is—but I know it is in me`,
		`I ascend to the foretruck`,
		`bar_DOES_NOT_EXIST`,
		`Waiting in gloom, protected by frost`,
		`With the twirl of my tongue I encompass worlds and volumes of worlds`,
		`The pleasures of heaven are with me and the pains of hell are with me`,
		`eyes that have shed tears`,
		`Undrape!`,
		`foo_DOES_NOT_EXIST`,
	}

	//go:embed ww.txt
	text string
)

func main() {

	// check out boyer-moore
	bm := search.NewBoyerMoore()
	TimeSearcher(bm)

	fmt.Println()

	// check out rabin-karp
	rk := search.NewRabinKarp()
	TimeSearcher(rk)

	fmt.Println()

	// check out knuth-morris-pratt
	kmp := search.NewKnuthMorrisPratt()
	TimeSearcher(kmp)
}

func TimeSearcher(s search.Searcher) {
	fmt.Printf("%s\n", s)
	t1 := time.Now()
	for i := range pattern {
		t3 := time.Now()
		n := s.FindIndexString(text, pattern[i])
		t4 := time.Since(t3)
		fmt.Printf("Found %q, at index %d (Took %.6fs)\n", pattern[i], n, t4.Seconds())
	}
	t2 := time.Since(t1)
	fmt.Printf("Took %.6fs, %dns\n", t2.Seconds(), t2.Nanoseconds())
}

var sonnet55 = `Not marble nor the gilded monuments
Of princes shall outlive this powerful rhyme;
But you shall shine more bright in these contents
Than unswept stone, besmear'd with sluttish time.
When wasteful war shall statues overturn,
And broils root out the work of masonry,
Nor Mars his sword nor war's quick fire shall burn
The living record of your memory.
'Gainst death and all-oblivious enmity
Shall you pace forth; your praise shall still find room,
Even in the eyes of all posterity
That wear this world out to the ending doom.
    So, till the judgment that yourself arise,
    You live in this, and dwell in lovers' eyes.`
