package virtfp

import (
	"bufio"
	"github.com/scottcagno/storage/pkg/_bio/buffer"
	"io/fs"
)

type VirtFile struct {
	name string
	buf  *buffer.Buffer
	r    *bufio.Reader
	w    *bufio.Writer
	dat  fs.File
}

func OpenVirtFile(name string) *VirtFile {
	buf := new(buffer.Buffer)
	return &VirtFile{
		name: name,
		buf:  buf,
		r:    bufio.NewReader(buf),
		w:    bufio.NewWriter(buf),
	}
}

func (vfp *VirtFile) Name() string {
	return vfp.name
}

func (vfp *VirtFile) Size() int {
	return vfp.buf.Cap()
}

func (vfp *VirtFile) Read(p []byte) (int, error) {
	return vfp.buf.Read(p)
}

func (vfp *VirtFile) ReadAt(p []byte, off int64) (int, error) {
	return vfp.buf.ReadAt(p, off)
}

func (vfp *VirtFile) Seek(off int64, whence int) (int64, error) {
	return vfp.buf.Seek(off, whence)
}

func (vfp *VirtFile) Sync() error {
	return nil
}

func (vfp *VirtFile) Truncate(size int64) error {
	vfp.buf.Truncate(int(size))
	return nil
}

func (vfp *VirtFile) Write(p []byte) (int, error) {
	return vfp.buf.Write(p)
}

func (vfp *VirtFile) WriteAt(p []byte, off int64) (int, error) {
	return vfp.buf.WriteAt(p, off)
}

func (vfp *VirtFile) Close() error {
	vfp.buf.Free()
	return nil
}

var data = []byte(`Born here of parents born here from parents the same, and their parents the same,
I, now thirty-seven years old in perfect health begin,
Hoping to cease not till death.

Creeds and schools in abeyance,
Retiring back a while sufficed at what they are, but never forgotten,
I harbor for good or bad, I permit to speak at every hazard,
Nature without check with original energy.

2
Houses and rooms are full of perfumes, the shelves are crowded with perfumes,
I breathe the fragrance myself and know it and like it,
The distillation would intoxicate me also, but I shall not let it.

The atmosphere is not a perfume, it has no taste of the distillation, it is odorless,
It is for my mouth forever, I am in love with it,
I will go to the bank by the wood and become undisguised and naked,
I am mad for it to be in contact with me.

The smoke of my own breath,
Echoes, ripples, buzz’d whispers, love-root, silk-thread, crotch and vine,
My respiration and inspiration, the beating of my heart, the passing of blood and air through my lungs,
The sniff of green leaves and dry leaves, and of the shore and dark-color’d sea-rocks, and of hay in the barn,
The sound of the belch’d words of my voice loos’d to the eddies of the wind,
A few light kisses, a few embraces, a reaching around of arms,
The play of shine and shade on the trees as the supple boughs wag,
The delight alone or in the rush of the streets, or along the fields and hill-sides,
The feeling of health, the full-noon trill, the song of me rising from bed and meeting the sun.

Have you reckon’d a thousand acres much? have you reckon’d the earth much?
Have you practis’d so long to learn to read?
Have you felt so proud to get at the meaning of poems?

Stop this day and night with me and you shall possess the origin of all poems,
You shall possess the good of the earth and sun, (there are millions of suns left,)
You shall no longer take things at second or third hand, nor look through the eyes of the dead, nor feed on the spectres in books,
You shall not look through my eyes either, nor take things from me,
You shall listen to all sides and filter them from your self.

3
I have heard what the talkers were talking, the talk of the beginning and the end,
But I do not talk of the beginning or the end.

There was never any more inception than there is now,
Nor any more youth or age than there is now,
And will never be any more perfection than there is now,
Nor any more heaven or hell than there is now.

Urge and urge and urge,
Always the procreant urge of the world.

Out of the dimness opposite equals advance, always substance and increase, always sex,
Always a knit of identity, always distinction, always a breed of life.

To elaborate is no avail, learn’d and unlearn’d feel that it is so.

Sure as the most certain sure, plumb in the uprights, well entretied, braced in the beams,
Stout as a horse, affectionate, haughty, electrical,
I and this mystery here we stand.

Clear and sweet is my soul, and clear and sweet is all that is not my soul.

Lack one lacks both, and the unseen is proved by the seen,
Till that becomes unseen and receives proof in its turn.

Showing the best and dividing it from the worst age vexes age,
Knowing the perfect fitness and equanimity of things, while they discuss I am silent, and go bathe and admire myself.

Welcome is every organ and attribute of me, and of any man hearty and clean,
Not an inch nor a particle of an inch is vile, and none shall be less familiar than the rest.

I am satisfied—I see, dance, laugh, sing;
As the hugging and loving bed-fellow sleeps at my side through the night, and withdraws at the peep of the day with stealthy tread,
Leaving me baskets cover’d with white towels swelling the house with their plenty,
Shall I postpone my acceptation and realization and scream at my eyes,
That they turn from gazing after and down the road,
And forthwith cipher and show me to a cent,
Exactly the value of one and exactly the value of two, and which is ahead?

4
Trippers and askers surround me,
People I meet, the effect upon me of my early life or the ward and city I live in, or the nation,
The latest dates, discoveries, inventions, societies, authors old and new,
My dinner, dress, associates, looks, compliments, dues,
The real or fancied indifference of some man or woman I love,
The sickness of one of my folks or of myself, or ill-doing or loss or lack of money, or depressions or exaltations,
Battles, the horrors of fratricidal war, the fever of doubtful news, the fitful events;
These come to me days and nights and go from me again,
But they are not the Me myself.

Apart from the pulling and hauling stands what I am,
Stands amused, complacent, compassionating, idle, unitary,
Looks down, is erect, or bends an arm on an impalpable certain rest,
Looking with side-curved head curious what will come next,
Both in and out of the game and watching and wondering at it.

Backward I see in my own days where I sweated through fog with linguists and contenders,
I have no mockings or arguments, I witness and wait.

5
I believe in you my soul, the other I am must not abase itself to you,
And you must not be abased to the other.

Loafe with me on the grass, loose the stop from your throat,
Not words, not music or rhyme I want, not custom or lecture, not even the best,
Only the lull I like, the hum of your valvèd voice.

I mind how once we lay such a transparent summer morning,
How you settled your head athwart my hips and gently turn’d over upon me,
And parted the shirt from my bosom-bone, and plunged your tongue to my bare-stript heart,
And reach’d till you felt my beard, and reach’d till you held my feet.

Swiftly arose and spread around me the peace and knowledge that pass all the argument of the earth,
And I know that the hand of God is the promise of my own,
And I know that the spirit of God is the brother of my own,
And that all the men ever born are also my brothers, and the women my sisters and lovers,
And that a kelson of the creation is love,
And limitless are leaves stiff or drooping in the fields,
And brown ants in the little wells beneath them,
And mossy scabs of the worm fence, heap’d stones, elder, mullein and poke-weed.
`)
