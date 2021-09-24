package openaddr

/*
	This hash map implementation uses a closed hashing (open addressing) technique with
	linear probing for resolving any hash collisions. The exact algorithm it utilizes
	is called 'robin hood hashing.' More information about this can technique can be found
	in the links provided below:
	01) https://andre.arko.net/2017/08/24/robin-hood-hashing/
	02) https://cs.uwaterloo.ca/research/tr/1986/CS-86-14.pdf
	03) https://www.dmtcs.org/pdfpapers/dmAD0127.pdf
	04) https://www.pvk.ca/Blog/numerical_experiments_in_hashing.html
	05) https://www.pvk.ca/Blog/more_numerical_experiments_in_hashing.html
	06) https://www.sebastiansylvan.com/post/robin-hood-hashing-should-be-your-default-hash-table-implementation/
	07) https://www.sebastiansylvan.com/post/more-on-robin-hood-hashing-2/
	08) http://codecapsule.com/2013/11/11/robin-hood-hashing/
	09) https://www.pvk.ca/Blog/2013/11/26/the-other-robin-hood-hashing/
	10) http://codecapsule.com/2013/11/17/robin-hood-hashing-backward-shift-deletion/
	The basic principal is:
	-----------------------
	1) Calculate the hash value and initial index of the entry to be inserted
	2) Search the position in the array linearly
	3) While searching, the distance from initial index is kept which is called DIB(Distance from Initial Bucket)
	4) If we can find the empty bucket, we can insert the entry with DIB here
	5) If we encounter a entry which has less DIB than the one of the entry to be inserted, swap them.
*/
