The 'lsmtree' package was going to be the production one, but now I am making this
package (the lsmt) package, in order to tidy everything up. I believe it will end
up being a bit more clean this way.

Basic architecture is as follows.

type LSMTree struct {
    base string // base is the base filepath for the database

}
