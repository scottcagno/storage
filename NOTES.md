Notes [ this is really nothing, just a sample file ]
---
This is how we make links [example](https://example.com/) will take you to example.com.

```
import "mydb"

...

// open an instance
db, err := mydb.Open(mydb.DefaultConfig("base/path"))
if err != nil {
    panic(err)
}

// dont forget to close
defer func(){
    err := db.Close()
    if err != nil {
        panic(err)
    }
}()
```
