package auto

import "sync"

// ai (auto increment)
var ai Incr // global instance

type Incr struct {
	sync.Mutex // ensures Incr is goroutine-safe
	id         int64
}

func (a *Incr) ID() (id int64) {
	a.Lock()
	defer a.Unlock()
	id = a.id
	a.id++
	return
}

func (a *Incr) Next() int64 {
	return a.ID()
}
