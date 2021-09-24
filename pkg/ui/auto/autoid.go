package auto

import "sync"

// ai (auto increment)
var ai IncrID // global instance

type IncrID struct {
	sync.Mutex // ensures IncrID is goroutine-safe
	id         int64
}

func (a *IncrID) ID() (id int64) {
	a.Lock()
	defer a.Unlock()
	id = a.id
	a.id++
	return
}

func (a *IncrID) Next() int64 {
	return a.ID()
}
