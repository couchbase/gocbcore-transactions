package transactions

import "sync"

type asyncCriticalSection struct {
	lock    sync.Mutex
	locked  bool
	waiters []func()
}

func (cs *asyncCriticalSection) Run(fn func()) {
	cs.lock.Lock()
	if cs.locked {
		cs.waiters = append(cs.waiters, fn)
		cs.lock.Unlock()
		return
	}

	cs.locked = true
	cs.lock.Unlock()

	fn()

	for {
		cs.lock.Lock()

		if len(cs.waiters) == 0 {
			cs.locked = false
			cs.lock.Unlock()
			return
		}

		waiter := cs.waiters[0]
		cs.waiters = cs.waiters[1:]

		cs.lock.Unlock()

		waiter()
	}
}
