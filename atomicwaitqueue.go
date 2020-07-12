package transactions

import (
	"sync"
)

type atomicWaitQueue struct {
	lock    sync.Mutex
	count   int
	waiters []func()
}

func (q *atomicWaitQueue) Add(n int) {
	var waiters []func()

	q.lock.Lock()
	q.count += n
	if q.count == 0 {
		waiters = q.waiters
		q.waiters = nil
	}
	q.lock.Unlock()

	for _, waiter := range waiters {
		waiter()
	}
}

func (q *atomicWaitQueue) Done() {
	q.Add(-1)
}

func (q *atomicWaitQueue) Wait(fn func()) {
	q.lock.Lock()
	if q.count == 0 {
		q.lock.Unlock()

		fn()
		return
	}

	q.waiters = append(q.waiters, fn)
	q.lock.Unlock()
}
