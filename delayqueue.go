package transactions

// A PriorityQueue implements heap.Interface and holds Items.
type delayQueue []*CleanupRequest

func (dq delayQueue) Len() int {
	return len(dq)
}

func (dq delayQueue) Less(i, j int) bool {
	return dq[i].readyTime.Before(dq[j].readyTime)
}

func (dq delayQueue) Swap(i, j int) {
	dq[i], dq[j] = dq[j], dq[i]
}

func (dq *delayQueue) Push(x interface{}) {
	item := x.(*CleanupRequest)
	*dq = append(*dq, item)
}

func (dq *delayQueue) Pop() interface{} {
	old := *dq
	n := len(old)
	item := old[n-1]
	if !item.ready() {
		return nil
	}
	old[n-1] = nil // avoid memory leak
	*dq = old[0 : n-1]
	return item
}

func (dq *delayQueue) ForcePop() interface{} {
	old := *dq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*dq = old[0 : n-1]
	return item
}
