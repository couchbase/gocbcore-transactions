package transactions

// A PriorityQueue implements heap.Interface and holds Items.
type priorityQueue []*CleanupRequest

func (dq priorityQueue) Len() int {
	return len(dq)
}

func (dq priorityQueue) Less(i, j int) bool {
	return dq[i].readyTime.Before(dq[j].readyTime)
}

func (dq priorityQueue) Swap(i, j int) {
	dq[i], dq[j] = dq[j], dq[i]
}

func (dq *priorityQueue) Push(x interface{}) {
	item := x.(*CleanupRequest)
	*dq = append(*dq, item)
}

func (dq *priorityQueue) Pop() interface{} {
	old := *dq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*dq = old[0 : n-1]
	return item
}

func (dq *priorityQueue) Peek() interface{} {
	return (*dq)[0]
}
