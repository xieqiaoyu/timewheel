package timewheel

import (
	"container/heap"
	"context"
	"sync/atomic"
	"time"
)

const defaultQueueCap = 20

type queueItem struct {
	Index      int
	Expiration time.Time
	bucket     *bucket
}

type priorityBucketQueue []*queueItem

func (pq priorityBucketQueue) Len() int {
	return len(pq)
}

func (pq priorityBucketQueue) Less(i, j int) bool {
	return pq[j].Expiration.After(pq[i].Expiration)
}

func (pq priorityBucketQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *priorityBucketQueue) Push(x interface{}) {
	n := len(*pq)
	c := cap(*pq)
	if n >= c {
		newPq := make(priorityBucketQueue, n, c*2)
		copy(newPq, *pq)
		*pq = newPq
	}
	*pq = (*pq)[0 : n+1]
	item := x.(*queueItem)
	item.Index = n
	(*pq)[n] = item
}

func (pq *priorityBucketQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	c := cap(old)
	halfCap := c >> 1
	if c > defaultQueueCap && n < halfCap {
		newPq := make(priorityBucketQueue, n, halfCap)
		copy(newPq, old)
		old = newPq
	}
	item := (old)[n-1]
	item.Index = -1
	old[n-1] = nil
	*pq = old[0 : n-1]
	return item
}

type DelayBucketQueue struct {
	C        chan *bucket
	mu       chan bool
	pq       priorityBucketQueue
	sleeping int32
	wakeupQ  chan struct{}
}

func NewDelayBucketQueue() *DelayBucketQueue {
	return &DelayBucketQueue{
		C:       make(chan *bucket),
		pq:      make(priorityBucketQueue, 0, defaultQueueCap),
		mu:      make(chan bool, 1),
		wakeupQ: make(chan struct{}),
	}
}

func (dq *DelayBucketQueue) Offer(b *bucket, expiration time.Time) {
	item := &queueItem{
		bucket:     b,
		Expiration: expiration,
		Index:      -1,
	}
	dq.mu <- true
	heap.Push(&dq.pq, item)
	index := item.Index
	<-dq.mu
	if index == 0 {
		go func() {
			if atomic.CompareAndSwapInt32(&dq.sleeping, 1, 0) {
				dq.wakeupQ <- struct{}{}
			}
		}()
	}

}

func (dq *DelayBucketQueue) Poll(ctx context.Context) {
	for {
		var delta time.Duration
		var nextItem *queueItem
		var noMoreItem bool
		now := time.Now()

		dq.mu <- true
		if dq.pq.Len() != 0 {
			nextItem = dq.pq[0]
			delta = nextItem.Expiration.Sub(now)
			if delta <= 0 {
				heap.Remove(&dq.pq, 0)
			} else {
				nextItem = nil
			}
		} else {
			noMoreItem = true
		}
		<-dq.mu

		if nextItem == nil {
			atomic.StoreInt32(&dq.sleeping, 1)
			if noMoreItem {
				select {
				case <-dq.wakeupQ:
					continue
				case <-ctx.Done():
					if atomic.SwapInt32(&dq.sleeping, 0) == 0 {
						// A caller of Offer() is being blocked on sending to wakeupQ,
						// drain wakeupQ to unblock the caller.
						<-dq.wakeupQ
					}
					return
				}
			} else if delta > 0 {
				ticker := time.NewTicker(delta)
				select {
				case <-dq.wakeupQ:
					ticker.Stop()
					continue
				case <-ticker.C:
					if atomic.SwapInt32(&dq.sleeping, 0) == 0 {
						<-dq.wakeupQ
					}
					continue
				case <-ctx.Done():
					ticker.Stop()
					if atomic.SwapInt32(&dq.sleeping, 0) == 0 {
						<-dq.wakeupQ
					}
					return
				}
			} else {
				panic("unexpect DelayBucketQueue poll item: nil item with delta <= 0")
			}
		}
		select {
		case dq.C <- nextItem.bucket:
		case <-ctx.Done():
			return
		}
	}
}
