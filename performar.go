package timewheel

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type Performar struct {
	ticking  int32
	interval time.Duration
	tick     time.Duration
	buckets  []*bucket
	queue    *DelayBucketQueue
	cancel   context.CancelFunc
	wg       *sync.WaitGroup
	mu       chan bool
}

var _ TimeWheel = (*Performar)(nil)

func NewPerformar(tick time.Duration, wheelSize int) *Performar {
	return &Performar{
		interval: tick * time.Duration(wheelSize),
		tick:     tick,
		buckets:  make([]*bucket, wheelSize),
		queue:    NewDelayBucketQueue(),
		wg:       new(sync.WaitGroup),
		mu:       make(chan bool, 1),
	}
}

func (tw *Performar) Add(t Task) error {
	if tw.ticking != 1 {
		return errors.New("TimeWheel is not ticking")
	}
	currentTime := time.Now()
	delta := t.ExpireAt().Sub(currentTime)
	if delta < tw.tick {
		// excute now
		go t.Excute()
		return nil
	} else if delta < tw.interval {
		bIndex := delta / tw.tick
		b := tw.buckets[bIndex]
		if b == nil {
			tw.buckets[bIndex] = newBucket()
			b = tw.buckets[bIndex]
		}
		formerCount := b.Add(t)
		if formerCount == 0 {
			// 任务执行实际误差为  -tick ~ 0 如果为了精密考虑可以再加一个1/2 的tick
			tw.queue.Offer(b, currentTime.Add(bIndex*tw.tick))
		}
		return nil
	}
	return errors.New("task exp overflow")
}

func (tw *Performar) Interval() time.Duration {
	return tw.interval
}

func (tw *Performar) Start() {
	tw.mu <- true
	if atomic.SwapInt32(&tw.ticking, 1) == 1 {
		<-tw.mu
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	tw.cancel = cancel
	tw.wg.Add(2)
	<-tw.mu

	go func() {
		tw.queue.Poll(ctx)
		tw.wg.Done()
	}()

	go func() {
		defer tw.wg.Done()
		for {
			select {
			case b := <-tw.queue.C:
				b.ExcuteAllTask()
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (tw *Performar) Stop() {
	tw.mu <- true
	if atomic.SwapInt32(&tw.ticking, 0) == 0 {
		<-tw.mu
		return
	}
	if tw.cancel != nil {
		tw.cancel()
	}
	tw.wg.Wait()
	tw.cancel = nil
	<-tw.mu
}
