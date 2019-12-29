package timewheel

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type Preloader struct {
	ticking  int32
	interval time.Duration
	tick     time.Duration
	buckets  []*bucket
	queue    *DelayBucketQueue
	cancel   context.CancelFunc
	wg       *sync.WaitGroup
	mu       chan bool
	subWheel TimeWheel
}

var _ TimeWheel = (*Preloader)(nil)

func NewPreloader(subWheel TimeWheel, wheelSize int) *Preloader {
	// tick 采用subWheel 的一半,用于填充
	tick := (subWheel.Interval() >> 1)
	return &Preloader{
		interval: tick * time.Duration(wheelSize),
		tick:     tick,
		buckets:  make([]*bucket, wheelSize),
		queue:    NewDelayBucketQueue(),
		wg:       new(sync.WaitGroup),
		mu:       make(chan bool, 1),
		subWheel: subWheel,
	}
}

func (tw *Preloader) Add(t Task) error {
	if tw.ticking != 1 {
		return errors.New("TimeWheel is not ticking")
	}
	currentTime := time.Now()
	delta := t.ExpireAt().Sub(currentTime)
	if delta < 0 {
		return errors.New("Expired task add")
	} else if delta < (tw.tick << 1) {
		return tw.subWheel.Add(t)
	} else if delta < tw.interval {
		bIndex := delta / tw.tick
		b := tw.buckets[bIndex]
		if b == nil {
			tw.buckets[bIndex] = newBucket()
			b = tw.buckets[bIndex]
		}
		formerCount := b.Add(t)
		if formerCount == 0 {
			// 提前至少一个tick 填充数据
			tw.queue.Offer(b, currentTime.Add((bIndex-1)*tw.tick))
		}
		return nil
	}
	return errors.New("task exp overflow")
}

func (tw *Preloader) Interval() time.Duration {
	return tw.interval
}

func (tw *Preloader) Start() {
	tw.mu <- true
	if atomic.SwapInt32(&tw.ticking, 1) == 1 {
		<-tw.mu
		return
	}
	tw.subWheel.Start()
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
				tasks := b.PopAllTask()
				for _, t := range tasks {
					tw.subWheel.Add(t)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (tw *Preloader) Stop() {
	tw.mu <- true
	if atomic.SwapInt32(&tw.ticking, 0) == 0 {
		return
	}
	if tw.cancel != nil {
		tw.cancel()
	}
	tw.wg.Wait()
	tw.cancel = nil
	tw.subWheel.Stop()
	<-tw.mu
}
