package timewheel

import (
	"container/list"
)

type bucket struct {
	mu    chan bool
	tasks *list.List
}

func newBucket() *bucket {
	return &bucket{
		mu:    make(chan bool, 1),
		tasks: list.New(),
	}
}

func (b *bucket) Add(t Task) (formerLen int) {
	b.mu <- true
	formerLen = b.tasks.Len()
	b.tasks.PushBack(t)
	<-b.mu
	return formerLen
}

func (b *bucket) ExcuteAllTask() {
	b.mu <- true
	e := b.tasks.Front()
	for e != nil {
		next := e.Next()
		t := e.Value.(Task)
		//TODO: 不remove 遍历后直接重置list?
		b.tasks.Remove(e)
		go t.Excute()
		e = next
	}
	<-b.mu
}

func (b *bucket) PopAllTask() (tasks []Task) {
	tasks = make([]Task, b.tasks.Len())
	b.mu <- true
	e := b.tasks.Front()
	for i := 0; e != nil; i++ {
		next := e.Next()
		t := e.Value.(Task)
		b.tasks.Remove(e)
		tasks[i] = t
		e = next
	}
	<-b.mu
	return tasks
}
