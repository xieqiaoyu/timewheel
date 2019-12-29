package timewheel

import (
	"fmt"
	"time"
)

type TimeWheel interface {
	Add(t Task) error
	Interval() time.Duration
	Start()
	Stop()
}

type TaskID interface {
	fmt.Stringer
}

type Task interface {
	Excute() error
	ExpireAt() time.Time
	//	GetID() TaskID
}
