package timewheel

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

type TestTaskID int

func (id TestTaskID) String() string {
	return strconv.Itoa(int(id))
}

type TestTask struct {
	ID         int
	Expiration time.Time
}

func (t *TestTask) ExpireAt() time.Time {
	return t.Expiration
}

func (t *TestTask) GetID() TaskID {
	return TestTaskID(t.ID)
}
func (t *TestTask) Excute() error {
	fmt.Printf("ID :%d plan on %s - diff:%s\n", t.ID, t.Expiration, time.Now().Sub(t.Expiration))
	return nil
}

func TestPerformar(t *testing.T) {
	tw := NewPerformar(500*time.Millisecond, 20)
	tw.Start()
	defer tw.Stop()
	fmt.Printf("start on %s\n", time.Now())

	exp1 := time.Now().Add(5 * time.Second)
	task1 := &TestTask{
		Expiration: exp1,
		ID:         1,
	}

	exp2 := time.Now().Add(2 * time.Second)
	task2 := &TestTask{
		Expiration: exp2,
		ID:         2,
	}

	exp3 := time.Now().Add(9 * time.Second)
	task3 := &TestTask{
		Expiration: exp3,
		ID:         3,
	}
	exp4 := time.Now().Add(5 * time.Second)
	task4 := &TestTask{
		Expiration: exp4,
		ID:         4,
	}
	tw.Add(task1)
	tw.Add(task2)
	tw.Add(task3)
	tw.Add(task4)
	time.Sleep(12 * time.Second)
}

func TestPreloader(t *testing.T) {
	subtw := NewPerformar(100*time.Millisecond, 50)
	tw := NewPreloader(subtw, 6)
	tw.Start()
	defer tw.Stop()
	fmt.Printf("start on %s\n", time.Now())
	tasks := []Task{
		&TestTask{
			Expiration: time.Now().Add(4020 * time.Millisecond),
			ID:         2,
		},
		&TestTask{
			Expiration: time.Now().Add(3999 * time.Millisecond),
			ID:         6,
		},
		&TestTask{
			Expiration: time.Now().Add(1 * time.Second),
			ID:         1,
		},
		&TestTask{
			Expiration: time.Now().Add(2 * time.Second),
			ID:         3,
		},
		&TestTask{
			Expiration: time.Now().Add(9 * time.Second),
			ID:         4,
		},
		&TestTask{
			Expiration: time.Now().Add(6 * time.Second),
			ID:         7,
		},
		&TestTask{
			Expiration: time.Now().Add(11 * time.Second),
			ID:         5,
		},
	}
	//time.Sleep(90 * time.Millisecond)

	for _, task := range tasks {
		err := tw.Add(task)
		if err != nil {
			t.Errorf("err:%s", err)
		}
	}
	time.Sleep(12 * time.Second)
}
