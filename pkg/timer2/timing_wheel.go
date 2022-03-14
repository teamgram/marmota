package timer2

import (
	"fmt"
	"sync"
	"time"
)

// var timingWheel = new(TimingWheel)

type Entry interface {
	AddRef()
	Release() int32
	TimerCallback()
}

type TimingWheel struct {
	running    bool
	bucketSize int
	bucketIdx  int
	buckets    [][]Entry
	mu         sync.Mutex
}

func NewTimingWheel(size int) *TimingWheel {
	if size <= 0 {
		size = 8
	}
	return &TimingWheel{
		bucketIdx:  0,
		bucketSize: size,
		buckets:    make([][]Entry, size),
	}
}

func (t *TimingWheel) Start() {
	t.running = true
	go t.timerLoop()
}

func (t *TimingWheel) Stop() {
	t.running = false
	t.bucketIdx = 0
}

func (t *TimingWheel) timerLoop() {
	for t.running {
		<-time.After(time.Second)
		t.onTimer()
	}
}

func (t *TimingWheel) onTimer() {
	var entries []Entry
	t.mu.Lock()
	entries = t.buckets[t.bucketIdx]
	t.buckets[t.bucketIdx] = []Entry{}
	t.mu.Unlock()

	if len(entries) > 0 {
		for _, v := range entries {
			if v.Release() == 0 {
				v.TimerCallback()
			}
		}
	}

	t.bucketIdx++
	if t.bucketIdx >= t.bucketSize {
		t.bucketIdx = 0
	}
}

func (t *TimingWheel) AddTimer(entry Entry, tm int) error {
	if tm < 1 || tm >= t.bucketSize {
		err := fmt.Errorf("invalid tm: %d", tm)
		return err
	}

	entry.AddRef()

	t.mu.Lock()
	slot := (t.bucketIdx + tm) % t.bucketSize
	t.buckets[slot] = append(t.buckets[slot], entry)
	t.mu.Unlock()

	return nil
}
