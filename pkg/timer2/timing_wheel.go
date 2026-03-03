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
	stopCh     chan struct{}
	stopOnce   sync.Once
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
	t.stopCh = make(chan struct{})
	go t.timerLoop()
}

func (t *TimingWheel) Stop() {
	t.stopOnce.Do(func() {
		if t.stopCh != nil {
			close(t.stopCh)
		}
	})
	t.mu.Lock()
	t.bucketIdx = 0
	t.mu.Unlock()
}

func (t *TimingWheel) timerLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.onTimer()
		case <-t.stopCh:
			return
		}
	}
}

func (t *TimingWheel) onTimer() {
	var entries []Entry
	t.mu.Lock()
	cur := t.buckets[t.bucketIdx]
	if len(cur) > 0 {
		entries = make([]Entry, len(cur))
		copy(entries, cur)
	}
	t.buckets[t.bucketIdx] = t.buckets[t.bucketIdx][:0]
	t.bucketIdx++
	if t.bucketIdx >= t.bucketSize {
		t.bucketIdx = 0
	}
	t.mu.Unlock()

	if len(entries) > 0 {
		for _, v := range entries {
			if v.Release() == 0 {
				v.TimerCallback()
			}
		}
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
