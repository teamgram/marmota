package timer2

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/teamgram/marmota/pkg/sync2"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type MockEntry struct {
	id       int
	refcount sync2.AtomicInt32
}

func (e *MockEntry) AddRef() {
	e.refcount.Add(1)
}

func (e *MockEntry) Release() int32 {
	return e.refcount.Add(-1)
}

func (e *MockEntry) TimerCallback() {
	fmt.Println("TimerCallback - ", e)
}

func TestTimingWheel(t *testing.T) {
	wheel := NewTimingWheel(8)
	wheel.Start()

	entries := make([]MockEntry, 100)

	for i := 0; i < 10; i++ {
		entries[i].id = i
		// wheel.AddTimer(&entries[i], rand.Intn(8))
	}

	for j := 0; j < 10; j++ {
		for i := 0; i < 10; i++ {
			// entries[i].id = i
			wheel.AddTimer(&entries[i], rand.Intn(8))
		}
		time.Sleep(time.Second)
	}

	time.Sleep(10 * time.Second)
}
