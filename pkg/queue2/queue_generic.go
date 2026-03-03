// Package queue2 generic queue: QueueGen[T] avoids interface{} boxing for value types.
// Use NewQueueGen[T]() for type-safe, zero-allocation enqueue/dequeue of T.

package queue2

// QueueGen is a generic ring-buffer queue. It is not thread-safe.
// Use SyncQueue for concurrent access.
type QueueGen[T any] struct {
	buf        []T
	head, tail int
	count      int
}

// NewQueueGen constructs a new generic queue.
func NewQueueGen[T any]() *QueueGen[T] {
	return &QueueGen[T]{
		buf: make([]T, minQueueLen),
	}
}

// Length returns the number of elements in the queue.
func (q *QueueGen[T]) Length() int {
	return q.count
}

func (q *QueueGen[T]) resize() {
	newBuf := make([]T, q.count<<1)
	if q.tail > q.head {
		copy(newBuf, q.buf[q.head:q.tail])
	} else {
		n := copy(newBuf, q.buf[q.head:])
		copy(newBuf[n:], q.buf[:q.tail])
	}
	q.head = 0
	q.tail = q.count
	q.buf = newBuf
}

// Add puts an element on the end of the queue.
func (q *QueueGen[T]) Add(elem T) {
	if q.count == len(q.buf) {
		q.resize()
	}
	q.buf[q.tail] = elem
	q.tail = (q.tail + 1) & (len(q.buf) - 1)
	q.count++
}

// Peek returns the element at the head without removing it. Panics if empty.
func (q *QueueGen[T]) Peek() T {
	if q.count <= 0 {
		panic("queue: Peek() called on empty queue")
	}
	return q.buf[q.head]
}

// Get returns the element at index i (0 = head, -1 = last). Panics if out of range.
func (q *QueueGen[T]) Get(i int) T {
	if i < 0 {
		i += q.count
	}
	if i < 0 || i >= q.count {
		panic("queue: Get() called with index out of range")
	}
	return q.buf[(q.head+i)&(len(q.buf)-1)]
}

// Remove removes and returns the element from the front. Panics if empty.
func (q *QueueGen[T]) Remove() T {
	if q.count <= 0 {
		panic("queue: Remove() called on empty queue")
	}
	ret := q.buf[q.head]
	var zero T
	q.buf[q.head] = zero
	q.head = (q.head + 1) & (len(q.buf) - 1)
	q.count--
	if len(q.buf) > minQueueLen && (q.count<<2) == len(q.buf) {
		q.resize()
	}
	return ret
}
