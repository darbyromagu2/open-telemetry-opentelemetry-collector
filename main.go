package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Request represents a batch of traces/data to be exported.
type Request interface {
	ID() string
	Export(ctx context.Context) error
}

// MockRequest implements Request.	ype MockRequest struct {
	id    string
	count int
	fn    func(ctx context.Context) error
}

func (m *MockRequest) ID() string {
	return m.id
}

func (m *MockRequest) Export(ctx context.Context) error {
	return m.fn(ctx)
}

// Queue represents a queue interface.	ype Queue interface {
	Produce(ctx context.Context, req Request) error
	Consume() (Request, func(bool), bool)
	Size() int
}

// MemoryQueue implements Queue with in-flight tracking.	ype MemoryQueue struct {
	mu        sync.Mutex
	items     []Request
	inFlight  map[string]bool
	cond      *sync.Cond
	isPaused  bool
}

func NewMemoryQueue() *MemoryQueue {
	q := &MemoryQueue{
		inFlight: make(map[string]bool),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *MemoryQueue) Produce(ctx context.Context, req Request) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, req)
	q.cond.Signal()
	return nil
}

func (q *MemoryQueue) Consume() (Request, func(bool), bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) == 0 || q.isPaused {
		q.cond.Wait()
	}

	// Find the first item that is not currently in-flight
	idx := -1
	for i, item := range q.items {
		if !q.inFlight[item.ID()] {
			idx = i
			break
		}
	}

	if idx == -1 {
		return nil, nil, false
	}

	req := q.items[idx]
	q.inFlight[req.ID()] = true

	// Remove from queue items list
	q.items = append(q.items[:idx], q.items[idx+1:]...)

	commitFn := func(success bool) {
		q.mu.Lock()
		defer q.mu.Unlock()
		delete(q.inFlight, req.ID())
		if !success {
			// Re-enqueue if failed and not retrying anymore
			q.items = append([]Request{req}, q.items...)
			q.cond.Signal()
		}
	}

	return req, commitFn, true
}

func (q *MemoryQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *MemoryQueue) Pause() {
	q.mu.Lock()
	q.isPaused = true
	q.mu.Unlock()
}

func (q *MemoryQueue) Resume() {
	q.mu.Lock()
	q.isPaused = false
	q.cond.Broadcast()
	q.mu.Unlock()
}

// QueuedRetrySender manages the queueing and retry loop wrapper.	ype QueuedRetrySender struct {
	queue          Queue
	inFlightRetries sync.Map
}

func NewQueuedRetrySender(q Queue) *QueuedRetrySender {
	return &QueuedRetrySender{
		queue: q,
	}
}

func (q *QueuedRetrySender) Send(ctx context.Context, req Request) error {
	return q.queue.Produce(ctx, req)
}

func (q *QueuedRetrySender) StartConsumers(ctx context.Context, numConsumers int, maxRetries int, backoff time.Duration) {
	for i := 0; i < numConsumers; i++ {
		go func() {
			for {
				req, commit, ok := q.queue.Consume()
				if !ok {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				// Ensure we don't concurrently process the same batch ID
				if _, loaded := q.inFlightRetries.LoadOrStore(req.ID(), true); loaded {
					// Already in-flight/retrying elsewhere, release without re-queueing
					commit(true)
					continue
				}

				go func(r Request, c func(bool)) {
					defer q.inFlightRetries.Delete(r.ID())
					
					success := false
					for attempt := 0; attempt <= maxRetries; attempt++ {
						err := r.Export(ctx)
						if err == nil {
							success = true
							break
						}
						// If it's a retryable error, backoff and retry
						if isRetryable(err) && attempt < maxRetries {
							time.Sleep(backoff)
							continue
						}
						break
					}
					c(success)
				}(req, commit)
			}
		}()
	}
}

func isRetryable(err error) bool {
	return err != nil && err.Error() == "retryable"
}

func main() {
	fmt.Println("Running Trace Duplication Fix Verification...")

	q := NewMemoryQueue()
	sender := NewQueuedRetrySender(q)

	var mu sync.Mutex
	attempts := 0
	successes := 0

	// Mock destination backend: returns retryable error twice, then succeeds
	mockExport := func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts <= 2 {
			return errors.New("retryable")
		}
		successes++
		return nil
	}

	req := &MockRequest{
		id: "batch-1",
		fn: mockExport,
	}

	ctx := context.Background()
	_ = sender.Send(ctx, req)

	// Start consumers and simulate queue pause/resume during retry
	sender.StartConsumers(ctx, 2, 3, 50*time.Millisecond)

	time.Sleep(25 * time.Millisecond) // Wait during first retry
	q.Pause()
	time.Sleep(50 * time.Millisecond) // Wait during second retry
	q.Resume()

	time.Sleep(100 * time.Millisecond) // Wait for completion

	mu.Lock()
	finalAttempts := attempts
	finalSuccesses := successes
	mu.Unlock()

	fmt.Printf("Total attempts: %d, Successes: %d\n", finalAttempts, finalSuccesses)
	if finalSuccesses == 1 && finalAttempts == 3 {
		fmt.Println("SUCCESS: No trace duplication detected during retry and queue recovery!")
	} else {
		fmt.Println("FAILURE: Trace duplication or incorrect attempt count detected!")
	}
}
