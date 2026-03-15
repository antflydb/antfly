// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package inflight

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"
)

// WindowQueue is a windowed, microbatching priority queue for database operations.
// Operations for the same ID and time window form a microbatch. Microbatches are dequeued in FIFO order.
// WindowQueue provides backpressure for both depth (i.e., number of microbatches in queue) and width (i.e., number of operations in a microbatch).
// WindowQueue is safe for concurrent use. Its zero value is not safe to use, use NewDbOpWindow().
type WindowQueue struct {
	mu sync.Mutex
	q  list.List // *dbQueueItem
	m  map[ID]*queueItem

	// These are selectable sync.Cond: use blocking read for Wait() and non-blocking write for Signal().
	queueHasItems chan struct{}
	queueHasSpace chan struct{}

	once sync.Once
	done chan struct{}

	depth      int
	width      int
	windowedBy time.Duration
}

// NewWindowQueue creates a new DbOpWindow.
//
//	depth: maximum number of entries in a queue
//	width: maximum number of entries in a microbatch.
//	windowedBy: window size.
func NewWindowQueue(depth, width int, windowedBy time.Duration) *WindowQueue {
	q := &WindowQueue{
		queueHasItems: make(chan struct{}),
		queueHasSpace: make(chan struct{}),
		done:          make(chan struct{}),
		depth:         depth,
		width:         width,
		windowedBy:    windowedBy,
		m:             make(map[ID]*queueItem),
	}
	q.q.Init()
	return q
}

func (q *WindowQueue) IsFull() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.q.Len() >= q.depth
}

func (q *WindowQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.q.Len()
}

// Close provides graceful shutdown: no new ops will be enqueued.
func (q *WindowQueue) Close() {
	q.once.Do(func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		close(q.done)
		// Set depth to zero so new entries are rejected.
		q.depth = 0
	})
}

// Enqueue adds a database operation into the queue, blocking until first of:
// - operation is enqueued
// - ID has hit max width
// - context is done
// - queue is closed
func (q *WindowQueue) Enqueue(ctx context.Context, id ID, op *Op) error {
	q.mu.Lock() // locked on returns below

	for {
		item, ok := q.m[id]
		if ok {
			if len(item.OpSet.set) >= q.width {
				if !item.IsFullClosed {
					close(item.IsFull)
					item.IsFullClosed = true
				}
				q.mu.Unlock()
				return ErrQueueSaturatedWidth
			}
			item.OpSet.append(op)
			q.mu.Unlock()
			return nil
		}

		if q.q.Len() >= q.depth {
			q.mu.Unlock()
			select {
			case <-ctx.Done():
				return fmt.Errorf("%w: %w", ErrQueueSaturatedDepth, ctx.Err())
			case <-q.done:
				return ErrQueueClosed
			case <-q.queueHasSpace:
				q.mu.Lock()
				continue
			}
		}

		item = &queueItem{
			ID:        id,
			ProcessAt: time.Now().Add(q.windowedBy),
			OpSet:     newOpSet(op),
			IsFull:    make(chan struct{}),
		}
		q.m[id] = item
		q.q.PushBack(item)
		q.mu.Unlock()

		select {
		case q.queueHasItems <- struct{}{}:
		default:
		}

		return nil
	}
}

// Dequeue removes and returns the oldest DbOpSet whose window has passed from the queue,
// blocking until first of: DbOpSet is ready, context is canceled, or queue is closed.
func (q *WindowQueue) Dequeue(ctx context.Context) (*OpSet, error) {
	q.mu.Lock() // unlocked on returns below

	var item *queueItem
	for item == nil {
		elem := q.q.Front()
		if elem == nil {
			q.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-q.done:
				return nil, ErrQueueClosed
			case <-q.queueHasItems:
				q.mu.Lock()
				continue
			}

		}
		item = q.q.Remove(elem).(*queueItem) // next caller will wait for a different item
	}

	waitFor := time.Until(item.ProcessAt)
	if waitFor > 0 {
		q.mu.Unlock()
		timer := time.NewTimer(waitFor)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			// Put the item back at the front so it can be dequeued again later.
			q.mu.Lock()
			q.q.PushFront(item)
			q.mu.Unlock()
			return nil, ctx.Err()
		case <-q.done:
			// process right away
		case <-item.IsFull:
			// process once full, regardless of windowing
		case <-timer.C:
		}
		q.mu.Lock()
	}

	ops := item.OpSet
	delete(q.m, item.ID)
	q.mu.Unlock()
	item = nil // gc

	select {
	case q.queueHasSpace <- struct{}{}:
	default:
	}
	return ops, nil
}
