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
	"sync"
)

// DedupeQueue is a thread-safe duplicate operation suppression queue, that combines
// duplicate operations (queue entires) into sets that will be dequeued together.
//
// For example, If you enqueue an item with a key that already exists, then that
// item will be appended to that key's set of items. Otherwise the item is
// inserted into the head of the list as a new item.
//
// FIXME (ajr) Add docs on how to use the dequeue method
type DedupeQueue struct {
	mu      sync.Mutex
	cond    sync.Cond
	depth   int
	width   int
	q       *list.List
	entries map[ID]*OpSet
	backup  map[ID]*OpSet
	closed  bool
}

func NewDedupeQueue(depth, width int) *DedupeQueue {
	q := DedupeQueue{
		depth:   depth,
		width:   width,
		q:       list.New(),
		entries: map[ID]*OpSet{},
		backup:  map[ID]*OpSet{},
	}
	q.cond.L = &q.mu
	return &q
}

// Close releases resources associated with this callgroup, by canceling the context.
// The owner of this OpQueue should either call Close or cancel the context, both are
// equivalent.
func (q *DedupeQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.q = nil
	q.entries = nil
	q.backup = nil
	q.mu.Unlock()
	q.cond.Broadcast() // alert all dequeue calls that they should wake up and return.
}

// Len returns the number of uniq IDs in the queue, that is the depth of the queue.
func (q *DedupeQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return 0
	}
	return q.q.Len()
}

// Enqueue add the op to the queue.  If the ID already exists then the Op
// is added to the existing OpSet for this ID, otherwise it's inserted as a new
// OpSet.
//
// Enqueue doesn't block if the queue if full, instead it returns a ErrQueueSaturated
// error.
func (q *DedupeQueue) Enqueue(op *Op) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	if set, ok := q.backup[op.ID]; ok {
		if len(set.Ops()) >= q.width {
			return ErrQueueSaturatedWidth
		}

		set.append(op)
		return nil
	}

	set, ok := q.entries[op.ID]
	if !ok {
		// This is a new item, so we need to insert it into the queue.
		if q.q.Len() >= q.depth {
			return ErrQueueSaturatedDepth
		}
		q.newEntry(op.ID, op)

		// Signal one waiting go routine to wake up and Dequeue
		// I believe we only need to signal if we enqueue a new item.
		// Consider the following possible states the queue could be in  :
		//   1. if no one is currently waiting in Dequeue, the signal isn't
		//      needed and all items will be dequeued on the next call to
		//      Dequeue.
		//   2. One or Many go-routines are waiting in Dequeue because it's
		//      empty, and calling Signal will wake up one.  Which will dequeue
		//      the item and return.
		//   3. At most One go-routine is in the act of Dequeueing existing items
		//      from the queue (i.e. only one can have the lock and be in the "if OK"
		//      condition within the forloop in Dequeue).  In which cause the signal
		//      is ignored and after returning we return to condition (1) above.
		// Note signaled waiting go-routines will not be able the acquire
		// the condition lock until this method call returns, finishing
		// its append of the new operation.
		q.cond.Signal()
		return nil
	}
	if len(set.Ops()) >= q.width {
		return ErrQueueSaturatedWidth
	}

	set.append(op)
	return nil
}

// Dequeue removes the oldest OpSet from the queue and returns it.
// Dequeue will block if the Queue is empty.  An Enqueue will wake the
// go routine up and it will continue on.
//
// If the OpQueue is closed, then Dequeue will return false
// for the second parameter.
func (q *DedupeQueue) Dequeue(callback func(*OpSet)) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}

	for {
		if id, set, ok := q.dequeue(); ok {
			q.mu.Unlock()
			callback(set)

			q.mu.Lock()
			defer q.mu.Unlock()
			if q.closed {
				return false
			}

			if q.backup[id].Size() > 0 {
				q.entries[id] = q.backup[id]
				q.q.PushBack(id)
			}
			delete(q.backup, id)
			return true
		}
		if q.closed {
			q.mu.Unlock()
			return false
		}
		q.cond.Wait()
	}
}

func (q *DedupeQueue) newEntry(id ID, op *Op) {
	set := newOpSet(op)
	q.entries[id] = set
	q.q.PushBack(id)
}

func (q *DedupeQueue) dequeue() (ID, *OpSet, bool) {
	if q.closed {
		return 0, nil, false
	}
	elem := q.q.Front()
	if elem == nil {
		return 0, nil, false
	}
	idt := q.q.Remove(elem)
	id := idt.(ID)

	set, ok := q.entries[id]
	if !ok {
		panic("invariant broken: we dequeued a value that isn't in the map")
	}
	delete(q.entries, id)
	q.backup[id] = &OpSet{}
	return id, set, true
}
