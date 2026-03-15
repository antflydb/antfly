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
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrBufferClosed error = errors.New("buffer is closed")

// Make sure to register the concrete types used for Op.Data with gob
// For example: gob.Register(MyDataType{})
// This should be done in the application's init() function.

// ID is a unique identifier for a database operation or a group of related operations.
type ID uint64

// Op represents a database operation to be performed.
type Op struct {
	ID   ID  `json:"id"`
	Data any `json:"data"` // The data for the operation

	cg *CallGroup // This field is NOT persisted by GobEncode/GobDecode
}

// Finish this DbOp.
func (o *Op) Finish(err error, resp any) {
	if o.cg == nil {
		return
	}
	o.cg.mu.Lock()
	defer o.cg.mu.Unlock()

	if err != nil {
		o.cg.finalState[o.ID] = &Response{Op: o, Err: err}
	} else {
		o.cg.finalState[o.ID] = &Response{Op: o, Result: resp}
	}
	delete(o.cg.outstandingOps, o.ID)

	o.cg.done()
}

// FinishAll a convenience func that calls finish on each Op in the set, passing the
// results or error to all the Ops in the OpSet.
//
// NOTE: The call group that owns this OP will not call it's finish function until all
// Ops are complete.  And one callgroup could be spread over multiple op sets or
// multiple op queues.
func (os *OpSet) FinishAll(err error, resp any) {
	for _, op := range os.set {
		op.Finish(err, resp)
	}
}

// OpSet represents a set of database operations that share the same ID.
type OpSet struct {
	set []*Op
}

// newOpSet creates a new operation set with the provided operation.
func newOpSet(op *Op) *OpSet {
	return &OpSet{
		set: []*Op{op},
	}
}

func (s *OpSet) append(op *Op) {
	s.set = append(s.set, op)
}

func (s *OpSet) Ops() []*Op {
	return s.set
}

// Size returns the number of operations in this set.
func (s *OpSet) Size() int {
	return len(s.set)
}

// ErrQueueSaturatedWidth is returned when the queue is saturated by width.
var ErrQueueSaturatedWidth = fmt.Errorf("queue saturated by width")

// ErrQueueSaturatedDepth is returned when the queue is saturated by depth.
var ErrQueueSaturatedDepth = fmt.Errorf("queue saturated by depth")

// ErrQueueClosed is returned when the queue is closed.
var ErrQueueClosed = fmt.Errorf("queue closed")

// queueItem represents an item in the operation queue.
type queueItem struct {
	ID           ID
	ProcessAt    time.Time
	OpSet        *OpSet
	IsFull       chan struct{}
	IsFullClosed bool
}

// CallGroup spawns off a group of operations for each call to Add() and
// calls the CallGroupCompletion func when the last operation have
// completed.  The CallGroupCompletion func can be thought of as a finalizer where
// one can gather errors and/or results from the function calls.
//
// Call Add for all our inflight tasks before calling the first
// call to Finish.  Once the last task finishes and the CallGroupCompletion
// is triggered, all future calls to Add will be ignored and orphaned.
type CallGroup struct {
	mu sync.Mutex

	cgcOnce             sync.Once
	callGroupCompletion CallGroupCompletion

	outstandingOps map[ID]*Op
	finalState     map[ID]*Response
}

// NewCallGroup return a new CallGroup.
// Takes a CallGroupCompletion func as an argument, which will be called when the last Op in
// the CallGroup has called Finish.
//
// In a way a CallGroup is like a Mapper-Reducer in other framworks, with
// the Ops being mapped out to workers and the CallGroupCompletion being the reducer step.
func NewCallGroup(cgc CallGroupCompletion) *CallGroup {
	return &CallGroup{
		outstandingOps:      map[ID]*Op{},
		finalState:          map[ID]*Response{},
		callGroupCompletion: cgc,
		cgcOnce:             sync.Once{},
	}
}

// Add a op to message to callgroup.
func (cg *CallGroup) Add(k uint64, msg any) *Op {
	key := ID(k)

	op := &Op{
		cg:   cg,
		ID:   key,
		Data: msg,
	}

	cg.mu.Lock()
	defer cg.mu.Unlock()

	cg.outstandingOps[key] = op

	return op
}

func (cg *CallGroup) done() {
	if len(cg.outstandingOps) > 0 {
		return
	}

	cg.cgcOnce.Do(func() {
		//callGroupCompletion should never be nil, so let it panic if it is.
		cg.callGroupCompletion(cg.finalState)
	})
}

// CallGroupCompletion is the reducer function for a callgroup, its called once all
// Ops in the callgroup have called Finished and the final state is passed to this
// function.
type CallGroupCompletion func(finalState map[ID]*Response)

// Response for an op.
type Response struct {
	Op     *Op
	Err    error
	Result any
}
