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
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

/*
To insure consistency I suggest running the test for a while with the following,
and if after 5 mins it never fails then we know the testcases are consistent.
  while go test -v  --race ; do echo `date` ; done
*/

func TestDedupeQueue(t *testing.T) {
	t.Parallel()
	completed1 := 0
	completed2 := 0
	cg1 := NewCallGroup(func(finalState map[ID]*Response) {
		completed1++
	})

	cg2 := NewCallGroup(func(finalState map[ID]*Response) {
		completed2++
	})

	now := time.Now()
	op1_1 := cg1.Add(1, &tsMsg{123, now})
	op1_2 := cg1.Add(2, &tsMsg{111, now})
	op2_1 := cg2.Add(1, &tsMsg{123, now})
	op2_2 := cg2.Add(2, &tsMsg{111, now})

	opq := NewDedupeQueue(10, 10)
	defer opq.Close()

	{
		err := opq.Enqueue(op1_1)
		assert.NoError(t, err)
		err = opq.Enqueue(op2_1)
		assert.NoError(t, err)
		err = opq.Enqueue(op1_2)
		assert.NoError(t, err)
		err = opq.Enqueue(op2_2)
		assert.NoError(t, err)
		assert.Equal(t, 2, opq.Len()) // only 2 IDs
	}

	opq.Dequeue(func(set1 *OpSet) {
		assert.Len(t, set1.Ops(), 2)
		for _, op := range set1.Ops() {
			op.Finish(nil, nil)
		}
	})
	assert.Equal(t, 0, completed1)
	assert.Equal(t, 0, completed2)
	opq.Dequeue(func(set2 *OpSet) {
		assert.Len(t, set2.Ops(), 2)
		set2.FinishAll(nil, nil)
	})

	assert.Equal(t, 1, completed1)
	assert.Equal(t, 1, completed2)
}

func TestDedupeQueueClose(t *testing.T) {
	t.Parallel()
	completed1 := 0
	cg1 := NewCallGroup(func(finalState map[ID]*Response) {
		completed1++
	})

	opq := NewDedupeQueue(10, 10)
	now := time.Now()

	for i := range 9 {
		op := cg1.Add(uint64(i), &tsMsg{uint64(i), now})
		err := opq.Enqueue(op)
		assert.NoError(t, err)
	}

	timer := time.AfterFunc(5*time.Second, func() {
		t.Fatalf("testcase timed out after 5 secs.")
	})
	for i := range 9 {
		opq.Dequeue(func(set1 *OpSet) {
			assert.Len(t, set1.Ops(), 1, " at loop:%v set1_len:%v", i, len(set1.Ops()))
		})
	}
	timer.Stop()

	st := time.Now()
	time.AfterFunc(10*time.Millisecond, func() {
		opq.Close() // calling close should release the call to opq.Dequeue()
	})
	open := opq.Dequeue(func(set1 *OpSet) {
		panic("should not be called")
	})
	assert.False(t, open)
	rt := time.Since(st)
	assert.GreaterOrEqual(t, rt, 10*time.Millisecond, "we shouldn't have returned until Close was called: returned after:%v", rt)

}

func TestDedupeQueueFullDepth(t *testing.T) {
	t.Parallel()
	completed1 := 0
	cg1 := NewCallGroup(func(finalState map[ID]*Response) {
		completed1++
	})

	opq := NewDedupeQueue(10, 10)
	defer opq.Close()

	succuess := 0
	depthErrors := 0
	widthErrors := 0
	now := time.Now()

	for i := range 100 {
		op := cg1.Add(uint64(i), &tsMsg{uint64(i), now})
		err := opq.Enqueue(op)
		switch err {
		case nil:
			succuess++
		case ErrQueueSaturatedDepth:
			depthErrors++
		case ErrQueueSaturatedWidth:
			widthErrors++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	for i := range 10 {
		op := cg1.Add(uint64(i), &tsMsg{uint64(i), now})
		err := opq.Enqueue(op)
		switch err {
		case nil:
			succuess++
		case ErrQueueSaturatedDepth:
			depthErrors++
		case ErrQueueSaturatedWidth:
			widthErrors++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	assert.Equalf(t, 20, succuess, "expected 10, got:%v", succuess)
	assert.Equalf(t, 90, depthErrors, "expected 90, got:%v", depthErrors)
	assert.Equalf(t, 0, widthErrors, "expected 0, got:%v", widthErrors)

	timer := time.AfterFunc(5*time.Second, func() {
		t.Fatalf("testcase timed out after 5 secs.")
	})
	for i := range 10 {
		open := opq.Dequeue(
			func(set1 *OpSet) {
				assert.Len(t, set1.Ops(), 2, " at loop:%v set1_len:%v", i, len(set1.Ops()))
			},
		)
		assert.True(t, open)
	}
	timer.Stop()
}

// TestDedupeQueueFullWidth exactly like the test above, except we enqueue the SAME ID each time,
// so that we get ErrQueueSaturatedWidth errrors instead of ErrQueueSaturatedDepth errors.
func TestDedupeQueueFullWidth(t *testing.T) {
	t.Parallel()
	completed1 := 0
	cg1 := NewCallGroup(func(finalState map[ID]*Response) {
		completed1++
	})

	opq := NewDedupeQueue(10, 10)
	defer opq.Close()

	succuess := 0
	depthErrors := 0
	widthErrors := 0
	now := time.Now()

	for i := range 100 {
		op := cg1.Add(0, &tsMsg{uint64(i), now})
		err := opq.Enqueue(op)
		switch err {
		case nil:
			succuess++
		case ErrQueueSaturatedDepth:
			depthErrors++
		case ErrQueueSaturatedWidth:
			widthErrors++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	for i := 1; i < 10; i++ {
		op := cg1.Add(uint64(i), &tsMsg{uint64(i), now})
		err := opq.Enqueue(op)
		switch err {
		case nil:
			succuess++
		case ErrQueueSaturatedDepth:
			depthErrors++
		case ErrQueueSaturatedWidth:
			widthErrors++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	assert.Equalf(t, 19, succuess, "expected 10, got:%v", succuess)
	assert.Equalf(t, 0, depthErrors, "expected 0, got:%v", depthErrors)
	assert.Equalf(t, 90, widthErrors, "expected 90, got:%v", widthErrors)

	timer := time.AfterFunc(5*time.Second, func() {
		t.Fatalf("testcase timed out after 5 secs.")
	})

	open := opq.Dequeue(
		func(set1 *OpSet) {
			assert.Len(t, set1.Ops(), 10, " at loop:%v set1_len:%v", 0, len(set1.Ops())) // max width is 10, so we should get 10 in the first batch
		})
	assert.True(t, open)
	for i := 1; i < 10; i++ {
		open := opq.Dequeue(
			func(set1 *OpSet) {
				assert.Len(t, set1.Ops(), 1, " at loop:%v set1_len:%v", i, len(set1.Ops()))
			})
		assert.True(t, open)
	}

	timer.Stop()
}

func TestDedupeQueueForRaceDetection(t *testing.T) {
	t.Parallel()
	completed1 := 0
	cg1 := NewCallGroup(func(finalState map[ID]*Response) {
		completed1++
	})

	enqueueCnt := atomic.Int64{}
	dequeueCnt := atomic.Int64{}
	mergeCnt := atomic.Int64{}
	depthErrorCnt := atomic.Int64{}
	widthErrorCnt := atomic.Int64{}

	opq := NewDedupeQueue(300, 500)
	defer opq.Close()

	startingLine1 := sync.WaitGroup{}
	startingLine2 := sync.WaitGroup{}
	// block all go routines until the loop has finished spinning them up.
	startingLine1.Add(1)
	startingLine2.Add(1)

	finishLine, finish := context.WithCancel(context.Background())
	dequeFinishLine, deqFinish := context.WithCancel(context.Background())
	const concurrency = 2
	now := time.Now()

	for w := range concurrency {
		go func(w int) {
			startingLine1.Wait()
			for i := range 1000000 {
				select {
				case <-finishLine.Done():
					t.Logf("worker %v exiting at %v", w, i)
					return
				default:
				}
				op := cg1.Add(uint64(i), &tsMsg{uint64(i), now})
				err := opq.Enqueue(op)
				switch err {
				case nil:
					enqueueCnt.Add(1)
				case ErrQueueSaturatedDepth:
					depthErrorCnt.Add(1)
				case ErrQueueSaturatedWidth:
					widthErrorCnt.Add(1)
				default:
					t.Errorf("unexpected error: %v", err)
				}
			}
		}(w)
	}

	for range concurrency {
		go func() {
			startingLine2.Wait()
			for {
				select {
				case <-dequeFinishLine.Done():
					return
				default:
				}
				open := opq.Dequeue(func(set1 *OpSet) {
					dequeueCnt.Add(int64(len(set1.Ops())))
					if len(set1.Ops()) > 1 {
						mergeCnt.Add(1)
					}
				})
				select {
				case <-dequeFinishLine.Done():
					return
				default:
				}
				assert.True(t, open)
			}
		}()
	}
	startingLine1.Done() //release all the waiting workers.
	startingLine2.Done() //release all the waiting workers.

	const runtime = 2
	timeout := time.AfterFunc((runtime+10)*time.Second, func() {
		t.Fatalf("testcase timed out after 5 secs.")
	})
	defer timeout.Stop()

	//let the testcase run for N seconds
	time.AfterFunc(runtime*time.Second, func() {
		finish()
	})
	<-finishLine.Done()
	// Sleep to give the dequeue workers plenty of time to drain the queue before exiting.
	time.Sleep(500 * time.Millisecond)
	deqFinish()

	enq := enqueueCnt.Load()
	deq := dequeueCnt.Load()
	if enq != deq {
		t.Fatalf("enqueueCnt and dequeueCnt should match: enq:% deq:%v", enq, deq)
	}
	// NOTE: I get the following performance on my laptop:
	//       dedupequeue_test.go:275: enqueue errors: 137075 mergedMsgs:2553 enqueueCnt:231437 dequeueCnt:231437 rate:115718 msgs/sec
	//       Over 100k msg a sec is more than fast enough...
	t.Logf("Run Stats [note errors are expect for this test]")
	t.Logf("  enqueue errors:[depth-errs:%v width-errs:%v]", depthErrorCnt.Load(), widthErrorCnt.Load())
	t.Logf("  mergedMsgs:%v enqueueCnt:%v dequeueCnt:%v rate:%v msgs/sec", mergeCnt.Load(), enq, deq, enq/runtime)
}

func TestDedupeQueueCloseConcurrent(t *testing.T) {
	t.Parallel()

	cg1 := NewCallGroup(func(finalState map[ID]*Response) {})
	cg2 := NewCallGroup(func(finalState map[ID]*Response) {})

	now := time.Now()

	op1 := cg1.Add(1, &tsMsg{123, now})
	op2 := cg2.Add(2, &tsMsg{321, now})

	oq := NewDedupeQueue(300, 500)

	var ops uint64
	var closes uint64
	const workers int = 12
	for range workers {
		go func() {
			for oq.Dequeue(func(set *OpSet) { atomic.AddUint64(&ops, 1) }) {
			}
			atomic.AddUint64(&closes, 1)
		}()
	}

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, uint64(0), atomic.LoadUint64(&ops)) // nothing should have been dequeued yet
	assert.Equal(t, uint64(0), atomic.LoadUint64(&closes))

	err := oq.Enqueue(op1)
	assert.NoError(t, err)
	err = oq.Enqueue(op2)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, uint64(2), atomic.LoadUint64(&ops)) // 2 uniq IDs are enqueued
	assert.Equal(t, uint64(0), atomic.LoadUint64(&closes))

	oq.Close()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, uint64(2), atomic.LoadUint64(&ops)) // we still only had 2 uniq IDs seen
	assert.Equal(t, uint64(workers), atomic.LoadUint64(&closes))
}
