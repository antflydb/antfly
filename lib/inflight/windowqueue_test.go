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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type tsMsg struct {
	ID   uint64
	Time time.Time
}

func TestOpWindow(t *testing.T) {
	t.Parallel()

	winTimes := []time.Duration{
		time.Duration(0),
		1 * time.Millisecond,
		10 * time.Millisecond,
		100 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
	}

	for _, winTime := range winTimes {
		// scope it locally so it can be correctly captured
		t.Run(fmt.Sprintf("windowed_by_%v", winTime), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			completed1 := 0
			completed2 := 0
			cg1 := NewCallGroup(func(map[ID]*Response) {
				completed1++
			})

			cg2 := NewCallGroup(func(map[ID]*Response) {
				completed2++
			})

			now := time.Now()

			op1_1 := cg1.Add(1, &tsMsg{123, now})
			op1_2 := cg1.Add(2, &tsMsg{111, now})
			op2_1 := cg2.Add(1, &tsMsg{123, now})
			op2_2 := cg2.Add(2, &tsMsg{111, now})

			window := NewWindowQueue(3, 3, winTime)
			t.Cleanup(window.Close)

			st := time.Now()
			{
				err := window.Enqueue(ctx, op1_1.ID, op1_1)
				require.NoError(t, err)
				err = window.Enqueue(ctx, op2_1.ID, op2_1)
				require.NoError(t, err)
				err = window.Enqueue(ctx, op1_2.ID, op1_2)
				require.NoError(t, err)
				err = window.Enqueue(ctx, op2_2.ID, op2_2)
				require.NoError(t, err)
			}

			require.Equal(t, 2, window.Len()) // only 2 unique keys

			_, err := window.Dequeue(ctx)
			assert.NoError(t, err)
			_, err = window.Dequeue(ctx)
			assert.NoError(t, err)

			rt := time.Since(st)
			assert.Greater(t, rt, winTime)
		})
	}
}

func TestOpWindowClose(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	winTime := 100 * time.Hour // we want everything to hang until we close the queue.

	cg1 := NewCallGroup(func(map[ID]*Response) {})
	cg2 := NewCallGroup(func(map[ID]*Response) {})

	now := time.Now()

	op1_1 := cg1.Add(1, &tsMsg{123, now})
	op1_2 := cg1.Add(2, &tsMsg{111, now})
	op2_1 := cg2.Add(1, &tsMsg{123, now})
	op2_2 := cg2.Add(2, &tsMsg{111, now})

	window := NewWindowQueue(3, 3, winTime)

	err := window.Enqueue(ctx, op1_1.ID, op1_1)
	require.NoError(t, err)
	err = window.Enqueue(ctx, op2_1.ID, op2_1)
	require.NoError(t, err)
	err = window.Enqueue(ctx, op1_2.ID, op1_2)
	require.NoError(t, err)
	err = window.Enqueue(ctx, op2_2.ID, op2_2)
	require.NoError(t, err)

	var ops uint64
	var closes uint64
	const workers int = 12
	for range workers {
		go func() {
			for {
				if _, err := window.Dequeue(ctx); err != nil {
					require.ErrorIs(t, err, ErrQueueClosed)
					atomic.AddUint64(&closes, 1)
					return
				}
				atomic.AddUint64(&ops, 1)
			}
		}()
	}

	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, uint64(0), atomic.LoadUint64(&ops)) // nothing should have been dequeued yet

	window.Close()
	time.Sleep(1000 * time.Millisecond)
	assert.Equal(t, uint64(workers), atomic.LoadUint64(&closes))
	assert.Equal(t, uint64(2), atomic.LoadUint64(&ops)) // 2 uniq keys are enqueued

	err = window.Enqueue(ctx, op1_1.ID, op1_1)
	require.ErrorIs(t, err, ErrQueueClosed)
}

func TestOpWindowErrQueueSaturatedWidth(t *testing.T) {
	t.Parallel()
	cg := NewCallGroup(func(map[ID]*Response) {})
	now := time.Now()

	op1 := cg.Add(1, &tsMsg{123, now})
	op2 := cg.Add(1, &tsMsg{123, now})

	window := NewWindowQueue(2, 1, time.Millisecond)
	ctx := context.Background()
	err := window.Enqueue(ctx, op1.ID, op1)
	require.NoError(t, err)

	err = window.Enqueue(ctx, op2.ID, op2)
	require.ErrorIs(t, err, ErrQueueSaturatedWidth)

	_, err = window.Dequeue(ctx)
	require.NoError(t, err)

	err = window.Enqueue(ctx, op2.ID, op2)
	require.NoError(t, err)
}

func TestOpWindowErrQueueSaturatedDepth(t *testing.T) {
	t.Parallel()
	cg := NewCallGroup(func(map[ID]*Response) {})
	now := time.Now()
	op1 := cg.Add(1, &tsMsg{123, now})
	op2 := cg.Add(2, &tsMsg{234, now})
	op3 := cg.Add(3, &tsMsg{332, now})

	window := NewWindowQueue(1, 1, time.Millisecond)
	ctx := context.Background()

	// First we enqueue the first op, which should be successful.
	err := window.Enqueue(ctx, op1.ID, op1)
	require.NoError(t, err)

	// Now let's try to enqueue a second op with a context that will timeout
	// while waiting for space in the queue.
	// We expect the background goroutine to dequeue the first op and then
	// the second op to be enqueued.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	go func() {
		time.Sleep(200 * time.Millisecond)
		_, err := window.Dequeue(ctx)
		require.NoError(t, err)
	}()
	err = window.Enqueue(ctx, op2.ID, op2)
	require.NoError(t, err)
	require.NoError(t, ctx.Err(), "expected context to be done, it seems the test timed out")

	// Now let's try to enqueue a third op with a context that will timeout
	// while waiting for space in the queue.
	ctx2, cancel2 := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel2()
	err = window.Enqueue(ctx2, op3.ID, op3) // We expect this to be a timeout while waiting for space
	require.ErrorIs(t, err, ErrQueueSaturatedDepth, "expected a queue saturation error, but got: %v", err)
	// We should be able to dequeue the op still.
	_, err = window.Dequeue(ctx)
	require.NoError(t, err)

	// Now we should be able to enqueue the forth op
	// without any errors, as the queue has space.
	err = window.Enqueue(ctx, op2.ID, op2)
	require.NoError(t, err)
}

func TestOpWindowErrQueueSaturatedDepthClose(t *testing.T) {
	t.Parallel()
	cg := NewCallGroup(func(map[ID]*Response) {})
	now := time.Now()
	op1 := cg.Add(1, &tsMsg{123, now})
	op2 := cg.Add(2, &tsMsg{234, now})

	window := NewWindowQueue(1, 1, time.Millisecond)
	ctx := context.Background()
	err := window.Enqueue(ctx, op1.ID, op1)
	require.NoError(t, err)

	go func() {
		time.Sleep(time.Millisecond)
		window.Close()
	}()

	err = window.Enqueue(ctx, op2.ID, op2)
	require.ErrorIs(t, err, ErrQueueClosed)
}

func TestOpWindowDequeueEmptyQueue(t *testing.T) {
	t.Parallel()
	window := NewWindowQueue(1, 1, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := window.Dequeue(ctx)
	require.ErrorIs(t, err, ctx.Err())
}
