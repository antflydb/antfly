// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockPipeline struct {
	id     int
	closed atomic.Bool
}

func TestNew_EagerSlot0(t *testing.T) {
	var created atomic.Int32
	p, first, err := New(Config[*mockPipeline]{
		Size: 4,
		Factory: func() (*mockPipeline, error) {
			id := int(created.Add(1))
			return &mockPipeline{id: id}, nil
		},
		Close: func(m *mockPipeline) error { m.closed.Store(true); return nil },
	})
	require.NoError(t, err)
	require.NotNil(t, p)
	assert.Equal(t, 1, first.id)
	// Only slot 0 should be created eagerly.
	assert.Equal(t, int32(1), created.Load())
	_ = p.Close()
}

func TestNew_FactoryError(t *testing.T) {
	_, _, err := New(Config[*mockPipeline]{
		Size:    2,
		Factory: func() (*mockPipeline, error) { return nil, errors.New("broken") },
		Close:   func(m *mockPipeline) error { return nil },
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating initial pool item")
}

func TestNew_InvalidSize(t *testing.T) {
	_, _, err := New(Config[*mockPipeline]{
		Size:    0,
		Factory: func() (*mockPipeline, error) { return &mockPipeline{}, nil },
		Close:   func(m *mockPipeline) error { return nil },
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pool size must be >= 1")
}

func TestAcquire_LazyInit(t *testing.T) {
	var created atomic.Int32
	p, _, err := New(Config[*mockPipeline]{
		Size: 3,
		Factory: func() (*mockPipeline, error) {
			id := int(created.Add(1))
			return &mockPipeline{id: id}, nil
		},
		Close: func(m *mockPipeline) error { m.closed.Store(true); return nil },
	})
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	ctx := context.Background()

	// Acquire 3 slots — slots 1 and 2 should be lazily created.
	items := make([]*mockPipeline, 3)
	for i := range 3 {
		item, _, err := p.Acquire(ctx)
		require.NoError(t, err)
		items[i] = item
	}

	// All 3 slots should now be initialized.
	assert.Equal(t, int32(3), created.Load())

	for range 3 {
		p.Release()
	}
}

func TestAcquire_FactoryRetryOnError(t *testing.T) {
	var calls atomic.Int32
	p, _, err := New(Config[*mockPipeline]{
		Size: 2,
		Factory: func() (*mockPipeline, error) {
			n := calls.Add(1)
			if n == 2 {
				// Second call (lazy init of slot 1) fails.
				return nil, errors.New("transient failure")
			}
			return &mockPipeline{id: int(n)}, nil
		},
		Close: func(m *mockPipeline) error { m.closed.Store(true); return nil },
	})
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	ctx := context.Background()

	// Round-robin with Add(1)%2: first=1%2=1, second=2%2=0, third=3%2=1, ...
	// First acquire hits slot 1 (not yet initialized) — factory call #2 fails.
	_, _, err = p.Acquire(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transient failure")

	// Second acquire hits slot 0 (already initialized) — succeeds.
	item, _, err := p.Acquire(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, item.id)
	p.Release()

	// Third acquire hits slot 1 again — retry factory, call #3 succeeds.
	item, _, err = p.Acquire(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, item.id)
	p.Release()
}

func TestFirst(t *testing.T) {
	p, first, err := New(Config[*mockPipeline]{
		Size:    2,
		Factory: func() (*mockPipeline, error) { return &mockPipeline{id: 42}, nil },
		Close:   func(m *mockPipeline) error { return nil },
	})
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	assert.Equal(t, first, p.First())
	assert.Equal(t, 42, p.First().id)
}

func TestClose_OnlyInitialized(t *testing.T) {
	var created atomic.Int32
	p, _, err := New(Config[*mockPipeline]{
		Size: 4,
		Factory: func() (*mockPipeline, error) {
			id := int(created.Add(1))
			return &mockPipeline{id: id}, nil
		},
		Close: func(m *mockPipeline) error { m.closed.Store(true); return nil },
	})
	require.NoError(t, err)

	// Only slot 0 is initialized (created=1). Acquire once to init slot 1.
	// Round-robin: Add(1)%4=1 → slot 1 (lazy init, created=2).
	ctx := context.Background()
	item1, idx1, err := p.Acquire(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, idx1)
	p.Release()

	// Close should only close slots 0 and 1.
	err = p.Close()
	require.NoError(t, err)

	assert.True(t, p.items[0].closed.Load())
	assert.True(t, item1.closed.Load())
	// Slots 2 and 3 were never initialized.
	assert.Equal(t, int32(2), created.Load())
}

func TestInitAll(t *testing.T) {
	var created atomic.Int32
	p, _, err := New(Config[*mockPipeline]{
		Size: 3,
		Factory: func() (*mockPipeline, error) {
			id := int(created.Add(1))
			return &mockPipeline{id: id}, nil
		},
		Close: func(m *mockPipeline) error { return nil },
	})
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	assert.Equal(t, int32(1), created.Load())

	err = p.InitAll()
	require.NoError(t, err)
	assert.Equal(t, int32(3), created.Load())

	// Calling InitAll again should be a no-op.
	err = p.InitAll()
	require.NoError(t, err)
	assert.Equal(t, int32(3), created.Load())
}

func TestForEachInitialized(t *testing.T) {
	p, _, err := New(Config[*mockPipeline]{
		Size:    4,
		Factory: func() (*mockPipeline, error) { return &mockPipeline{}, nil },
		Close:   func(m *mockPipeline) error { return nil },
	})
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	// Only slot 0 initialized.
	var count int
	p.ForEachInitialized(func(_ *mockPipeline) { count++ })
	assert.Equal(t, 1, count)

	// Init all, then check again.
	_ = p.InitAll()
	count = 0
	p.ForEachInitialized(func(_ *mockPipeline) { count++ })
	assert.Equal(t, 4, count)
}

func TestAcquire_ContextCancellation(t *testing.T) {
	p, _, err := New(Config[*mockPipeline]{
		Size:    1,
		Factory: func() (*mockPipeline, error) { return &mockPipeline{id: 1}, nil },
		Close:   func(m *mockPipeline) error { return nil },
	})
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	// Acquire the only slot.
	_, _, err = p.Acquire(context.Background())
	require.NoError(t, err)

	// A second acquire with an already-canceled context should fail immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err = p.Acquire(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "acquiring pool slot")

	p.Release()
}

func TestSize1_NoLazyInit(t *testing.T) {
	var created atomic.Int32
	p, _, err := New(Config[*mockPipeline]{
		Size: 1,
		Factory: func() (*mockPipeline, error) {
			id := int(created.Add(1))
			return &mockPipeline{id: id}, nil
		},
		Close: func(m *mockPipeline) error { return nil },
	})
	require.NoError(t, err)
	defer func() { _ = p.Close() }()

	ctx := context.Background()
	item, idx, err := p.Acquire(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, idx)
	assert.Equal(t, 1, item.id)
	p.Release()

	// Only 1 item ever created.
	assert.Equal(t, int32(1), created.Load())
}
