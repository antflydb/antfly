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

package termite

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antflydb/antfly/pkg/libaf/reranking"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// --- Concurrency regression tests (Bug 4: evictedHandles orphan tracking) ---
// Proven by TLA+ formal verification; these tests verify the fix properties hold at runtime.

// closeTrackingReranker implements reranking.Model and tracks Close() calls.
type closeTrackingReranker struct {
	closed atomic.Bool
}

var _ reranking.Model = (*closeTrackingReranker)(nil)

func (m *closeTrackingReranker) Rerank(_ context.Context, _ string, docs []string) ([]float32, error) {
	return make([]float32, len(docs)), nil
}

func (m *closeTrackingReranker) Close() error {
	m.closed.Store(true)
	return nil
}

// TestRegistryEvictionRespectsRefcount verifies that the eviction callback
// does NOT close a model when refcount > 0, and instead tracks it as an
// orphan in evictedHandles for deferred cleanup.
func TestRegistryEvictionRespectsRefcount(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewRerankerRegistry(RerankerConfig{
		ModelsDir: dir,
		KeepAlive: 20 * time.Millisecond,
	}, nil, nil, zap.NewNop())
	require.NoError(t, err)
	defer func() { _ = reg.Close() }()

	mock := &closeTrackingReranker{}

	// Simulate an active acquire (refcount > 0)
	reg.base.refs.incRef("test")

	// Add model to cache — TTL eviction will fire after keepAlive
	reg.base.cache.Set("test", mock, reg.base.keepAlive)

	// Wait for TTL eviction
	time.Sleep(200 * time.Millisecond)

	// With the fix: callback sees refcount=1 > 0, must NOT close
	assert.False(t, mock.closed.Load(),
		"model must not be closed while refcount > 0")

	// Evicted model must be tracked in evictedHandles
	reg.base.refs.mu.Lock()
	orphanCount := len(reg.base.refs.evictedHandles["test"])
	reg.base.refs.mu.Unlock()
	assert.Positive(t, orphanCount,
		"evicted model must be tracked in evictedHandles")

	// Release — should close orphaned handle
	reg.Release("test")

	assert.True(t, mock.closed.Load(),
		"Release must close orphaned model when refcount hits 0")
}

// TestRegistryOrphanCleanup verifies that Release() properly cleans up
// multiple orphaned handles when refcount hits 0.
func TestRegistryOrphanCleanup(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewRerankerRegistry(RerankerConfig{
		ModelsDir: dir,
		KeepAlive: 10 * time.Millisecond,
	}, nil, nil, zap.NewNop())
	require.NoError(t, err)
	defer func() { _ = reg.Close() }()

	const evictions = 3
	mocks := make([]*closeTrackingReranker, evictions)
	for i := range mocks {
		mocks[i] = &closeTrackingReranker{}
	}

	// Simulate an active acquire (refcount > 0)
	reg.base.refs.incRef("test")

	// Simulate multiple evictions: each adds a model to cache, waits for eviction
	for i := range evictions {
		reg.base.cache.Set("test", mocks[i], reg.base.keepAlive)
		time.Sleep(50 * time.Millisecond)
	}

	// All models should still be alive (refcount > 0 prevents closing)
	for i, mock := range mocks {
		assert.False(t, mock.closed.Load(),
			"model %d must not be closed while refcount > 0", i)
	}

	// All should be tracked as orphans
	reg.base.refs.mu.Lock()
	orphanCount := len(reg.base.refs.evictedHandles["test"])
	reg.base.refs.mu.Unlock()
	assert.Equal(t, evictions, orphanCount,
		"all evicted models must be tracked in evictedHandles")

	// Release — should close all orphans
	reg.Release("test")

	for i, mock := range mocks {
		assert.True(t, mock.closed.Load(),
			"orphaned model %d must be closed after Release", i)
	}

	// evictedHandles should be empty
	reg.base.refs.mu.Lock()
	remaining := len(reg.base.refs.evictedHandles["test"])
	reg.base.refs.mu.Unlock()
	assert.Equal(t, 0, remaining,
		"evictedHandles must be empty after Release cleanup")
}
