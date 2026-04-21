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
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefTracker_IncRefRelease(t *testing.T) {
	rt := newRefTracker()

	// incRef returns the new count
	assert.Equal(t, 1, rt.incRef("a"))
	assert.Equal(t, 2, rt.incRef("a"))
	assert.Equal(t, 1, rt.incRef("b"))

	// releaseRef decrements and returns count
	count, orphans := rt.releaseRef("a")
	assert.Equal(t, 1, count)
	assert.Nil(t, orphans)

	count, orphans = rt.releaseRef("a")
	assert.Equal(t, 0, count)
	assert.Nil(t, orphans)

	// Map entry should be cleaned up when count reaches 0
	rt.mu.Lock()
	_, exists := rt.refCounts["a"]
	rt.mu.Unlock()
	assert.False(t, exists, "refCounts entry must be deleted when count reaches 0")
}

func TestRefTracker_RollbackRef(t *testing.T) {
	rt := newRefTracker()

	rt.incRef("a")
	rt.incRef("a")

	rt.rollbackRef("a")
	rt.mu.Lock()
	assert.Equal(t, 1, rt.refCounts["a"])
	rt.mu.Unlock()

	rt.rollbackRef("a")
	rt.mu.Lock()
	_, exists := rt.refCounts["a"]
	rt.mu.Unlock()
	assert.False(t, exists, "rollbackRef must delete map entry when count reaches 0")
}

func TestRefTracker_SpuriousReleaseIsNoOp(t *testing.T) {
	rt := newRefTracker()

	// Add an orphan for key "a" without any matching incRef
	rt.mu.Lock()
	rt.evictedHandles["a"] = []func() error{func() error { return nil }}
	rt.mu.Unlock()

	// Spurious release (no matching Acquire) should NOT drain orphans
	count, orphans := rt.releaseRef("a")
	assert.Equal(t, 0, count)
	assert.Nil(t, orphans, "spurious Release must not drain orphans")

	// Orphans should still be there
	rt.mu.Lock()
	assert.Len(t, rt.evictedHandles["a"], 1, "orphans must not be drained by spurious Release")
	rt.mu.Unlock()
}

func TestRefTracker_DeferCloseIfInUse(t *testing.T) {
	rt := newRefTracker()

	closeCalled := false
	closeFn := func() error {
		closeCalled = true
		return nil
	}

	// No active refs — should return false (caller should close immediately)
	deferred := rt.deferCloseIfInUse("a", closeFn)
	assert.False(t, deferred)
	assert.False(t, closeCalled)

	// With active ref — should return true (deferred)
	rt.incRef("a")
	deferred = rt.deferCloseIfInUse("a", closeFn)
	assert.True(t, deferred)
	assert.False(t, closeCalled, "closeFn must not be called yet")

	// Release should return the deferred close function
	count, orphans := rt.releaseRef("a")
	assert.Equal(t, 0, count)
	require.Len(t, orphans, 1)

	// Execute the orphan
	err := orphans[0]()
	assert.NoError(t, err)
	assert.True(t, closeCalled)
}

func TestRefTracker_MultipleOrphans(t *testing.T) {
	rt := newRefTracker()
	rt.incRef("a")

	var closed []int
	for i := range 3 {
		rt.deferCloseIfInUse("a", func() error {
			closed = append(closed, i)
			return nil
		})
	}

	count, orphans := rt.releaseRef("a")
	assert.Equal(t, 0, count)
	assert.Len(t, orphans, 3)

	for _, fn := range orphans {
		_ = fn()
	}
	assert.Equal(t, []int{0, 1, 2}, closed)
}

func TestRefTracker_DrainOrphans(t *testing.T) {
	rt := newRefTracker()

	// Add orphans directly (simulating eviction during shutdown)
	rt.mu.Lock()
	rt.evictedHandles["a"] = []func() error{
		func() error { return nil },
		func() error { return errors.New("close error") },
	}
	rt.evictedHandles["b"] = []func() error{
		func() error { return nil },
	}
	rt.mu.Unlock()

	errs := rt.drainOrphans()
	assert.Len(t, errs["a"], 1, "should report one error for key a")
	assert.Empty(t, errs["b"], "should have no errors for key b")

	// evictedHandles should be empty after drain
	rt.mu.Lock()
	assert.Empty(t, rt.evictedHandles)
	rt.mu.Unlock()
}

func TestRefTracker_ConcurrentAcquireRelease(t *testing.T) {
	rt := newRefTracker()
	const goroutines = 100

	// Phase 1: all goroutines incRef concurrently
	var wgAcquire sync.WaitGroup
	wgAcquire.Add(goroutines)
	for range goroutines {
		go func() {
			defer wgAcquire.Done()
			rt.incRef("key")
		}()
	}
	wgAcquire.Wait()

	// Phase 2: all goroutines releaseRef concurrently
	var wgRelease sync.WaitGroup
	wgRelease.Add(goroutines)
	for range goroutines {
		go func() {
			defer wgRelease.Done()
			rt.releaseRef("key")
		}()
	}
	wgRelease.Wait()

	// Final state: refcount should be 0 and map entry cleaned up
	rt.mu.Lock()
	_, exists := rt.refCounts["key"]
	_, hasOrphans := rt.evictedHandles["key"]
	rt.mu.Unlock()
	assert.False(t, exists, "refCounts entry should be cleaned up")
	assert.False(t, hasOrphans, "no orphans should exist")
}
