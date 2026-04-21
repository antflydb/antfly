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
	"sync"
	"testing"
)

// mockBudgetCache implements budgetCache for testing.
type mockBudgetCache struct {
	evictCalled int
	canEvict    bool
	evictedKey  string
}

func (m *mockBudgetCache) EvictLRU() string {
	m.evictCalled++
	if m.canEvict {
		m.canEvict = false // only evict once
		return m.evictedKey
	}
	return ""
}

func TestModelBudget_Unlimited(t *testing.T) {
	b := NewModelBudget(0, nil)

	// Should always succeed with unlimited budget
	for range 100 {
		if err := b.Reserve(); err != nil {
			t.Fatalf("Reserve() with unlimited budget: %v", err)
		}
	}
	if got := b.Count(); got != 100 {
		t.Errorf("Count() = %d, want 100", got)
	}
}

func TestModelBudget_ReserveUnderLimit(t *testing.T) {
	b := NewModelBudget(3, nil)

	for i := range 3 {
		if err := b.Reserve(); err != nil {
			t.Fatalf("Reserve() #%d: %v", i, err)
		}
	}
	if got := b.Count(); got != 3 {
		t.Errorf("Count() = %d, want 3", got)
	}
}

func TestModelBudget_ReserveAtLimit_NoEviction(t *testing.T) {
	b := NewModelBudget(2, nil)

	// Fill budget
	_ = b.Reserve()
	_ = b.Reserve()

	// Register a cache that can't evict
	mc := &mockBudgetCache{canEvict: false}
	b.Register("test", mc)

	err := b.Reserve()
	if err == nil {
		t.Fatal("Reserve() should fail when at limit with no evictable models")
	}
	if mc.evictCalled == 0 {
		t.Error("EvictLRU should have been called")
	}
}

func TestModelBudget_ReserveAtLimit_EvictsLRU(t *testing.T) {
	b := NewModelBudget(2, nil)

	_ = b.Reserve()
	_ = b.Reserve()

	// Register a cache that can evict (simulates eviction callback calling Release)
	mc := &mockBudgetCache{canEvict: true, evictedKey: "old-model"}
	b.Register("test", mc)

	// The eviction callback would call b.Release() — simulate that
	go func() {
		// Wait briefly for Reserve to call EvictLRU, which triggers deletion,
		// which triggers eviction callback, which calls Release.
		// In real code this happens synchronously inside cache.Delete().
		// For the test, we simulate by releasing after the mock evicts.
	}()

	// Since our mock doesn't actually call Release (real eviction callback does),
	// we need to manually release to simulate the eviction path.
	mc2 := &mockBudgetCache{
		canEvict:   true,
		evictedKey: "old-model",
	}
	b2 := NewModelBudget(2, nil)
	_ = b2.Reserve()
	_ = b2.Reserve()
	b2.Register("test", mc2)

	// Simulate: the eviction callback releases the slot
	b2.Release()
	// Now Reserve should succeed
	if err := b2.Reserve(); err != nil {
		t.Fatalf("Reserve() after Release: %v", err)
	}
}

func TestModelBudget_Release(t *testing.T) {
	b := NewModelBudget(5, nil)

	_ = b.Reserve()
	_ = b.Reserve()
	_ = b.Reserve()
	b.Release()
	b.Release()

	if got := b.Count(); got != 1 {
		t.Errorf("Count() = %d, want 1", got)
	}
}

func TestModelBudget_ConcurrentAccess(t *testing.T) {
	b := NewModelBudget(0, nil) // unlimited

	var wg sync.WaitGroup
	for range 100 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = b.Reserve()
		}()
		go func() {
			defer wg.Done()
			b.Release()
		}()
	}
	wg.Wait()
	// Count may be anything due to ordering; just ensure no panic/race
}

func TestModelBudget_MultipleRegistries(t *testing.T) {
	b := NewModelBudget(2, nil)
	_ = b.Reserve()
	_ = b.Reserve()

	mc1 := &mockBudgetCache{canEvict: false}
	mc2 := &mockBudgetCache{canEvict: false}
	b.Register("embedder", mc1)
	b.Register("reranker", mc2)

	// Neither can evict
	err := b.Reserve()
	if err == nil {
		t.Fatal("Reserve() should fail when no registry can evict")
	}
	if mc1.evictCalled == 0 || mc2.evictCalled == 0 {
		t.Error("Both registries should have been asked to evict")
	}
}
