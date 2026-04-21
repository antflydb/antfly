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
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testModelInfo is a minimal model info for testing.
type testModelInfo struct {
	Name string
	Path string
}

// testModel is a closeable model for testing.
type testModel struct {
	name   string
	closed atomic.Bool
}

func (m *testModel) Close() error {
	m.closed.Store(true)
	return nil
}

// newTestRegistry creates a BaseRegistry with test defaults.
func newTestRegistry(t *testing.T, discovered map[string]*testModelInfo, opts ...func(*BaseRegistryConfig[testModelInfo, *testModel])) *BaseRegistry[testModelInfo, *testModel] {
	t.Helper()

	cfg := BaseRegistryConfig[testModelInfo, *testModel]{
		ModelType: "test",
		KeepAlive: 5 * time.Minute,
		NameFunc:  func(info *testModelInfo) string { return info.Name },
		LoadFn: func(info *testModelInfo) (*testModel, error) {
			return &testModel{name: info.Name}, nil
		},
		CloseFn: func(m *testModel) error { return m.Close() },
		DiscoverFn: func() error {
			return nil // no-op discovery for tests
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	r := newBaseRegistry(cfg)
	r.mu.Lock()
	maps.Copy(r.discovered, discovered)
	r.mu.Unlock()

	t.Cleanup(func() { _ = r.close() })

	return r
}

func TestBaseRegistry_AcquireRelease(t *testing.T) {
	r := newTestRegistry(t, map[string]*testModelInfo{
		"model-a": {Name: "model-a", Path: "/models/a"},
	})

	model, err := r.acquire("model-a")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if model.name != "model-a" {
		t.Errorf("got name %q, want %q", model.name, "model-a")
	}

	// Model should be in cache
	if !r.isLoaded("model-a") {
		t.Error("model-a should be loaded after acquire")
	}

	r.release("model-a")
}

func TestBaseRegistry_Get(t *testing.T) {
	r := newTestRegistry(t, map[string]*testModelInfo{
		"model-b": {Name: "model-b", Path: "/models/b"},
	})

	model, err := r.get("model-b")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if model.name != "model-b" {
		t.Errorf("got name %q, want %q", model.name, "model-b")
	}
}

func TestBaseRegistry_DoubleCheckAfterLock(t *testing.T) {
	loadCount := atomic.Int32{}
	r := newTestRegistry(t, map[string]*testModelInfo{
		"model-c": {Name: "model-c", Path: "/models/c"},
	}, func(cfg *BaseRegistryConfig[testModelInfo, *testModel]) {
		cfg.LoadFn = func(info *testModelInfo) (*testModel, error) {
			loadCount.Add(1)
			return &testModel{name: info.Name}, nil
		}
	})

	// Load concurrently
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			_, _ = r.get("model-c")
		})
	}
	wg.Wait()

	if got := loadCount.Load(); got != 1 {
		t.Errorf("loadFn called %d times, want 1 (double-check-after-lock should prevent duplicates)", got)
	}
}

func TestBaseRegistry_VariantResolution(t *testing.T) {
	r := newTestRegistry(t, map[string]*testModelInfo{
		"owner/model-i8": {Name: "owner/model-i8", Path: "/models/i8"},
	})

	// Request without variant suffix should resolve to the variant
	model, err := r.acquire("owner/model")
	if err != nil {
		t.Fatalf("acquire with variant: %v", err)
	}
	if model.name != "owner/model-i8" {
		t.Errorf("got name %q, want %q", model.name, "owner/model-i8")
	}

	r.release("owner/model")
}

func TestBaseRegistry_NotFound(t *testing.T) {
	r := newTestRegistry(t, nil)

	_, err := r.acquire("nonexistent")
	if err == nil {
		t.Fatal("acquire should fail for nonexistent model")
	}
}

func TestBaseRegistry_LoadError_RollsBackRef(t *testing.T) {
	r := newTestRegistry(t, map[string]*testModelInfo{
		"bad-model": {Name: "bad-model", Path: "/models/bad"},
	}, func(cfg *BaseRegistryConfig[testModelInfo, *testModel]) {
		cfg.LoadFn = func(info *testModelInfo) (*testModel, error) {
			return nil, fmt.Errorf("load failed")
		}
	})

	_, err := r.acquire("bad-model")
	if err == nil {
		t.Fatal("acquire should fail when loadFn fails")
	}

	// Ref count should be 0 (rolled back)
	r.refs.mu.Lock()
	count := r.refs.refCounts["bad-model"]
	r.refs.mu.Unlock()
	if count != 0 {
		t.Errorf("refCount = %d, want 0 after failed acquire", count)
	}
}

func TestBaseRegistry_List(t *testing.T) {
	r := newTestRegistry(t, map[string]*testModelInfo{
		"model-x": {Name: "model-x", Path: "/x"},
		"model-y": {Name: "model-y", Path: "/y"},
	})

	names := r.list()
	if len(names) != 2 {
		t.Errorf("list() returned %d names, want 2", len(names))
	}
}

func TestBaseRegistry_Preload(t *testing.T) {
	r := newTestRegistry(t, map[string]*testModelInfo{
		"model-p": {Name: "model-p", Path: "/p"},
	})

	err := r.preload([]string{"model-p"})
	if err != nil {
		t.Fatalf("preload: %v", err)
	}
	if !r.isLoaded("model-p") {
		t.Error("model-p should be loaded after preload")
	}
}

func TestBaseRegistry_Close(t *testing.T) {
	closedModels := sync.Map{}
	discovered := map[string]*testModelInfo{
		"model-1": {Name: "model-1", Path: "/1"},
		"model-2": {Name: "model-2", Path: "/2"},
	}

	cfg := BaseRegistryConfig[testModelInfo, *testModel]{
		ModelType: "test",
		KeepAlive: 5 * time.Minute,
		NameFunc:  func(info *testModelInfo) string { return info.Name },
		LoadFn: func(info *testModelInfo) (*testModel, error) {
			return &testModel{name: info.Name}, nil
		},
		CloseFn: func(m *testModel) error {
			closedModels.Store(m.name, true)
			return nil
		},
		DiscoverFn: func() error { return nil },
	}

	r := newBaseRegistry(cfg)
	r.mu.Lock()
	maps.Copy(r.discovered, discovered)
	r.mu.Unlock()

	// Load both models
	_, _ = r.get("model-1")
	_, _ = r.get("model-2")

	// Close should close both
	_ = r.close()

	if _, ok := closedModels.Load("model-1"); !ok {
		t.Error("model-1 should be closed")
	}
	if _, ok := closedModels.Load("model-2"); !ok {
		t.Error("model-2 should be closed")
	}
}

func TestBaseRegistry_EvictLRU(t *testing.T) {
	r := newTestRegistry(t, map[string]*testModelInfo{
		"old": {Name: "old", Path: "/old"},
		"new": {Name: "new", Path: "/new"},
	})

	// Load old first, then new
	_, _ = r.get("old")
	time.Sleep(10 * time.Millisecond)
	_, _ = r.get("new")

	// EvictLRU should evict "old" (least recently used)
	evicted := r.EvictLRU()
	if evicted != "old" {
		t.Errorf("EvictLRU() = %q, want %q", evicted, "old")
	}
	if r.isLoaded("old") {
		t.Error("old should no longer be loaded")
	}
	if !r.isLoaded("new") {
		t.Error("new should still be loaded")
	}
}

func TestBaseRegistry_EvictLRU_SkipsInUse(t *testing.T) {
	r := newTestRegistry(t, map[string]*testModelInfo{
		"acquired": {Name: "acquired", Path: "/a"},
		"idle":     {Name: "idle", Path: "/i"},
	})

	// Load both, acquire "acquired"
	_, _ = r.acquire("acquired")
	_, _ = r.get("idle")

	evicted := r.EvictLRU()
	// Should evict "idle" (the one not acquired), even though cache order may vary
	if evicted == "acquired" {
		t.Error("EvictLRU should skip in-use model")
	}

	r.release("acquired")
}

func TestBaseRegistry_WithBudget(t *testing.T) {
	budget := NewModelBudget(2, nil)

	r := newTestRegistry(t, map[string]*testModelInfo{
		"m1": {Name: "m1", Path: "/1"},
		"m2": {Name: "m2", Path: "/2"},
		"m3": {Name: "m3", Path: "/3"},
	}, func(cfg *BaseRegistryConfig[testModelInfo, *testModel]) {
		cfg.Budget = budget
	})

	// Load 2 models (budget full)
	_, _ = r.get("m1")
	_, _ = r.get("m2")

	if budget.Count() != 2 {
		t.Errorf("budget.Count() = %d, want 2", budget.Count())
	}

	// Third model should trigger eviction of LRU
	_, err := r.get("m3")
	if err != nil {
		t.Fatalf("get m3 should succeed via LRU eviction: %v", err)
	}

	// Budget should still be at 2 (one evicted, one added)
	if budget.Count() != 2 {
		t.Errorf("budget.Count() = %d, want 2 after eviction", budget.Count())
	}
}
