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
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

// budgetCache is implemented by each BaseRegistry to allow the ModelBudget
// to find and evict the globally least-recently-used model.
type budgetCache interface {
	// EvictLRU finds the least-recently-used non-in-use item and deletes it
	// from the cache. Returns the evicted key, or "" if nothing could be evicted.
	// The eviction callback handles Close + budget.Release.
	EvictLRU() string
}

// ModelBudget tracks the total number of loaded models across all registries
// and coordinates cross-registry LRU eviction when the global limit is reached.
//
// Thread-safety: current uses atomic operations so Release() is lock-free
// (safe to call from eviction callbacks without deadlock). evictMu serializes
// eviction attempts to prevent thundering-herd eviction races.
type ModelBudget struct {
	current   atomic.Int64
	maxGlobal int64 // 0 = unlimited

	evictMu sync.Mutex // serializes eviction attempts
	caches  []namedCache
	logger  *zap.Logger
}

type namedCache struct {
	name  string
	cache budgetCache
}

// NewModelBudget creates a new cross-registry model budget.
// maxGlobal of 0 means unlimited (no eviction coordination).
func NewModelBudget(maxGlobal uint64, logger *zap.Logger) *ModelBudget {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &ModelBudget{
		maxGlobal: int64(maxGlobal),
		logger:    logger,
	}
}

// Register adds a cache to the budget's eviction pool.
// Must be called during initialization, before any Reserve() calls.
func (b *ModelBudget) Register(name string, cache budgetCache) {
	b.caches = append(b.caches, namedCache{name: name, cache: cache})
}

// Reserve attempts to reserve a slot for loading a new model.
// If the global limit is reached, it tries to evict the LRU model from any
// registry. Returns an error only if all models are in use or pinned.
func (b *ModelBudget) Reserve() error {
	if b.maxGlobal <= 0 {
		b.current.Add(1)
		return nil
	}

	for {
		cur := b.current.Load()
		if cur < b.maxGlobal {
			if b.current.CompareAndSwap(cur, cur+1) {
				return nil
			}
			continue // CAS contention, retry
		}

		// At limit — try to evict
		b.evictMu.Lock()
		// Re-check: another goroutine may have freed a slot while we waited
		if b.current.Load() < b.maxGlobal {
			b.evictMu.Unlock()
			continue
		}

		evicted := false
		for _, nc := range b.caches {
			if key := nc.cache.EvictLRU(); key != "" {
				b.logger.Info("Budget-driven cross-registry eviction",
					zap.String("registry", nc.name),
					zap.String("model", key),
					zap.Int64("budget_current", b.current.Load()),
					zap.Int64("budget_max", b.maxGlobal))
				evicted = true
				break
			}
		}
		b.evictMu.Unlock()

		if !evicted {
			return fmt.Errorf("model budget exhausted (%d/%d loaded, all models in use or pinned)", cur, b.maxGlobal)
		}
		// Eviction freed a slot via Release(), retry CAS
	}
}

// Release frees a slot in the budget. Lock-free via atomic decrement,
// safe to call from eviction callbacks.
func (b *ModelBudget) Release() {
	b.current.Add(-1)
}

// Count returns the current number of loaded models across all registries.
func (b *ModelBudget) Count() int {
	return int(b.current.Load())
}
