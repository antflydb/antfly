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
	"fmt"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// LazyPool manages a pool of lazily-initialized items with semaphore-based
// concurrency control and round-robin selection.
//
// Slot 0 is always initialized eagerly by New to validate the factory.
// Slots 1..N-1 are created on first access via Acquire.
type LazyPool[T any] struct {
	items   []T
	ready   []atomic.Bool
	initMu  []sync.Mutex // per-slot mutex for lazy init
	sem     *semaphore.Weighted
	next    atomic.Uint64
	size    int
	factory func() (T, error)
	closeFn func(T) error
	logger  *zap.Logger
}

// Config holds parameters for creating a LazyPool.
type Config[T any] struct {
	// Size is the maximum number of concurrent items (must be >= 1).
	Size int

	// Factory creates a new item. Called at most Size times.
	Factory func() (T, error)

	// Close releases an item's resources. Called once per initialized item
	// during pool shutdown.
	Close func(T) error

	// Logger for logging (nil = no logging).
	Logger *zap.Logger
}

// New creates a LazyPool and eagerly initializes slot 0. Returns the pool
// and the first item (useful for inspecting backend type, capabilities, etc.).
func New[T any](cfg Config[T]) (*LazyPool[T], T, error) {
	if cfg.Size < 1 {
		var zero T
		return nil, zero, fmt.Errorf("pool size must be >= 1, got %d", cfg.Size)
	}
	if cfg.Close == nil {
		var zero T
		return nil, zero, fmt.Errorf("pool Close function must not be nil")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	p := &LazyPool[T]{
		items:   make([]T, cfg.Size),
		ready:   make([]atomic.Bool, cfg.Size),
		initMu:  make([]sync.Mutex, cfg.Size),
		sem:     semaphore.NewWeighted(int64(cfg.Size)),
		size:    cfg.Size,
		factory: cfg.Factory,
		closeFn: cfg.Close,
		logger:  logger,
	}

	// Eagerly initialize slot 0 to validate the factory.
	first, err := cfg.Factory()
	if err != nil {
		var zero T
		return nil, zero, fmt.Errorf("creating initial pool item: %w", err)
	}
	p.items[0] = first
	p.ready[0].Store(true)

	if cfg.Size > 1 {
		logger.Debug("Created lazy pool", zap.Int("size", cfg.Size))
	}

	return p, first, nil
}

// Acquire acquires a pool slot and returns the item at that slot. If the slot
// hasn't been initialized yet, calls Factory to create it. Blocks if all slots
// are in use. The caller MUST call Release when done.
func (p *LazyPool[T]) Acquire(ctx context.Context) (T, int, error) {
	if err := p.sem.Acquire(ctx, 1); err != nil {
		var zero T
		return zero, 0, fmt.Errorf("acquiring pool slot: %w", err)
	}

	idx := int(p.next.Add(1) % uint64(p.size))

	if p.ready[idx].Load() {
		return p.items[idx], idx, nil
	}

	// Lazy init with mutex for retry safety (don't cache failures).
	p.initMu[idx].Lock()
	if p.ready[idx].Load() {
		p.initMu[idx].Unlock()
		return p.items[idx], idx, nil
	}
	item, err := p.factory()
	if err != nil {
		p.initMu[idx].Unlock()
		p.sem.Release(1)
		var zero T
		return zero, 0, fmt.Errorf("lazy-initializing pool slot %d: %w", idx, err)
	}
	p.items[idx] = item
	p.ready[idx].Store(true)
	p.initMu[idx].Unlock()

	p.logger.Debug("Lazily initialized pool slot", zap.Int("slot", idx))
	return item, idx, nil
}

// Release releases a previously acquired pool slot.
func (p *LazyPool[T]) Release() {
	p.sem.Release(1)
}

// First returns slot 0 (always initialized). Useful for inspecting
// capabilities without acquiring.
func (p *LazyPool[T]) First() T {
	return p.items[0]
}

// ForEachInitialized calls fn for each initialized slot.
// Does not initialize uninitialized slots.
func (p *LazyPool[T]) ForEachInitialized(fn func(T)) {
	for i := 0; i < p.size; i++ {
		if p.ready[i].Load() {
			fn(p.items[i])
		}
	}
}

// InitAll forces all slots to be initialized. Returns on first failure.
func (p *LazyPool[T]) InitAll() error {
	for i := 0; i < p.size; i++ {
		if p.ready[i].Load() {
			continue
		}
		p.initMu[i].Lock()
		if p.ready[i].Load() {
			p.initMu[i].Unlock()
			continue
		}
		item, err := p.factory()
		if err != nil {
			p.initMu[i].Unlock()
			return fmt.Errorf("initializing pool slot %d: %w", i, err)
		}
		p.items[i] = item
		p.ready[i].Store(true)
		p.initMu[i].Unlock()
	}
	return nil
}

// Size returns the pool size.
func (p *LazyPool[T]) Size() int {
	return p.size
}

// Close closes all initialized items. Not safe for concurrent use with Acquire.
func (p *LazyPool[T]) Close() error {
	var errs []error
	for i := 0; i < p.size; i++ {
		if p.ready[i].Load() {
			if err := p.closeFn(p.items[i]); err != nil {
				errs = append(errs, fmt.Errorf("closing pool slot %d: %w", i, err))
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing pool: %v", errs)
	}
	return nil
}
