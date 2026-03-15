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

// Package workerpool provides a shared goroutine pool with errgroup-like
// semantics. It wraps ants/v2 for goroutine reuse and bounded parallelism,
// and adds Group (error propagation + context cancellation) and ResultGroup
// (ordered result collection) on top.
package workerpool

import (
	"runtime"

	"github.com/panjf2000/ants/v2"
)

// Pool wraps an ants.Pool providing bounded, reusable goroutine scheduling.
type Pool struct {
	inner *ants.Pool
}

// PoolOption configures a Pool.
type PoolOption func(*poolConfig)

type poolConfig struct {
	size     int
	antsOpts []ants.Option
}

// WithSize sets the pool size. Default: runtime.NumCPU() * 4.
func WithSize(n int) PoolOption {
	return func(c *poolConfig) { c.size = n }
}

// WithAntsOptions passes additional ants.Option values to the underlying pool.
func WithAntsOptions(opts ...ants.Option) PoolOption {
	return func(c *poolConfig) { c.antsOpts = append(c.antsOpts, opts...) }
}

// NewPool creates a Pool. The caller must call Close when done.
func NewPool(opts ...PoolOption) (*Pool, error) {
	cfg := poolConfig{size: runtime.NumCPU() * 4}
	for _, o := range opts {
		o(&cfg)
	}
	// Blocking submit: callers block when pool is saturated (backpressure).
	antsOpts := append([]ants.Option{ants.WithNonblocking(false)}, cfg.antsOpts...)
	inner, err := ants.NewPool(cfg.size, antsOpts...)
	if err != nil {
		return nil, err
	}
	return &Pool{inner: inner}, nil
}

// Close releases pool resources. Blocks until running workers finish.
func (p *Pool) Close() { p.inner.Release() }

// Cap returns the pool capacity.
func (p *Pool) Cap() int { return p.inner.Cap() }

// Running returns the number of goroutines currently executing tasks.
func (p *Pool) Running() int { return p.inner.Running() }

// submit schedules f on a pooled goroutine. It blocks if the pool is full.
func (p *Pool) submit(f func()) error { return p.inner.Submit(f) }
