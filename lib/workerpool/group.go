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

package workerpool

import (
	"context"
	"sync"
)

// Group provides errgroup-like semantics over a shared Pool.
// On first error the derived context is canceled and Wait returns that error.
type Group struct {
	pool   *Pool
	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup

	errOnce sync.Once
	err     error
}

// NewGroup returns a Group bound to pool and a derived context. The context is
// canceled when the first task returns a non-nil error or when Wait returns.
func NewGroup(ctx context.Context, pool *Pool) (*Group, context.Context) {
	ctx, cancel := context.WithCancelCause(ctx)
	g := &Group{pool: pool, ctx: ctx, cancel: cancel}
	return g, ctx
}

// Go submits f to the pool. If the group context is already canceled the call
// is a no-op. If pool submission fails the error is recorded.
func (g *Group) Go(f func(ctx context.Context) error) {
	if g.ctx.Err() != nil {
		return
	}
	g.wg.Add(1)
	if err := g.pool.submit(func() {
		defer g.wg.Done()
		if err := f(g.ctx); err != nil {
			g.errOnce.Do(func() {
				g.err = err
				g.cancel(err)
			})
		}
	}); err != nil {
		// Pool submission failed (e.g. pool closed).
		g.wg.Done()
		g.errOnce.Do(func() {
			g.err = err
			g.cancel(err)
		})
	}
}

// Wait blocks until all submitted tasks complete and returns the first error.
func (g *Group) Wait() error {
	g.wg.Wait()
	g.cancel(nil)
	return g.err
}
