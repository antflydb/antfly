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

// ResultGroup collects ordered results from parallel tasks run on a Pool.
// Each Go call is assigned an index; results are returned in submission order.
// On error, Wait returns partial results (failed slots hold the zero value of T)
// and the first error.
type ResultGroup[T any] struct {
	group   *Group
	mu      sync.Mutex
	results []T
	n       int
}

// NewResultGroup returns a ResultGroup bound to pool and a derived context.
func NewResultGroup[T any](ctx context.Context, pool *Pool) (*ResultGroup[T], context.Context) {
	g, ctx := NewGroup(ctx, pool)
	return &ResultGroup[T]{group: g}, ctx
}

// Go submits f to the pool. The result is stored at the submission index.
func (rg *ResultGroup[T]) Go(f func(ctx context.Context) (T, error)) {
	rg.mu.Lock()
	idx := rg.n
	rg.n++
	// Pre-grow results slice so the index is valid when the worker runs.
	for len(rg.results) < rg.n {
		var zero T
		rg.results = append(rg.results, zero)
	}
	rg.mu.Unlock()

	rg.group.Go(func(ctx context.Context) error {
		val, err := f(ctx)
		if err != nil {
			return err
		}
		// Lock required: a concurrent Go() call may append/reallocate rg.results,
		// so we must synchronize the read of the slice header.
		rg.mu.Lock()
		rg.results[idx] = val
		rg.mu.Unlock()
		return nil
	})
}

// Wait blocks until all tasks complete. Returns results in submission order and
// the first error (if any).
func (rg *ResultGroup[T]) Wait() ([]T, error) {
	err := rg.group.Wait()
	return rg.results, err
}
