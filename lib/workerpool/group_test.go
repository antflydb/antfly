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
	"errors"
	"sync/atomic"
	"testing"
)

func testPool(t *testing.T) *Pool {
	t.Helper()
	p, err := NewPool(WithSize(4))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestGroupSuccess(t *testing.T) {
	pool := testPool(t)
	g, _ := NewGroup(context.Background(), pool)

	var sum atomic.Int64
	for i := range 10 {
		g.Go(func(_ context.Context) error {
			sum.Add(int64(i))
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if got := sum.Load(); got != 45 {
		t.Errorf("sum = %d, want 45", got)
	}
}

func TestGroupFirstError(t *testing.T) {
	pool := testPool(t)
	g, _ := NewGroup(context.Background(), pool)

	sentinel := errors.New("boom")
	g.Go(func(_ context.Context) error { return sentinel })
	g.Go(func(_ context.Context) error { return nil })

	if err := g.Wait(); !errors.Is(err, sentinel) {
		t.Fatalf("Wait = %v, want %v", err, sentinel)
	}
}

func TestGroupContextCanceled(t *testing.T) {
	pool := testPool(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	g, gctx := NewGroup(ctx, pool)

	called := false
	g.Go(func(_ context.Context) error {
		called = true
		return nil
	})

	_ = g.Wait()
	if gctx.Err() == nil {
		t.Error("group context should be canceled")
	}
	// The task may or may not run (race with pre-cancel); that's fine.
	_ = called
}

func TestGroupCancelsOnError(t *testing.T) {
	pool := testPool(t)
	g, gctx := NewGroup(context.Background(), pool)

	sentinel := errors.New("fail")
	g.Go(func(_ context.Context) error { return sentinel })

	_ = g.Wait()
	if gctx.Err() == nil {
		t.Error("group context should be canceled after error")
	}
}
