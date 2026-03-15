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
	"fmt"
	"testing"
)

func TestResultGroupOrdered(t *testing.T) {
	pool := testPool(t)
	rg, _ := NewResultGroup[int](context.Background(), pool)

	for i := range 10 {
		rg.Go(func(_ context.Context) (int, error) {
			return i * 2, nil
		})
	}

	results, err := rg.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if len(results) != 10 {
		t.Fatalf("len(results) = %d, want 10", len(results))
	}
	for i, v := range results {
		if want := i * 2; v != want {
			t.Errorf("results[%d] = %d, want %d", i, v, want)
		}
	}
}

func TestResultGroupError(t *testing.T) {
	pool := testPool(t)
	rg, _ := NewResultGroup[string](context.Background(), pool)

	sentinel := errors.New("fail")
	rg.Go(func(_ context.Context) (string, error) { return "ok", nil })
	rg.Go(func(_ context.Context) (string, error) { return "", sentinel })
	rg.Go(func(_ context.Context) (string, error) { return "also ok", nil })

	results, err := rg.Wait()
	if !errors.Is(err, sentinel) {
		t.Fatalf("Wait error = %v, want %v", err, sentinel)
	}
	// Partial results: slot 0 should have "ok", slot 1 zero value.
	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}
	if results[0] != "ok" {
		t.Errorf("results[0] = %q, want %q", results[0], "ok")
	}
	if results[1] != "" {
		t.Errorf("results[1] = %q, want zero value", results[1])
	}
}

func TestResultGroupSliceType(t *testing.T) {
	pool := testPool(t)
	rg, _ := NewResultGroup[[]int](context.Background(), pool)

	for i := range 5 {
		rg.Go(func(_ context.Context) ([]int, error) {
			return []int{i, i + 1}, nil
		})
	}

	results, err := rg.Wait()
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	for i, v := range results {
		want := fmt.Sprintf("[%d %d]", i, i+1)
		got := fmt.Sprintf("%v", v)
		if got != want {
			t.Errorf("results[%d] = %s, want %s", i, got, want)
		}
	}
}
