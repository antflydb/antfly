// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package proxy

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestThunderingHerd demonstrates that multiple goroutines can
// enter HalfOpen state simultaneously when the timeout expires.
// This violates the standard circuit breaker pattern which should
// allow exactly 1 request to test if the service has recovered.
func TestThunderingHerd(t *testing.T) {
	// Create circuit breaker with threshold=2, timeout=100ms
	cb := NewCircuitBreaker(2, 100*time.Millisecond)

	// Open the circuit by recording threshold failures
	cb.Allow() // Client 1 gets in
	cb.RecordFailure()
	cb.Allow() // Client 2 gets in
	cb.RecordFailure()

	// Circuit is now Open
	if state := atomic.LoadInt32(&cb.state); state != 1 {
		t.Fatalf("Expected circuit to be Open (state=1), got state=%d", state)
	}

	// Wait for timeout to expire
	time.Sleep(150 * time.Millisecond)

	// Launch multiple goroutines that all try to enter simultaneously
	numGoroutines := 10
	var wg sync.WaitGroup
	allowedCount := int32(0)

	// Use a barrier to ensure all goroutines start at roughly the same time
	barrier := make(chan struct{})

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Wait for barrier
			<-barrier

			// Try to enter
			if cb.Allow() {
				atomic.AddInt32(&allowedCount, 1)
				// Don't record success/failure - we want to see how many get in
			}
		}(i)
	}

	// Release all goroutines simultaneously
	close(barrier)
	wg.Wait()

	count := atomic.LoadInt32(&allowedCount)
	t.Logf("Allowed %d goroutines into HalfOpen state (expected: 1, got: %d)", count, count)

	// Standard circuit breaker should allow exactly 1 request in HalfOpen
	// But the current implementation allows multiple (thundering herd)
	if count > 1 {
		t.Logf("BUG CONFIRMED: Thundering herd - %d concurrent requests in HalfOpen state", count)
		// This is the bug - we expect this to fail the test
		t.Fatalf("Expected at most 1 request in HalfOpen, but got %d", count)
	} else if count == 1 {
		t.Log("PASS: Exactly 1 request allowed in HalfOpen (bug is fixed)")
	} else {
		t.Fatalf("Unexpected: no requests allowed, expected 1")
	}
}
