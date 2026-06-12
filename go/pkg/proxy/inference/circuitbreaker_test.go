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
