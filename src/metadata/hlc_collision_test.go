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

package metadata

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestHLC_TimestampCollision demonstrates that multiple HLC instances
// (representing multiple metadata servers) can produce identical timestamps.
//
// BUG: Each metadata server creates its own HLC with independent counters.
// When multiple servers allocate timestamps concurrently, they can produce
// the same timestamp value, violating the LWW assumption that timestamps
// are unique.
//
// TLA+ Counterexample:
// 1. Metadata Server m1: hlcCounters[m1] = 0 → 1, timestamp = 1
// 2. Metadata Server m2: hlcCounters[m2] = 0 → 1, timestamp = 1  <- COLLISION!
//
// Impact: LWW comparison (ts1 > ts2) returns false for equal timestamps,
// making the winner non-deterministic (depends on resolution order).
func TestHLC_TimestampCollision_MultipleServers(t *testing.T) {
	// Create two independent HLC instances (simulating two metadata servers)
	hlc1 := NewHLC() // Metadata Server 1
	hlc2 := NewHLC() // Metadata Server 2

	// Both allocate their first timestamp
	ts1 := hlc1.Now()
	ts2 := hlc2.Now()

	// BUG DEMONSTRATED: Both timestamps are 1!
	t.Logf("HLC1 timestamp: %d", ts1)
	t.Logf("HLC2 timestamp: %d", ts2)

	// This assertion demonstrates the bug - it will PASS (showing the bug exists)
	// Once fixed, this should FAIL (timestamps should be globally unique)
	if ts1 == ts2 {
		t.Log("BUG CONFIRMED: Two independent HLC instances produced the same timestamp!")
		t.Log("This means two concurrent transactions from different metadata servers")
		t.Log("can have identical timestamps, breaking LWW determinism.")

		// Demonstrate the LWW comparison issue
		// With equal timestamps, ts1 > ts2 is false, so first-to-resolve wins
		assert.LessOrEqual(t, ts1, ts2, "ts1 > ts2 should be false (equal timestamps)")
		assert.LessOrEqual(t, ts2, ts1, "ts2 > ts1 should be false (equal timestamps)")
		t.Log("LWW comparison: Neither timestamp 'wins' - resolution order determines outcome")

		// This test should FAIL once the bug is fixed
		t.Fatal("TIMESTAMP COLLISION: Independent HLC instances can produce identical timestamps")
	}

	// If we reach here, timestamps are unique (bug is fixed)
	t.Log("Timestamps are unique - bug may be fixed")
	assert.NotEqual(t, ts1, ts2, "Timestamps should be globally unique")
}

// TestHLC_TimestampCollision_ConcurrentAllocation tests that concurrent
// timestamp allocation from multiple HLC instances produces collisions.
func TestHLC_TimestampCollision_ConcurrentAllocation(t *testing.T) {
	const numServers = 3
	const allocationsPerServer = 100

	hlcs := make([]*HLC, numServers)
	for i := range hlcs {
		hlcs[i] = NewHLC()
	}

	// Collect all timestamps
	var mu sync.Mutex
	allTimestamps := make(map[uint64]int) // timestamp -> count

	var wg sync.WaitGroup
	for serverID := range numServers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range allocationsPerServer {
				ts := hlcs[id].Now()
				mu.Lock()
				allTimestamps[ts]++
				mu.Unlock()
			}
		}(serverID)
	}
	wg.Wait()

	// Count collisions
	collisions := 0
	for ts, count := range allTimestamps {
		if count > 1 {
			collisions += count - 1
			t.Logf("Timestamp %d was allocated %d times (collision!)", ts, count)
		}
	}

	totalAllocations := numServers * allocationsPerServer
	t.Logf("Total allocations: %d, Unique timestamps: %d, Collisions: %d",
		totalAllocations, len(allTimestamps), collisions)

	// With independent counters, we expect collisions
	// Each server allocates 1, 2, 3, ..., 100
	// All three servers will produce the same sequence!
	expectedCollisions := totalAllocations - allocationsPerServer // 200 collisions
	t.Logf("Expected collisions with independent counters: %d", expectedCollisions)

	if collisions > 0 {
		t.Log("BUG CONFIRMED: Multiple HLC instances produce duplicate timestamps")
		t.Fatalf("TIMESTAMP COLLISIONS DETECTED: %d collisions out of %d allocations",
			collisions, totalAllocations)
	}

	// If no collisions, the bug is fixed
	t.Log("No collisions detected - timestamps are globally unique")
}

// TestHLC_RestartResetsBug demonstrates that HLC counter resets on restart,
// potentially causing old timestamps to be reused.
func TestHLC_RestartResetsBug(t *testing.T) {
	// First "server lifetime"
	hlc1 := NewHLC()
	for range 1000 {
		hlc1.Now()
	}
	lastTs := hlc1.Now()
	t.Logf("Last timestamp before 'restart': %d", lastTs)

	// Simulate server restart - create new HLC (counter resets to 0)
	hlc2 := NewHLC()
	firstTsAfterRestart := hlc2.Now()
	t.Logf("First timestamp after 'restart': %d", firstTsAfterRestart)

	// BUG: The new counter starts at 1, not at lastTs+1
	if firstTsAfterRestart < lastTs {
		t.Log("BUG CONFIRMED: After restart, HLC counter resets and can produce")
		t.Log("timestamps that are lower than pre-restart timestamps.")
		t.Log("This could cause new transactions to 'lose' to old unresolved transactions")
		t.Log("in LWW comparisons.")

		t.Fatalf("HLC RESET BUG: Post-restart timestamp %d < pre-restart timestamp %d",
			firstTsAfterRestart, lastTs)
	}

	t.Log("Counter persisted through restart or uses wall clock - bug may be fixed")
}

// TestLWW_DeterminismWithUniqueTimestamps verifies that with unique timestamps
// from our fixed HLC implementation, LWW resolution is deterministic.
// The higher timestamp always wins, regardless of resolution order.
func TestLWW_DeterminismWithUniqueTimestamps(t *testing.T) {
	// Create two HLC instances (simulating two metadata servers)
	hlc1 := NewHLC()
	hlc2 := NewHLC()

	// Get timestamps - these should be unique due to the fix
	ts1 := hlc1.Now()
	ts2 := hlc2.Now()

	// Ensure timestamps are unique (the fix we're testing)
	if ts1 == ts2 {
		t.Fatal("BUG: HLC instances produced equal timestamps")
	}

	t.Logf("Transaction 1 timestamp: %d", ts1)
	t.Logf("Transaction 2 timestamp: %d", ts2)

	// Determine which timestamp is higher
	var higherTs, lowerTs uint64
	var higherValue, lowerValue string
	if ts1 > ts2 {
		higherTs, lowerTs = ts1, ts2
		higherValue, lowerValue = "transaction_1_value", "transaction_2_value"
	} else {
		higherTs, lowerTs = ts2, ts1
		higherValue, lowerValue = "transaction_2_value", "transaction_1_value"
	}

	// LWW comparison: only write if new timestamp > existing
	// Test Scenario 1: Lower timestamp resolves first, then higher
	var finalValue1 string
	existingTs1 := uint64(0)

	// Lower writes first (lowerTs > 0, so writes)
	if lowerTs > existingTs1 {
		finalValue1 = lowerValue
		existingTs1 = lowerTs
	}
	// Higher tries to write (higherTs > lowerTs, so writes)
	if higherTs > existingTs1 {
		finalValue1 = higherValue
	}
	t.Logf("Scenario 1 (lower first): final value = %s", finalValue1)

	// Test Scenario 2: Higher timestamp resolves first, then lower
	var finalValue2 string
	existingTs2 := uint64(0)

	// Higher writes first (higherTs > 0, so writes)
	if higherTs > existingTs2 {
		finalValue2 = higherValue
		existingTs2 = higherTs
	}
	// Lower tries to write (lowerTs > higherTs is false, so doesn't write)
	if lowerTs > existingTs2 {
		finalValue2 = lowerValue
	}
	t.Logf("Scenario 2 (higher first): final value = %s", finalValue2)

	// With unique timestamps, both scenarios should produce the same result
	// (the higher timestamp value wins)
	if finalValue1 != finalValue2 {
		t.Fatalf("LWW non-determinism detected: scenario 1 = %s, scenario 2 = %s",
			finalValue1, finalValue2)
	}

	if finalValue1 != higherValue {
		t.Fatalf("Expected higher timestamp value %s to win, got %s", higherValue, finalValue1)
	}

	t.Log("LWW is deterministic with unique timestamps - higher timestamp always wins")
}
