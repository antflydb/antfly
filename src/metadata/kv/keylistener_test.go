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

package kv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMetadataKV_KeyPatternListener(t *testing.T) {
	// Create a simple metadataKV instance for testing
	// We'll mock the necessary parts
	logger := zap.NewNop()
	mkv := &metadataKV{
		logger: logger,
	}

	// Track listener invocations using a map (order doesn't matter for async handlers)
	var mu sync.Mutex
	calls := make(map[string]struct {
		value    string
		isDelete bool
		params   map[string]string
	})

	// Register a pattern listener
	err := mkv.RegisterKeyPattern(
		"tables/{tableID}/shards/{shardID}",
		func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
			mu.Lock()
			defer mu.Unlock()
			calls[string(key)] = struct {
				value    string
				isDelete bool
				params   map[string]string
			}{
				value:    string(value),
				isDelete: isDelete,
				params:   params,
			}
			return nil
		},
	)
	require.NoError(t, err)

	// Simulate key modifications
	modifiedKeys := []keyChange{
		{key: []byte("tables/users/shards/shard1"), value: []byte("data1")},
		{key: []byte("tables/products/shards/shard2"), value: []byte("data2")},
		{key: []byte("other/key"), value: []byte("data3")}, // Should not match
	}

	// Notify listeners (this simulates what happens in applyOpBatch)
	mkv.notifyListeners(context.Background(), modifiedKeys)

	// Wait for async handlers
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(calls) == 2
	}, time.Second, 10*time.Millisecond, "Should have 2 matching calls")

	// Verify calls (order doesn't matter since handlers run async)
	mu.Lock()
	defer mu.Unlock()

	// Check users/shard1 was processed
	assert.Contains(t, calls, "tables/users/shards/shard1")
	assert.Equal(t, "data1", calls["tables/users/shards/shard1"].value)
	assert.False(t, calls["tables/users/shards/shard1"].isDelete)
	assert.Equal(t, "users", calls["tables/users/shards/shard1"].params["tableID"])
	assert.Equal(t, "shard1", calls["tables/users/shards/shard1"].params["shardID"])

	// Check products/shard2 was processed
	assert.Contains(t, calls, "tables/products/shards/shard2")
	assert.Equal(t, "data2", calls["tables/products/shards/shard2"].value)
	assert.False(t, calls["tables/products/shards/shard2"].isDelete)
	assert.Equal(t, "products", calls["tables/products/shards/shard2"].params["tableID"])
	assert.Equal(t, "shard2", calls["tables/products/shards/shard2"].params["shardID"])
}

func TestMetadataKV_KeyPrefixListener(t *testing.T) {
	logger := zap.NewNop()
	mkv := &metadataKV{
		logger: logger,
	}

	// Track listener invocations using a map (order doesn't matter for async handlers)
	var mu sync.Mutex
	calls := make(map[string]struct {
		value    string
		isDelete bool
	})

	// Register a prefix listener
	mkv.RegisterKeyPrefixListener(
		[]byte("tm:shs:"),
		func(ctx context.Context, key, value []byte, isDelete bool) error {
			mu.Lock()
			defer mu.Unlock()
			calls[string(key)] = struct {
				value    string
				isDelete bool
			}{
				value:    string(value),
				isDelete: isDelete,
			}
			return nil
		},
	)

	// Simulate key modifications
	modifiedKeys := []keyChange{
		{key: []byte("tm:shs:shard1"), value: []byte("status1")},
		{key: []byte("tm:shs:shard2"), value: []byte("status2")},
		{key: []byte("tm:t:table1"), value: []byte("data")}, // Should not match
	}

	// Notify listeners
	mkv.notifyListeners(context.Background(), modifiedKeys)

	// Wait for async handlers
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(calls) == 2
	}, time.Second, 10*time.Millisecond, "Should have 2 matching calls")

	// Verify calls (order doesn't matter since handlers run async)
	mu.Lock()
	defer mu.Unlock()

	// Check shard1 was processed
	assert.Contains(t, calls, "tm:shs:shard1")
	assert.Equal(t, "status1", calls["tm:shs:shard1"].value)
	assert.False(t, calls["tm:shs:shard1"].isDelete)

	// Check shard2 was processed
	assert.Contains(t, calls, "tm:shs:shard2")
	assert.Equal(t, "status2", calls["tm:shs:shard2"].value)
	assert.False(t, calls["tm:shs:shard2"].isDelete)
}

func TestMetadataKV_DeleteListener(t *testing.T) {
	logger := zap.NewNop()
	mkv := &metadataKV{
		logger: logger,
	}

	// Track listener invocations
	var mu sync.Mutex
	var deleteCount int

	// Register a listener that checks for deletes
	mkv.RegisterKeyPrefixListener(
		[]byte("tm:t:"),
		func(ctx context.Context, key, value []byte, isDelete bool) error {
			mu.Lock()
			defer mu.Unlock()
			if isDelete {
				deleteCount++
			}
			return nil
		},
	)

	// Simulate deletions
	modifiedKeys := []keyChange{
		{key: []byte("tm:t:table1"), isDelete: true},     // Delete
		{key: []byte("tm:t:table2"), value: []byte("v")}, // Write
		{key: []byte("tm:t:table3"), isDelete: true},     // Delete
	}

	mkv.notifyListeners(context.Background(), modifiedKeys)

	// Wait for async handlers
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return deleteCount == 2
	}, time.Second, 10*time.Millisecond, "Should have 2 delete notifications")
}

func TestMetadataKV_ClearListeners(t *testing.T) {
	logger := zap.NewNop()
	mkv := &metadataKV{
		logger: logger,
	}

	// Register listeners
	err := mkv.RegisterKeyPattern("tables/{id}", func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
		return nil
	})
	require.NoError(t, err)

	mkv.RegisterKeyPrefixListener([]byte("prefix:"), func(ctx context.Context, key, value []byte, isDelete bool) error {
		return nil
	})

	// Verify listeners are registered
	assert.Len(t, mkv.keyPatterns, 1)
	assert.Len(t, mkv.keyPrefixListeners, 1)

	// Clear listeners
	mkv.ClearKeyListeners()

	// Verify listeners are cleared
	assert.Empty(t, mkv.keyPatterns)
	assert.Empty(t, mkv.keyPrefixListeners)
}

func TestMetadataKV_MultiplePatternListeners(t *testing.T) {
	logger := zap.NewNop()
	mkv := &metadataKV{
		logger: logger,
	}

	// Track listener invocations
	var mu sync.Mutex
	var pattern1Calls, pattern2Calls int

	// Register multiple pattern listeners
	err := mkv.RegisterKeyPattern(
		"tables/{tableID}/config",
		func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
			mu.Lock()
			defer mu.Unlock()
			pattern1Calls++
			return nil
		},
	)
	require.NoError(t, err)

	err = mkv.RegisterKeyPattern(
		"tables/{tableID}/shards/{shardID}",
		func(ctx context.Context, key []byte, value []byte, isDelete bool, params map[string]string) error {
			mu.Lock()
			defer mu.Unlock()
			pattern2Calls++
			return nil
		},
	)
	require.NoError(t, err)

	// Simulate key modifications
	modifiedKeys := []keyChange{
		{key: []byte("tables/users/config"), value: []byte("cfg")},
		{key: []byte("tables/users/shards/s1"), value: []byte("shard")},
		{key: []byte("tables/products/config"), value: []byte("cfg2")},
	}

	mkv.notifyListeners(context.Background(), modifiedKeys)

	// Wait for async handlers
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return pattern1Calls == 2 && pattern2Calls == 1
	}, time.Second, 10*time.Millisecond, "Pattern 1 should be called 2 times and Pattern 2 should be called 1 time")
}
