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

package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestShardCooldownWithMockTime(t *testing.T) {
	mockTime := &MockTimeProvider{
		current: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	reconciler := NewReconcilerWithTimeProvider(
		nil, nil, nil,
		ReconciliationConfig{},
		zaptest.NewLogger(t),
		mockTime,
	)

	shardID := types.ID(123)

	// Set cooldown for 1 minute
	reconciler.SetShardCooldown(shardID, time.Minute)

	// Should be in cooldown immediately
	assert.True(t, reconciler.IsShardInCooldown(shardID))

	// Advance time by 30 seconds - still in cooldown
	mockTime.Advance(30 * time.Second)
	assert.True(t, reconciler.IsShardInCooldown(shardID))

	// Advance time by another 31 seconds - cooldown expired
	mockTime.Advance(31 * time.Second)
	assert.False(t, reconciler.IsShardInCooldown(shardID))

	// Entry should be cleaned up
	assert.NotContains(t, reconciler.shardCooldown, shardID)
}

func TestShardForNodeCooldownWithMockTime(t *testing.T) {
	mockTime := &MockTimeProvider{
		current: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	reconciler := NewReconcilerWithTimeProvider(
		nil, nil, nil,
		ReconciliationConfig{},
		zaptest.NewLogger(t),
		mockTime,
	)

	shardID := types.ID(123)
	nodeID := types.ID(456)

	// Set cooldown for 30 seconds
	reconciler.SetShardForNodeCooldown(shardID, nodeID, 30*time.Second)

	// Should be in cooldown
	assert.True(t, reconciler.IsShardForNodeInCooldown(shardID, nodeID))

	// Advance time past cooldown
	mockTime.Advance(31 * time.Second)
	assert.False(t, reconciler.IsShardForNodeInCooldown(shardID, nodeID))
}

func TestCleanupExpiredCooldowns(t *testing.T) {
	mockTime := &MockTimeProvider{
		current: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	reconciler := NewReconcilerWithTimeProvider(
		nil, nil, nil,
		ReconciliationConfig{},
		zaptest.NewLogger(t),
		mockTime,
	)

	// Set multiple cooldowns
	shard1 := types.ID(1)
	shard2 := types.ID(2)
	shard3 := types.ID(3)

	reconciler.SetShardCooldown(shard1, 1*time.Minute)
	reconciler.SetShardCooldown(shard2, 2*time.Minute)
	reconciler.SetShardCooldown(shard3, 3*time.Minute)

	require.Len(t, reconciler.shardCooldown, 3)

	// Advance time by 90 seconds
	mockTime.Advance(90 * time.Second)

	// Clean up - should remove shard1 only
	reconciler.CleanupExpiredCooldowns()

	assert.NotContains(t, reconciler.shardCooldown, shard1)
	assert.Contains(t, reconciler.shardCooldown, shard2)
	assert.Contains(t, reconciler.shardCooldown, shard3)
	assert.Len(t, reconciler.shardCooldown, 2)

	// Advance another 90 seconds
	mockTime.Advance(90 * time.Second)
	reconciler.CleanupExpiredCooldowns()

	assert.NotContains(t, reconciler.shardCooldown, shard2)
	assert.Contains(t, reconciler.shardCooldown, shard3)
	assert.Len(t, reconciler.shardCooldown, 1)
}

func TestShardForNodeCooldownCleanup(t *testing.T) {
	mockTime := &MockTimeProvider{
		current: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	reconciler := NewReconcilerWithTimeProvider(
		nil, nil, nil,
		ReconciliationConfig{},
		zaptest.NewLogger(t),
		mockTime,
	)

	// Set multiple shard-node cooldowns
	shard1 := types.ID(1)
	shard2 := types.ID(2)
	node1 := types.ID(10)
	node2 := types.ID(20)

	reconciler.SetShardForNodeCooldown(shard1, node1, 1*time.Minute)
	reconciler.SetShardForNodeCooldown(shard2, node2, 2*time.Minute)

	require.Len(t, reconciler.shardForNodeCooldown, 2)

	// Advance time by 90 seconds
	mockTime.Advance(90 * time.Second)

	// Clean up - should remove first entry only
	reconciler.CleanupExpiredCooldowns()

	assert.Len(t, reconciler.shardForNodeCooldown, 1)
	assert.False(t, reconciler.IsShardForNodeInCooldown(shard1, node1))
	assert.True(t, reconciler.IsShardForNodeInCooldown(shard2, node2))
}

func TestComputeSplitTransitions_WithCooldown(t *testing.T) {
	mockTime := &MockTimeProvider{
		current: time.Now(), // Use current time to avoid stale stats
	}

	shardID := types.ID(1)

	// Create shard that exceeds size threshold
	shards := map[types.ID]*store.ShardStatus{
		shardID: {
			ShardInfo: store.ShardInfo{
				ShardConfig: store.ShardConfig{
					ByteRange: [2][]byte{{0x00}, {0xff}},
				},
				RaftStatus: &common.RaftStatus{
					Lead:   1, // Must have a leader for split to be considered
					Voters: common.NewPeerSet(1, 2, 3),
				},
				ShardStats: &store.ShardStats{
					Storage: &store.StorageStats{
						DiskSize: 1000 * 1024 * 1024, // 1000 MB
					},
					Updated: mockTime.Now(),
				},
			},
			ID:    shardID,
			Table: "test",
			State: store.ShardState_Default,
		},
	}

	// Set cooldown
	shardCooldown := map[types.ID]time.Time{
		shardID: mockTime.Now().Add(time.Minute),
	}

	// Should not split because of cooldown
	splits, _ := computeSplitTransitions(
		t.Context(),
		3, shards, 500*1024*1024, 100, shardCooldown,
		func(ctx context.Context, id types.ID) ([]byte, error) { return []byte("median"), nil },
		mockTime,
	)

	assert.Empty(t, splits, "Should not split during cooldown")

	// Advance time past cooldown
	mockTime.Advance(61 * time.Second)

	// Now should split
	splits, _ = computeSplitTransitions(
		t.Context(),
		3, shards, 500*1024*1024, 100, shardCooldown,
		func(ctx context.Context, id types.ID) ([]byte, error) { return []byte("median"), nil },
		mockTime,
	)

	assert.NotEmpty(t, splits, "Should split after cooldown expires")
}

func TestMultipleCooldownsWithDifferentDurations(t *testing.T) {
	mockTime := &MockTimeProvider{
		current: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	reconciler := NewReconcilerWithTimeProvider(
		nil, nil, nil,
		ReconciliationConfig{},
		zaptest.NewLogger(t),
		mockTime,
	)

	// Set cooldowns with different durations
	shards := []types.ID{1, 2, 3, 4, 5}
	durations := []time.Duration{
		10 * time.Second,
		20 * time.Second,
		30 * time.Second,
		40 * time.Second,
		50 * time.Second,
	}

	for i, shardID := range shards {
		reconciler.SetShardCooldown(shardID, durations[i])
	}

	// All should be in cooldown initially
	for _, shardID := range shards {
		assert.True(t, reconciler.IsShardInCooldown(shardID))
	}

	// Advance 15 seconds - only first shard should be out of cooldown
	mockTime.Advance(15 * time.Second)
	assert.False(t, reconciler.IsShardInCooldown(shards[0]))
	assert.True(t, reconciler.IsShardInCooldown(shards[1]))
	assert.True(t, reconciler.IsShardInCooldown(shards[2]))
	assert.True(t, reconciler.IsShardInCooldown(shards[3]))
	assert.True(t, reconciler.IsShardInCooldown(shards[4]))

	// Advance another 10 seconds (total 25) - first two should be out
	mockTime.Advance(10 * time.Second)
	assert.False(t, reconciler.IsShardInCooldown(shards[0]))
	assert.False(t, reconciler.IsShardInCooldown(shards[1]))
	assert.True(t, reconciler.IsShardInCooldown(shards[2]))
	assert.True(t, reconciler.IsShardInCooldown(shards[3]))
	assert.True(t, reconciler.IsShardInCooldown(shards[4]))

	// Advance to 55 seconds total - all should be out
	mockTime.Advance(30 * time.Second)
	for _, shardID := range shards {
		assert.False(t, reconciler.IsShardInCooldown(shardID))
	}

	// Map should be empty (cleaned up on checks)
	assert.Empty(t, reconciler.shardCooldown)
}

func TestCooldownPreventsRapidSplits(t *testing.T) {
	mockTime := &MockTimeProvider{
		current: time.Now(), // Use current time to avoid stale stats
	}

	reconciler := NewReconcilerWithTimeProvider(
		nil, nil, nil,
		ReconciliationConfig{
			ReplicationFactor: 3,
			MaxShardSizeBytes: 500 * 1024 * 1024, // 500MB
			MaxShardsPerTable: 100,
		},
		zaptest.NewLogger(t),
		mockTime,
	)

	shardID := types.ID(1)

	// Create large shard
	shards := map[types.ID]*store.ShardStatus{
		shardID: {
			ShardInfo: store.ShardInfo{
				ShardConfig: store.ShardConfig{
					ByteRange: [2][]byte{{0x00}, {0xff}},
				},
				RaftStatus: &common.RaftStatus{
					Lead:   1, // Must have a leader for split to be considered
					Voters: common.NewPeerSet(1, 2, 3),
				},
				ShardStats: &store.ShardStats{
					Storage: &store.StorageStats{
						DiskSize: 1000 * 1024 * 1024, // 1000 MB - exceeds threshold
					},
					Updated: mockTime.Now(),
				},
			},
			ID:    shardID,
			Table: "test",
			State: store.ShardState_Default,
		},
	}

	getMedianKey := func(ctx context.Context, id types.ID) ([]byte, error) {
		return []byte("median"), nil
	}

	ctx := context.Background()

	// First split should succeed
	splits, _ := computeSplitTransitions(
		ctx,
		reconciler.config.ReplicationFactor,
		shards,
		reconciler.config.MaxShardSizeBytes,
		reconciler.config.MaxShardsPerTable,
		reconciler.shardCooldown,
		getMedianKey,
		mockTime,
	)
	assert.Len(t, splits, 1, "First split should be planned")

	// Set cooldown after split
	reconciler.SetShardCooldown(shardID, time.Minute)

	// Immediate retry should be blocked
	splits, _ = computeSplitTransitions(
		ctx,
		reconciler.config.ReplicationFactor,
		shards,
		reconciler.config.MaxShardSizeBytes,
		reconciler.config.MaxShardsPerTable,
		reconciler.shardCooldown,
		getMedianKey,
		mockTime,
	)
	assert.Empty(t, splits, "Split should be blocked by cooldown")

	// After cooldown expires, split should be allowed
	mockTime.Advance(61 * time.Second)
	splits, _ = computeSplitTransitions(
		ctx,
		reconciler.config.ReplicationFactor,
		shards,
		reconciler.config.MaxShardSizeBytes,
		reconciler.config.MaxShardsPerTable,
		reconciler.shardCooldown,
		getMedianKey,
		mockTime,
	)
	assert.Len(t, splits, 1, "Split should be allowed after cooldown")
}

func TestMockTimeProviderAdvance(t *testing.T) {
	mockTime := &MockTimeProvider{
		current: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	initialTime := mockTime.Now()

	// Advance by 1 hour
	mockTime.Advance(time.Hour)
	assert.Equal(t, initialTime.Add(time.Hour), mockTime.Now())

	// Advance by 30 minutes
	mockTime.Advance(30 * time.Minute)
	assert.Equal(t, initialTime.Add(time.Hour+30*time.Minute), mockTime.Now())

	// Set to specific time
	newTime := time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)
	mockTime.Set(newTime)
	assert.Equal(t, newTime, mockTime.Now())
}

func TestRealTimeProvider(t *testing.T) {
	provider := RealTimeProvider{}

	before := time.Now()
	providerTime := provider.Now()
	after := time.Now()

	assert.True(t, providerTime.After(before) || providerTime.Equal(before))
	assert.True(t, providerTime.Before(after) || providerTime.Equal(after))
}

func TestReconcilerUsesTimeProvider(t *testing.T) {
	// Create reconciler with real time provider
	realTimeReconciler := NewReconciler(nil, nil, nil, ReconciliationConfig{}, zaptest.NewLogger(t))
	assert.IsType(t, RealTimeProvider{}, realTimeReconciler.timeProvider)

	// Create reconciler with mock time provider
	mockTime := &MockTimeProvider{current: time.Now()}
	mockTimeReconciler := NewReconcilerWithTimeProvider(
		nil, nil, nil,
		ReconciliationConfig{},
		zaptest.NewLogger(t),
		mockTime,
	)
	assert.IsType(t, &MockTimeProvider{}, mockTimeReconciler.timeProvider)
}

func TestCooldownWithContextCancellation(t *testing.T) {
	mockTime := &MockTimeProvider{
		current: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	reconciler := NewReconcilerWithTimeProvider(
		nil, nil, nil,
		ReconciliationConfig{},
		zaptest.NewLogger(t),
		mockTime,
	)

	shardID := types.ID(123)
	reconciler.SetShardCooldown(shardID, time.Minute)

	// Context cancellation shouldn't affect cooldown state
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = ctx // Use ctx to avoid linter warning

	// Cooldown should still work
	assert.True(t, reconciler.IsShardInCooldown(shardID))
	mockTime.Advance(61 * time.Second)
	assert.False(t, reconciler.IsShardInCooldown(shardID))
}
