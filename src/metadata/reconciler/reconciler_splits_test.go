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
	"github.com/antflydb/antfly/src/store/db"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

// TestComputeSplitTransitions demonstrates testing the split/merge logic
// with pure functions and no I/O
func TestComputeSplitTransitions(t *testing.T) {
	t.Run("empty shards list", func(t *testing.T) {
		splits, merges := computeSplitTransitions(
			t.Context(),
			3,                                 // replicationFactor
			map[types.ID]*store.ShardStatus{}, // empty shards
			1024*1024*100,                     // maxShardSizeBytes
			100,                               // maxShardsPerTable
			map[types.ID]time.Time{},          // shardCooldown
			nil,                               // getMedianKey not needed
			RealTimeProvider{},                // timeProvider
			1,                                 // autoSplitPerTableLimit
			1,                                 // autoSplitClusterLimit
		)

		assert.Empty(t, splits)
		assert.Empty(t, merges)
	})

	t.Run("shard in cooldown is skipped", func(t *testing.T) {
		shardID := types.ID(1)
		shards := map[types.ID]*store.ShardStatus{
			shardID: {
				ShardInfo: store.ShardInfo{
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2, 3),
					},
					ShardStats: &store.ShardStats{
						Updated: time.Now(),
						Storage: &store.StorageStats{
							DiskSize: 1024 * 1024 * 200, // Over threshold
						},
					},
				},
				Table: "test",
			},
		}

		// Shard is in cooldown for 1 hour
		cooldown := map[types.ID]time.Time{
			shardID: time.Now().Add(1 * time.Hour),
		}

		splits, merges := computeSplitTransitions(
			t.Context(),
			3,
			shards,
			1024*1024*100, // 100MB threshold
			100,
			cooldown,
			func(ctx context.Context, id types.ID) ([]byte, error) {
				return []byte("median-key"), nil
			},
			RealTimeProvider{},
			1,
			1,
		)

		assert.Empty(t, splits, "Shard in cooldown should be skipped")
		assert.Empty(t, merges)
	})

	t.Run("shard without proper raft status is skipped", func(t *testing.T) {
		shards := map[types.ID]*store.ShardStatus{
			1: {
				ShardInfo: store.ShardInfo{
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2), // Only 2 voters, need 3
					},
					ShardStats: &store.ShardStats{
						Updated: time.Now(),
						Storage: &store.StorageStats{
							DiskSize: 1024 * 1024 * 200,
						},
					},
				},
				Table: "test",
			},
		}

		splits, merges := computeSplitTransitions(
			t.Context(),
			3, // Need 3 voters
			shards,
			1024*1024*100,
			100,
			map[types.ID]time.Time{},
			func(ctx context.Context, id types.ID) ([]byte, error) {
				return []byte("median-key"), nil
			},
			RealTimeProvider{},
			1,
			1,
		)

		assert.Empty(t, splits, "Shard without proper raft status should be skipped")
		assert.Empty(t, merges)
	})

	t.Run("transitioning shard is skipped", func(t *testing.T) {
		shards := map[types.ID]*store.ShardStatus{
			1: {
				ShardInfo: store.ShardInfo{
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2, 3),
					},
					ShardStats: &store.ShardStats{
						Updated: time.Now(),
						Storage: &store.StorageStats{
							DiskSize: 1024 * 1024 * 200,
						},
					},
				},
				Table: "test",
				State: store.ShardState_SplittingOff, // Transitioning
			},
		}

		splits, merges := computeSplitTransitions(
			t.Context(),
			3,
			shards,
			1024*1024*100,
			100,
			map[types.ID]time.Time{},
			func(ctx context.Context, id types.ID) ([]byte, error) {
				return []byte("median-key"), nil
			},
			RealTimeProvider{},
			1,
			1,
		)

		assert.Empty(t, splits, "Transitioning shard should be skipped")
		assert.Empty(t, merges)
	})

	t.Run("split child awaiting cutover is skipped", func(t *testing.T) {
		shards := map[types.ID]*store.ShardStatus{
			1: {
				ShardInfo: store.ShardInfo{
					RaftStatus: &common.RaftStatus{
						Lead:   1,
						Voters: common.NewPeerSet(1, 2, 3),
					},
					ShardStats: &store.ShardStats{
						Updated: time.Now(),
						Storage: &store.StorageStats{
							DiskSize: 1024 * 1024 * 200,
						},
					},
					HasSnapshot:         true,
					SplitReplayRequired: true,
					SplitReplayCaughtUp: true,
					SplitCutoverReady:   false,
				},
				ID:    1,
				Table: "test",
				State: store.ShardState_Default,
			},
		}

		getMedianKeyCalled := false
		splits, merges := computeSplitTransitions(
			t.Context(),
			3,
			shards,
			1024*1024*100,
			100,
			map[types.ID]time.Time{},
			func(ctx context.Context, id types.ID) ([]byte, error) {
				getMedianKeyCalled = true
				return []byte("median-key"), nil
			},
			RealTimeProvider{},
			1,
			1,
		)

		assert.False(t, getMedianKeyCalled, "split child awaiting cutover should be skipped")
		assert.Empty(t, splits)
		assert.Empty(t, merges)
	})

	t.Run("shard with stale stats is skipped", func(t *testing.T) {
		shards := map[types.ID]*store.ShardStatus{
			1: {
				ShardInfo: store.ShardInfo{
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2, 3),
					},
					ShardStats: &store.ShardStats{
						Updated: time.Now().Add(-2 * time.Minute), // 2 minutes old
						Storage: &store.StorageStats{
							DiskSize: 1024 * 1024 * 200,
						},
					},
				},
				Table: "test",
			},
		}

		splits, merges := computeSplitTransitions(
			t.Context(),
			3,
			shards,
			1024*1024*100,
			100,
			map[types.ID]time.Time{},
			func(ctx context.Context, id types.ID) ([]byte, error) {
				return []byte("median-key"), nil
			},
			RealTimeProvider{},
			1,
			1,
		)

		assert.Empty(t, splits, "Shard with stale stats should be skipped")
		assert.Empty(t, merges)
	})

	t.Run("large shard is split", func(t *testing.T) {
		shardID := types.ID(1)
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
						Updated: time.Now(),
						Storage: &store.StorageStats{
							DiskSize: 1024 * 1024 * 200, // 200MB - over 100MB threshold
						},
					},
				},
				ID:    shardID,
				Table: "test",
				State: store.ShardState_Default,
			},
		}

		medianKey := []byte("median-key")
		getMedianKeyCalled := false

		splits, merges := computeSplitTransitions(
			t.Context(),
			3,
			shards,
			1024*1024*100, // 100MB threshold
			100,
			map[types.ID]time.Time{},
			func(ctx context.Context, id types.ID) ([]byte, error) {
				assert.Equal(t, shardID, id)
				getMedianKeyCalled = true
				return medianKey, nil
			},
			RealTimeProvider{},
			1,
			1,
		)

		assert.True(t, getMedianKeyCalled, "getMedianKey should be called for large shard")
		assert.Len(t, splits, 1)
		assert.Empty(t, merges)
		assert.Equal(t, shardID, splits[0].ShardID)
		assert.Equal(t, medianKey, splits[0].SplitKey)
		assert.Equal(t, "test", splits[0].TableName)
	})

	t.Run("empty old shard is merged", func(t *testing.T) {
		now := time.Now()
		shard1 := types.ID(1)
		shard2 := types.ID(2)

		shards := map[types.ID]*store.ShardStatus{
			shard1: {
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{{0x00}, {0x80}},
					},
					RaftStatus: &common.RaftStatus{
						Lead:   1,
						Voters: common.NewPeerSet(1, 2, 3),
					},
					ShardStats: &store.ShardStats{
						Created: now.Add(-10 * time.Minute), // Old enough
						Updated: now,
						Storage: &store.StorageStats{
							DiskSize: 1024 * 100, // Small but non-zero (database overhead)
							Empty:    true,       // Marked as logically empty
						},
					},
				},
				ID:    shard1,
				Table: "test",
				State: store.ShardState_Default,
			},
			shard2: {
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{{0x80}, {0xff}}, // Adjacent to shard1
					},
					RaftStatus: &common.RaftStatus{
						Lead:   1,
						Voters: common.NewPeerSet(1, 2, 3),
					},
					ShardStats: &store.ShardStats{
						Created: now.Add(-10 * time.Minute),
						Updated: now,
						Storage: &store.StorageStats{
							DiskSize: 1024 * 1024 * 50, // Not empty
						},
					},
				},
				ID:    shard2,
				Table: "test",
				State: store.ShardState_Default,
			},
		}

		splits, merges := computeSplitTransitions(
			t.Context(),
			3,
			shards,
			1024*1024*100,
			100,
			map[types.ID]time.Time{},
			func(ctx context.Context, id types.ID) ([]byte, error) {
				return []byte("median-key"), nil
			},
			RealTimeProvider{},
			1,
			1,
		)

		assert.Empty(t, splits)
		if assert.Len(t, merges, 1, "Empty old shard should have merge transition") {
			assert.Equal(t, shard2, merges[0].ShardID, "Neighbor shard should be target")
			assert.Equal(t, shard1, merges[0].MergeShardID, "Empty shard should be merged")
			assert.Equal(t, "test", merges[0].TableName)
		}
	})

	t.Run("max shards per table is respected", func(t *testing.T) {
		// Create 3 shards for table "test", all over threshold
		shards := map[types.ID]*store.ShardStatus{}
		for i := 1; i <= 3; i++ {
			shardID := types.ID(i)
			shards[shardID] = &store.ShardStatus{
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{{byte(i * 10)}, {byte(i*10 + 10)}},
					},
					RaftStatus: &common.RaftStatus{
						Lead:   1,
						Voters: common.NewPeerSet(1, 2, 3),
					},
					ShardStats: &store.ShardStats{
						Updated: time.Now(),
						Storage: &store.StorageStats{
							DiskSize: 1024 * 1024 * 200, // All over threshold
						},
					},
				},
				ID:    shardID,
				Table: "test",
				State: store.ShardState_Default,
			}
		}

		splits, merges := computeSplitTransitions(
			t.Context(),
			3,
			shards,
			1024*1024*100,
			3, // Max 3 shards per table - already at limit
			map[types.ID]time.Time{},
			func(ctx context.Context, id types.ID) ([]byte, error) {
				return []byte("median-key"), nil
			},
			RealTimeProvider{},
			1,
			1,
		)

		assert.Empty(t, splits, "Should not split when at max shards per table")
		assert.Empty(t, merges)
	})

	t.Run("multiple tables are processed independently", func(t *testing.T) {
		shards := map[types.ID]*store.ShardStatus{
			1: {
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{{0x00}, {0xff}},
					},
					RaftStatus: &common.RaftStatus{
						Lead:   1,
						Voters: common.NewPeerSet(1, 2, 3),
					},
					ShardStats: &store.ShardStats{
						Updated: time.Now(),
						Storage: &store.StorageStats{
							DiskSize: 1024 * 1024 * 200,
						},
					},
				},
				ID:    1,
				Table: "table1",
				State: store.ShardState_Default,
			},
			2: {
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{{0x00}, {0xff}},
					},
					RaftStatus: &common.RaftStatus{
						Lead:   1,
						Voters: common.NewPeerSet(1, 2, 3),
					},
					ShardStats: &store.ShardStats{
						Updated: time.Now(),
						Storage: &store.StorageStats{
							DiskSize: 1024 * 1024 * 200,
						},
					},
				},
				ID:    2,
				Table: "table2",
				State: store.ShardState_Default,
			},
		}

		splits, merges := computeSplitTransitions(
			t.Context(),
			3,
			shards,
			1024*1024*100,
			100,
			map[types.ID]time.Time{},
			func(ctx context.Context, id types.ID) ([]byte, error) {
				return []byte("median-key"), nil
			},
			RealTimeProvider{},
			1,
			1,
		)

		assert.Len(t, splits, 1, "Automatic split planning should be throttled cluster-wide")
		assert.Empty(t, merges)
	})

	t.Run("active split blocks new automatic split planning", func(t *testing.T) {
		shards := map[types.ID]*store.ShardStatus{
			1: {
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{{0x00}, {0x80}},
					},
					RaftStatus: &common.RaftStatus{
						Lead:   1,
						Voters: common.NewPeerSet(1, 2, 3),
					},
					ShardStats: &store.ShardStats{
						Updated: time.Now(),
						Storage: &store.StorageStats{
							DiskSize: 1024 * 1024 * 200,
						},
					},
					SplitState: func() *db.SplitState {
						state := &db.SplitState{}
						state.SetPhase(db.SplitState_PHASE_SPLITTING)
						return state
					}(),
				},
				ID:    1,
				Table: "table1",
				State: store.ShardState_Splitting,
			},
			2: {
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{{0x80}, {0xff}},
					},
					RaftStatus: &common.RaftStatus{
						Lead:   1,
						Voters: common.NewPeerSet(1, 2, 3),
					},
					ShardStats: &store.ShardStats{
						Updated: time.Now(),
						Storage: &store.StorageStats{
							DiskSize: 1024 * 1024 * 200,
						},
					},
				},
				ID:    2,
				Table: "table2",
				State: store.ShardState_Default,
			},
		}

		splits, merges := computeSplitTransitions(
			t.Context(),
			3,
			shards,
			1024*1024*100,
			100,
			map[types.ID]time.Time{},
			func(ctx context.Context, id types.ID) ([]byte, error) {
				return []byte("median-key"), nil
			},
			RealTimeProvider{},
			1,
			1,
		)

		assert.Empty(t, splits, "No new automatic splits should be planned while another split is active")
		assert.Empty(t, merges)
	})
}

// TestSplitStatePhaseActions tests the new zero-downtime split state machine actions
func TestSplitStatePhaseActions(t *testing.T) {
	logger := zaptest.NewLogger(t)

	t.Run("PHASE_PREPARE transitions to SPLITTING when ready", func(t *testing.T) {
		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{SplitTimeout: 5 * time.Minute},
			logger,
			RealTimeProvider{},
		)

		shardID := types.ID(1)
		newShardID := types.ID(2)
		splitKey := []byte("split-key")
		now := time.Now()

		splitState := &db.SplitState{}
		splitState.SetPhase(db.SplitState_PHASE_PREPARE)
		splitState.SetSplitKey(splitKey)
		splitState.SetNewShardId(uint64(newShardID))
		splitState.SetStartedAtUnixNanos(now.UnixNano())

		shardStatus := &store.ShardStatus{
			ShardInfo: store.ShardInfo{
				SplitState: splitState,
				Splitting:  false, // Shadow index is ready
			},
		}

		action := reconciler.computeSplitStatePhaseAction(
			shardID,
			shardStatus,
			splitState,
			map[types.ID]*store.ShardStatus{},
		)

		assert.NotNil(t, action)
		assert.Equal(t, SplitStateActionTransitionToSplit, action.Action)
		assert.Equal(t, shardID, action.ShardID)
		assert.Equal(t, newShardID, action.NewShardID)
		assert.Equal(t, splitKey, action.SplitKey)
	})

	t.Run("PHASE_PREPARE waits when shadow index still building", func(t *testing.T) {
		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{SplitTimeout: 5 * time.Minute},
			logger,
			RealTimeProvider{},
		)

		shardID := types.ID(1)
		now := time.Now()

		splitState := &db.SplitState{}
		splitState.SetPhase(db.SplitState_PHASE_PREPARE)
		splitState.SetStartedAtUnixNanos(now.UnixNano())

		shardStatus := &store.ShardStatus{
			ShardInfo: store.ShardInfo{
				SplitState: splitState,
				Splitting:  true, // Still building shadow index
			},
		}

		action := reconciler.computeSplitStatePhaseAction(
			shardID,
			shardStatus,
			splitState,
			map[types.ID]*store.ShardStatus{},
		)

		assert.Nil(t, action, "Should wait for shadow index to be ready")
	})

	t.Run("PHASE_SPLITTING defers finalize until grace period elapses", func(t *testing.T) {
		mockTime := &MockTimeProvider{}
		now := time.Now()
		mockTime.Set(now)

		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{
				SplitTimeout:             5 * time.Minute,
				SplitFinalizeGracePeriod: 15 * time.Second,
			},
			logger,
			mockTime,
		)

		shardID := types.ID(1)
		newShardID := types.ID(2)
		splitKey := []byte("split-key")

		splitState := &db.SplitState{}
		splitState.SetPhase(db.SplitState_PHASE_SPLITTING)
		splitState.SetSplitKey(splitKey)
		splitState.SetNewShardId(uint64(newShardID))
		splitState.SetStartedAtUnixNanos(now.UnixNano())

		shardStatus := &store.ShardStatus{
			ShardInfo: store.ShardInfo{
				ShardConfig: store.ShardConfig{
					ByteRange: [2][]byte{{0x00}, splitKey},
				},
				SplitState: splitState,
			},
		}

		allShards := map[types.ID]*store.ShardStatus{
			newShardID: {
				ShardInfo: store.ShardInfo{
					HasSnapshot: true,
					RaftStatus:  &common.RaftStatus{Lead: 10},
				},
			},
		}

		// First call: shard is ready, but grace period hasn't elapsed yet
		action := reconciler.computeSplitStatePhaseAction(
			shardID, shardStatus, splitState, allShards,
		)
		assert.Nil(t, action, "Should not finalize immediately — grace period not elapsed")

		// Second call after 10s: still within grace period
		mockTime.Advance(10 * time.Second)
		action = reconciler.computeSplitStatePhaseAction(
			shardID, shardStatus, splitState, allShards,
		)
		assert.Nil(t, action, "Should not finalize — only 10s of 15s grace period elapsed")

		// Third call after another 6s (total 16s): grace period elapsed
		mockTime.Advance(6 * time.Second)
		action = reconciler.computeSplitStatePhaseAction(
			shardID, shardStatus, splitState, allShards,
		)
		assert.NotNil(t, action)
		assert.Equal(t, SplitStateActionFinalizeSplit, action.Action)
		assert.Equal(t, shardID, action.ShardID)
		assert.Equal(t, newShardID, action.NewShardID)
	})

	t.Run("PHASE_SPLITTING resets grace period when shard loses readiness", func(t *testing.T) {
		mockTime := &MockTimeProvider{}
		now := time.Now()
		mockTime.Set(now)

		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{
				SplitTimeout:             5 * time.Minute,
				SplitFinalizeGracePeriod: 15 * time.Second,
			},
			logger,
			mockTime,
		)

		shardID := types.ID(1)
		newShardID := types.ID(2)
		splitKey := []byte("split-key")

		splitState := &db.SplitState{}
		splitState.SetPhase(db.SplitState_PHASE_SPLITTING)
		splitState.SetSplitKey(splitKey)
		splitState.SetNewShardId(uint64(newShardID))
		splitState.SetStartedAtUnixNanos(now.UnixNano())

		shardStatus := &store.ShardStatus{
			ShardInfo: store.ShardInfo{
				ShardConfig: store.ShardConfig{
					ByteRange: [2][]byte{{0x00}, splitKey},
				},
				SplitState: splitState,
			},
		}

		readyShards := map[types.ID]*store.ShardStatus{
			newShardID: {
				ShardInfo: store.ShardInfo{
					HasSnapshot: true,
					RaftStatus:  &common.RaftStatus{Lead: 10},
				},
			},
		}
		noLeaderShards := map[types.ID]*store.ShardStatus{
			newShardID: {
				ShardInfo: store.ShardInfo{
					HasSnapshot: true,
					// No leader
				},
			},
		}

		// First call: ready, starts grace period
		action := reconciler.computeSplitStatePhaseAction(
			shardID, shardStatus, splitState, readyShards,
		)
		assert.Nil(t, action)

		// 10s later: shard loses leader — grace period resets
		mockTime.Advance(10 * time.Second)
		action = reconciler.computeSplitStatePhaseAction(
			shardID, shardStatus, splitState, noLeaderShards,
		)
		assert.Nil(t, action)

		// Shard becomes ready again, starts new grace period
		mockTime.Advance(1 * time.Second)
		action = reconciler.computeSplitStatePhaseAction(
			shardID, shardStatus, splitState, readyShards,
		)
		assert.Nil(t, action)

		// Only 14s since re-ready: not enough
		mockTime.Advance(14 * time.Second)
		action = reconciler.computeSplitStatePhaseAction(
			shardID, shardStatus, splitState, readyShards,
		)
		assert.Nil(t, action, "Should not finalize — grace period restarted after leader loss")

		// 2 more seconds (total 16s since re-ready): should finalize
		mockTime.Advance(2 * time.Second)
		action = reconciler.computeSplitStatePhaseAction(
			shardID, shardStatus, splitState, readyShards,
		)
		assert.NotNil(t, action)
		assert.Equal(t, SplitStateActionFinalizeSplit, action.Action)
	})

	t.Run("PHASE_SPLITTING waits when new shard has snapshot but no leader", func(t *testing.T) {
		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{SplitTimeout: 5 * time.Minute},
			logger,
			RealTimeProvider{},
		)

		shardID := types.ID(1)
		newShardID := types.ID(2)
		now := time.Now()

		splitState := &db.SplitState{}
		splitState.SetPhase(db.SplitState_PHASE_SPLITTING)
		splitState.SetNewShardId(uint64(newShardID))
		splitState.SetStartedAtUnixNanos(now.UnixNano())

		shardStatus := &store.ShardStatus{
			ShardInfo: store.ShardInfo{
				SplitState: splitState,
			},
		}

		allShards := map[types.ID]*store.ShardStatus{
			newShardID: {
				ShardInfo: store.ShardInfo{
					HasSnapshot: true, // Has data
					// No RaftStatus / no leader — shard can't serve writes yet
				},
			},
		}

		action := reconciler.computeSplitStatePhaseAction(
			shardID,
			shardStatus,
			splitState,
			allShards,
		)

		assert.Nil(t, action, "Should wait for leader election before finalizing")
	})

	t.Run("PHASE_SPLITTING waits when new shard not ready", func(t *testing.T) {
		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{SplitTimeout: 5 * time.Minute},
			logger,
			RealTimeProvider{},
		)

		shardID := types.ID(1)
		newShardID := types.ID(2)
		now := time.Now()

		splitState := &db.SplitState{}
		splitState.SetPhase(db.SplitState_PHASE_SPLITTING)
		splitState.SetNewShardId(uint64(newShardID))
		splitState.SetStartedAtUnixNanos(now.UnixNano())

		shardStatus := &store.ShardStatus{
			ShardInfo: store.ShardInfo{
				SplitState: splitState,
			},
		}

		allShards := map[types.ID]*store.ShardStatus{
			newShardID: {
				ShardInfo: store.ShardInfo{
					HasSnapshot: false, // Not ready yet
				},
			},
		}

		action := reconciler.computeSplitStatePhaseAction(
			shardID,
			shardStatus,
			splitState,
			allShards,
		)

		assert.Nil(t, action, "Should wait for new shard to be ready")
	})

	t.Run("PHASE_SPLITTING infers new shard when split state child id is missing", func(t *testing.T) {
		mockTime := &MockTimeProvider{}
		now := time.Now()
		mockTime.Set(now)

		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{
				SplitTimeout:             5 * time.Minute,
				SplitFinalizeGracePeriod: time.Second,
			},
			logger,
			mockTime,
		)

		shardID := types.ID(1)
		newShardID := types.ID(2)
		splitKey := []byte("split-key")

		splitState := &db.SplitState{}
		splitState.SetPhase(db.SplitState_PHASE_SPLITTING)
		splitState.SetSplitKey(splitKey)
		splitState.SetStartedAtUnixNanos(now.UnixNano())

		shardStatus := &store.ShardStatus{
			ID:    shardID,
			Table: "docs",
			ShardInfo: store.ShardInfo{
				ShardConfig: store.ShardConfig{
					ByteRange: [2][]byte{{0x00}, splitKey},
				},
				SplitState: splitState,
			},
		}

		allShards := map[types.ID]*store.ShardStatus{
			shardID: shardStatus,
			newShardID: {
				ID:    newShardID,
				Table: "docs",
				State: store.ShardState_SplitOffPreSnap,
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{splitKey, {0xff}},
					},
					HasSnapshot:         true,
					SplitReplayRequired: true,
					SplitReplayCaughtUp: true,
					RaftStatus:          &common.RaftStatus{Lead: 10},
				},
			},
		}

		action := reconciler.computeSplitStatePhaseActionWithDesired(
			shardID,
			shardStatus,
			splitState,
			allShards,
			allShards,
		)
		assert.Nil(t, action, "Should start grace tracking before finalizing")

		mockTime.Advance(1100 * time.Millisecond)
		action = reconciler.computeSplitStatePhaseActionWithDesired(
			shardID,
			shardStatus,
			splitState,
			allShards,
			allShards,
		)
		assert.NotNil(t, action)
		assert.Equal(t, SplitStateActionFinalizeSplit, action.Action)
		assert.Equal(t, newShardID, action.NewShardID)
	})

	t.Run("PHASE_SPLITTING waits when new shard has snapshot but still initializing", func(t *testing.T) {
		// This test verifies the fix for flaky e2e tests where a shard could have
		// HasSnapshot=true (SnapshotIndex > 0) but still be Initializing=true
		// (hasn't finished loading indexes). Finalizing too early would cause
		// requests to fail while the new shard is still loading.
		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{SplitTimeout: 5 * time.Minute},
			logger,
			RealTimeProvider{},
		)

		shardID := types.ID(1)
		newShardID := types.ID(2)
		now := time.Now()

		splitState := &db.SplitState{}
		splitState.SetPhase(db.SplitState_PHASE_SPLITTING)
		splitState.SetNewShardId(uint64(newShardID))
		splitState.SetStartedAtUnixNanos(now.UnixNano())

		shardStatus := &store.ShardStatus{
			ShardInfo: store.ShardInfo{
				SplitState: splitState,
			},
		}

		allShards := map[types.ID]*store.ShardStatus{
			newShardID: {
				ShardInfo: store.ShardInfo{
					HasSnapshot:  true, // Has snapshot (SnapshotIndex > 0)
					Initializing: true, // But still initializing (loading indexes)
				},
			},
		}

		action := reconciler.computeSplitStatePhaseAction(
			shardID,
			shardStatus,
			splitState,
			allShards,
		)

		assert.Nil(t, action, "Should wait for new shard to finish initializing even if it has snapshot")
	})

	t.Run("PHASE_FINALIZING retries finalize once new shard can serve reads", func(t *testing.T) {
		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{SplitTimeout: 5 * time.Minute},
			logger,
			RealTimeProvider{},
		)

		shardID := types.ID(1)
		newShardID := types.ID(2)
		splitKey := []byte("split-key")
		now := time.Now()

		splitState := &db.SplitState{}
		splitState.SetPhase(db.SplitState_PHASE_FINALIZING)
		splitState.SetSplitKey(splitKey)
		splitState.SetNewShardId(uint64(newShardID))
		splitState.SetStartedAtUnixNanos(now.UnixNano())

		shardStatus := &store.ShardStatus{
			ShardInfo: store.ShardInfo{
				ShardConfig: store.ShardConfig{
					ByteRange: [2][]byte{{0x00}, splitKey},
				},
				SplitState: splitState,
			},
		}

		allShards := map[types.ID]*store.ShardStatus{
			newShardID: {
				ShardInfo: store.ShardInfo{
					HasSnapshot:         true,
					SplitReplayRequired: true,
					SplitReplayCaughtUp: true,
					SplitCutoverReady:   true,
					RaftStatus:          &common.RaftStatus{Lead: 10},
				},
			},
		}

		action := reconciler.computeSplitStatePhaseAction(
			shardID,
			shardStatus,
			splitState,
			allShards,
		)

		assert.NotNil(t, action)
		assert.Equal(t, SplitStateActionFinalizeSplit, action.Action)
		assert.Equal(t, newShardID, action.NewShardID)
	})

	t.Run("timeout triggers rollback", func(t *testing.T) {
		mockTime := &MockTimeProvider{}
		mockTime.Set(time.Now())

		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{SplitTimeout: 5 * time.Minute},
			logger,
			mockTime,
		)

		shardID := types.ID(1)
		originalRangeEnd := []byte{0xff}

		// Started 10 minutes ago (past the 5 minute timeout)
		startedAt := mockTime.Now().Add(-10 * time.Minute)

		splitState := &db.SplitState{}
		splitState.SetPhase(db.SplitState_PHASE_PREPARE)
		splitState.SetStartedAtUnixNanos(startedAt.UnixNano())
		splitState.SetSplitKey([]byte("split-key"))
		splitState.SetOriginalRangeEnd(originalRangeEnd)

		shardStatus := &store.ShardStatus{
			ShardInfo: store.ShardInfo{
				ShardConfig: store.ShardConfig{
					ByteRange: [2][]byte{{0x00}, {0x80}},
				},
				SplitState: splitState,
				Splitting:  true,
			},
		}

		action := reconciler.computeSplitStatePhaseAction(
			shardID,
			shardStatus,
			splitState,
			map[types.ID]*store.ShardStatus{},
		)

		assert.NotNil(t, action)
		assert.Equal(t, SplitStateActionRollback, action.Action)
		assert.Equal(t, shardID, action.ShardID)
		assert.Equal(t, originalRangeEnd, action.TargetRange[1])
	})

	t.Run("PHASE_NONE returns no action", func(t *testing.T) {
		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{},
			logger,
			RealTimeProvider{},
		)

		shardID := types.ID(1)

		splitState := &db.SplitState{}
		splitState.SetPhase(db.SplitState_PHASE_NONE)

		shardStatus := &store.ShardStatus{
			ShardInfo: store.ShardInfo{
				SplitState: splitState,
			},
		}

		action := reconciler.computeSplitStatePhaseAction(
			shardID,
			shardStatus,
			splitState,
			map[types.ID]*store.ShardStatus{},
		)

		assert.Nil(t, action)
	})

	t.Run("PHASE_ROLLING_BACK returns rollback action", func(t *testing.T) {
		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{},
			logger,
			RealTimeProvider{},
		)

		shardID := types.ID(1)
		originalRangeEnd := []byte{0xff}

		splitState := &db.SplitState{}
		splitState.SetPhase(db.SplitState_PHASE_ROLLING_BACK)
		splitState.SetSplitKey([]byte("split-key"))
		splitState.SetOriginalRangeEnd(originalRangeEnd)

		shardStatus := &store.ShardStatus{
			ShardInfo: store.ShardInfo{
				ShardConfig: store.ShardConfig{
					ByteRange: [2][]byte{{0x00}, {0x80}},
				},
				SplitState: splitState,
			},
		}

		action := reconciler.computeSplitStatePhaseAction(
			shardID,
			shardStatus,
			splitState,
			map[types.ID]*store.ShardStatus{},
		)

		assert.NotNil(t, action)
		assert.Equal(t, SplitStateActionRollback, action.Action)
		assert.Equal(t, shardID, action.ShardID)
	})
}

// TestSplitStatePreventsDuplicateSplit verifies that when a shard has an active
// SplitState (e.g., PHASE_SPLITTING waiting for new shard), the old PreSplit
// ShardState handler is skipped. Without this, the old path would try to
// re-split the shard using ByteRange[1] (already narrowed to the split key),
// causing "key out of range" errors.
func TestSplitStatePreventsDuplicateSplit(t *testing.T) {
	logger := zaptest.NewLogger(t)
	reconciler := NewReconcilerWithTimeProvider(
		nil, nil, nil,
		ReconciliationConfig{SplitTimeout: 5 * time.Minute},
		logger,
		RealTimeProvider{},
	)

	parentID := types.ID(1)
	splitOffID := types.ID(2)
	splitKey := []byte("split-key")
	now := time.Now()

	// Parent shard: has PreSplit ShardState AND active SplitState (PHASE_SPLITTING)
	// This happens after the split transition executes: SplitShard narrows the
	// range, sets SplitState to PHASE_SPLITTING, then ReassignShardsForSplit
	// sets ShardState to PreSplit.
	splitState := &db.SplitState{}
	splitState.SetPhase(db.SplitState_PHASE_SPLITTING)
	splitState.SetSplitKey(splitKey)
	splitState.SetNewShardId(uint64(splitOffID))
	splitState.SetStartedAtUnixNanos(now.UnixNano())

	desired := DesiredClusterState{
		Shards: map[types.ID]*store.ShardStatus{
			parentID: {
				ID:    parentID,
				Table: "test",
				State: store.ShardState_PreSplit,
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						// Range already narrowed by SplitShard
						ByteRange: [2][]byte{{0x00}, splitKey},
					},
					SplitState: splitState,
				},
			},
			splitOffID: {
				ID:    splitOffID,
				Table: "test",
				State: store.ShardState_SplittingOff,
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{splitKey, {0xFF}},
					},
					HasSnapshot: false, // Not ready yet
				},
			},
		},
	}

	actions := reconciler.computeSplitStateActions(CurrentClusterState{}, desired)

	// Should NOT produce a SplitStateActionSplit (which would try to re-split
	// with the already-narrowed ByteRange[1] and fail with "key out of range")
	for _, action := range actions {
		if action.ShardID == parentID {
			assert.NotEqual(t, SplitStateActionSplit, action.Action,
				"Active SplitState should prevent old PreSplit path from generating a duplicate split action")
		}
	}
}

// TestGetSplitTimeout tests the split timeout configuration
func TestGetSplitTimeout(t *testing.T) {
	logger := zaptest.NewLogger(t)

	t.Run("uses configured timeout", func(t *testing.T) {
		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{SplitTimeout: 10 * time.Minute},
			logger,
			RealTimeProvider{},
		)

		assert.Equal(t, 10*time.Minute, reconciler.getSplitTimeout())
	})

	t.Run("uses default timeout when not configured", func(t *testing.T) {
		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{SplitTimeout: 0}, // Not configured
			logger,
			RealTimeProvider{},
		)

		assert.Equal(t, 5*time.Minute, reconciler.getSplitTimeout())
	})
}

// TestIsSplitTimedOut tests the timeout detection
func TestIsSplitTimedOut(t *testing.T) {
	logger := zaptest.NewLogger(t)

	t.Run("returns false for nil split state", func(t *testing.T) {
		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{SplitTimeout: 5 * time.Minute},
			logger,
			RealTimeProvider{},
		)

		assert.False(t, reconciler.isSplitTimedOut(nil))
	})

	t.Run("returns false for zero started time", func(t *testing.T) {
		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{SplitTimeout: 5 * time.Minute},
			logger,
			RealTimeProvider{},
		)

		splitState := &db.SplitState{}
		splitState.SetStartedAtUnixNanos(0)

		assert.False(t, reconciler.isSplitTimedOut(splitState))
	})

	t.Run("returns true when timed out", func(t *testing.T) {
		mockTime := &MockTimeProvider{}
		mockTime.Set(time.Now())

		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{SplitTimeout: 5 * time.Minute},
			logger,
			mockTime,
		)

		startedAt := mockTime.Now().Add(-10 * time.Minute) // 10 minutes ago
		splitState := &db.SplitState{}
		splitState.SetStartedAtUnixNanos(startedAt.UnixNano())

		assert.True(t, reconciler.isSplitTimedOut(splitState))
	})

	t.Run("returns false when not timed out", func(t *testing.T) {
		mockTime := &MockTimeProvider{}
		mockTime.Set(time.Now())

		reconciler := NewReconcilerWithTimeProvider(
			nil, nil, nil,
			ReconciliationConfig{SplitTimeout: 5 * time.Minute},
			logger,
			mockTime,
		)

		startedAt := mockTime.Now().Add(-2 * time.Minute) // 2 minutes ago (within 5 min timeout)
		splitState := &db.SplitState{}
		splitState.SetStartedAtUnixNanos(startedAt.UnixNano())

		assert.False(t, reconciler.isSplitTimedOut(splitState))
	})
}

func TestFindCorrespondingSplittingShard(t *testing.T) {
	logger := zaptest.NewLogger(t)
	reconciler := NewReconciler(nil, nil, nil, ReconciliationConfig{}, logger)

	parentShardID := types.ID(1)
	splitOffShardID := types.ID(2)
	splitKey := []byte("M")

	parentStatus := &store.ShardStatus{
		ID:    parentShardID,
		Table: "test",
		State: store.ShardState_PreSplit,
		ShardInfo: store.ShardInfo{
			ShardConfig: store.ShardConfig{
				ByteRange: [2][]byte{{0x00}, splitKey},
			},
		},
	}

	t.Run("finds SplittingOff shard", func(t *testing.T) {
		allShards := map[types.ID]*store.ShardStatus{
			parentShardID: parentStatus,
			splitOffShardID: {
				ID:    splitOffShardID,
				Table: "test",
				State: store.ShardState_SplittingOff,
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{splitKey, {0xFF}},
					},
				},
			},
		}

		id, found := reconciler.findCorrespondingSplittingShard(parentShardID, parentStatus, allShards)
		assert.True(t, found)
		assert.Equal(t, splitOffShardID, id)
	})

	t.Run("finds SplitOffPreSnap shard", func(t *testing.T) {
		allShards := map[types.ID]*store.ShardStatus{
			parentShardID: parentStatus,
			splitOffShardID: {
				ID:    splitOffShardID,
				Table: "test",
				State: store.ShardState_SplitOffPreSnap,
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{splitKey, {0xFF}},
					},
					HasSnapshot: true,
				},
			},
		}

		id, found := reconciler.findCorrespondingSplittingShard(parentShardID, parentStatus, allShards)
		assert.True(t, found)
		assert.Equal(t, splitOffShardID, id)
	})

	t.Run("finds Default shard when ranges still indicate active split relationship", func(t *testing.T) {
		allShards := map[types.ID]*store.ShardStatus{
			parentShardID: parentStatus,
			splitOffShardID: {
				ID:    splitOffShardID,
				Table: "test",
				State: store.ShardState_Default,
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{splitKey, {0xFF}},
					},
				},
			},
		}

		id, found := reconciler.findCorrespondingSplittingShard(parentShardID, parentStatus, allShards)
		assert.True(t, found)
		assert.Equal(t, splitOffShardID, id)
	})

	t.Run("prefers split state new shard id when available", func(t *testing.T) {
		parentWithSplitState := parentStatus.DeepCopy()
		parentWithSplitState.SplitState = &db.SplitState{}
		parentWithSplitState.SplitState.SetPhase(db.SplitState_PHASE_SPLITTING)
		parentWithSplitState.SplitState.SetNewShardId(uint64(splitOffShardID))

		allShards := map[types.ID]*store.ShardStatus{
			parentShardID: parentWithSplitState,
			splitOffShardID: {
				ID:    splitOffShardID,
				Table: "test",
				State: store.ShardState_Default,
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{splitKey, {0xFF}},
					},
				},
			},
		}

		id, found := reconciler.findCorrespondingSplittingShard(parentShardID, parentWithSplitState, allShards)
		assert.True(t, found)
		assert.Equal(t, splitOffShardID, id)
	})

	t.Run("does not find shard with wrong range", func(t *testing.T) {
		allShards := map[types.ID]*store.ShardStatus{
			parentShardID: parentStatus,
			splitOffShardID: {
				ID:    splitOffShardID,
				Table: "test",
				State: store.ShardState_SplittingOff,
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{{0x50}, {0xFF}}, // wrong start key
					},
				},
			},
		}

		_, found := reconciler.findCorrespondingSplittingShard(parentShardID, parentStatus, allShards)
		assert.False(t, found)
	})

	t.Run("does not find shard from different table", func(t *testing.T) {
		allShards := map[types.ID]*store.ShardStatus{
			parentShardID: parentStatus,
			splitOffShardID: {
				ID:    splitOffShardID,
				Table: "other_table",
				State: store.ShardState_SplittingOff,
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{splitKey, {0xFF}},
					},
				},
			},
		}

		_, found := reconciler.findCorrespondingSplittingShard(parentShardID, parentStatus, allShards)
		assert.False(t, found)
	})

	t.Run("does not find Default shard when parent is not actively splitting", func(t *testing.T) {
		defaultParent := &store.ShardStatus{
			ID:    parentShardID,
			Table: "test",
			State: store.ShardState_Default,
			ShardInfo: store.ShardInfo{
				ShardConfig: store.ShardConfig{
					ByteRange: [2][]byte{{0x00}, splitKey},
				},
			},
		}
		allShards := map[types.ID]*store.ShardStatus{
			parentShardID: defaultParent,
			splitOffShardID: {
				ID:    splitOffShardID,
				Table: "test",
				State: store.ShardState_Default,
				ShardInfo: store.ShardInfo{
					ShardConfig: store.ShardConfig{
						ByteRange: [2][]byte{splitKey, {0xFF}},
					},
				},
			},
		}

		_, found := reconciler.findCorrespondingSplittingShard(parentShardID, defaultParent, allShards)
		assert.False(t, found)
	})
}
