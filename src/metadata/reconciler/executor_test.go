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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type stubStoreRPC struct {
	client.StoreRPC
	id        types.ID
	status    *store.StoreStatus
	statusErr error
}

func (s *stubStoreRPC) ID() types.ID {
	if s.id != 0 {
		return s.id
	}
	if s.StoreRPC != nil {
		return s.StoreRPC.ID()
	}
	return 0
}

func (s *stubStoreRPC) Status(ctx context.Context) (*store.StoreStatus, error) {
	return s.status, s.statusErr
}

func (s *stubStoreRPC) ApplyMergeChunk(
	ctx context.Context,
	shardID types.ID,
	writes [][2][]byte,
	deletes [][]byte,
	syncLevel db.Op_SyncLevel,
) error {
	if s.StoreRPC != nil {
		return s.StoreRPC.ApplyMergeChunk(ctx, shardID, writes, deletes, syncLevel)
	}
	return nil
}

func makeLiveMergeShardInfo(
	shardID types.ID,
	peers common.PeerSet,
	lead types.ID,
	diskSize uint64,
	empty bool,
) *store.ShardInfo {
	return &store.ShardInfo{
		Peers: peers,
		RaftStatus: &common.RaftStatus{
			Lead:   lead,
			Voters: peers,
		},
		ShardStats: &store.ShardStats{
			Storage: &store.StorageStats{
				DiskSize: diskSize,
				Empty:    empty,
			},
			Updated: time.Now(),
		},
	}
}

func TestExecutePlan_RemovedStoreCleanup(t *testing.T) {
	t.Run("removes peers from shards", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		removedStore := types.ID(100)
		shardID := types.ID(1)
		leaderClient := &client.StoreClient{}

		// Setup: Shard has the removed store as a voter
		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				shardID: {
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2, removedStore),
					},
				},
			},
		}

		plan := &ReconciliationPlan{
			RemovedStorePeerRemovals: []types.ID{removedStore},
		}

		// Expect GetLeaderClientForShard to be called
		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)

		// Expect RemovePeer to be called (sync removal for tombstoned stores)
		mockShardOps.On("RemovePeer", mock.Anything, shardID, leaderClient, removedStore, false).
			Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{ReplicationFactor: 2},
			zap.NewNop(),
		)
		err := reconciler.ExecutePlan(context.Background(), plan, current, DesiredClusterState{})
		assert.NoError(t, err)

		mockStoreOps.AssertExpectations(t)
		mockShardOps.AssertExpectations(t)
	})

	t.Run("skips shards without the removed store", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		removedStore := types.ID(100)
		shardID := types.ID(1)

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				shardID: {
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2, 3), // No removed store
					},
				},
			},
		}

		plan := &ReconciliationPlan{
			RemovedStorePeerRemovals: []types.ID{removedStore},
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{ReplicationFactor: 2},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(context.Background(), plan, current, DesiredClusterState{})
		assert.NoError(t, err)

		// Should not call any operations
		mockStoreOps.AssertNotCalled(t, "GetLeaderClientForShard")
		mockShardOps.AssertNotCalled(t, "RemovePeer")
	})

	t.Run("skips removal when removed store is the only voter", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		removedStore := types.ID(100)
		shardID := types.ID(1)

		// Setup: Shard has ONLY the removed store as a voter (replication factor 1)
		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				shardID: {
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(removedStore), // Only voter is the one being removed
					},
				},
			},
		}

		plan := &ReconciliationPlan{
			RemovedStorePeerRemovals: []types.ID{removedStore},
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{ReplicationFactor: 2},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(context.Background(), plan, current, DesiredClusterState{})
		assert.NoError(t, err)

		// Should NOT call RemovePeer because it would leave 0 voters
		mockStoreOps.AssertNotCalled(t, "GetLeaderClientForShard")
		mockShardOps.AssertNotCalled(t, "RemovePeer")
	})
}

func TestExecutePlan_RaftVoterFixes(t *testing.T) {
	t.Run("stops shards for non-voter peers", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerID := types.ID(2)

		plan := &ReconciliationPlan{
			RaftVoterFixes: []RaftVoterFix{
				{
					ShardID: shardID,
					PeerID:  peerID,
					Action:  RaftVoterActionStopShard,
				},
			},
		}

		mockShardOps.On("StopShard", mock.Anything, shardID, peerID).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)

		mockShardOps.AssertExpectations(t)
	})

	t.Run("continues on errors", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerID := types.ID(2)

		plan := &ReconciliationPlan{
			RaftVoterFixes: []RaftVoterFix{
				{
					ShardID: shardID,
					PeerID:  peerID,
					Action:  RaftVoterActionStopShard,
				},
			},
		}

		mockShardOps.On("StopShard", mock.Anything, shardID, peerID).
			Return(errors.New("test error"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err) // Should not return error, just log

		mockShardOps.AssertExpectations(t)
	})
}

func TestExecutePlan_IndexOperations(t *testing.T) {
	t.Run("adds indexes", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		indexName := "test_index"
		indexConfig := &indexes.IndexConfig{
			Type: indexes.IndexTypeFullTextV0,
		}

		plan := &ReconciliationPlan{
			IndexOperations: []IndexOperation{
				{
					ShardID:   shardID,
					IndexName: indexName,
					Operation: IndexOpAdd,
					Config:    indexConfig,
				},
			},
		}

		mockTableOps.On("AddIndex", mock.Anything, shardID, indexName, indexConfig).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)

		mockTableOps.AssertExpectations(t)
	})

	t.Run("drops indexes", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		indexName := "test_index"

		plan := &ReconciliationPlan{
			IndexOperations: []IndexOperation{
				{
					ShardID:   shardID,
					IndexName: indexName,
					Operation: IndexOpDrop,
				},
			},
		}

		mockTableOps.On("DropIndex", mock.Anything, shardID, indexName).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)

		mockTableOps.AssertExpectations(t)
	})

	t.Run("updates schema", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		schema := &schema.TableSchema{}

		plan := &ReconciliationPlan{
			IndexOperations: []IndexOperation{
				{
					ShardID:   shardID,
					Operation: IndexOpUpdateSchema,
					Schema:    schema,
				},
			},
		}

		mockTableOps.On("UpdateSchema", mock.Anything, shardID, schema).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)

		mockTableOps.AssertExpectations(t)
	})
}

func TestExecutePlan_SplitStateActions(t *testing.T) {
	t.Run("starts shard for SplittingOff state", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peers := types.IDSlice{1, 2, 3}
		config := &store.ShardConfig{
			ByteRange: [2][]byte{{0x00}, {0x80}},
		}

		plan := &ReconciliationPlan{
			SplitStateActions: []SplitStateAction{
				{
					ShardID:      shardID,
					Action:       SplitStateActionStartShard,
					PeersToStart: peers,
					ShardConfig:  config,
				},
			},
		}

		mockShardOps.On("StartShard", mock.Anything, shardID, peers, config, false, true).
			Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)

		mockShardOps.AssertExpectations(t)
	})

	t.Run("executes split for PreSplit state", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		newShardID := types.ID(2)
		splitKey := []byte{0x80}
		peers := types.IDSlice{1, 2, 3}
		config := &store.ShardConfig{
			ByteRange: [2][]byte{{0x00}, {0x80}},
		}

		plan := &ReconciliationPlan{
			SplitStateActions: []SplitStateAction{
				{
					ShardID:      shardID,
					Action:       SplitStateActionSplit,
					NewShardID:   newShardID,
					SplitKey:     splitKey,
					PeersToStart: peers,
					ShardConfig:  config,
				},
			},
		}

		mockShardOps.On("SplitShard", mock.Anything, shardID, newShardID, splitKey).Return(nil)
		mockStoreOps.On("UpdateShardSplitState", mock.Anything, shardID, mock.AnythingOfType("*db.SplitState")).Return(nil)
		mockShardOps.On("StartShard", mock.Anything, shardID, peers, config, false, true).
			Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)

		mockShardOps.AssertExpectations(t)
	})

	t.Run("executes merge for PreMerge state", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		targetRange := [2][]byte{{0x00}, {0xff}}

		plan := &ReconciliationPlan{
			SplitStateActions: []SplitStateAction{
				{
					ShardID:     shardID,
					Action:      SplitStateActionMerge,
					TargetRange: targetRange,
				},
			},
		}

		mockShardOps.On("MergeRange", mock.Anything, shardID, targetRange).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)

		// Verify cooldown was set
		assert.True(t, reconciler.IsShardInCooldown(shardID))

		mockShardOps.AssertExpectations(t)
	})

	t.Run("finalizes split and clears split state in metadata", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		newShardID := types.ID(2)
		splitKey := []byte{0x80}

		plan := &ReconciliationPlan{
			SplitStateActions: []SplitStateAction{
				{
					ShardID:    shardID,
					Action:     SplitStateActionFinalizeSplit,
					NewShardID: newShardID,
					SplitKey:   splitKey,
				},
			},
		}

		// Expect FinalizeSplit to be called
		mockShardOps.On("FinalizeSplit", mock.Anything, shardID, splitKey).Return(nil)

		// FinalizeSplit clears the parent split state once cleanup is complete.
		mockStoreOps.On("UpdateShardSplitState", mock.Anything, shardID, (*db.SplitState)(nil)).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)

		// Verify cooldown was set
		assert.True(t, reconciler.IsShardInCooldown(shardID))

		mockShardOps.AssertExpectations(t)
		mockStoreOps.AssertExpectations(t)
	})

	t.Run("continues if UpdateShardSplitState fails after FinalizeSplit", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		newShardID := types.ID(2)
		splitKey := []byte{0x80}

		plan := &ReconciliationPlan{
			SplitStateActions: []SplitStateAction{
				{
					ShardID:    shardID,
					Action:     SplitStateActionFinalizeSplit,
					NewShardID: newShardID,
					SplitKey:   splitKey,
				},
			},
		}

		// FinalizeSplit succeeds
		mockShardOps.On("FinalizeSplit", mock.Anything, shardID, splitKey).Return(nil)

		// UpdateShardSplitState fails but should not prevent completion
		mockStoreOps.On("UpdateShardSplitState", mock.Anything, shardID, mock.Anything).
			Return(errors.New("metadata update failed"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		// Should not return error - metadata update failure is logged but not fatal
		assert.NoError(t, err)

		// Cooldown should still be set
		assert.True(t, reconciler.IsShardInCooldown(shardID))

		mockShardOps.AssertExpectations(t)
		mockStoreOps.AssertExpectations(t)
	})
}

func TestExecutePlan_MergeTransitions(t *testing.T) {
	t.Run("executes merge transition", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		mergeShardID := types.ID(2)
		targetLeaderClient := &stubStoreRPC{
			id: shardID,
			status: &store.StoreStatus{
				ID: shardID,
				Shards: map[types.ID]*store.ShardInfo{
					shardID: makeLiveMergeShardInfo(shardID, common.NewPeerSet(10, 20), 10, 1024, false),
				},
			},
		}
		donorLeaderClient := &stubStoreRPC{
			id: mergeShardID,
			status: &store.StoreStatus{
				ID: mergeShardID,
				Shards: map[types.ID]*store.ShardInfo{
					mergeShardID: makeLiveMergeShardInfo(mergeShardID, common.NewPeerSet(10, 20), 10, 0, true),
				},
			},
		}
		peers := common.NewPeerSet(types.ID(10), types.ID(20))

		transition := tablemgr.MergeTransition{
			ShardID:      shardID,
			MergeShardID: mergeShardID,
			TableName:    "test",
		}

		newConfig := &store.ShardConfig{
			ByteRange: [2][]byte{{0x00}, {0xff}},
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				mergeShardID: {
					Peers: peers,
				},
			},
		}

		plan := &ReconciliationPlan{
			MergeTransitions: []tablemgr.MergeTransition{transition},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, mergeShardID).
			Return(donorLeaderClient, nil)
		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).
			Return(targetLeaderClient, nil)
		mockTableOps.On("ReassignShardsForMerge", transition).Return(newConfig, nil)
		mockShardOps.On("StopShard", mock.Anything, mergeShardID, mock.Anything).Return(nil)
		mockShardOps.On("MergeRange", mock.Anything, shardID, mock.MatchedBy(func(r [2][]byte) bool {
			return len(r[0]) == 1 && r[0][0] == 0x00 && len(r[1]) == 1 && r[1][0] == 0xff
		})).
			Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{ReplicationFactor: 2},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(context.Background(), plan, current, DesiredClusterState{})
		assert.NoError(t, err)

		mockStoreOps.AssertExpectations(t)
		mockTableOps.AssertExpectations(t)
		mockShardOps.AssertExpectations(t)
	})
}

func TestExecutePlan_SplitTransitions(t *testing.T) {
	t.Run("executes split transition", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		newShardID := types.ID(2)
		splitKey := []byte{0x80}
		leaderClient := &client.StoreClient{}
		peers := types.IDSlice{1, 2, 3}

		transition := tablemgr.SplitTransition{
			ShardID:      shardID,
			SplitShardID: newShardID,
			SplitKey:     splitKey,
			TableName:    "test",
		}

		newConfig := &store.ShardConfig{
			ByteRange: [2][]byte{{0x80}, {0xff}},
		}

		plan := &ReconciliationPlan{
			SplitTransitions: []tablemgr.SplitTransition{transition},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		// SplitShard is called first (before ReassignShardsForSplit) to prevent data loss
		mockShardOps.On("SplitShard", mock.Anything, shardID, newShardID, splitKey).Return(nil)
		// UpdateShardSplitState is called after SplitShard to immediately propagate the SplitState
		mockStoreOps.On("UpdateShardSplitState", mock.Anything, shardID, mock.AnythingOfType("*db.SplitState")).Return(nil)
		mockTableOps.On("ReassignShardsForSplit", transition).Return(peers, newConfig, nil)
		mockShardOps.On("StartShard", mock.Anything, newShardID, peers, newConfig, false, true).
			Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)

		mockStoreOps.AssertExpectations(t)
		mockTableOps.AssertExpectations(t)
		mockShardOps.AssertExpectations(t)
	})
}

func TestExecutePlan_SkipRemainingSteps(t *testing.T) {
	t.Run("skips execution when SkipRemainingSteps is true", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		plan := &ReconciliationPlan{
			SkipRemainingSteps: true,
			// Add some operations that should be skipped
			RaftVoterFixes: []RaftVoterFix{
				{ShardID: 1, PeerID: 2, Action: RaftVoterActionStopShard},
			},
			IndexOperations: []IndexOperation{
				{ShardID: 1, IndexName: "test", Operation: IndexOpAdd},
			},
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)

		// Verify no operations were called
		mockShardOps.AssertNotCalled(t, "StopShard")
		mockTableOps.AssertNotCalled(t, "AddIndex")
	})
}

func TestExecutePlan_Integration(t *testing.T) {
	t.Run("executes complete reconciliation plan", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		// Setup a complete plan with multiple operations
		removedStore := types.ID(100)
		shardID := types.ID(1)
		leaderClient := &client.StoreClient{}
		indexName := "test_index"
		indexConfig := &indexes.IndexConfig{Type: indexes.IndexTypeFullTextV0}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				shardID: {
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2, removedStore),
					},
				},
			},
		}

		plan := &ReconciliationPlan{
			RemovedStorePeerRemovals: []types.ID{removedStore},
			RaftVoterFixes: []RaftVoterFix{
				{ShardID: shardID, PeerID: 2, Action: RaftVoterActionStopShard},
			},
			IndexOperations: []IndexOperation{
				{
					ShardID:   shardID,
					IndexName: indexName,
					Operation: IndexOpAdd,
					Config:    indexConfig,
				},
			},
		}

		// Setup expectations
		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		mockShardOps.On("RemovePeer", mock.Anything, shardID, leaderClient, removedStore, false).
			Return(nil)
		mockShardOps.On("StopShard", mock.Anything, shardID, types.ID(2)).Return(nil)
		mockTableOps.On("AddIndex", mock.Anything, shardID, indexName, indexConfig).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(context.Background(), plan, current, DesiredClusterState{})
		assert.NoError(t, err)

		// Verify all operations were called
		mockStoreOps.AssertExpectations(t)
		mockShardOps.AssertExpectations(t)
		mockTableOps.AssertExpectations(t)
	})
}

func TestExecutePlan_ErrHandling(t *testing.T) {
	t.Run("continues after removed store cleanup error", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		removedStore := types.ID(100)
		shardID := types.ID(1)
		indexName := "test_index"
		indexConfig := &indexes.IndexConfig{Type: indexes.IndexTypeFullTextV0}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				shardID: {
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2, removedStore),
					},
				},
			},
		}

		plan := &ReconciliationPlan{
			RemovedStorePeerRemovals: []types.ID{removedStore},
			IndexOperations: []IndexOperation{
				{
					ShardID:   shardID,
					IndexName: indexName,
					Operation: IndexOpAdd,
					Config:    indexConfig,
				},
			},
		}

		// Setup removed store cleanup to fail
		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).
			Return(nil, errors.New("no leader"))
		// But index operation should still execute
		mockTableOps.On("AddIndex", mock.Anything, shardID, indexName, indexConfig).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(context.Background(), plan, current, DesiredClusterState{})
		assert.NoError(t, err) // Should not fail even though cleanup errored

		// Verify index operation was still called
		mockTableOps.AssertExpectations(t)
	})

	t.Run("continues after raft voter fixes error", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerID := types.ID(2)
		indexName := "test_index"
		indexConfig := &indexes.IndexConfig{Type: indexes.IndexTypeFullTextV0}

		plan := &ReconciliationPlan{
			RaftVoterFixes: []RaftVoterFix{
				{ShardID: shardID, PeerID: peerID, Action: RaftVoterActionStopShard},
			},
			IndexOperations: []IndexOperation{
				{
					ShardID:   shardID,
					IndexName: indexName,
					Operation: IndexOpAdd,
					Config:    indexConfig,
				},
			},
		}

		mockShardOps.On("StopShard", mock.Anything, shardID, peerID).
			Return(errors.New("network error"))
		mockTableOps.On("AddIndex", mock.Anything, shardID, indexName, indexConfig).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)

		mockTableOps.AssertExpectations(t)
	})

	t.Run("continues after index operations error", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)

		plan := &ReconciliationPlan{
			IndexOperations: []IndexOperation{
				{
					ShardID:   shardID,
					IndexName: "idx1",
					Operation: IndexOpAdd,
					Config:    &indexes.IndexConfig{},
				},
			},
			SplitStateActions: []SplitStateAction{
				{
					ShardID:     shardID,
					Action:      SplitStateActionMerge,
					TargetRange: [2][]byte{{0x00}, {0xff}},
				},
			},
		}

		mockTableOps.On("AddIndex", mock.Anything, shardID, "idx1", mock.Anything).
			Return(errors.New("failed to add"))
		mockShardOps.On("MergeRange", mock.Anything, shardID, mock.Anything).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)

		mockShardOps.AssertExpectations(t)
	})

	t.Run("continues after split state actions error", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID1 := types.ID(1)
		shardID2 := types.ID(2)

		plan := &ReconciliationPlan{
			SplitStateActions: []SplitStateAction{
				{
					ShardID:     shardID1,
					Action:      SplitStateActionMerge,
					TargetRange: [2][]byte{{0x00}, {0xff}},
				},
				{
					ShardID:     shardID2,
					Action:      SplitStateActionMerge,
					TargetRange: [2][]byte{{0x00}, {0xff}},
				},
			},
		}

		mockShardOps.On("MergeRange", mock.Anything, shardID1, mock.Anything).
			Return(errors.New("merge failed"))
		mockShardOps.On("MergeRange", mock.Anything, shardID2, mock.Anything).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)

		// Both should be called despite first one failing
		mockShardOps.AssertExpectations(t)
	})
}

func TestExecutePlan_ContextCancellation(t *testing.T) {
	t.Run("respects context cancellation during execution", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerID := types.ID(2)

		plan := &ReconciliationPlan{
			RaftVoterFixes: []RaftVoterFix{
				{ShardID: shardID, PeerID: peerID, Action: RaftVoterActionStopShard},
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		mockShardOps.On("StopShard", mock.Anything, shardID, peerID).
			Return(context.Canceled)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(ctx, plan, CurrentClusterState{}, DesiredClusterState{})
		// With the new context cancellation checks, we now properly propagate cancellation errors
		assert.ErrorIs(
			t,
			err,
			context.Canceled,
			"Should return context.Canceled when context is already cancelled",
		)
	})

	t.Run("context timeout during split transitions", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		newShardID := types.ID(2)
		splitKey := []byte{0x80}
		leaderClient := &client.StoreClient{}

		transition := tablemgr.SplitTransition{
			ShardID:      shardID,
			SplitShardID: newShardID,
			SplitKey:     splitKey,
			TableName:    "test",
		}

		plan := &ReconciliationPlan{
			SplitTransitions: []tablemgr.SplitTransition{transition},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		// SplitShard is called first and times out
		mockShardOps.On("SplitShard", mock.Anything, shardID, newShardID, splitKey).
			Run(func(args mock.Arguments) {
				time.Sleep(10 * time.Millisecond) // Simulate slow operation
			}).
			Return(context.DeadlineExceeded)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(ctx, plan, CurrentClusterState{}, DesiredClusterState{})
		// Error may or may not be returned depending on timing
		_ = err
	})
}

func TestExecutePlan_ConcurrentOperations(t *testing.T) {
	t.Run("executes multiple operations concurrently", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		// Create multiple operations to execute concurrently
		operations := []IndexOperation{}
		for i := 1; i <= 10; i++ {
			operations = append(operations, IndexOperation{
				ShardID:   types.ID(i),
				IndexName: "test_index",
				Operation: IndexOpAdd,
				Config:    &indexes.IndexConfig{Type: indexes.IndexTypeFullTextV0},
			})
		}

		plan := &ReconciliationPlan{
			IndexOperations: operations,
		}

		// All operations should be called
		for i := 1; i <= 10; i++ {
			mockTableOps.On("AddIndex", mock.Anything, types.ID(i), "test_index", mock.Anything).
				Return(nil)
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)

		mockTableOps.AssertExpectations(t)
	})

	t.Run("handles partial failures in concurrent operations", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		plan := &ReconciliationPlan{
			IndexOperations: []IndexOperation{
				{
					ShardID:   1,
					IndexName: "idx1",
					Operation: IndexOpAdd,
					Config:    &indexes.IndexConfig{},
				},
				{
					ShardID:   2,
					IndexName: "idx2",
					Operation: IndexOpAdd,
					Config:    &indexes.IndexConfig{},
				},
				{
					ShardID:   3,
					IndexName: "idx3",
					Operation: IndexOpAdd,
					Config:    &indexes.IndexConfig{},
				},
			},
		}

		mockTableOps.On("AddIndex", mock.Anything, types.ID(1), "idx1", mock.Anything).Return(nil)
		mockTableOps.On("AddIndex", mock.Anything, types.ID(2), "idx2", mock.Anything).
			Return(errors.New("failed"))
		mockTableOps.On("AddIndex", mock.Anything, types.ID(3), "idx3", mock.Anything).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err) // Should continue despite partial failures

		mockTableOps.AssertExpectations(t)
	})
}

func TestExecutePlan_FullIntegration(t *testing.T) {
	t.Run("executes complete plan with all operation types", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		removedStore := types.ID(100)
		shardID := types.ID(1)
		leaderClient := &client.StoreClient{}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				shardID: {
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2, removedStore),
					},
				},
			},
		}

		plan := &ReconciliationPlan{
			RemovedStorePeerRemovals: []types.ID{removedStore},
			RaftVoterFixes: []RaftVoterFix{
				{ShardID: shardID, PeerID: 2, Action: RaftVoterActionStopShard},
			},
			IndexOperations: []IndexOperation{
				{
					ShardID:   shardID,
					IndexName: "idx1",
					Operation: IndexOpAdd,
					Config:    &indexes.IndexConfig{Type: indexes.IndexTypeFullTextV0},
				},
			},
			SplitStateActions: []SplitStateAction{
				{
					ShardID:     shardID,
					Action:      SplitStateActionMerge,
					TargetRange: [2][]byte{{0x00}, {0xff}},
				},
			},
		}

		// Setup all expectations
		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		mockShardOps.On("RemovePeer", mock.Anything, shardID, leaderClient, removedStore, false).
			Return(nil)
		mockShardOps.On("StopShard", mock.Anything, shardID, types.ID(2)).Return(nil)
		mockTableOps.On("AddIndex", mock.Anything, shardID, "idx1", mock.Anything).Return(nil)
		mockShardOps.On("MergeRange", mock.Anything, shardID, mock.MatchedBy(func(r [2][]byte) bool {
			return len(r[0]) == 1 && r[0][0] == 0x00
		})).
			Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(context.Background(), plan, current, DesiredClusterState{})
		assert.NoError(t, err)

		// Verify all operations were called
		mockStoreOps.AssertExpectations(t)
		mockShardOps.AssertExpectations(t)
		mockTableOps.AssertExpectations(t)
	})

	t.Run("executes split and merge transitions with shard transitions", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		newShardID := types.ID(2)
		splitKey := []byte{0x80}
		leaderClient := &client.StoreClient{}
		peers := types.IDSlice{1, 2, 3}

		splitTransition := tablemgr.SplitTransition{
			ShardID:      shardID,
			SplitShardID: newShardID,
			SplitKey:     splitKey,
			TableName:    "test",
		}

		newConfig := &store.ShardConfig{ByteRange: [2][]byte{{0x80}, {0xff}}}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						ShardConfig: *newConfig,
						RaftStatus:  &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
					},
					State: store.ShardState_Default,
				},
				// Add new shard for the ShardTransitionPlan to work
				newShardID: {
					ShardInfo: store.ShardInfo{
						ShardConfig: *newConfig,
						RaftStatus:  &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
					},
					State: store.ShardState_Default,
				},
			},
		}

		plan := &ReconciliationPlan{
			SplitTransitions: []tablemgr.SplitTransition{splitTransition},
			ShardTransitions: &ShardTransitionPlan{
				Starts: []tablemgr.ShardTransition{
					{ShardID: newShardID, AddPeers: peers},
				},
			},
		}

		// Setup expectations for split (SplitShard first to prevent data loss)
		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		mockShardOps.On("SplitShard", mock.Anything, shardID, newShardID, splitKey).Return(nil)
		// UpdateShardSplitState is called after SplitShard to immediately propagate the SplitState
		mockStoreOps.On("UpdateShardSplitState", mock.Anything, shardID, mock.AnythingOfType("*db.SplitState")).Return(nil)
		mockTableOps.On("ReassignShardsForSplit", splitTransition).Return(peers, newConfig, nil)
		mockShardOps.On("StartShard", mock.Anything, newShardID, peers, newConfig, false, true).
			Return(nil)

		// Setup expectations for shard transition (start) - called after split
		mockShardOps.On("StartShard", mock.Anything, newShardID, peers, newConfig, false, false).
			Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(context.Background(), plan, CurrentClusterState{}, desired)
		assert.NoError(t, err)

		mockStoreOps.AssertExpectations(t)
		mockTableOps.AssertExpectations(t)
		mockShardOps.AssertExpectations(t)
	})
}

func TestExecuteSplitAndMergeTransitions_MergeOperations(t *testing.T) {
	t.Run("executes merge transition successfully", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		mergeShardID := types.ID(2)
		peer1 := types.ID(10)
		peer2 := types.ID(20)
		peer3 := types.ID(30)
		targetLeaderClient := &stubStoreRPC{
			id: shardID,
			status: &store.StoreStatus{
				ID: shardID,
				Shards: map[types.ID]*store.ShardInfo{
					shardID: makeLiveMergeShardInfo(shardID, common.NewPeerSet(peer1, peer2, peer3), peer1, 1024, false),
				},
			},
		}
		donorLeaderClient := &stubStoreRPC{
			id: mergeShardID,
			status: &store.StoreStatus{
				ID: mergeShardID,
				Shards: map[types.ID]*store.ShardInfo{
					mergeShardID: makeLiveMergeShardInfo(mergeShardID, common.NewPeerSet(peer1, peer2, peer3), peer1, 0, true),
				},
			},
		}

		mergeTransition := tablemgr.MergeTransition{
			ShardID:      shardID,
			MergeShardID: mergeShardID,
		}

		newShardConfig := &store.ShardConfig{
			ByteRange: types.Range{[]byte("a"), []byte("z")},
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				mergeShardID: {
					ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xFF}}},
					Peers:       common.NewPeerSet(peer1, peer2, peer3),
					RaftStatus:  &common.RaftStatus{Voters: common.NewPeerSet(peer1, peer2, peer3)},
				},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, mergeShardID).
			Return(donorLeaderClient, nil)
		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).
			Return(targetLeaderClient, nil)
		mockTableOps.On("ReassignShardsForMerge", mergeTransition).Return(newShardConfig, nil)
		mockShardOps.On("StopShard", mock.Anything, mergeShardID, peer1).Return(nil)
		mockShardOps.On("StopShard", mock.Anything, mergeShardID, peer2).Return(nil)
		mockShardOps.On("StopShard", mock.Anything, mergeShardID, peer3).Return(nil)
		mockShardOps.On("MergeRange", mock.Anything, shardID, mock.Anything).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{ReplicationFactor: 3},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeSplitAndMergeTransitions(
			ctx,
			eg,
			current,
			[]tablemgr.SplitTransition{},
			[]tablemgr.MergeTransition{mergeTransition},
		)
		err := eg.Wait()

		assert.NoError(t, err)
		mockStoreOps.AssertExpectations(t)
		mockTableOps.AssertExpectations(t)
		mockShardOps.AssertExpectations(t)

		// Verify cooldown was set
		assert.True(t, reconciler.IsShardInCooldown(mergeShardID))
	})

	t.Run("skips merge when no leader found", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		mergeShardID := types.ID(2)
		mergeTransition := tablemgr.MergeTransition{
			ShardID:      types.ID(1),
			MergeShardID: mergeShardID,
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, mergeShardID).
			Return(nil, errors.New("no leader"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{ReplicationFactor: 1},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeSplitAndMergeTransitions(
			ctx,
			eg,
			CurrentClusterState{},
			[]tablemgr.SplitTransition{},
			[]tablemgr.MergeTransition{mergeTransition},
		)
		err := eg.Wait()

		assert.NoError(t, err)
		mockStoreOps.AssertExpectations(t)
		mockTableOps.AssertNotCalled(t, "ReassignShardsForMerge")
	})

	t.Run("continues on ReassignShardsForMerge error", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		mergeShardID := types.ID(2)
		targetLeaderClient := &stubStoreRPC{
			id: shardID,
			status: &store.StoreStatus{
				ID: shardID,
				Shards: map[types.ID]*store.ShardInfo{
					shardID: makeLiveMergeShardInfo(shardID, common.NewPeerSet(10), 10, 1024, false),
				},
			},
		}
		donorLeaderClient := &stubStoreRPC{
			id: mergeShardID,
			status: &store.StoreStatus{
				ID: mergeShardID,
				Shards: map[types.ID]*store.ShardInfo{
					mergeShardID: makeLiveMergeShardInfo(mergeShardID, common.NewPeerSet(10), 10, 0, true),
				},
			},
		}

		mergeTransition := tablemgr.MergeTransition{
			ShardID:      shardID,
			MergeShardID: mergeShardID,
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				mergeShardID: {
					ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xFF}}},
					Peers:       common.NewPeerSet(10),
					RaftStatus:  &common.RaftStatus{Voters: common.NewPeerSet(10)},
				},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, mergeShardID).
			Return(donorLeaderClient, nil)
		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).
			Return(targetLeaderClient, nil)
		mockTableOps.On("ReassignShardsForMerge", mergeTransition).
			Return(nil, errors.New("reassignment failed"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{ReplicationFactor: 1},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeSplitAndMergeTransitions(
			ctx,
			eg,
			current,
			[]tablemgr.SplitTransition{},
			[]tablemgr.MergeTransition{mergeTransition},
		)
		err := eg.Wait()

		assert.NoError(t, err)
		mockStoreOps.AssertExpectations(t)
		mockTableOps.AssertExpectations(t)
		mockShardOps.AssertNotCalled(t, "StopShard")
		mockShardOps.AssertNotCalled(t, "MergeRange")
	})

	t.Run("ignores not found error when stopping shard for merge", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		mergeShardID := types.ID(2)
		peer1 := types.ID(10)
		targetLeaderClient := &stubStoreRPC{
			id: shardID,
			status: &store.StoreStatus{
				ID: shardID,
				Shards: map[types.ID]*store.ShardInfo{
					shardID: makeLiveMergeShardInfo(shardID, common.NewPeerSet(peer1), peer1, 1024, false),
				},
			},
		}
		donorLeaderClient := &stubStoreRPC{
			id: mergeShardID,
			status: &store.StoreStatus{
				ID: mergeShardID,
				Shards: map[types.ID]*store.ShardInfo{
					mergeShardID: makeLiveMergeShardInfo(mergeShardID, common.NewPeerSet(peer1), peer1, 0, true),
				},
			},
		}
		mergeTransition := tablemgr.MergeTransition{
			ShardID:      shardID,
			MergeShardID: mergeShardID,
		}

		newShardConfig := &store.ShardConfig{
			ByteRange: types.Range{[]byte("a"), []byte("z")},
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				mergeShardID: {
					ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xFF}}},
					Peers:       common.NewPeerSet(peer1),
					RaftStatus:  &common.RaftStatus{Voters: common.NewPeerSet(peer1)},
				},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, mergeShardID).
			Return(donorLeaderClient, nil)
		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).
			Return(targetLeaderClient, nil)
		mockTableOps.On("ReassignShardsForMerge", mergeTransition).Return(newShardConfig, nil)
		mockShardOps.On("StopShard", mock.Anything, mergeShardID, peer1).
			Return(fmt.Errorf("not found: %w", client.ErrNotFound))
		mockShardOps.On("MergeRange", mock.Anything, shardID, mock.Anything).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{ReplicationFactor: 1},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeSplitAndMergeTransitions(
			ctx,
			eg,
			current,
			[]tablemgr.SplitTransition{},
			[]tablemgr.MergeTransition{mergeTransition},
		)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
	})

	t.Run("continues on MergeRange error", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		mergeShardID := types.ID(2)
		peer1 := types.ID(10)
		targetLeaderClient := &stubStoreRPC{
			id: shardID,
			status: &store.StoreStatus{
				ID: shardID,
				Shards: map[types.ID]*store.ShardInfo{
					shardID: makeLiveMergeShardInfo(shardID, common.NewPeerSet(peer1), peer1, 1024, false),
				},
			},
		}
		donorLeaderClient := &stubStoreRPC{
			id: mergeShardID,
			status: &store.StoreStatus{
				ID: mergeShardID,
				Shards: map[types.ID]*store.ShardInfo{
					mergeShardID: makeLiveMergeShardInfo(mergeShardID, common.NewPeerSet(peer1), peer1, 0, true),
				},
			},
		}
		mergeTransition := tablemgr.MergeTransition{
			ShardID:      shardID,
			MergeShardID: mergeShardID,
		}

		newShardConfig := &store.ShardConfig{
			ByteRange: types.Range{[]byte("a"), []byte("z")},
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				mergeShardID: {
					ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xFF}}},
					Peers:       common.NewPeerSet(peer1),
					RaftStatus:  &common.RaftStatus{Voters: common.NewPeerSet(peer1)},
				},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, mergeShardID).
			Return(donorLeaderClient, nil)
		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).
			Return(targetLeaderClient, nil)
		mockTableOps.On("ReassignShardsForMerge", mergeTransition).Return(newShardConfig, nil)
		mockShardOps.On("StopShard", mock.Anything, mergeShardID, peer1).Return(nil)
		mockShardOps.On("MergeRange", mock.Anything, shardID, mock.Anything).
			Return(errors.New("merge failed"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{ReplicationFactor: 1},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeSplitAndMergeTransitions(
			ctx,
			eg,
			current,
			[]tablemgr.SplitTransition{},
			[]tablemgr.MergeTransition{mergeTransition},
		)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
	})

	t.Run("starts online merge when donor is not empty in live status", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		mergeShardID := types.ID(2)
		peer1 := types.ID(10)

		targetLeaderClient := &stubStoreRPC{
			id: shardID,
			status: &store.StoreStatus{
				ID: shardID,
				Shards: map[types.ID]*store.ShardInfo{
					shardID: makeLiveMergeShardInfo(shardID, common.NewPeerSet(peer1), peer1, 1024, false),
				},
			},
		}
		donorLeaderClient := &stubStoreRPC{
			id: mergeShardID,
			status: &store.StoreStatus{
				ID: mergeShardID,
				Shards: map[types.ID]*store.ShardInfo{
					mergeShardID: makeLiveMergeShardInfo(mergeShardID, common.NewPeerSet(peer1), peer1, 512, false),
				},
			},
		}

		mergeTransition := tablemgr.MergeTransition{
			ShardID:      shardID,
			MergeShardID: mergeShardID,
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				mergeShardID: {
					Peers:      common.NewPeerSet(peer1),
					RaftStatus: &common.RaftStatus{Lead: peer1, Voters: common.NewPeerSet(peer1)},
				},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, mergeShardID).
			Return(donorLeaderClient, nil)
		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).
			Return(targetLeaderClient, nil)
		mockTableOps.On("PrepareShardsForMerge", mergeTransition).
			Return(&store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0x80}}}, nil)
		mockShardOps.On(
			"SetMergeState",
			mock.Anything,
			mergeShardID,
			mock.MatchedBy(func(state *db.MergeState) bool {
				return state != nil &&
					state.GetPhase() == db.MergeState_PHASE_PREPARE &&
					state.GetDonorShardId() == uint64(mergeShardID) &&
					state.GetReceiverShardId() == uint64(shardID) &&
					state.GetCaptureDeltas() &&
					!state.GetAcceptDonorRange()
			}),
		).Return(nil)
		mockShardOps.On(
			"SetMergeState",
			mock.Anything,
			shardID,
			mock.MatchedBy(func(state *db.MergeState) bool {
				return state != nil &&
					state.GetPhase() == db.MergeState_PHASE_PREPARE &&
					state.GetDonorShardId() == uint64(mergeShardID) &&
					state.GetReceiverShardId() == uint64(shardID) &&
					!state.GetCaptureDeltas() &&
					state.GetAcceptDonorRange()
			}),
		).Return(nil)
		mockStoreOps.On("UpdateShardMergeState", mock.Anything, mergeShardID, mock.Anything).Return(nil)
		mockStoreOps.On("UpdateShardMergeState", mock.Anything, shardID, mock.Anything).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{ReplicationFactor: 1},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeSplitAndMergeTransitions(
			ctx,
			eg,
			current,
			[]tablemgr.SplitTransition{},
			[]tablemgr.MergeTransition{mergeTransition},
		)
		err := eg.Wait()

		assert.NoError(t, err)
		mockTableOps.AssertCalled(t, "PrepareShardsForMerge", mergeTransition)
		mockShardOps.AssertNotCalled(t, "StopShard")
		mockShardOps.AssertNotCalled(t, "MergeRange")
	})
}

func TestExecuteMergeRollback_ResetsMetadata(t *testing.T) {
	mockShardOps := &MockShardOperations{}
	mockTableOps := &MockTableOperations{}
	mockStoreOps := &MockStoreOperations{}

	receiverID := types.ID(1)
	donorID := types.ID(2)
	transition := tablemgr.MergeTransition{
		ShardID:      receiverID,
		MergeShardID: donorID,
		TableName:    "users",
	}

	receiverMergeState := &db.MergeState{}
	receiverMergeState.SetPhase(db.MergeState_PHASE_CATCHUP)
	receiverMergeState.SetDonorShardId(uint64(donorID))
	receiverMergeState.SetReceiverShardId(uint64(receiverID))
	receiverMergeState.SetAcceptDonorRange(true)

	donorMergeState := &db.MergeState{}
	donorMergeState.SetPhase(db.MergeState_PHASE_CATCHUP)
	donorMergeState.SetDonorShardId(uint64(donorID))
	donorMergeState.SetReceiverShardId(uint64(receiverID))

	mockStoreOps.On("GetShardStatuses").Return(map[types.ID]*store.ShardStatus{
		receiverID: {
			ID:    receiverID,
			Table: "users",
			State: store.ShardState_PreMerge,
			ShardInfo: store.ShardInfo{
				MergeState: receiverMergeState,
			},
		},
		donorID: {
			ID:    donorID,
			Table: "users",
			ShardInfo: store.ShardInfo{
				MergeState: donorMergeState,
			},
		},
	}, nil)
	mockShardOps.On(
		"SetMergeState",
		mock.Anything,
		receiverID,
		mock.MatchedBy(func(state *db.MergeState) bool {
			return state != nil && state.GetPhase() == db.MergeState_PHASE_ROLLING_BACK
		}),
	).Return(nil)
	mockShardOps.On("SetMergeState", mock.Anything, donorID, (*db.MergeState)(nil)).Return(nil)
	mockShardOps.On("SetMergeState", mock.Anything, receiverID, (*db.MergeState)(nil)).Return(nil)
	mockTableOps.On("RollbackShardsForMerge", transition).
		Return(&store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0x80}}}, nil)
	mockStoreOps.On("UpdateShardMergeState", mock.Anything, donorID, (*db.MergeState)(nil)).Return(nil)
	mockStoreOps.On("UpdateShardMergeState", mock.Anything, receiverID, (*db.MergeState)(nil)).Return(nil)

	reconciler := NewReconciler(
		mockShardOps,
		mockTableOps,
		mockStoreOps,
		ReconciliationConfig{ReplicationFactor: 1},
		zap.NewNop(),
	)

	err := reconciler.executeMergeRollback(context.Background(), receiverID, donorID)

	assert.NoError(t, err)
	mockStoreOps.AssertExpectations(t)
	mockShardOps.AssertExpectations(t)
	mockTableOps.AssertExpectations(t)
	assert.True(t, reconciler.IsShardInCooldown(receiverID))
	assert.True(t, reconciler.IsShardInCooldown(donorID))
}

func TestExecuteSplitAndMergeTransitions_SplitOperations(t *testing.T) {
	t.Run("executes split transition successfully", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		splitShardID := types.ID(2)
		leaderClient := &client.StoreClient{}
		peers := types.IDSlice{10, 20, 30}
		splitKey := []byte("m")

		splitTransition := tablemgr.SplitTransition{
			ShardID:      shardID,
			SplitShardID: splitShardID,
			SplitKey:     splitKey,
		}

		newShardConfig := &store.ShardConfig{
			ByteRange: types.Range{splitKey, []byte("z")},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		// SplitShard is called first (before ReassignShardsForSplit) to prevent data loss
		mockShardOps.On("SplitShard", mock.Anything, shardID, splitShardID, splitKey).Return(nil)
		// UpdateShardSplitState is called after SplitShard to immediately propagate the SplitState
		mockStoreOps.On("UpdateShardSplitState", mock.Anything, shardID, mock.AnythingOfType("*db.SplitState")).Return(nil)
		mockTableOps.On("ReassignShardsForSplit", splitTransition).
			Return(peers, newShardConfig, nil)
		mockShardOps.On("StartShard", mock.Anything, splitShardID, peers, newShardConfig, false, true).
			Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeSplitAndMergeTransitions(
			ctx,
			eg,
			CurrentClusterState{},
			[]tablemgr.SplitTransition{splitTransition},
			[]tablemgr.MergeTransition{},
		)
		err := eg.Wait()

		assert.NoError(t, err)
		mockStoreOps.AssertExpectations(t)
		mockTableOps.AssertExpectations(t)
		mockShardOps.AssertExpectations(t)
	})

	t.Run("skips split when no leader found", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		splitTransition := tablemgr.SplitTransition{
			ShardID:      shardID,
			SplitShardID: types.ID(2),
			SplitKey:     []byte("m"),
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).
			Return(nil, errors.New("no leader"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeSplitAndMergeTransitions(
			ctx,
			eg,
			CurrentClusterState{},
			[]tablemgr.SplitTransition{splitTransition},
			[]tablemgr.MergeTransition{},
		)
		err := eg.Wait()

		assert.NoError(t, err)
		mockStoreOps.AssertExpectations(t)
		mockShardOps.AssertNotCalled(t, "SplitShard")
		mockTableOps.AssertNotCalled(t, "ReassignShardsForSplit")
	})

	t.Run("continues on ReassignShardsForSplit error", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		splitShardID := types.ID(2)
		splitKey := []byte("m")
		leaderClient := &client.StoreClient{}
		splitTransition := tablemgr.SplitTransition{
			ShardID:      shardID,
			SplitShardID: splitShardID,
			SplitKey:     splitKey,
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		// SplitShard is called first, then ReassignShardsForSplit fails
		mockShardOps.On("SplitShard", mock.Anything, shardID, splitShardID, splitKey).Return(nil)
		// UpdateShardSplitState is called after SplitShard
		mockStoreOps.On("UpdateShardSplitState", mock.Anything, shardID, mock.AnythingOfType("*db.SplitState")).Return(nil)
		mockTableOps.On("ReassignShardsForSplit", splitTransition).
			Return(nil, nil, errors.New("reassignment failed"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeSplitAndMergeTransitions(
			ctx,
			eg,
			CurrentClusterState{},
			[]tablemgr.SplitTransition{splitTransition},
			[]tablemgr.MergeTransition{},
		)
		err := eg.Wait()

		assert.NoError(t, err)
		mockStoreOps.AssertExpectations(t)
		mockTableOps.AssertExpectations(t)
		mockShardOps.AssertExpectations(t)
		mockShardOps.AssertNotCalled(t, "StartShard")
	})

	t.Run("continues on SplitShard error", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		splitShardID := types.ID(2)
		leaderClient := &client.StoreClient{}
		splitKey := []byte("m")

		splitTransition := tablemgr.SplitTransition{
			ShardID:      shardID,
			SplitShardID: splitShardID,
			SplitKey:     splitKey,
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		// SplitShard is called first - when it fails, we return early
		// without calling ReassignShardsForSplit or StartShard
		mockShardOps.On("SplitShard", mock.Anything, shardID, splitShardID, splitKey).
			Return(errors.New("split failed"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeSplitAndMergeTransitions(
			ctx,
			eg,
			CurrentClusterState{},
			[]tablemgr.SplitTransition{splitTransition},
			[]tablemgr.MergeTransition{},
		)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
		mockTableOps.AssertNotCalled(t, "ReassignShardsForSplit")
		mockShardOps.AssertNotCalled(t, "StartShard")
	})

	t.Run("continues on StartShard error", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		splitShardID := types.ID(2)
		leaderClient := &client.StoreClient{}
		peers := types.IDSlice{10, 20, 30}
		splitKey := []byte("m")

		splitTransition := tablemgr.SplitTransition{
			ShardID:      shardID,
			SplitShardID: splitShardID,
			SplitKey:     splitKey,
		}

		newShardConfig := &store.ShardConfig{
			ByteRange: types.Range{splitKey, []byte("z")},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		// SplitShard first, then ReassignShardsForSplit, then StartShard fails
		mockShardOps.On("SplitShard", mock.Anything, shardID, splitShardID, splitKey).Return(nil)
		// UpdateShardSplitState is called after SplitShard
		mockStoreOps.On("UpdateShardSplitState", mock.Anything, shardID, mock.AnythingOfType("*db.SplitState")).Return(nil)
		mockTableOps.On("ReassignShardsForSplit", splitTransition).
			Return(peers, newShardConfig, nil)
		mockShardOps.On("StartShard", mock.Anything, splitShardID, peers, newShardConfig, false, true).
			Return(errors.New("start failed"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeSplitAndMergeTransitions(
			ctx,
			eg,
			CurrentClusterState{},
			[]tablemgr.SplitTransition{splitTransition},
			[]tablemgr.MergeTransition{},
		)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
	})
}

func TestExecuteSplitAndMergeTransitions_ConcurrentOperations(t *testing.T) {
	t.Run("executes multiple splits and merges concurrently", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		// Setup split transition
		shardID1 := types.ID(1)
		splitShardID := types.ID(2)
		leaderClient1 := &client.StoreClient{}
		peers := types.IDSlice{10, 20}
		splitKey := []byte("m")

		splitTransition := tablemgr.SplitTransition{
			ShardID:      shardID1,
			SplitShardID: splitShardID,
			SplitKey:     splitKey,
		}

		newShardConfig1 := &store.ShardConfig{
			ByteRange: types.Range{splitKey, []byte("z")},
		}

		// Setup merge transition
		shardID2 := types.ID(3)
		mergeShardID := types.ID(4)
		peer1 := types.ID(30)
		peer2 := types.ID(40)
		targetLeaderClient := &stubStoreRPC{
			id: shardID2,
			status: &store.StoreStatus{
				ID: shardID2,
				Shards: map[types.ID]*store.ShardInfo{
					shardID2: makeLiveMergeShardInfo(shardID2, common.NewPeerSet(peer1, peer2), peer1, 1024, false),
				},
			},
		}
		donorLeaderClient := &stubStoreRPC{
			id: mergeShardID,
			status: &store.StoreStatus{
				ID: mergeShardID,
				Shards: map[types.ID]*store.ShardInfo{
					mergeShardID: makeLiveMergeShardInfo(mergeShardID, common.NewPeerSet(peer1, peer2), peer1, 0, true),
				},
			},
		}

		mergeTransition := tablemgr.MergeTransition{
			ShardID:      shardID2,
			MergeShardID: mergeShardID,
		}

		newShardConfig2 := &store.ShardConfig{
			ByteRange: types.Range{[]byte("a"), []byte("z")},
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				mergeShardID: {
					ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xFF}}},
					Peers:       common.NewPeerSet(peer1, peer2),
					RaftStatus:  &common.RaftStatus{Voters: common.NewPeerSet(peer1, peer2)},
				},
			},
		}

		// Split expectations (SplitShard first to prevent data loss)
		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID1).
			Return(leaderClient1, nil)
		mockShardOps.On("SplitShard", mock.Anything, shardID1, splitShardID, splitKey).Return(nil)
		// UpdateShardSplitState is called after SplitShard
		mockStoreOps.On("UpdateShardSplitState", mock.Anything, shardID1, mock.AnythingOfType("*db.SplitState")).Return(nil)
		mockTableOps.On("ReassignShardsForSplit", splitTransition).
			Return(peers, newShardConfig1, nil)
		mockShardOps.On("StartShard", mock.Anything, splitShardID, peers, newShardConfig1, false, true).
			Return(nil)

		// Merge expectations
		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, mergeShardID).
			Return(donorLeaderClient, nil)
		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID2).
			Return(targetLeaderClient, nil)
		mockTableOps.On("ReassignShardsForMerge", mergeTransition).Return(newShardConfig2, nil)
		mockShardOps.On("StopShard", mock.Anything, mergeShardID, peer1).Return(nil)
		mockShardOps.On("StopShard", mock.Anything, mergeShardID, peer2).Return(nil)
		mockShardOps.On("MergeRange", mock.Anything, shardID2, mock.Anything).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{ReplicationFactor: 2},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeSplitAndMergeTransitions(
			ctx,
			eg,
			current,
			[]tablemgr.SplitTransition{splitTransition},
			[]tablemgr.MergeTransition{mergeTransition},
		)
		err := eg.Wait()

		assert.NoError(t, err)
		mockStoreOps.AssertExpectations(t)
		mockTableOps.AssertExpectations(t)
		mockShardOps.AssertExpectations(t)
	})
}

func TestExecutePlan_NilAndEmptyEdgeCases(t *testing.T) {
	t.Run("handles empty plan gracefully", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		plan := &ReconciliationPlan{
			RemovedStorePeerRemovals: []types.ID{},
			RaftVoterFixes:           []RaftVoterFix{},
			IndexOperations:          []IndexOperation{},
			SplitStateActions:        []SplitStateAction{},
			SplitTransitions:         []tablemgr.SplitTransition{},
			MergeTransitions:         []tablemgr.MergeTransition{},
			ShardTransitions:         nil,
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)
	})

	t.Run("handles nil ShardTransitions in plan", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		plan := &ReconciliationPlan{
			ShardTransitions: nil,
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(
			context.Background(),
			plan,
			CurrentClusterState{},
			DesiredClusterState{},
		)
		assert.NoError(t, err)
	})

	t.Run("handles empty current and desired states", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		plan := &ReconciliationPlan{
			ShardTransitions: &ShardTransitionPlan{
				Starts:      []tablemgr.ShardTransition{},
				Stops:       []tablemgr.ShardTransition{},
				Transitions: []tablemgr.ShardTransition{},
			},
		}

		current := CurrentClusterState{
			Shards:        map[types.ID]*store.ShardInfo{},
			RemovedStores: map[types.ID]struct{}{},
		}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{},
			Tables: map[string]*store.Table{},
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		err := reconciler.ExecutePlan(context.Background(), plan, current, desired)
		assert.NoError(t, err)
	})

	t.Run("handles shard with nil ShardInfo in current state", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(100)
		nodeID := types.ID(1)

		plan := &ReconciliationPlan{
			ShardTransitions: &ShardTransitionPlan{
				Starts: []tablemgr.ShardTransition{
					{ShardID: shardID, AddPeers: types.IDSlice{nodeID}},
				},
			},
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				shardID: nil, // Nil shard info
			},
		}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{},
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		// Should not crash
		err := reconciler.ExecutePlan(context.Background(), plan, current, desired)
		assert.NoError(t, err)

		// StartShard should not be called because shard info is nil in current state (no desired state)
		mockShardOps.AssertNotCalled(t, "StartShard")
	})

	t.Run("handles shard with nil RaftStatus in transitions", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(100)
		peerToAdd := types.ID(5)

		plan := &ReconciliationPlan{
			ShardTransitions: &ShardTransitionPlan{
				Transitions: []tablemgr.ShardTransition{
					{ShardID: shardID, AddPeers: types.IDSlice{peerToAdd}},
				},
			},
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				shardID: {
					ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xFF}}},
					RaftStatus:  nil, // Nil RaftStatus
				},
			},
		}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: *current.Shards[shardID],
					State:     store.ShardState_Default,
				},
			},
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		// Should not crash
		err := reconciler.ExecutePlan(context.Background(), plan, current, desired)
		assert.NoError(t, err)

		// AddPeer should not be called because RaftStatus is nil
		mockShardOps.AssertNotCalled(t, "AddPeer")
		mockStoreOps.AssertNotCalled(t, "GetLeaderClientForShard")
	})

	t.Run("handles missing shard in desired state for transitions", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(100)
		nodeID := types.ID(1)

		plan := &ReconciliationPlan{
			ShardTransitions: &ShardTransitionPlan{
				Starts: []tablemgr.ShardTransition{
					{ShardID: shardID, AddPeers: types.IDSlice{nodeID}},
				},
			},
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				shardID: {
					ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xFF}}},
					RaftStatus:  &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
				},
			},
		}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{},
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		// Should not crash
		err := reconciler.ExecutePlan(context.Background(), plan, current, desired)
		assert.NoError(t, err)

		// StartShard should not be called because shard not in desired state
		mockShardOps.AssertNotCalled(t, "StartShard")
	})

	t.Run("handles removed store cleanup with nil shard info", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		removedStore := types.ID(100)
		shardID := types.ID(1)

		plan := &ReconciliationPlan{
			RemovedStorePeerRemovals: []types.ID{removedStore},
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				shardID: nil, // Nil shard info
			},
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		// Should not crash
		err := reconciler.ExecutePlan(context.Background(), plan, current, DesiredClusterState{})
		assert.NoError(t, err)

		// No operations should be called
		mockShardOps.AssertNotCalled(t, "RemovePeer")
		mockStoreOps.AssertNotCalled(t, "GetLeaderClientForShard")
	})

	t.Run("handles removed store cleanup with nil RaftStatus", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		removedStore := types.ID(100)
		shardID := types.ID(1)

		plan := &ReconciliationPlan{
			RemovedStorePeerRemovals: []types.ID{removedStore},
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				shardID: {
					ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xFF}}},
					RaftStatus:  nil, // Nil RaftStatus
				},
			},
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		// Should not crash
		err := reconciler.ExecutePlan(context.Background(), plan, current, DesiredClusterState{})
		assert.NoError(t, err)

		// No operations should be called
		mockShardOps.AssertNotCalled(t, "RemovePeer")
		mockStoreOps.AssertNotCalled(t, "GetLeaderClientForShard")
	})
}
