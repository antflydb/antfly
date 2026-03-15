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
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func TestExecuteShardTransitionPlan_Starts(t *testing.T) {
	t.Run("starts new shard on nodes", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peers := types.IDSlice{1, 2, 3}
		config := &store.ShardConfig{
			ByteRange: [2][]byte{{0x00}, {0x80}},
		}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						ShardConfig: *config,
					},
					State: store.ShardState_Default,
				},
			},
		}

		plan := &ShardTransitionPlan{
			Starts: []tablemgr.ShardTransition{
				{ShardID: shardID, AddPeers: peers},
			},
		}

		mockShardOps.On("StartShard", mock.Anything, shardID, peers, config, false, false).
			Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
		assert.True(
			t,
			reconciler.IsShardInCooldown(shardID),
			"Shard should be in cooldown after start",
		)
	})

	t.Run("starts shard with SplitOffPreSnap state uses splitStart=true", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peers := types.IDSlice{1, 2, 3}
		config := &store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0x80}}}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{ShardConfig: *config},
					State:     store.ShardState_SplitOffPreSnap,
				},
			},
		}

		plan := &ShardTransitionPlan{
			Starts: []tablemgr.ShardTransition{{ShardID: shardID, AddPeers: peers}},
		}

		// Expect splitStart=true for SplitOffPreSnap state
		mockShardOps.On("StartShard", mock.Anything, shardID, peers, config, false, true).
			Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
	})

	t.Run("skips starting shard in SplittingOff state", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peers := types.IDSlice{1, 2, 3}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0x80}}},
					},
					State: store.ShardState_SplittingOff,
				},
			},
		}

		plan := &ShardTransitionPlan{
			Starts: []tablemgr.ShardTransition{{ShardID: shardID, AddPeers: peers}},
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertNotCalled(t, "StartShard")
	})

	t.Run("handles missing shard info gracefully", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peers := types.IDSlice{1, 2, 3}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{}, // Empty - no shard info
		}

		plan := &ShardTransitionPlan{
			Starts: []tablemgr.ShardTransition{{ShardID: shardID, AddPeers: peers}},
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertNotCalled(t, "StartShard")
	})

	t.Run("continues on StartShard error", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peers := types.IDSlice{1, 2, 3}
		config := &store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0x80}}}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{ShardConfig: *config},
					State:     store.ShardState_Default,
				},
			},
		}

		plan := &ShardTransitionPlan{
			Starts: []tablemgr.ShardTransition{{ShardID: shardID, AddPeers: peers}},
		}

		mockShardOps.On("StartShard", mock.Anything, shardID, peers, config, false, false).
			Return(errors.New("network error"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err) // Should not propagate error
		mockShardOps.AssertExpectations(t)
	})
}

func TestExecuteShardTransitionPlan_Stops(t *testing.T) {
	t.Run("stops shard on nodes", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		nodesToStop := types.IDSlice{10, 20}

		plan := &ShardTransitionPlan{
			Stops: []tablemgr.ShardTransition{
				{ShardID: shardID, RemovePeers: nodesToStop},
			},
		}

		mockShardOps.On("StopShard", mock.Anything, shardID, types.ID(10)).Return(nil)
		mockShardOps.On("StopShard", mock.Anything, shardID, types.ID(20)).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(
			ctx,
			eg,
			CurrentClusterState{},
			DesiredClusterState{},
			plan,
		)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
		assert.True(t, reconciler.IsShardInCooldown(shardID))
	})

	t.Run("ignores not found errors when stopping", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		nodeID := types.ID(10)

		plan := &ShardTransitionPlan{
			Stops: []tablemgr.ShardTransition{
				{ShardID: shardID, RemovePeers: types.IDSlice{nodeID}},
			},
		}

		mockShardOps.On("StopShard", mock.Anything, shardID, nodeID).
			Return(fmt.Errorf("shard not found: %w", client.ErrNotFound))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(
			ctx,
			eg,
			CurrentClusterState{},
			DesiredClusterState{},
			plan,
		)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
	})

	t.Run("logs other errors but continues", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		nodeID := types.ID(10)

		plan := &ShardTransitionPlan{
			Stops: []tablemgr.ShardTransition{
				{ShardID: shardID, RemovePeers: types.IDSlice{nodeID}},
			},
		}

		mockShardOps.On("StopShard", mock.Anything, shardID, nodeID).
			Return(errors.New("network timeout"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(
			ctx,
			eg,
			CurrentClusterState{},
			DesiredClusterState{},
			plan,
		)
		err := eg.Wait()

		assert.NoError(t, err) // Should not propagate error
		mockShardOps.AssertExpectations(t)
	})
}

func TestExecuteShardTransitionPlan_Transitions_AddPeers(t *testing.T) {
	t.Run("adds peer to shard and starts it", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToAdd := types.ID(10)
		leaderClient := &client.StoreClient{}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xff}}},
						RaftStatus: &common.RaftStatus{
							Voters: common.NewPeerSet(1, 2, 3),
						},
					},
					State: store.ShardState_Default,
				},
			},
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, AddPeers: types.IDSlice{peerToAdd}},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		mockShardOps.On("AddPeer", mock.Anything, shardID, leaderClient, peerToAdd).Return(nil)
		// Now expect StartShardOnNode with the new peer joining the existing voters
		// The voters list should include the new peer + all existing voters (4 total)
		mockShardOps.On("StartShardOnNode", mock.Anything, peerToAdd, shardID, mock.MatchedBy(func(peers types.IDSlice) bool {
			if len(peers) != 4 {
				return false
			}
			// Check that peerToAdd is in the list
			return slices.Contains(peers, peerToAdd)
		}), mock.Anything, false).
			Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
		mockStoreOps.AssertExpectations(t)
	})

	t.Run("adds peer to shard in SplittingOff state with splitStart=true", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToAdd := types.ID(10)
		leaderClient := &client.StoreClient{}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xff}}},
						RaftStatus:  &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
					},
					State: store.ShardState_SplittingOff,
				},
			},
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, AddPeers: types.IDSlice{peerToAdd}},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		mockShardOps.On("AddPeer", mock.Anything, shardID, leaderClient, peerToAdd).Return(nil)
		// Expect StartShardOnNode with splitStart=true for SplittingOff state
		mockShardOps.On("StartShardOnNode", mock.Anything, peerToAdd, shardID, mock.Anything, mock.Anything, true).
			Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
	})

	t.Run("sets cooldown after successfully adding peer", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToAdd := types.ID(10)
		leaderClient := &client.StoreClient{}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xff}}},
						RaftStatus:  &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
					},
					State: store.ShardState_Default,
				},
			},
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, AddPeers: types.IDSlice{peerToAdd}},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		mockShardOps.On("AddPeer", mock.Anything, shardID, leaderClient, peerToAdd).Return(nil)
		mockShardOps.On("StartShardOnNode", mock.Anything, peerToAdd, shardID, mock.Anything, mock.Anything, false).
			Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		// Verify no cooldown before
		assert.False(t, reconciler.IsShardForNodeInCooldown(shardID, peerToAdd))

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
		mockStoreOps.AssertExpectations(t)

		// Verify cooldown is now set after adding peer
		assert.True(t,
			reconciler.IsShardForNodeInCooldown(shardID, peerToAdd),
			"Cooldown should be set after successfully adding peer",
		)
	})

	t.Run("skips adding peer in cooldown", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToAdd := types.ID(10)
		leaderClient := &client.StoreClient{}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xff}}},
						RaftStatus:  &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
					},
				},
			},
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, AddPeers: types.IDSlice{peerToAdd}},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)
		reconciler.SetShardForNodeCooldown(shardID, peerToAdd, time.Hour) // Set cooldown

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertNotCalled(t, "AddPeer")
		mockStoreOps.AssertExpectations(t)
	})

	t.Run("handles AddPeer error gracefully", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToAdd := types.ID(10)
		leaderClient := &client.StoreClient{}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xff}}},
						RaftStatus:  &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
					},
				},
			},
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, AddPeers: types.IDSlice{peerToAdd}},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		mockShardOps.On("AddPeer", mock.Anything, shardID, leaderClient, peerToAdd).
			Return(errors.New("raft proposal failed"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err) // Should not propagate error
		mockShardOps.AssertExpectations(t)
	})

	t.Run("handles unreachable store when starting shard", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToAdd := types.ID(10)
		leaderClient := &client.StoreClient{}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xff}}},
						RaftStatus:  &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
					},
				},
			},
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, AddPeers: types.IDSlice{peerToAdd}},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		mockShardOps.On("AddPeer", mock.Anything, shardID, leaderClient, peerToAdd).Return(nil)
		// StartShardOnNode will be called and should return an error about unreachable node
		mockShardOps.On("StartShardOnNode", mock.Anything, peerToAdd, shardID, mock.Anything, mock.Anything, false).
			Return(errors.New("node not reachable"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		// Error is logged but not propagated (continues reconciling other shards)
		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
	})

	t.Run("ignores already exists error when starting shard", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToAdd := types.ID(10)
		leaderClient := &client.StoreClient{}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						ShardConfig: store.ShardConfig{ByteRange: [2][]byte{{0x00}, {0xff}}},
						RaftStatus:  &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
					},
				},
			},
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, AddPeers: types.IDSlice{peerToAdd}},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		mockShardOps.On("AddPeer", mock.Anything, shardID, leaderClient, peerToAdd).Return(nil)
		// Expect StartShardOnNode to return "already exists" error
		mockShardOps.On("StartShardOnNode", mock.Anything, peerToAdd, shardID, mock.Anything, mock.Anything, false).
			Return(errors.New("shard already exists"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err) // Should ignore "already exists" error
		mockShardOps.AssertExpectations(t)
	})
}

func TestExecuteShardTransitionPlan_Transitions_RemovePeers(t *testing.T) {
	t.Run("removes peer from shard and stops it", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToRemove := types.ID(10)
		leaderClient := &client.StoreClient{}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						RaftStatus: &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
					},
				},
			},
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, RemovePeers: types.IDSlice{peerToRemove}},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		mockShardOps.On("RemovePeer", mock.Anything, shardID, leaderClient, peerToRemove, true).
			Return(nil)
		mockShardOps.On("IsIDRemoved", mock.Anything, shardID, leaderClient, peerToRemove).
			Return(true, nil)
		mockShardOps.On("StopShard", mock.Anything, shardID, peerToRemove).Return(nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
		assert.True(
			t,
			reconciler.IsShardForNodeInCooldown(shardID, peerToRemove),
			"Should set cooldown after removal",
		)
	})

	t.Run("skips removing peer from removed stores", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToRemove := types.ID(10)
		leaderClient := &client.StoreClient{}

		current := CurrentClusterState{
			RemovedStores: map[types.ID]struct{}{
				peerToRemove: {}, // Peer is in removed stores
			},
		}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						RaftStatus: &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
					},
				},
			},
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, RemovePeers: types.IDSlice{peerToRemove}},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, current, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertNotCalled(t, "RemovePeer")
		mockStoreOps.AssertExpectations(t)
	})

	t.Run("handles RemovePeer error gracefully", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToRemove := types.ID(10)
		leaderClient := &client.StoreClient{}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						RaftStatus: &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
					},
				},
			},
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, RemovePeers: types.IDSlice{peerToRemove}},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		mockShardOps.On("RemovePeer", mock.Anything, shardID, leaderClient, peerToRemove, true).
			Return(errors.New("proposal timeout"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err) // Should not propagate error
		mockShardOps.AssertExpectations(t)
	})

	t.Run("handles IsIDRemoved error gracefully", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			mockShardOps := &MockShardOperations{}
			mockTableOps := &MockTableOperations{}
			mockStoreOps := &MockStoreOperations{}

			shardID := types.ID(1)
			peerToRemove := types.ID(10)
			leaderClient := &client.StoreClient{}

			desired := DesiredClusterState{
				Shards: map[types.ID]*store.ShardStatus{
					shardID: {
						ShardInfo: store.ShardInfo{
							RaftStatus: &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
						},
					},
				},
			}

			plan := &ShardTransitionPlan{
				Transitions: []tablemgr.ShardTransition{
					{ShardID: shardID, RemovePeers: types.IDSlice{peerToRemove}},
				},
			}

			mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).
				Return(leaderClient, nil)
			mockShardOps.On("RemovePeer", mock.Anything, shardID, leaderClient, peerToRemove, true).
				Return(nil)
			mockShardOps.On("IsIDRemoved", mock.Anything, shardID, leaderClient, peerToRemove).
				Return(false, errors.New("connection lost"))

			reconciler := NewReconciler(
				mockShardOps,
				mockTableOps,
				mockStoreOps,
				ReconciliationConfig{},
				zap.NewNop(),
			)

			eg, ctx := errgroup.WithContext(context.Background())
			reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
			err := eg.Wait()

			assert.NoError(t, err) // Should not propagate error
			mockShardOps.AssertExpectations(t)
			mockShardOps.AssertNotCalled(
				t,
				"StopShard",
			) // Should not call StopShard if IsIDRemoved fails
		})
	})

	t.Run("does not stop shard if peer not removed", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToRemove := types.ID(10)
		leaderClient := &client.StoreClient{}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						RaftStatus: &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
					},
				},
			},
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, RemovePeers: types.IDSlice{peerToRemove}},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		mockShardOps.On("RemovePeer", mock.Anything, shardID, leaderClient, peerToRemove, true).
			Return(nil)
		mockShardOps.On("IsIDRemoved", mock.Anything, shardID, leaderClient, peerToRemove).
			Return(false, nil)
			// Not removed yet

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
		mockShardOps.AssertNotCalled(t, "StopShard")
	})

	t.Run("ignores not found error when stopping", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToRemove := types.ID(10)
		leaderClient := &client.StoreClient{}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						RaftStatus: &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
					},
				},
			},
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, RemovePeers: types.IDSlice{peerToRemove}},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).Return(leaderClient, nil)
		mockShardOps.On("RemovePeer", mock.Anything, shardID, leaderClient, peerToRemove, true).
			Return(nil)
		mockShardOps.On("IsIDRemoved", mock.Anything, shardID, leaderClient, peerToRemove).
			Return(true, nil)
		mockShardOps.On("StopShard", mock.Anything, shardID, peerToRemove).
			Return(fmt.Errorf("not found: %w", client.ErrNotFound))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		mockShardOps.AssertExpectations(t)
		assert.True(t, reconciler.IsShardForNodeInCooldown(shardID, peerToRemove))
	})
}

func TestExecuteShardTransitionPlan_EdgeCases(t *testing.T) {
	t.Run("handles missing shard info in transitions", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToAdd := types.ID(10)

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{}, // Empty - no shard info
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, AddPeers: types.IDSlice{peerToAdd}},
			},
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		// Should not process transition at all
	})

	t.Run("handles missing raft status in transitions", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToAdd := types.ID(10)

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						RaftStatus: nil, // No raft status
					},
				},
			},
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, AddPeers: types.IDSlice{peerToAdd}},
			},
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		// Should not process transition
	})

	t.Run("handles leader not found", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		shardID := types.ID(1)
		peerToAdd := types.ID(10)

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				shardID: {
					ShardInfo: store.ShardInfo{
						RaftStatus: &common.RaftStatus{Voters: common.NewPeerSet(1, 2, 3)},
					},
				},
			},
		}

		plan := &ShardTransitionPlan{
			Transitions: []tablemgr.ShardTransition{
				{ShardID: shardID, AddPeers: types.IDSlice{peerToAdd}},
			},
		}

		mockStoreOps.On("GetLeaderClientForShard", mock.Anything, shardID).
			Return(nil, errors.New("no leader elected"))

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(ctx, eg, CurrentClusterState{}, desired, plan)
		err := eg.Wait()

		assert.NoError(t, err)
		mockStoreOps.AssertExpectations(t)
		mockShardOps.AssertNotCalled(t, "AddPeer")
	})

	t.Run("processes empty plan without errors", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		plan := &ShardTransitionPlan{
			Starts:      []tablemgr.ShardTransition{},
			Stops:       []tablemgr.ShardTransition{},
			Transitions: []tablemgr.ShardTransition{},
		}

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{},
			zap.NewNop(),
		)

		eg, ctx := errgroup.WithContext(context.Background())
		reconciler.executeShardTransitionPlan(
			ctx,
			eg,
			CurrentClusterState{},
			DesiredClusterState{},
			plan,
		)
		err := eg.Wait()

		assert.NoError(t, err)
	})
}
