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
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// ============================================================================
// MOCK IMPLEMENTATIONS - For testing
// ============================================================================

// MockShardOperations is a mock implementation of ShardOperations
type MockShardOperations struct {
	mock.Mock
}

func (m *MockShardOperations) StartShard(
	ctx context.Context,
	shardID types.ID,
	peers types.IDSlice,
	config *store.ShardConfig,
	join, splitStart bool,
) error {
	args := m.Called(ctx, shardID, peers, config, join, splitStart)
	return args.Error(0)
}

func (m *MockShardOperations) StartShardOnNode(
	ctx context.Context,
	nodeID types.ID,
	shardID types.ID,
	existingPeers types.IDSlice,
	config *store.ShardConfig,
	splitStart bool,
) error {
	args := m.Called(ctx, nodeID, shardID, existingPeers, config, splitStart)
	return args.Error(0)
}

func (m *MockShardOperations) StopShard(
	ctx context.Context,
	shardID types.ID,
	nodeID types.ID,
) error {
	args := m.Called(ctx, shardID, nodeID)
	return args.Error(0)
}

func (m *MockShardOperations) PrepareSplit(
	ctx context.Context,
	shardID types.ID,
	splitKey []byte,
) error {
	args := m.Called(ctx, shardID, splitKey)
	return args.Error(0)
}

func (m *MockShardOperations) SplitShard(
	ctx context.Context,
	shardID, newShardID types.ID,
	splitKey []byte,
) error {
	args := m.Called(ctx, shardID, newShardID, splitKey)
	return args.Error(0)
}

func (m *MockShardOperations) FinalizeSplit(
	ctx context.Context,
	shardID types.ID,
	newRangeEnd []byte,
) error {
	args := m.Called(ctx, shardID, newRangeEnd)
	return args.Error(0)
}

func (m *MockShardOperations) MergeRange(
	ctx context.Context,
	shardID types.ID,
	byteRange [2][]byte,
) error {
	args := m.Called(ctx, shardID, byteRange)
	return args.Error(0)
}

func (m *MockShardOperations) SetMergeState(
	ctx context.Context,
	shardID types.ID,
	mergeState *db.MergeState,
) error {
	for _, call := range m.ExpectedCalls {
		if call.Method == "SetMergeState" {
			args := m.Called(ctx, shardID, mergeState)
			return args.Error(0)
		}
	}
	return nil
}

func (m *MockShardOperations) FinalizeMerge(
	ctx context.Context,
	shardID types.ID,
	byteRange [2][]byte,
) error {
	for _, call := range m.ExpectedCalls {
		if call.Method == "FinalizeMerge" {
			args := m.Called(ctx, shardID, byteRange)
			return args.Error(0)
		}
	}
	return nil
}

func (m *MockShardOperations) ExportRangeChunk(
	ctx context.Context,
	shardID types.ID,
	startKey, endKey, afterKey []byte,
	limit int,
) ([][2][]byte, []byte, bool, error) {
	for _, call := range m.ExpectedCalls {
		if call.Method == "ExportRangeChunk" {
			args := m.Called(ctx, shardID, startKey, endKey, afterKey, limit)
			var writes [][2][]byte
			if args.Get(0) != nil {
				writes = args.Get(0).([][2][]byte)
			}
			var nextKey []byte
			if args.Get(1) != nil {
				nextKey = args.Get(1).([]byte)
			}
			return writes, nextKey, args.Bool(2), args.Error(3)
		}
	}
	return nil, nil, true, nil
}

func (m *MockShardOperations) ListMergeDeltaEntriesAfter(
	ctx context.Context,
	shardID types.ID,
	afterSeq uint64,
) ([]*db.MergeDeltaEntry, error) {
	for _, call := range m.ExpectedCalls {
		if call.Method == "ListMergeDeltaEntriesAfter" {
			args := m.Called(ctx, shardID, afterSeq)
			if args.Get(0) == nil {
				return nil, args.Error(1)
			}
			return args.Get(0).([]*db.MergeDeltaEntry), args.Error(1)
		}
	}
	return nil, nil
}

func (m *MockShardOperations) RollbackSplit(
	ctx context.Context,
	shardID types.ID,
) error {
	args := m.Called(ctx, shardID)
	return args.Error(0)
}

func (m *MockShardOperations) AddPeer(
	ctx context.Context,
	shardID types.ID,
	leaderClient client.StoreRPC,
	newPeerID types.ID,
) error {
	args := m.Called(ctx, shardID, leaderClient, newPeerID)
	return args.Error(0)
}

func (m *MockShardOperations) RemovePeer(
	ctx context.Context,
	shardID types.ID,
	leaderClient client.StoreRPC,
	peerToRemoveID types.ID,
	async bool,
) error {
	args := m.Called(ctx, shardID, leaderClient, peerToRemoveID, async)
	return args.Error(0)
}

func (m *MockShardOperations) GetMedianKey(ctx context.Context, shardID types.ID) ([]byte, error) {
	args := m.Called(ctx, shardID)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockShardOperations) IsIDRemoved(
	ctx context.Context,
	shardID types.ID,
	leaderClient client.StoreRPC,
	peerID types.ID,
) (bool, error) {
	args := m.Called(ctx, shardID, leaderClient, peerID)
	return args.Bool(0), args.Error(1)
}

// MockTableOperations is a mock implementation of TableOperations
type MockTableOperations struct {
	mock.Mock
}

func (m *MockTableOperations) AddIndex(
	ctx context.Context,
	shardID types.ID,
	name string,
	config *indexes.IndexConfig,
) error {
	args := m.Called(ctx, shardID, name, config)
	return args.Error(0)
}

func (m *MockTableOperations) DropIndex(ctx context.Context, shardID types.ID, name string) error {
	args := m.Called(ctx, shardID, name)
	return args.Error(0)
}

func (m *MockTableOperations) UpdateSchema(
	ctx context.Context,
	shardID types.ID,
	schema *schema.TableSchema,
) error {
	args := m.Called(ctx, shardID, schema)
	return args.Error(0)
}

func (m *MockTableOperations) ReassignShardsForSplit(
	transition tablemgr.SplitTransition,
) (types.IDSlice, *store.ShardConfig, error) {
	args := m.Called(transition)
	if args.Get(0) == nil || args.Get(1) == nil {
		return nil, nil, args.Error(2)
	}
	return args.Get(0).(types.IDSlice), args.Get(1).(*store.ShardConfig), args.Error(2)
}

func (m *MockTableOperations) RollbackShardsForSplit(
	transition tablemgr.SplitTransition,
) (*store.ShardConfig, error) {
	args := m.Called(transition)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*store.ShardConfig), args.Error(1)
}

func (m *MockTableOperations) ReassignShardsForMerge(
	transition tablemgr.MergeTransition,
) (*store.ShardConfig, error) {
	args := m.Called(transition)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*store.ShardConfig), args.Error(1)
}

func (m *MockTableOperations) PrepareShardsForMerge(
	transition tablemgr.MergeTransition,
) (*store.ShardConfig, error) {
	for _, call := range m.ExpectedCalls {
		if call.Method == "PrepareShardsForMerge" {
			args := m.Called(transition)
			if args.Get(0) == nil {
				return nil, args.Error(1)
			}
			return args.Get(0).(*store.ShardConfig), args.Error(1)
		}
	}
	return &store.ShardConfig{}, nil
}

func (m *MockTableOperations) FinalizeShardsForMerge(
	transition tablemgr.MergeTransition,
) (*store.ShardConfig, error) {
	args := m.Called(transition)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*store.ShardConfig), args.Error(1)
}

func (m *MockTableOperations) RollbackShardsForMerge(
	transition tablemgr.MergeTransition,
) (*store.ShardConfig, error) {
	for _, call := range m.ExpectedCalls {
		if call.Method == "RollbackShardsForMerge" {
			args := m.Called(transition)
			if args.Get(0) == nil {
				return nil, args.Error(1)
			}
			return args.Get(0).(*store.ShardConfig), args.Error(1)
		}
	}
	return &store.ShardConfig{}, nil
}

func (m *MockTableOperations) HasReallocationRequest(ctx context.Context) (bool, error) {
	args := m.Called(ctx)
	return args.Bool(0), args.Error(1)
}

func (m *MockTableOperations) ClearReallocationRequest(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockTableOperations) DropReadSchema(tableName string) error {
	args := m.Called(tableName)
	return args.Error(0)
}

func (m *MockTableOperations) GetTablesWithReadSchemas() ([]*store.Table, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*store.Table), args.Error(1)
}

func (m *MockTableOperations) DeleteTombstones(ctx context.Context, storeIDs []types.ID) error {
	args := m.Called(ctx, storeIDs)
	return args.Error(0)
}

// MockStoreOperations is a mock implementation of StoreOperations
type MockStoreOperations struct {
	mock.Mock
}

func (m *MockStoreOperations) GetStoreClient(
	ctx context.Context,
	nodeID types.ID,
) (client.StoreRPC, bool, error) {
	args := m.Called(ctx, nodeID)
	if args.Get(0) == nil {
		return nil, args.Bool(1), args.Error(2)
	}
	return args.Get(0).(client.StoreRPC), args.Bool(1), args.Error(2)
}

func (m *MockStoreOperations) GetStoreStatus(
	ctx context.Context,
	nodeID types.ID,
) (*tablemgr.StoreStatus, error) {
	args := m.Called(ctx, nodeID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*tablemgr.StoreStatus), args.Error(1)
}

func (m *MockStoreOperations) GetShardStatuses() (map[types.ID]*store.ShardStatus, error) {
	args := m.Called()
	return args.Get(0).(map[types.ID]*store.ShardStatus), args.Error(1)
}

func (m *MockStoreOperations) GetLeaderClientForShard(
	ctx context.Context,
	shardID types.ID,
) (client.StoreRPC, error) {
	args := m.Called(ctx, shardID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(client.StoreRPC), args.Error(1)
}

func (m *MockStoreOperations) UpdateShardSplitState(
	ctx context.Context,
	shardID types.ID,
	splitState *db.SplitState,
) error {
	args := m.Called(ctx, shardID, splitState)
	return args.Error(0)
}

func (m *MockStoreOperations) UpdateShardMergeState(
	ctx context.Context,
	shardID types.ID,
	mergeState *db.MergeState,
) error {
	for _, call := range m.ExpectedCalls {
		if call.Method == "UpdateShardMergeState" {
			args := m.Called(ctx, shardID, mergeState)
			return args.Error(0)
		}
	}
	return nil
}

// ============================================================================
// TEST HELPERS - Builders for test data
// ============================================================================

// ShardStatusBuilder helps build ShardStatus for tests
type ShardStatusBuilder struct {
	status *store.ShardStatus
}

func NewShardStatusBuilder() *ShardStatusBuilder {
	return &ShardStatusBuilder{
		status: &store.ShardStatus{
			ShardInfo: store.ShardInfo{
				ShardConfig: store.ShardConfig{
					ByteRange: [2][]byte{{0x00}, {0xff}},
				},
				Peers: common.NewPeerSet(),
			},
			State: store.ShardState_Default,
		},
	}
}

func (b *ShardStatusBuilder) WithID(id types.ID) *ShardStatusBuilder {
	b.status.ID = id
	return b
}

func (b *ShardStatusBuilder) WithTable(table string) *ShardStatusBuilder {
	b.status.Table = table
	return b
}

func (b *ShardStatusBuilder) WithPeers(peers ...types.ID) *ShardStatusBuilder {
	b.status.Peers = common.NewPeerSet(peers...)
	return b
}

func (b *ShardStatusBuilder) WithState(state store.ShardState) *ShardStatusBuilder {
	b.status.State = state
	return b
}

func (b *ShardStatusBuilder) WithByteRange(start, end []byte) *ShardStatusBuilder {
	b.status.ByteRange = [2][]byte{start, end}
	return b
}

func (b *ShardStatusBuilder) WithRaftStatus(lead types.ID, voters ...types.ID) *ShardStatusBuilder {
	b.status.RaftStatus = &common.RaftStatus{
		Lead:   lead,
		Voters: common.NewPeerSet(voters...),
	}
	return b
}

func (b *ShardStatusBuilder) Build() *store.ShardStatus {
	return b.status
}

// newFullTextIndexStats creates an IndexStats for testing full text indexes
func newFullTextIndexStats(rebuilding bool) indexes.IndexStats {
	return indexes.FullTextIndexStats{
		Rebuilding: rebuilding,
	}.AsIndexStats()
}

// ============================================================================
// UNIT TESTS - Pure functions (no mocks needed!)
// ============================================================================

func TestComputeTombstoneDeletions(t *testing.T) {
	reconciler := NewReconciler(nil, nil, nil, ReconciliationConfig{}, zaptest.NewLogger(t))

	t.Run("no removed stores", func(t *testing.T) {
		current := CurrentClusterState{
			RemovedStores: map[types.ID]struct{}{},
			Shards:        map[types.ID]*store.ShardInfo{},
		}

		deletions := reconciler.computeTombstoneDeletions(current)
		assert.Empty(t, deletions)
	})

	t.Run("removed store with no voters - can delete", func(t *testing.T) {
		removedStore := types.ID(100)
		current := CurrentClusterState{
			RemovedStores: map[types.ID]struct{}{
				removedStore: {},
			},
			Shards: map[types.ID]*store.ShardInfo{
				1: {
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2, 3), // Doesn't include removed store
					},
				},
			},
		}

		deletions := reconciler.computeTombstoneDeletions(current)
		assert.Equal(t, []types.ID{removedStore}, deletions)
	})

	t.Run("removed store still has voters - cannot delete", func(t *testing.T) {
		removedStore := types.ID(100)
		current := CurrentClusterState{
			RemovedStores: map[types.ID]struct{}{
				removedStore: {},
			},
			Shards: map[types.ID]*store.ShardInfo{
				1: {
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2, removedStore), // Still a voter
					},
				},
			},
		}

		deletions := reconciler.computeTombstoneDeletions(current)
		assert.Empty(t, deletions)
	})
}

func TestComputeRemovedStorePeerRemovals(t *testing.T) {
	reconciler := NewReconciler(nil, nil, nil, ReconciliationConfig{}, zaptest.NewLogger(t))

	t.Run("no removed stores", func(t *testing.T) {
		current := CurrentClusterState{
			RemovedStores: map[types.ID]struct{}{},
			Shards:        map[types.ID]*store.ShardInfo{},
		}

		removals := reconciler.computeRemovedStorePeerRemovals(current)
		assert.Empty(t, removals)
	})

	t.Run("removed store with no voters - no removal needed", func(t *testing.T) {
		removedStore := types.ID(100)
		current := CurrentClusterState{
			RemovedStores: map[types.ID]struct{}{
				removedStore: {},
			},
			Shards: map[types.ID]*store.ShardInfo{
				1: {
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2, 3), // Doesn't include removed store
					},
				},
			},
		}

		removals := reconciler.computeRemovedStorePeerRemovals(current)
		assert.Empty(t, removals)
	})

	t.Run("removed store still has voters - needs removal", func(t *testing.T) {
		removedStore := types.ID(100)
		current := CurrentClusterState{
			RemovedStores: map[types.ID]struct{}{
				removedStore: {},
			},
			Shards: map[types.ID]*store.ShardInfo{
				1: {
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2, removedStore), // Still a voter
					},
				},
			},
		}

		removals := reconciler.computeRemovedStorePeerRemovals(current)
		assert.Equal(t, []types.ID{removedStore}, removals)
	})
}

func TestComputeRaftVoterFixes(t *testing.T) {
	reconciler := NewReconciler(nil, nil, nil, ReconciliationConfig{}, zaptest.NewLogger(t))

	t.Run("voters match peers", func(t *testing.T) {
		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				1: NewShardStatusBuilder().
					WithPeers(1, 2, 3).
					WithRaftStatus(1, 1, 2, 3).
					Build(),
			},
		}

		fixes := reconciler.computeRaftVoterFixes(desired)
		assert.Empty(t, fixes)
	})

	t.Run("peer not in voters", func(t *testing.T) {
		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				1: NewShardStatusBuilder().
					WithPeers(1, 2, 3, 4).      // Peer 4 is running
					WithRaftStatus(1, 1, 2, 3). // But not in voters
					Build(),
			},
		}

		fixes := reconciler.computeRaftVoterFixes(desired)
		assert.Len(t, fixes, 1)
		assert.Equal(t, types.ID(1), fixes[0].ShardID)
		assert.Equal(t, types.ID(4), fixes[0].PeerID)
		assert.Equal(t, RaftVoterActionStopShard, fixes[0].Action)
	})
}

func TestComputeSplitStateActions(t *testing.T) {
	reconciler := NewReconciler(nil, nil, nil, ReconciliationConfig{}, zaptest.NewLogger(t))

	t.Run("splitting off shard ready to start", func(t *testing.T) {
		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				1: NewShardStatusBuilder().
					WithState(store.ShardState_SplittingOff).
					WithPeers(1, 2, 3).
					Build(),
			},
		}

		actions := reconciler.computeSplitStateActions(CurrentClusterState{}, desired)
		assert.Len(t, actions, 1)
		assert.Equal(t, SplitStateActionStartShard, actions[0].Action)
		assert.Equal(t, store.ShardState_SplittingOff, actions[0].State)
	})

	t.Run("pre-merge shard", func(t *testing.T) {
		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				1: NewShardStatusBuilder().
					WithState(store.ShardState_PreMerge).
					WithByteRange([]byte{0x00}, []byte{0x80}).
					Build(),
			},
		}

		actions := reconciler.computeSplitStateActions(CurrentClusterState{}, desired)
		assert.Empty(t, actions)
	})

	t.Run("online merge only advances from receiver", func(t *testing.T) {
		receiverID := types.ID(1)
		donorID := types.ID(2)
		startedAt := time.Now().UnixNano()

		donorMergeState := &db.MergeState{}
		donorMergeState.SetPhase(db.MergeState_PHASE_PREPARE)
		donorMergeState.SetDonorShardId(uint64(donorID))
		donorMergeState.SetReceiverShardId(uint64(receiverID))
		donorMergeState.SetCaptureDeltas(true)
		donorMergeState.SetStartedAtUnixNanos(startedAt)

		receiverMergeState := &db.MergeState{}
		receiverMergeState.SetPhase(db.MergeState_PHASE_PREPARE)
		receiverMergeState.SetDonorShardId(uint64(donorID))
		receiverMergeState.SetReceiverShardId(uint64(receiverID))
		receiverMergeState.SetAcceptDonorRange(true)
		receiverMergeState.SetStartedAtUnixNanos(startedAt)

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				receiverID: NewShardStatusBuilder().
					WithID(receiverID).
					WithState(store.ShardState_PreMerge).
					Build(),
				donorID: NewShardStatusBuilder().
					WithID(donorID).
					Build(),
			},
		}
		desired.Shards[receiverID].MergeState = receiverMergeState
		desired.Shards[donorID].MergeState = donorMergeState

		actions := reconciler.computeSplitStateActions(CurrentClusterState{}, desired)
		require.Len(t, actions, 1)
		assert.Equal(t, receiverID, actions[0].ShardID)
		assert.Equal(t, donorID, actions[0].MergeShardID)
		assert.Equal(t, SplitStateActionCopyMerge, actions[0].Action)
	})
}

func TestComputeIndexOperations(t *testing.T) {
	reconciler := NewReconciler(nil, nil, nil, ReconciliationConfig{}, zaptest.NewLogger(t))

	schema := &schema.TableSchema{
		Version: 1,
	}

	t.Run("no operations needed", func(t *testing.T) {
		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				1: {
					ShardConfig: store.ShardConfig{
						Schema:  schema,
						Indexes: map[string]indexes.IndexConfig{},
					},
				},
			},
			Tables: map[string]*store.Table{
				"test": {
					Schema:  schema,
					Indexes: map[string]indexes.IndexConfig{},
				},
			},
		}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				1: {
					ShardInfo: store.ShardInfo{
						ShardConfig: store.ShardConfig{},
						RaftStatus:  &common.RaftStatus{Lead: 1},
					},
					Table: "test",
				},
			},
		}

		ops := reconciler.computeIndexOperations(current, desired)
		assert.Empty(t, ops)
	})

	t.Run("missing index", func(t *testing.T) {
		indexConfig := indexes.IndexConfig{
			Type: indexes.IndexTypeFullTextV0,
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				1: {
					ShardConfig: store.ShardConfig{
						Schema:  schema,
						Indexes: map[string]indexes.IndexConfig{}, // Missing index
					},
				},
			},
			Tables: map[string]*store.Table{
				"test": {
					Schema: schema,
					Indexes: map[string]indexes.IndexConfig{
						"idx1": indexConfig, // Desired index
					},
				},
			},
		}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				1: {
					ShardInfo: store.ShardInfo{
						RaftStatus: &common.RaftStatus{Lead: 1},
					},
					Table: "test",
				},
			},
		}

		ops := reconciler.computeIndexOperations(current, desired)
		assert.Len(t, ops, 1)
		assert.Equal(t, IndexOpAdd, ops[0].Operation)
		assert.Equal(t, "idx1", ops[0].IndexName)
	})

	// Regression: during schema migration, table.Indexes gains full_text_index_v1
	// but the shard only has full_text_index_v0. The reconciler must detect the
	// missing v1 and generate an IndexOpAdd. In swarm mode RaftStatus is nil
	// because raft is bypassed entirely.
	t.Run("schema migration adds versioned full text index in swarm mode", func(t *testing.T) {
		v0Config := indexes.IndexConfig{
			Name: "full_text_index_v0",
			Type: indexes.IndexTypeFullTextV0,
		}
		v1Config := indexes.IndexConfig{
			Name: "full_text_index_v1",
			Type: indexes.IndexTypeFullTextV0,
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				1: {
					ShardConfig: store.ShardConfig{
						Schema: schema,
						Indexes: map[string]indexes.IndexConfig{
							"full_text_index_v0": v0Config, // Shard only has v0
						},
					},
				},
			},
			Tables: map[string]*store.Table{
				"test": {
					Schema: schema,
					Indexes: map[string]indexes.IndexConfig{
						"full_text_index_v0": v0Config,
						"full_text_index_v1": v1Config, // Table wants both v0 and v1
					},
				},
			},
		}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				1: {
					ShardInfo: store.ShardInfo{
						RaftStatus: nil, // swarm mode: no raft
					},
					Table: "test",
				},
			},
		}

		ops := reconciler.computeIndexOperations(current, desired)
		assert.Len(t, ops, 1)
		assert.Equal(t, IndexOpAdd, ops[0].Operation)
		assert.Equal(t, "full_text_index_v1", ops[0].IndexName)
	})

	t.Run("skips shard with raft but no leader", func(t *testing.T) {
		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				1: {
					ShardConfig: store.ShardConfig{
						Schema:  schema,
						Indexes: map[string]indexes.IndexConfig{},
					},
				},
			},
			Tables: map[string]*store.Table{
				"test": {
					Schema: schema,
					Indexes: map[string]indexes.IndexConfig{
						"idx1": {Type: indexes.IndexTypeFullTextV0},
					},
				},
			},
		}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				1: {
					ShardInfo: store.ShardInfo{
						RaftStatus: &common.RaftStatus{Lead: 0}, // raft exists, no leader
					},
					Table: "test",
				},
			},
		}

		ops := reconciler.computeIndexOperations(current, desired)
		assert.Empty(t, ops, "should skip shards where raft exists but no leader elected")
	})

	t.Run("extra index", func(t *testing.T) {
		indexConfig := indexes.IndexConfig{
			Type: indexes.IndexTypeFullTextV0,
		}

		current := CurrentClusterState{
			Shards: map[types.ID]*store.ShardInfo{
				1: {
					ShardConfig: store.ShardConfig{
						Schema: schema,
						Indexes: map[string]indexes.IndexConfig{
							"idx1": indexConfig, // Extra index
						},
					},
				},
			},
			Tables: map[string]*store.Table{
				"test": {
					Schema:  schema,
					Indexes: map[string]indexes.IndexConfig{}, // No indexes desired
				},
			},
		}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				1: {
					ShardInfo: store.ShardInfo{
						RaftStatus: &common.RaftStatus{Lead: 1},
					},
					Table: "test",
				},
			},
		}

		ops := reconciler.computeIndexOperations(current, desired)
		assert.Len(t, ops, 1)
		assert.Equal(t, IndexOpDrop, ops[0].Operation)
		assert.Equal(t, "idx1", ops[0].IndexName)
	})
}

func TestShardCooldown(t *testing.T) {
	reconciler := NewReconciler(nil, nil, nil, ReconciliationConfig{}, zaptest.NewLogger(t))

	shardID := types.ID(1)

	t.Run("shard not in cooldown", func(t *testing.T) {
		assert.False(t, reconciler.IsShardInCooldown(shardID))
	})

	t.Run("shard in cooldown", func(t *testing.T) {
		reconciler.SetShardCooldown(shardID, 1*time.Hour)
		assert.True(t, reconciler.IsShardInCooldown(shardID))
	})

	t.Run("cooldown expires", func(t *testing.T) {
		reconciler.SetShardCooldown(shardID, -1*time.Second) // Already expired
		assert.False(t, reconciler.IsShardInCooldown(shardID))
	})
}

func TestShardForNodeCooldown(t *testing.T) {
	reconciler := NewReconciler(nil, nil, nil, ReconciliationConfig{}, zaptest.NewLogger(t))

	shardID := types.ID(1)
	nodeID := types.ID(2)

	t.Run("shard-node not in cooldown", func(t *testing.T) {
		assert.False(t, reconciler.IsShardForNodeInCooldown(shardID, nodeID))
	})

	t.Run("shard-node in cooldown", func(t *testing.T) {
		reconciler.SetShardForNodeCooldown(shardID, nodeID, 1*time.Hour)
		assert.True(t, reconciler.IsShardForNodeInCooldown(shardID, nodeID))
	})

	t.Run("cooldown expires", func(t *testing.T) {
		reconciler.SetShardForNodeCooldown(shardID, nodeID, -1*time.Second)
		assert.False(t, reconciler.IsShardForNodeInCooldown(shardID, nodeID))
	})
}

func TestSortShardsByStatePriority(t *testing.T) {
	reconciler := NewReconciler(nil, nil, nil, ReconciliationConfig{}, zaptest.NewLogger(t))

	t.Run("sorts by priority: PreMerge > PreSplit > SplittingOff > others", func(t *testing.T) {
		shards := map[types.ID]*store.ShardStatus{
			1: {State: store.ShardState_Default},
			2: {State: store.ShardState_SplittingOff},
			3: {State: store.ShardState_PreMerge},
			4: {State: store.ShardState_PreSplit},
			5: {State: store.ShardState_Initializing},
		}

		sorted := reconciler.sortShardsByStatePriority(shards)

		// PreMerge should be first
		assert.Equal(t, types.ID(3), sorted[0], "PreMerge should be first")

		// PreSplit should be second
		assert.Equal(t, types.ID(4), sorted[1], "PreSplit should be second")

		// SplittingOff should be third
		assert.Equal(t, types.ID(2), sorted[2], "SplittingOff should be third")

		// Default and Initializing states should be after transitioning states
		// (order between them is by ID)
		assert.Contains(t, []types.ID{1, 5}, sorted[3])
		assert.Contains(t, []types.ID{1, 5}, sorted[4])
	})

	t.Run("same state sorted by ID", func(t *testing.T) {
		shards := map[types.ID]*store.ShardStatus{
			5: {State: store.ShardState_PreMerge},
			2: {State: store.ShardState_PreMerge},
			8: {State: store.ShardState_PreMerge},
		}

		sorted := reconciler.sortShardsByStatePriority(shards)

		// Should be sorted by ID
		assert.Equal(t, types.ID(2), sorted[0])
		assert.Equal(t, types.ID(5), sorted[1])
		assert.Equal(t, types.ID(8), sorted[2])
	})

	t.Run("mixed states with multiple of each", func(t *testing.T) {
		shards := map[types.ID]*store.ShardStatus{
			10: {State: store.ShardState_Default},
			20: {State: store.ShardState_SplittingOff},
			30: {State: store.ShardState_PreMerge},
			40: {State: store.ShardState_PreSplit},
			50: {State: store.ShardState_SplittingOff},
			60: {State: store.ShardState_PreMerge},
		}

		sorted := reconciler.sortShardsByStatePriority(shards)

		// Print actual order for debugging
		t.Logf("Actual sorted order: %v", sorted)
		for i, id := range sorted {
			t.Logf("  [%d] ID=%d State=%v", i, id, shards[id].State)
		}

		// First two should be PreMerge (sorted by ID)
		assert.Equal(t, types.ID(30), sorted[0], "First should be PreMerge ID=30")
		assert.Equal(t, types.ID(60), sorted[1], "Second should be PreMerge ID=60")

		// Next should be PreSplit
		assert.Equal(t, types.ID(40), sorted[2], "Third should be PreSplit ID=40")

		// Next two should be SplittingOff (sorted by ID)
		assert.Equal(t, types.ID(20), sorted[3], "Fourth should be SplittingOff ID=20")
		assert.Equal(t, types.ID(50), sorted[4], "Fifth should be SplittingOff ID=50")

		// Last should be Default
		assert.Equal(t, types.ID(10), sorted[5], "Last should be Default ID=10")
	})
}

// ============================================================================
// INTEGRATION TESTS - Using mocks
// ============================================================================

func TestReconciler_ComputePlan_Integration(t *testing.T) {
	mockShardOps := &MockShardOperations{}
	mockTableOps := &MockTableOperations{}
	mockStoreOps := &MockStoreOperations{}

	config := ReconciliationConfig{
		ReplicationFactor: 3,
	}

	reconciler := NewReconciler(
		mockShardOps,
		mockTableOps,
		mockStoreOps,
		config,
		zaptest.NewLogger(t),
	)

	t.Run("plan skips when removed stores need reconciliation", func(t *testing.T) {
		removedStore := types.ID(100)
		current := CurrentClusterState{
			Stores: []types.ID{1, 2, 3},
			RemovedStores: map[types.ID]struct{}{
				removedStore: {},
			},
			Shards: map[types.ID]*store.ShardInfo{
				1: {
					Peers: common.NewPeerSet(1, 2, removedStore),
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2, removedStore),
					},
				},
			},
		}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{},
		}

		plan := reconciler.ComputePlan(context.Background(), current, desired)

		// Should skip remaining steps when removed stores need reconciliation
		assert.True(t, plan.SkipRemainingSteps)
		assert.Empty(t, plan.IndexOperations)
		assert.Empty(t, plan.SplitTransitions)
	})

	t.Run("plan includes split state actions", func(t *testing.T) {
		current := CurrentClusterState{
			Stores:        []types.ID{1, 2, 3},
			Shards:        map[types.ID]*store.ShardInfo{},
			RemovedStores: map[types.ID]struct{}{},
		}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{
				1: NewShardStatusBuilder().
					WithState(store.ShardState_SplittingOff).
					WithPeers(1, 2, 3).
					Build(),
			},
		}

		plan := reconciler.ComputePlan(context.Background(), current, desired)

		assert.True(t, plan.HasUnreconciledSplitOrMerge)
		assert.True(t, plan.SkipRemainingSteps)
		assert.Len(t, plan.SplitStateActions, 1)
	})
}

// ============================================================================
// COMPARISON TEST - Shows testability improvement
// ============================================================================

// This test demonstrates how much easier it is to test the refactored code
// compared to the original metadata.go implementation
func TestTestabilityComparison(t *testing.T) {
	t.Run("OLD WAY - would need full MetadataStore setup", func(t *testing.T) {
		// To test the original checkShardAssignments, you would need:
		// - A real or mocked MetadataStore
		// - A real or mocked TableManager
		// - A real or mocked UserManager
		// - Real or mocked network clients
		// - Complex setup of raft groups
		// - Database instances
		// - Cache instances
		// This makes writing tests extremely difficult and slow
		t.Skip("Original code is too complex to test easily")
	})

	t.Run("NEW WAY - pure functions need no setup", func(t *testing.T) {
		// With the refactored code, we just call pure functions
		reconciler := NewReconciler(nil, nil, nil, ReconciliationConfig{}, zaptest.NewLogger(t))

		// Test tombstone deletions - no mocks needed!
		current := CurrentClusterState{
			RemovedStores: map[types.ID]struct{}{100: {}},
			Shards: map[types.ID]*store.ShardInfo{
				1: {
					RaftStatus: &common.RaftStatus{
						Voters: common.NewPeerSet(1, 2, 3),
					},
				},
			},
		}

		deletions := reconciler.computeTombstoneDeletions(current)
		assert.Equal(t, []types.ID{100}, deletions)

		// Fast, simple, deterministic!
	})

	t.Run("NEW WAY - integration tests use simple mocks", func(t *testing.T) {
		mockShardOps := &MockShardOperations{}
		mockTableOps := &MockTableOperations{}
		mockStoreOps := &MockStoreOperations{}

		// Setup mocks for ComputePlan
		mockTableOps.On("HasReallocationRequest", mock.Anything).Return(false, nil)
		mockTableOps.On("GetTablesWithReadSchemas").Return([]*store.Table{}, nil)

		reconciler := NewReconciler(
			mockShardOps,
			mockTableOps,
			mockStoreOps,
			ReconciliationConfig{ReplicationFactor: 3},
			zaptest.NewLogger(t),
		)

		// Now we can test the full reconciliation with controlled mocks
		// without needing a real cluster!
		current := CurrentClusterState{
			Stores: []types.ID{1, 2, 3},
			Shards: map[types.ID]*store.ShardInfo{},
		}

		desired := DesiredClusterState{
			Shards: map[types.ID]*store.ShardStatus{},
		}

		plan := reconciler.ComputePlan(context.Background(), current, desired)
		assert.NotNil(t, plan)

		// Easy to verify behavior without I/O!
	})
}

// ============================================================================
// TESTS FOR NEW FEATURES - Reallocation Request Handling
// ============================================================================

func TestComputePlan_ReallocationRequest_ForcesComputation(t *testing.T) {
	mockShardOps := &MockShardOperations{}
	mockTableOps := &MockTableOperations{}
	mockStoreOps := &MockStoreOperations{}

	// Setup: Reallocation request is present
	mockTableOps.On("HasReallocationRequest", mock.Anything).Return(true, nil)
	mockTableOps.On("GetTablesWithReadSchemas").Return([]*store.Table{}, nil)

	reconciler := NewReconciler(
		mockShardOps,
		mockTableOps,
		mockStoreOps,
		ReconciliationConfig{
			ReplicationFactor: 3,
			DisableShardAlloc: true, // Even with allocation disabled, should compute
		},
		zaptest.NewLogger(t),
	)

	current := CurrentClusterState{
		Stores: []types.ID{1, 2, 3},
		Shards: map[types.ID]*store.ShardInfo{},
	}

	desired := DesiredClusterState{
		Shards: map[types.ID]*store.ShardStatus{},
	}

	plan := reconciler.ComputePlan(context.Background(), current, desired)

	// Verify forced reallocation flags are set
	assert.True(
		t,
		plan.ForcedReallocation,
		"ForcedReallocation should be true when reallocation request is present",
	)
	assert.True(t, plan.ClearReallocationRequest, "ClearReallocationRequest should be true")

	mockTableOps.AssertExpectations(t)
}

func TestComputePlan_NoReallocationRequest_RespectsDisableShardAlloc(t *testing.T) {
	mockShardOps := &MockShardOperations{}
	mockTableOps := &MockTableOperations{}
	mockStoreOps := &MockStoreOperations{}

	// Setup: No reallocation request
	mockTableOps.On("HasReallocationRequest", mock.Anything).Return(false, nil)
	mockTableOps.On("GetTablesWithReadSchemas").Return([]*store.Table{}, nil)

	reconciler := NewReconciler(
		mockShardOps,
		mockTableOps,
		mockStoreOps,
		ReconciliationConfig{
			ReplicationFactor: 3,
			DisableShardAlloc: true, // Allocation disabled and no request
		},
		zaptest.NewLogger(t),
	)

	current := CurrentClusterState{
		Stores: []types.ID{1, 2, 3},
		Shards: map[types.ID]*store.ShardInfo{},
	}

	desired := DesiredClusterState{
		Shards: map[types.ID]*store.ShardStatus{},
	}

	plan := reconciler.ComputePlan(context.Background(), current, desired)

	// Should not compute splits/merges when disabled and no request
	assert.Empty(t, plan.SplitTransitions, "Should not compute splits when allocation disabled")
	assert.Empty(t, plan.MergeTransitions, "Should not compute merges when allocation disabled")
	assert.False(t, plan.ForcedReallocation)
	assert.False(t, plan.ClearReallocationRequest)

	mockTableOps.AssertExpectations(t)
}

func TestComputePlan_ReallocationRequest_ErrHandling(t *testing.T) {
	mockShardOps := &MockShardOperations{}
	mockTableOps := &MockTableOperations{}
	mockStoreOps := &MockStoreOperations{}

	// Setup: Error checking reallocation request
	mockTableOps.On("HasReallocationRequest", mock.Anything).Return(false, assert.AnError)
	mockTableOps.On("GetTablesWithReadSchemas").Return([]*store.Table{}, nil)

	reconciler := NewReconciler(
		mockShardOps,
		mockTableOps,
		mockStoreOps,
		ReconciliationConfig{
			ReplicationFactor:   3,
			DisableShardAlloc:   true,
			MaxShardSizeBytes:   1000,
			MaxShardsPerTable:   100,
			ShardCooldownPeriod: time.Minute,
		},
		zaptest.NewLogger(t),
	)

	current := CurrentClusterState{
		Stores: []types.ID{1, 2, 3},
		Shards: map[types.ID]*store.ShardInfo{},
	}

	desired := DesiredClusterState{
		Shards: map[types.ID]*store.ShardStatus{},
	}

	// Should not panic and should handle error gracefully
	plan := reconciler.ComputePlan(context.Background(), current, desired)
	assert.NotNil(t, plan)
	assert.False(t, plan.ForcedReallocation)

	mockTableOps.AssertExpectations(t)
}

// ============================================================================
// TESTS FOR NEW FEATURES - Index Rebuild Completion Detection
// ============================================================================

func TestComputeIndexRebuildCompletions_AllShardsComplete(t *testing.T) {
	mockTableOps := &MockTableOperations{}

	table := &store.Table{
		Name: "users",
		Schema: &schema.TableSchema{
			Version: 2,
		},
		ReadSchema: &schema.TableSchema{
			Version: 1,
		},
		Shards: map[types.ID]*store.ShardConfig{
			1: {},
			2: {},
		},
	}

	mockTableOps.On("GetTablesWithReadSchemas").Return([]*store.Table{table}, nil)

	reconciler := NewReconciler(
		nil,
		mockTableOps,
		nil,
		ReconciliationConfig{ReplicationFactor: 3},
		zaptest.NewLogger(t),
	)

	// Current state: both shards have completed the rebuild
	current := CurrentClusterState{
		Shards: map[types.ID]*store.ShardInfo{
			1: {
				ShardStats: &store.ShardStats{
					Indexes: map[string]indexes.IndexStats{
						"full_text_index_v2": newFullTextIndexStats(false), // Completed
					},
				},
			},
			2: {
				ShardStats: &store.ShardStats{
					Indexes: map[string]indexes.IndexStats{
						"full_text_index_v2": newFullTextIndexStats(false), // Completed
					},
				},
			},
		},
	}

	completions := reconciler.computeIndexRebuildCompletions(current)

	assert.Len(t, completions, 1, "Should detect one completed rebuild")
	assert.Equal(t, "users", completions[0].TableName)
	assert.Equal(t, "full_text_index_v2", completions[0].IndexName)

	mockTableOps.AssertExpectations(t)
}

func TestComputeIndexRebuildCompletions_SomeStillRebuilding(t *testing.T) {
	mockTableOps := &MockTableOperations{}

	table := &store.Table{
		Name: "users",
		Schema: &schema.TableSchema{
			Version: 2,
		},
		ReadSchema: &schema.TableSchema{
			Version: 1,
		},
		Shards: map[types.ID]*store.ShardConfig{
			1: {},
			2: {},
		},
	}

	mockTableOps.On("GetTablesWithReadSchemas").Return([]*store.Table{table}, nil)

	reconciler := NewReconciler(
		nil,
		mockTableOps,
		nil,
		ReconciliationConfig{ReplicationFactor: 3},
		zaptest.NewLogger(t),
	)

	// Shard 1 complete, shard 2 still rebuilding
	current := CurrentClusterState{
		Shards: map[types.ID]*store.ShardInfo{
			1: {
				ShardStats: &store.ShardStats{
					Indexes: map[string]indexes.IndexStats{
						"full_text_index_v2": newFullTextIndexStats(false),
					},
				},
			},
			2: {
				ShardStats: &store.ShardStats{
					Indexes: map[string]indexes.IndexStats{
						"full_text_index_v2": newFullTextIndexStats(true), // Still rebuilding!
					},
				},
			},
		},
	}

	completions := reconciler.computeIndexRebuildCompletions(current)

	assert.Empty(t, completions, "Should not mark as complete when some shards still rebuilding")

	mockTableOps.AssertExpectations(t)
}

func TestComputeIndexRebuildCompletions_MissingShardStats(t *testing.T) {
	mockTableOps := &MockTableOperations{}

	table := &store.Table{
		Name: "users",
		Schema: &schema.TableSchema{
			Version: 2,
		},
		ReadSchema: &schema.TableSchema{
			Version: 1,
		},
		Shards: map[types.ID]*store.ShardConfig{
			1: {},
			2: {},
		},
	}

	mockTableOps.On("GetTablesWithReadSchemas").Return([]*store.Table{table}, nil)

	reconciler := NewReconciler(
		nil,
		mockTableOps,
		nil,
		ReconciliationConfig{ReplicationFactor: 3},
		zaptest.NewLogger(t),
	)

	// Shard 2 is missing from current state
	current := CurrentClusterState{
		Shards: map[types.ID]*store.ShardInfo{
			1: {
				ShardStats: &store.ShardStats{
					Indexes: map[string]indexes.IndexStats{
						"full_text_index_v2": newFullTextIndexStats(false),
					},
				},
			},
			// Shard 2 missing!
		},
	}

	completions := reconciler.computeIndexRebuildCompletions(current)

	assert.Empty(t, completions, "Should not mark as complete when shard is missing")

	mockTableOps.AssertExpectations(t)
}

// Regression test: the reconciler must check Schema.Version (the new version
// being built), not ReadSchema.Version (the old version still serving reads).
// Shards here have completed full_text_index_v1 (the old version) but not
// full_text_index_v2 (the new version) — rebuild should NOT be marked complete.
func TestComputeIndexRebuildCompletions_ChecksSchemaNotReadSchema(t *testing.T) {
	mockTableOps := &MockTableOperations{}

	table := &store.Table{
		Name: "users",
		Schema: &schema.TableSchema{
			Version: 2,
		},
		ReadSchema: &schema.TableSchema{
			Version: 1,
		},
		Shards: map[types.ID]*store.ShardConfig{
			1: {},
		},
	}

	mockTableOps.On("GetTablesWithReadSchemas").Return([]*store.Table{table}, nil)

	reconciler := NewReconciler(
		nil,
		mockTableOps,
		nil,
		ReconciliationConfig{ReplicationFactor: 3},
		zaptest.NewLogger(t),
	)

	// Shard has the OLD version's index complete, but NOT the new version
	current := CurrentClusterState{
		Shards: map[types.ID]*store.ShardInfo{
			1: {
				ShardStats: &store.ShardStats{
					Indexes: map[string]indexes.IndexStats{
						"full_text_index_v1": newFullTextIndexStats(false),
					},
				},
			},
		},
	}

	completions := reconciler.computeIndexRebuildCompletions(current)

	assert.Empty(t, completions, "old version's index being complete should not trigger completion")

	mockTableOps.AssertExpectations(t)
}

func TestComputeIndexRebuildCompletions_NoTablesWithReadSchemas(t *testing.T) {
	mockTableOps := &MockTableOperations{}

	// No tables with read schemas
	mockTableOps.On("GetTablesWithReadSchemas").Return([]*store.Table{}, nil)

	reconciler := NewReconciler(
		nil,
		mockTableOps,
		nil,
		ReconciliationConfig{ReplicationFactor: 3},
		zaptest.NewLogger(t),
	)

	current := CurrentClusterState{
		Shards: map[types.ID]*store.ShardInfo{},
	}

	completions := reconciler.computeIndexRebuildCompletions(current)

	assert.Empty(t, completions, "Should return empty when no tables have read schemas")

	mockTableOps.AssertExpectations(t)
}

func TestComputeIndexRebuildCompletions_ErrGettingTables(t *testing.T) {
	mockTableOps := &MockTableOperations{}

	// Error getting tables
	mockTableOps.On("GetTablesWithReadSchemas").Return(([]*store.Table)(nil), assert.AnError)

	reconciler := NewReconciler(
		nil,
		mockTableOps,
		nil,
		ReconciliationConfig{ReplicationFactor: 3},
		zaptest.NewLogger(t),
	)

	current := CurrentClusterState{
		Shards: map[types.ID]*store.ShardInfo{},
	}

	// Should handle error gracefully
	completions := reconciler.computeIndexRebuildCompletions(current)

	assert.Empty(t, completions, "Should return empty on error")

	mockTableOps.AssertExpectations(t)
}
