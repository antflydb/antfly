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
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/antflydb/antfly/src/store"
	storedb "github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// setupTestMetadataStore creates a MetadataStore with an in-memory Pebble-backed
// TableManager for unit testing shard routing logic.
func setupTestMetadataStore(t *testing.T) (*MetadataStore, kv.DB) {
	t.Helper()

	db, err := pebble.Open(t.TempDir(), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	kvDB := &kv.PebbleDB{DB: db}

	tm, err := tablemgr.NewTableManager(kvDB, http.DefaultClient, 0)
	require.NoError(t, err)

	ms := &MetadataStore{
		logger: zaptest.NewLogger(t),
		config: &common.Config{SwarmMode: false},
		tm:     tm,
	}

	return ms, kvDB
}

// writeShardStatus persists a ShardStatus to the test DB so GetShardStatus can find it.
func writeShardStatus(t *testing.T, db kv.DB, status *store.ShardStatus) {
	t.Helper()
	data, err := json.Marshal(status)
	require.NoError(t, err)
	err = db.Batch(context.Background(),
		[][2][]byte{{[]byte("tm:shs:" + status.ID.String()), data}},
		nil,
	)
	require.NoError(t, err)
}

// writeStoreStatus persists a StoreStatus so GetStoreClient returns a reachable client.
func writeStoreStatus(t *testing.T, db kv.DB, nodeID types.ID, healthy bool) {
	t.Helper()
	state := store.StoreState_Healthy
	if !healthy {
		state = store.StoreState_Unhealthy
	}
	status := &tablemgr.StoreStatus{
		StoreInfo: store.StoreInfo{
			ID:     nodeID,
			ApiURL: "http://127.0.0.1:0", // Placeholder; we never make real HTTP calls
		},
		State: state,
	}
	data, err := json.Marshal(status)
	require.NoError(t, err)
	err = db.Batch(context.Background(),
		[][2][]byte{{[]byte("tm:sts:" + nodeID.String()), data}},
		nil,
	)
	require.NoError(t, err)
}

// newShardStatus builds a ShardStatus for routing tests.
func newShardStatus(id types.ID, leadID types.ID, reportedBy []types.ID, peers []types.ID) *store.ShardStatus {
	status := &store.ShardStatus{
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{{0x00}, {0xff}},
			},
			Peers:      common.NewPeerSet(peers...),
			ReportedBy: common.NewPeerSet(reportedBy...),
		},
		ID:    id,
		Table: "test_table",
		State: store.ShardState_Default,
	}
	if leadID != 0 {
		status.RaftStatus = &common.RaftStatus{
			Lead:   leadID,
			Voters: common.NewPeerSet(peers...),
		}
	} else {
		status.RaftStatus = &common.RaftStatus{
			Lead:   0,
			Voters: common.NewPeerSet(peers...),
		}
	}
	return status
}

func splitShardStatus(
	id types.ID,
	state store.ShardState,
	byteRange [2][]byte,
	hasSnapshot bool,
	initializing bool,
	leadID types.ID,
) *store.ShardStatus {
	status := &store.ShardStatus{
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: byteRange,
			},
			HasSnapshot:  hasSnapshot,
			Initializing: initializing,
			Peers:        common.NewPeerSet(1, 2, 3),
			ReportedBy:   common.NewPeerSet(1, 2, 3),
		},
		ID:    id,
		Table: "test_table",
		State: state,
	}
	status.RaftStatus = &common.RaftStatus{
		Lead:   leadID,
		Voters: common.NewPeerSet(1, 2, 3),
	}
	return status
}

func TestFindWriteShardForKey_FallsBackToParentUntilSplitOffReady(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentID := types.ID(10)
	childID := types.ID(11)
	splitKey := []byte("m:\x00")

	table := &store.Table{
		Name: "test_table",
		Shards: map[types.ID]*store.ShardConfig{
			parentID: {ByteRange: [2][]byte{[]byte{0x00}, splitKey}},
			childID:  {ByteRange: [2][]byte{splitKey, []byte{0xff}}},
		},
	}

	writeShardStatus(t, db, splitShardStatus(
		parentID,
		store.ShardState_PreSplit,
		[2][]byte{[]byte{0x00}, splitKey},
		false,
		false,
		1,
	))
	parentStatus, err := ms.tm.GetShardStatus(parentID)
	require.NoError(t, err)
	parentStatus.SplitState = &storedb.SplitState{}
	parentStatus.SplitState.SetPhase(storedb.SplitState_PHASE_SPLITTING)
	parentStatus.SplitState.SetSplitKey(splitKey)
	parentStatus.SplitState.SetNewShardId(uint64(childID))
	writeShardStatus(t, db, parentStatus)
	writeShardStatus(t, db, splitShardStatus(
		childID,
		store.ShardState_SplitOffPreSnap,
		[2][]byte{splitKey, []byte{0xff}},
		true,
		false,
		0,
	))

	shardID, err := findWriteShardForKey(ms.tm, table, "mango")
	require.NoError(t, err)
	assert.Equal(t, parentID, shardID)
}

func TestPartitionWriteKeysByShard_KeepsParentAsWriteOwnerWhileParentSplitIsActive(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentID := types.ID(10)
	childID := types.ID(11)
	splitKey := []byte("m:\x00")

	table := &store.Table{
		Name: "test_table",
		Shards: map[types.ID]*store.ShardConfig{
			parentID: {ByteRange: [2][]byte{[]byte{0x00}, splitKey}},
			childID:  {ByteRange: [2][]byte{splitKey, []byte{0xff}}},
		},
	}

	writeShardStatus(t, db, splitShardStatus(
		parentID,
		store.ShardState_Splitting,
		[2][]byte{[]byte{0x00}, splitKey},
		false,
		false,
		1,
	))
	parentStatus, err := ms.tm.GetShardStatus(parentID)
	require.NoError(t, err)
	parentStatus.SplitState = &storedb.SplitState{}
	parentStatus.SplitState.SetPhase(storedb.SplitState_PHASE_SPLITTING)
	parentStatus.SplitState.SetSplitKey(splitKey)
	parentStatus.SplitState.SetNewShardId(uint64(childID))
	writeShardStatus(t, db, parentStatus)
	writeShardStatus(t, db, splitShardStatus(
		childID,
		store.ShardState_SplitOffPreSnap,
		[2][]byte{splitKey, []byte{0xff}},
		true,
		false,
		2,
	))

	partitions, unfound, err := partitionWriteKeysByShard(ms.tm, table, []string{"apple", "mango"})
	require.NoError(t, err)
	assert.Empty(t, unfound)
	assert.Equal(t, []string{"apple", "mango"}, partitions[parentID])
	assert.Nil(t, partitions[childID])
}

func TestPartitionWriteKeysByShard_UsesChildOnceParentSplitIsInactive(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentID := types.ID(10)
	childID := types.ID(11)
	splitKey := []byte("m:\x00")

	table := &store.Table{
		Name: "test_table",
		Shards: map[types.ID]*store.ShardConfig{
			parentID: {ByteRange: [2][]byte{[]byte{0x00}, splitKey}},
			childID:  {ByteRange: [2][]byte{splitKey, []byte{0xff}}},
		},
	}

	writeShardStatus(t, db, splitShardStatus(
		parentID,
		store.ShardState_Default,
		[2][]byte{[]byte{0x00}, splitKey},
		false,
		false,
		1,
	))
	writeShardStatus(t, db, splitShardStatus(
		childID,
		store.ShardState_Default,
		[2][]byte{splitKey, []byte{0xff}},
		true,
		false,
		2,
	))

	partitions, unfound, err := partitionWriteKeysByShard(ms.tm, table, []string{"apple", "mango"})
	require.NoError(t, err)
	assert.Empty(t, unfound)
	assert.Equal(t, []string{"apple"}, partitions[parentID])
	assert.Equal(t, []string{"mango"}, partitions[childID])
}

func TestResolveWriteShardID_AdjacentShardNotReroutedDuringSplit(t *testing.T) {
	// Verifies that a normal shard adjacent to an actively-splitting parent
	// is NOT incorrectly rerouted to the splitting parent. The split key
	// ensures ByteRange boundaries don't collide.
	splittingParentID := types.ID(10)
	splitChildID := types.ID(11)
	adjacentShardID := types.ID(12)
	splitKey := []byte("m:\x00")
	activeSplitState := &storedb.SplitState{}
	activeSplitState.SetPhase(storedb.SplitState_PHASE_SPLITTING)
	activeSplitState.SetSplitKey(splitKey)
	activeSplitState.SetNewShardId(uint64(splitChildID))

	statuses := map[types.ID]*store.ShardStatus{
		splittingParentID: {
			ShardInfo: storedb.ShardInfo{
				ShardConfig: storedb.ShardConfig{
					ByteRange: [2][]byte{[]byte{0x00}, splitKey},
				},
				SplitState: activeSplitState,
			},
			Table: "test_table",
			State: store.ShardState_Splitting,
		},
		splitChildID: {
			ShardInfo: storedb.ShardInfo{
				ShardConfig: storedb.ShardConfig{
					ByteRange: [2][]byte{splitKey, []byte{0x80}},
				},
			},
			Table: "test_table",
			State: store.ShardState_SplitOffPreSnap,
		},
		adjacentShardID: {
			ShardInfo: storedb.ShardInfo{
				ShardConfig: storedb.ShardConfig{
					ByteRange: [2][]byte{[]byte{0x80}, []byte{0xff}},
				},
			},
			Table: "test_table",
			State: store.ShardState_Default,
		},
	}

	// The adjacent shard should resolve to itself, not to the splitting parent.
	resolved, err := resolveWriteShardID(statuses, adjacentShardID)
	require.NoError(t, err)
	assert.Equal(t, adjacentShardID, resolved)

	// The split child should resolve to the parent.
	resolved, err = resolveWriteShardID(statuses, splitChildID)
	require.NoError(t, err)
	assert.Equal(t, splittingParentID, resolved)
}

func TestResolveWriteShardID_FinalizingParentIsNotWriteOwner(t *testing.T) {
	parentID := types.ID(20)
	childID := types.ID(21)
	splitKey := []byte("m:\x00")

	finalizingState := &storedb.SplitState{}
	finalizingState.SetPhase(storedb.SplitState_PHASE_FINALIZING)
	finalizingState.SetSplitKey(splitKey)
	finalizingState.SetNewShardId(uint64(childID))

	statuses := map[types.ID]*store.ShardStatus{
		parentID: {
			ShardInfo: storedb.ShardInfo{
				ShardConfig: storedb.ShardConfig{
					ByteRange: [2][]byte{[]byte{0x00}, splitKey},
				},
				SplitState: finalizingState,
			},
			Table: "test_table",
			State: store.ShardState_Splitting,
		},
		childID: {
			ShardInfo: storedb.ShardInfo{
				ShardConfig: storedb.ShardConfig{
					ByteRange: [2][]byte{splitKey, []byte{0xff}},
				},
				SplitReplayRequired: true,
				SplitReplayCaughtUp: true,
				SplitCutoverReady:   false,
				HasSnapshot:         true,
				RaftStatus:          &common.RaftStatus{Lead: 1},
			},
			Table: "test_table",
			State: store.ShardState_SplitOffPreSnap,
		},
	}

	_, err := resolveWriteShardID(statuses, childID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parent shard not found")
}

func TestResolveWriteShardID_PreSplitParentWithoutSplitStateIsNotWriteOwner(t *testing.T) {
	parentID := types.ID(30)
	childID := types.ID(31)
	splitKey := []byte("m:\x00")

	statuses := map[types.ID]*store.ShardStatus{
		parentID: {
			ShardInfo: storedb.ShardInfo{
				ShardConfig: storedb.ShardConfig{
					ByteRange: [2][]byte{[]byte{0x00}, splitKey},
				},
			},
			Table: "test_table",
			State: store.ShardState_PreSplit,
		},
		childID: {
			ShardInfo: storedb.ShardInfo{
				ShardConfig: storedb.ShardConfig{
					ByteRange: [2][]byte{splitKey, []byte{0xff}},
				},
				SplitReplayRequired: true,
				SplitReplayCaughtUp: true,
				SplitCutoverReady:   false,
				HasSnapshot:         true,
				RaftStatus:          &common.RaftStatus{Lead: 1},
			},
			Table: "test_table",
			State: store.ShardState_SplitOffPreSnap,
		},
	}

	_, err := resolveWriteShardID(statuses, childID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parent shard not found")
}

func TestResolveWriteShardID_ErrorWhenStatusMissing(t *testing.T) {
	statuses := map[types.ID]*store.ShardStatus{}
	_, err := resolveWriteShardID(statuses, types.ID(99))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no status info available")
}

func TestShouldRouteWriteToParent_NormalShardReturnsFalse(t *testing.T) {
	status := &store.ShardStatus{
		State: store.ShardState_Default,
	}
	assert.False(t, shouldRouteWriteToParent(status))
}

func TestFindParentShardForSplitOffStatus_NoParent(t *testing.T) {
	childStatus := &store.ShardStatus{
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{[]byte("m:\x00"), []byte{0xff}},
			},
		},
		Table: "test_table",
		State: store.ShardState_SplitOffPreSnap,
	}
	_, err := findParentShardForSplitOffStatus(map[types.ID]*store.ShardStatus{}, childStatus)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parent shard not found")
}

func TestLeaderClientForShardWithEffectiveID_NoLeader_FallsBackToReportedBy(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	shardID := types.ID(100)
	node1 := types.ID(1)
	node2 := types.ID(2)

	// Shard has no leader (Lead=0), but nodes 1 and 2 have reported having the shard.
	status := newShardStatus(shardID, 0, []types.ID{node1, node2}, []types.ID{node1, node2})
	writeShardStatus(t, db, status)
	writeStoreStatus(t, db, node1, true)
	writeStoreStatus(t, db, node2, true)

	// leaderClientForShardWithEffectiveID should fall back to a reporting node.
	effectiveID, client, err := ms.leaderClientForShardWithEffectiveID(context.Background(), shardID)
	require.NoError(t, err, "expected fallback to succeed when reporting nodes are available")
	assert.NotNil(t, client)
	assert.Equal(t, shardID, effectiveID)
}

func TestStrictLeaderClientForShardWithEffectiveID_NoLeader_ReturnsError(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	shardID := types.ID(100)
	node1 := types.ID(1)
	node2 := types.ID(2)

	// Same setup: no leader, but reporting nodes exist.
	status := newShardStatus(shardID, 0, []types.ID{node1, node2}, []types.ID{node1, node2})
	writeShardStatus(t, db, status)
	writeStoreStatus(t, db, node1, true)
	writeStoreStatus(t, db, node2, true)

	// strictLeaderClientForShardWithEffectiveID must NOT fall back — returns ErrNoLeaderElected.
	_, _, err := ms.strictLeaderClientForShardWithEffectiveID(context.Background(), shardID)
	require.ErrorIs(t, err, ErrNoLeaderElected)
}

func TestLeaderClientForShardWithEffectiveID_NoLeader_FallsBackToPeers(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	shardID := types.ID(100)
	node1 := types.ID(1)

	// No leader, ReportedBy is empty, but Peers has node1.
	status := newShardStatus(shardID, 0, nil, []types.ID{node1})
	writeShardStatus(t, db, status)
	writeStoreStatus(t, db, node1, true)

	effectiveID, client, err := ms.leaderClientForShardWithEffectiveID(context.Background(), shardID)
	require.NoError(t, err, "expected fallback to peers when ReportedBy is empty")
	assert.NotNil(t, client)
	assert.Equal(t, shardID, effectiveID)
}

func TestLeaderClientForShardWithEffectiveID_NoLeader_NoHealthyNodes_ReturnsError(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	shardID := types.ID(100)
	node1 := types.ID(1)

	// No leader, node1 is unhealthy.
	status := newShardStatus(shardID, 0, []types.ID{node1}, []types.ID{node1})
	writeShardStatus(t, db, status)
	writeStoreStatus(t, db, node1, false) // Suspect / not reachable

	_, _, err := ms.leaderClientForShardWithEffectiveID(context.Background(), shardID)
	require.ErrorIs(t, err, ErrNoLeaderElected, "should return ErrNoLeaderElected when no healthy nodes available for fallback")
}

func TestLeaderClientForShardWithEffectiveID_LeaderElected_ReturnsLeader(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	shardID := types.ID(100)
	leaderNode := types.ID(1)
	follower := types.ID(2)

	status := newShardStatus(shardID, leaderNode, []types.ID{leaderNode, follower}, []types.ID{leaderNode, follower})
	writeShardStatus(t, db, status)
	writeStoreStatus(t, db, leaderNode, true)
	writeStoreStatus(t, db, follower, true)

	effectiveID, client, err := ms.leaderClientForShardWithEffectiveID(context.Background(), shardID)
	require.NoError(t, err)
	assert.NotNil(t, client)
	assert.Equal(t, shardID, effectiveID)
}

func TestLeaderClientForShard_NoLeader_ReturnsErrNoLeaderElected(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	shardID := types.ID(100)
	node1 := types.ID(1)

	status := newShardStatus(shardID, 0, []types.ID{node1}, []types.ID{node1})
	writeShardStatus(t, db, status)
	writeStoreStatus(t, db, node1, true)

	// leaderClientForShard always returns ErrNoLeaderElected when Lead=0,
	// regardless of available nodes — the fallback is in the WithEffectiveID layer.
	_, err := ms.leaderClientForShard(context.Background(), shardID)
	require.ErrorIs(t, err, ErrNoLeaderElected)
}

func TestShouldFallbackToParentShard_UsesFullReadinessGate(t *testing.T) {
	ms, _ := setupTestMetadataStore(t)

	tests := []struct {
		name     string
		status   *store.ShardStatus
		expected bool
	}{
		{
			name: "splitting off without snapshot falls back",
			status: &store.ShardStatus{
				State: store.ShardState_SplittingOff,
			},
			expected: true,
		},
		{
			name: "pre-snap shard with snapshot but still initializing falls back",
			status: &store.ShardStatus{
				State: store.ShardState_SplitOffPreSnap,
				ShardInfo: storedb.ShardInfo{
					HasSnapshot:  true,
					Initializing: true,
					RaftStatus:   &common.RaftStatus{Lead: 1},
				},
			},
			expected: true,
		},
		{
			name: "pre-snap shard with snapshot but no leader falls back",
			status: &store.ShardStatus{
				State: store.ShardState_SplitOffPreSnap,
				ShardInfo: storedb.ShardInfo{
					HasSnapshot:  true,
					Initializing: false,
					RaftStatus:   &common.RaftStatus{Lead: 0},
				},
			},
			expected: true,
		},
		{
			name: "pre-snap shard that is fully ready stops falling back",
			status: &store.ShardStatus{
				State: store.ShardState_SplitOffPreSnap,
				ShardInfo: storedb.ShardInfo{
					HasSnapshot:  true,
					Initializing: false,
					RaftStatus:   &common.RaftStatus{Lead: 1},
				},
			},
			expected: false,
		},
		{
			name: "default shard does not fall back",
			status: &store.ShardStatus{
				State: store.ShardState_Default,
				ShardInfo: storedb.ShardInfo{
					HasSnapshot:  true,
					Initializing: false,
					RaftStatus:   &common.RaftStatus{Lead: 1},
				},
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, ms.shouldFallbackToParentShard(tc.status))
		})
	}
}

func TestLeaderClientForShardWithEffectiveID_SplitOffPreSnapFallsBackToParentUntilReady(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentShardID := types.ID(100)
	splitOffShardID := types.ID(101)
	parentNode := types.ID(1)
	splitKey := []byte("m")

	parentStatus := &store.ShardStatus{
		ID:    parentShardID,
		Table: "test_table",
		State: store.ShardState_Splitting,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{{0x00}, splitKey},
			},
			Peers:      common.NewPeerSet(parentNode),
			ReportedBy: common.NewPeerSet(parentNode),
			RaftStatus: &common.RaftStatus{
				Lead:   parentNode,
				Voters: common.NewPeerSet(parentNode),
			},
		},
	}
	parentStatus.SplitState = &storedb.SplitState{}
	parentStatus.SplitState.SetPhase(storedb.SplitState_PHASE_SPLITTING)
	parentStatus.SplitState.SetSplitKey(splitKey)
	parentStatus.SplitState.SetNewShardId(uint64(splitOffShardID))
	splitOffStatus := &store.ShardStatus{
		ID:    splitOffShardID,
		Table: "test_table",
		State: store.ShardState_SplitOffPreSnap,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xff}},
			},
			Peers:        common.NewPeerSet(parentNode),
			ReportedBy:   common.NewPeerSet(parentNode),
			HasSnapshot:  true,
			Initializing: true,
			RaftStatus: &common.RaftStatus{
				Lead:   parentNode,
				Voters: common.NewPeerSet(parentNode),
			},
		},
	}

	writeShardStatus(t, db, parentStatus)
	writeShardStatus(t, db, splitOffStatus)
	writeStoreStatus(t, db, parentNode, true)

	effectiveID, client, err := ms.leaderClientForShardWithEffectiveID(context.Background(), splitOffShardID)
	require.NoError(t, err)
	assert.NotNil(t, client)
	assert.Equal(t, parentShardID, effectiveID)
}

func TestStrictLeaderClientForShardWithEffectiveID_LeaderUnreachable_ReturnsError(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	shardID := types.ID(100)
	leaderNode := types.ID(1)
	follower := types.ID(2)

	// Leader is elected but unhealthy. Follower is healthy.
	// The strict function must NOT fall back to the follower.
	status := newShardStatus(shardID, leaderNode, []types.ID{leaderNode, follower}, []types.ID{leaderNode, follower})
	writeShardStatus(t, db, status)
	writeStoreStatus(t, db, leaderNode, false) // unhealthy
	writeStoreStatus(t, db, follower, true)    // healthy

	_, _, err := ms.strictLeaderClientForShardWithEffectiveID(context.Background(), shardID)
	require.ErrorIs(t, err, ErrNoLeaderElected,
		"strict path must NOT fall back to follower when leader is unreachable")
}

func TestStrictLeaderClientForShardWithEffectiveID_LeaderNotReported_StillReturnsLeader(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	shardID := types.ID(100)
	leaderNode := types.ID(1)
	follower := types.ID(2)

	// Leader elected and healthy, but ReportedBy only has the follower (leader hasn't
	// reported yet, e.g., during rebalancing). The strict function should still return
	// the leader — we trust Raft's leader election.
	status := newShardStatus(shardID, leaderNode, []types.ID{follower}, []types.ID{leaderNode, follower})
	writeShardStatus(t, db, status)
	writeStoreStatus(t, db, leaderNode, true)
	writeStoreStatus(t, db, follower, true)

	effectiveID, client, err := ms.strictLeaderClientForShardWithEffectiveID(context.Background(), shardID)
	require.NoError(t, err, "strict path should return leader even if not in ReportedBy")
	assert.NotNil(t, client)
	assert.Equal(t, shardID, effectiveID)
}

func TestIsTransientShardError_NoLeaderElected(t *testing.T) {
	assert.True(t, isTransientShardError(ErrNoLeaderElected),
		"ErrNoLeaderElected should be classified as transient")
}
