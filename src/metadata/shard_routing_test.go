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
	"slices"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/client"
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

func writeTable(t *testing.T, db kv.DB, table *store.Table) {
	t.Helper()
	data, err := json.Marshal(table)
	require.NoError(t, err)
	err = db.Batch(context.Background(),
		[][2][]byte{{[]byte("tm:t:" + table.Name), data}},
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

func writeStoreStatusWithShards(
	t *testing.T,
	db kv.DB,
	nodeID types.ID,
	healthy bool,
	shards map[types.ID]*store.ShardInfo,
) {
	t.Helper()
	state := store.StoreState_Healthy
	if !healthy {
		state = store.StoreState_Unhealthy
	}
	status := &tablemgr.StoreStatus{
		StoreInfo: store.StoreInfo{
			ID:     nodeID,
			ApiURL: "http://127.0.0.1:0",
		},
		State:  state,
		Shards: shards,
	}
	data, err := json.Marshal(status)
	require.NoError(t, err)
	err = db.Batch(context.Background(),
		[][2][]byte{{[]byte("tm:sts:" + nodeID.String()), data}},
		nil,
	)
	require.NoError(t, err)
}

type stubStoreRPC struct {
	client.StoreRPC
	id                  types.ID
	batchFn             func(shardID types.ID, writes [][2][]byte) error
	lookupWithVersionFn func(shardID types.ID, key string) ([]byte, uint64, error)
	batchHit            int
	lookupHit           int
}

func (s *stubStoreRPC) ID() types.ID { return s.id }

func (s *stubStoreRPC) Batch(
	_ context.Context,
	shardID types.ID,
	writes [][2][]byte,
	_ [][]byte,
	_ []*storedb.Transform,
	_ storedb.Op_SyncLevel,
) error {
	s.batchHit++
	if s.batchFn != nil {
		return s.batchFn(shardID, writes)
	}
	return nil
}

func (s *stubStoreRPC) LookupWithVersion(
	_ context.Context,
	shardID types.ID,
	key string,
) ([]byte, uint64, error) {
	s.lookupHit++
	if s.lookupWithVersionFn != nil {
		return s.lookupWithVersionFn(shardID, key)
	}
	return nil, 0, client.ErrNotFound
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
			parentID: {ByteRange: [2][]byte{{0x00}, splitKey}},
			childID:  {ByteRange: [2][]byte{splitKey, {0xff}}},
		},
	}

	writeShardStatus(t, db, splitShardStatus(
		parentID,
		store.ShardState_PreSplit,
		[2][]byte{{0x00}, splitKey},
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
		[2][]byte{splitKey, {0xff}},
		true,
		false,
		0,
	))

	shardID, err := findWriteShardForKey(ms.tm, table, "mango")
	require.NoError(t, err)
	assert.Equal(t, parentID, shardID)
}

func TestFindWriteShardForKey_UsesChildWhenParentSplitIsActiveButTableStillPointsAtParent(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentID := types.ID(10)
	childID := types.ID(11)
	splitKey := []byte("m:\x00")

	table := &store.Table{
		Name: "test_table",
		Shards: map[types.ID]*store.ShardConfig{
			parentID: {ByteRange: [2][]byte{{0x00}, {0xff}}},
		},
	}

	parentState := &storedb.SplitState{}
	parentState.SetPhase(storedb.SplitState_PHASE_SPLITTING)
	parentState.SetSplitKey(splitKey)
	parentState.SetNewShardId(uint64(childID))
	writeShardStatus(t, db, &store.ShardStatus{
		ID:    parentID,
		Table: "test_table",
		State: store.ShardState_Splitting,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{{0x00}, splitKey},
			},
			SplitState: parentState,
		},
	})
	writeShardStatus(t, db, &store.ShardStatus{
		ID:    childID,
		Table: "test_table",
		State: store.ShardState_SplitOffPreSnap,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xff}},
			},
			HasSnapshot:         true,
			SplitReplayRequired: true,
			SplitReplayCaughtUp: true,
			RaftStatus:          &common.RaftStatus{Lead: 2, Voters: common.NewPeerSet(1, 2, 3)},
		},
	})

	shardID, err := findWriteShardForKey(ms.tm, table, "mango")
	require.NoError(t, err)
	assert.Equal(t, childID, shardID)
}

func TestFindWriteShardForKey_KeepsParentWhenSplitIsRollingBack(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentID := types.ID(10)
	childID := types.ID(11)
	splitKey := []byte("m:\x00")

	table := &store.Table{
		Name: "test_table",
		Shards: map[types.ID]*store.ShardConfig{
			parentID: {ByteRange: [2][]byte{{0x00}, {0xff}}},
		},
	}

	parentState := &storedb.SplitState{}
	parentState.SetPhase(storedb.SplitState_PHASE_ROLLING_BACK)
	parentState.SetSplitKey(splitKey)
	parentState.SetNewShardId(uint64(childID))
	writeShardStatus(t, db, &store.ShardStatus{
		ID:    parentID,
		Table: "test_table",
		State: store.ShardState_Default,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{{0x00}, {0xff}},
			},
			SplitState: parentState,
		},
	})
	writeShardStatus(t, db, &store.ShardStatus{
		ID:    childID,
		Table: "test_table",
		State: store.ShardState_SplitOffPreSnap,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xff}},
			},
			HasSnapshot:         true,
			SplitReplayRequired: true,
			SplitReplayCaughtUp: true,
			SplitCutoverReady:   true,
			RaftStatus:          &common.RaftStatus{Lead: 2, Voters: common.NewPeerSet(1, 2, 3)},
		},
	})

	shardID, err := findWriteShardForKey(ms.tm, table, "mango")
	require.NoError(t, err)
	assert.Equal(t, parentID, shardID)
}

func TestFindReadShardForKey_KeepsParentUntilChildIsReadReady(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentID := types.ID(10)
	childID := types.ID(11)
	splitKey := []byte("m:\x00")

	table := &store.Table{
		Name: "test_table",
		Shards: map[types.ID]*store.ShardConfig{
			parentID: {ByteRange: [2][]byte{{0x00}, {0xff}}},
		},
	}

	parentState := &storedb.SplitState{}
	parentState.SetPhase(storedb.SplitState_PHASE_SPLITTING)
	parentState.SetSplitKey(splitKey)
	parentState.SetNewShardId(uint64(childID))
	writeShardStatus(t, db, &store.ShardStatus{
		ID:    parentID,
		Table: "test_table",
		State: store.ShardState_Splitting,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{{0x00}, splitKey},
			},
			SplitState: parentState,
		},
	})
	writeShardStatus(t, db, &store.ShardStatus{
		ID:    childID,
		Table: "test_table",
		State: store.ShardState_SplitOffPreSnap,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xff}},
			},
			HasSnapshot:         true,
			Initializing:        false,
			SplitReplayRequired: true,
			SplitReplayCaughtUp: true,
			SplitCutoverReady:   false,
			RaftStatus:          &common.RaftStatus{Lead: 2, Voters: common.NewPeerSet(1, 2, 3)},
		},
	})

	shardID, err := findReadShardForKey(ms.tm, table, "mango")
	require.NoError(t, err)
	assert.Equal(t, parentID, shardID)
}

func TestFindReadShardForKey_UsesChildWhenParentSplitIsActiveAndChildReadReady(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentID := types.ID(10)
	childID := types.ID(11)
	splitKey := []byte("m:\x00")

	table := &store.Table{
		Name: "test_table",
		Shards: map[types.ID]*store.ShardConfig{
			parentID: {ByteRange: [2][]byte{{0x00}, {0xff}}},
		},
	}

	parentState := &storedb.SplitState{}
	parentState.SetPhase(storedb.SplitState_PHASE_FINALIZING)
	parentState.SetSplitKey(splitKey)
	parentState.SetNewShardId(uint64(childID))
	writeShardStatus(t, db, &store.ShardStatus{
		ID:    parentID,
		Table: "test_table",
		State: store.ShardState_Default,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{{0x00}, splitKey},
			},
			SplitState: parentState,
		},
	})
	writeShardStatus(t, db, &store.ShardStatus{
		ID:    childID,
		Table: "test_table",
		State: store.ShardState_Default,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xff}},
			},
			HasSnapshot:         true,
			Initializing:        false,
			SplitReplayRequired: true,
			SplitReplayCaughtUp: true,
			SplitCutoverReady:   true,
			SplitParentShardID:  parentID,
			RaftStatus:          &common.RaftStatus{Lead: 2, Voters: common.NewPeerSet(1, 2, 3)},
		},
	})

	shardID, err := findReadShardForKey(ms.tm, table, "mango")
	require.NoError(t, err)
	assert.Equal(t, childID, shardID)
}

func TestPartitionWriteKeysByShard_KeepsParentAsWriteOwnerUntilChildCatchesUp(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentID := types.ID(10)
	childID := types.ID(11)
	splitKey := []byte("m:\x00")

	table := &store.Table{
		Name: "test_table",
		Shards: map[types.ID]*store.ShardConfig{
			parentID: {ByteRange: [2][]byte{{0x00}, splitKey}},
			childID:  {ByteRange: [2][]byte{splitKey, {0xff}}},
		},
	}

	writeShardStatus(t, db, splitShardStatus(
		parentID,
		store.ShardState_Splitting,
		[2][]byte{{0x00}, splitKey},
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
		[2][]byte{splitKey, {0xff}},
		true,
		false,
		2,
	))
	childStatus, err := ms.tm.GetShardStatus(childID)
	require.NoError(t, err)
	childStatus.SplitReplayRequired = true
	childStatus.SplitReplayCaughtUp = false
	writeShardStatus(t, db, childStatus)

	partitions, unfound, err := partitionWriteKeysByShard(ms.tm, table, []string{"apple", "mango"})
	require.NoError(t, err)
	assert.Empty(t, unfound)
	assert.Equal(t, []string{"apple", "mango"}, partitions[parentID])
	assert.Nil(t, partitions[childID])
}

func TestPartitionWriteKeysByShard_UsesChildOnceItCanInitiateCutover(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentID := types.ID(10)
	childID := types.ID(11)
	splitKey := []byte("m:\x00")

	table := &store.Table{
		Name: "test_table",
		Shards: map[types.ID]*store.ShardConfig{
			parentID: {ByteRange: [2][]byte{{0x00}, splitKey}},
			childID:  {ByteRange: [2][]byte{splitKey, {0xff}}},
		},
	}

	writeShardStatus(t, db, splitShardStatus(
		parentID,
		store.ShardState_Splitting,
		[2][]byte{{0x00}, splitKey},
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
	writeShardStatus(t, db, &store.ShardStatus{
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xff}},
			},
			HasSnapshot:         true,
			Initializing:        false,
			Peers:               common.NewPeerSet(1, 2, 3),
			ReportedBy:          common.NewPeerSet(1, 2, 3),
			SplitReplayRequired: true,
			SplitReplayCaughtUp: true,
			SplitCutoverReady:   false,
			RaftStatus:          &common.RaftStatus{Lead: 2, Voters: common.NewPeerSet(1, 2, 3)},
		},
		ID:    childID,
		Table: "test_table",
		State: store.ShardState_SplitOffPreSnap,
	})

	partitions, unfound, err := partitionWriteKeysByShard(ms.tm, table, []string{"apple", "mango"})
	require.NoError(t, err)
	assert.Empty(t, unfound)
	assert.Equal(t, []string{"apple"}, partitions[parentID])
	assert.Equal(t, []string{"mango"}, partitions[childID])
}

func TestPartitionWriteKeysByShard_UsesChildOnceParentSplitIsInactive(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentID := types.ID(10)
	childID := types.ID(11)
	splitKey := []byte("m:\x00")

	table := &store.Table{
		Name: "test_table",
		Shards: map[types.ID]*store.ShardConfig{
			parentID: {ByteRange: [2][]byte{{0x00}, splitKey}},
			childID:  {ByteRange: [2][]byte{splitKey, {0xff}}},
		},
	}

	writeShardStatus(t, db, splitShardStatus(
		parentID,
		store.ShardState_Default,
		[2][]byte{{0x00}, splitKey},
		false,
		false,
		1,
	))
	writeShardStatus(t, db, splitShardStatus(
		childID,
		store.ShardState_Default,
		[2][]byte{splitKey, {0xff}},
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
					ByteRange: [2][]byte{{0x00}, splitKey},
				},
				SplitState: activeSplitState,
			},
			Table: "test_table",
			State: store.ShardState_Splitting,
		},
		splitChildID: {
			ShardInfo: storedb.ShardInfo{
				ShardConfig: storedb.ShardConfig{
					ByteRange: [2][]byte{splitKey, {0x80}},
				},
				HasSnapshot:  true,
				Initializing: false,
				RaftStatus:   &common.RaftStatus{Lead: 1},
			},
			Table: "test_table",
			State: store.ShardState_SplitOffPreSnap,
		},
		adjacentShardID: {
			ShardInfo: storedb.ShardInfo{
				ShardConfig: storedb.ShardConfig{
					ByteRange: [2][]byte{{0x80}, {0xff}},
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

	childStatus := statuses[splitChildID]
	childStatus.SplitReplayRequired = true
	childStatus.SplitReplayCaughtUp = false

	// The split child should resolve to the parent until it catches up.
	resolved, err = resolveWriteShardID(statuses, splitChildID)
	require.NoError(t, err)
	assert.Equal(t, splittingParentID, resolved)

	childStatus.SplitReplayCaughtUp = true
	resolved, err = resolveWriteShardID(statuses, splitChildID)
	require.NoError(t, err)
	assert.Equal(t, splitChildID, resolved)
}

func TestForwardInsertToShard_UsesChildDirectlyOnceItCanInitiateCutover(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	tableName := "test_table"
	table, err := ms.tm.CreateTable(tableName, tablemgr.TableConfig{
		NumShards: 1,
		StartID:   100,
	})
	require.NoError(t, err)
	require.Len(t, table.Shards, 1)

	var parentShardID types.ID
	for shardID := range table.Shards {
		parentShardID = shardID
	}
	childShardID := types.ID(101)
	parentNodeID := types.ID(1)
	childNodeID := types.ID(2)
	splitKey := []byte("m:\x00")
	parentRPC := &stubStoreRPC{
		id: parentNodeID,
		batchFn: func(shardID types.ID, writes [][2][]byte) error {
			assert.Equal(t, parentShardID, shardID)
			require.Len(t, writes, 1)
			return &client.ResponseError{
				StatusCode: http.StatusBadRequest,
				Body:       "key 6c617267652d303433 out of range [,6d3a00)",
			}
		},
	}
	childRPC := &stubStoreRPC{
		id: childNodeID,
		batchFn: func(shardID types.ID, writes [][2][]byte) error {
			assert.Equal(t, childShardID, shardID)
			require.Len(t, writes, 1)
			assert.Equal(t, []byte("mango"), writes[0][0])
			return nil
		},
	}
	ms.tm.SetStoreClientFactory(func(_ *http.Client, id types.ID, _ string) client.StoreRPC {
		switch id {
		case parentNodeID:
			return parentRPC
		case childNodeID:
			return childRPC
		default:
			return &stubStoreRPC{id: id}
		}
	})

	parentStatus, err := ms.tm.GetShardStatus(parentShardID)
	require.NoError(t, err)
	parentStatus.Peers = common.NewPeerSet(parentNodeID)
	parentStatus.ReportedBy = common.NewPeerSet(parentNodeID)
	parentStatus.RaftStatus = &common.RaftStatus{
		Lead:   parentNodeID,
		Voters: common.NewPeerSet(parentNodeID),
	}
	writeShardStatus(t, db, parentStatus)

	_, _, err = ms.tm.ReassignShardsForSplit(tablemgr.SplitTransition{
		ShardID:      parentShardID,
		SplitShardID: childShardID,
		SplitKey:     splitKey,
		TableName:    tableName,
	})
	require.NoError(t, err)

	parentStatus, err = ms.tm.GetShardStatus(parentShardID)
	require.NoError(t, err)
	parentStatus.State = store.ShardState_Splitting
	parentStatus.SplitState = &storedb.SplitState{}
	parentStatus.SplitState.SetPhase(storedb.SplitState_PHASE_SPLITTING)
	parentStatus.SplitState.SetSplitKey(splitKey)
	parentStatus.SplitState.SetNewShardId(uint64(childShardID))
	parentStatus.SplitState.SetOriginalRangeEnd([]byte{0xff})
	parentStatus.Peers = common.NewPeerSet(parentNodeID)
	parentStatus.ReportedBy = common.NewPeerSet(parentNodeID)
	parentStatus.RaftStatus = &common.RaftStatus{
		Lead:   parentNodeID,
		Voters: common.NewPeerSet(parentNodeID),
	}
	writeShardStatus(t, db, parentStatus)

	childStatus, err := ms.tm.GetShardStatus(childShardID)
	require.NoError(t, err)
	childStatus.State = store.ShardState_SplitOffPreSnap
	childStatus.HasSnapshot = true
	childStatus.Initializing = false
	childStatus.SplitReplayRequired = true
	childStatus.SplitReplayCaughtUp = true
	childStatus.SplitCutoverReady = true
	childStatus.Peers = common.NewPeerSet(childNodeID)
	childStatus.ReportedBy = common.NewPeerSet(childNodeID)
	childStatus.RaftStatus = &common.RaftStatus{
		Lead:   childNodeID,
		Voters: common.NewPeerSet(childNodeID),
	}
	writeShardStatus(t, db, childStatus)

	writeStoreStatus(t, db, parentNodeID, true)
	writeStoreStatus(t, db, childNodeID, true)

	err = ms.forwardInsertToShard(
		context.Background(),
		tableName,
		"mango",
		map[string]any{"content": "value"},
		storedb.Op_SyncLevelWrite,
	)
	require.NoError(t, err)
	assert.Equal(t, 0, parentRPC.batchHit)
	assert.Equal(t, 1, childRPC.batchHit)
}

func TestForwardInsertToShard_DoesNotBypassToChildBeforeCutover(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	tableName := "test_table"
	table, err := ms.tm.CreateTable(tableName, tablemgr.TableConfig{
		NumShards: 1,
		StartID:   100,
	})
	require.NoError(t, err)
	require.Len(t, table.Shards, 1)

	var parentShardID types.ID
	for shardID := range table.Shards {
		parentShardID = shardID
	}
	childShardID := types.ID(101)
	parentNodeID := types.ID(1)
	childNodeID := types.ID(2)
	splitKey := []byte("m:\x00")

	parentRPC := &stubStoreRPC{
		id: parentNodeID,
		batchFn: func(shardID types.ID, writes [][2][]byte) error {
			assert.Equal(t, parentShardID, shardID)
			require.Len(t, writes, 1)
			return &client.ResponseError{
				StatusCode: http.StatusBadRequest,
				Body:       "key 6d616e676f out of range [,6d3a00)",
			}
		},
	}
	childRPC := &stubStoreRPC{
		id: childNodeID,
		batchFn: func(shardID types.ID, writes [][2][]byte) error {
			t.Fatalf("unexpected direct child write to shard %s before cutover", shardID)
			return nil
		},
	}
	ms.tm.SetStoreClientFactory(func(_ *http.Client, id types.ID, _ string) client.StoreRPC {
		switch id {
		case parentNodeID:
			return parentRPC
		case childNodeID:
			return childRPC
		default:
			return &stubStoreRPC{id: id}
		}
	})

	parentStatus, err := ms.tm.GetShardStatus(parentShardID)
	require.NoError(t, err)
	parentStatus.Peers = common.NewPeerSet(parentNodeID)
	parentStatus.ReportedBy = common.NewPeerSet(parentNodeID)
	parentStatus.RaftStatus = &common.RaftStatus{
		Lead:   parentNodeID,
		Voters: common.NewPeerSet(parentNodeID),
	}
	writeShardStatus(t, db, parentStatus)

	_, _, err = ms.tm.ReassignShardsForSplit(tablemgr.SplitTransition{
		ShardID:      parentShardID,
		SplitShardID: childShardID,
		SplitKey:     splitKey,
		TableName:    tableName,
	})
	require.NoError(t, err)

	parentStatus, err = ms.tm.GetShardStatus(parentShardID)
	require.NoError(t, err)
	parentStatus.State = store.ShardState_Splitting
	parentStatus.SplitState = &storedb.SplitState{}
	parentStatus.SplitState.SetPhase(storedb.SplitState_PHASE_SPLITTING)
	parentStatus.SplitState.SetSplitKey(splitKey)
	parentStatus.SplitState.SetNewShardId(uint64(childShardID))
	parentStatus.SplitState.SetOriginalRangeEnd([]byte{0xff})
	parentStatus.Peers = common.NewPeerSet(parentNodeID)
	parentStatus.ReportedBy = common.NewPeerSet(parentNodeID)
	parentStatus.RaftStatus = &common.RaftStatus{
		Lead:   parentNodeID,
		Voters: common.NewPeerSet(parentNodeID),
	}
	writeShardStatus(t, db, parentStatus)

	childStatus, err := ms.tm.GetShardStatus(childShardID)
	require.NoError(t, err)
	childStatus.State = store.ShardState_SplitOffPreSnap
	childStatus.HasSnapshot = true
	childStatus.Initializing = false
	childStatus.SplitReplayRequired = true
	childStatus.SplitReplayCaughtUp = false
	childStatus.SplitCutoverReady = false
	childStatus.Peers = common.NewPeerSet(childNodeID)
	childStatus.ReportedBy = common.NewPeerSet(childNodeID)
	childStatus.RaftStatus = &common.RaftStatus{
		Lead:   childNodeID,
		Voters: common.NewPeerSet(childNodeID),
	}
	writeShardStatus(t, db, childStatus)

	writeStoreStatus(t, db, parentNodeID, true)
	writeStoreStatus(t, db, childNodeID, true)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err = ms.forwardInsertToShard(
		ctx,
		tableName,
		"mango",
		map[string]any{"content": "value"},
		storedb.Op_SyncLevelWrite,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, parentRPC.batchHit, 1)
	assert.Equal(t, 0, childRPC.batchHit)
}

func TestForwardBatchToShard_ReroutesToChildAfterParentOutOfRange(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	tableName := "test_table"
	table, err := ms.tm.CreateTable(tableName, tablemgr.TableConfig{
		NumShards: 1,
		StartID:   100,
	})
	require.NoError(t, err)
	require.Len(t, table.Shards, 1)

	var parentShardID types.ID
	for shardID := range table.Shards {
		parentShardID = shardID
	}
	childShardID := types.ID(101)
	parentNodeID := types.ID(1)
	childNodeID := types.ID(2)
	splitKey := []byte("m:\x00")

	parentRPC := &stubStoreRPC{
		id: parentNodeID,
		batchFn: func(shardID types.ID, writes [][2][]byte) error {
			assert.Equal(t, parentShardID, shardID)
			require.Len(t, writes, 1)
			assert.Equal(t, []byte("mango"), writes[0][0])
			return &client.ResponseError{
				StatusCode: http.StatusBadRequest,
				Body:       "key 6d616e676f out of range [,6d3a00)",
			}
		},
	}
	childRPC := &stubStoreRPC{
		id: childNodeID,
		batchFn: func(shardID types.ID, writes [][2][]byte) error {
			assert.Equal(t, childShardID, shardID)
			require.Len(t, writes, 1)
			assert.Equal(t, []byte("mango"), writes[0][0])
			return nil
		},
	}
	ms.tm.SetStoreClientFactory(func(_ *http.Client, id types.ID, _ string) client.StoreRPC {
		switch id {
		case parentNodeID:
			return parentRPC
		case childNodeID:
			return childRPC
		default:
			return &stubStoreRPC{id: id}
		}
	})

	parentStatus, err := ms.tm.GetShardStatus(parentShardID)
	require.NoError(t, err)
	parentStatus.Peers = common.NewPeerSet(parentNodeID)
	parentStatus.ReportedBy = common.NewPeerSet(parentNodeID)
	parentStatus.RaftStatus = &common.RaftStatus{
		Lead:   parentNodeID,
		Voters: common.NewPeerSet(parentNodeID),
	}
	writeShardStatus(t, db, parentStatus)

	_, _, err = ms.tm.ReassignShardsForSplit(tablemgr.SplitTransition{
		ShardID:      parentShardID,
		SplitShardID: childShardID,
		SplitKey:     splitKey,
		TableName:    tableName,
	})
	require.NoError(t, err)

	parentStatus, err = ms.tm.GetShardStatus(parentShardID)
	require.NoError(t, err)
	parentStatus.State = store.ShardState_Splitting
	parentStatus.SplitState = &storedb.SplitState{}
	parentStatus.SplitState.SetPhase(storedb.SplitState_PHASE_SPLITTING)
	parentStatus.SplitState.SetSplitKey(splitKey)
	parentStatus.SplitState.SetNewShardId(uint64(childShardID))
	parentStatus.SplitState.SetOriginalRangeEnd([]byte{0xff})
	parentStatus.Peers = common.NewPeerSet(parentNodeID)
	parentStatus.ReportedBy = common.NewPeerSet(parentNodeID)
	parentStatus.RaftStatus = &common.RaftStatus{
		Lead:   parentNodeID,
		Voters: common.NewPeerSet(parentNodeID),
	}
	writeShardStatus(t, db, parentStatus)

	childStatus, err := ms.tm.GetShardStatus(childShardID)
	require.NoError(t, err)
	childStatus.State = store.ShardState_SplitOffPreSnap
	childStatus.HasSnapshot = true
	childStatus.Initializing = false
	childStatus.SplitReplayRequired = true
	childStatus.SplitReplayCaughtUp = true
	childStatus.SplitCutoverReady = true
	childStatus.Peers = common.NewPeerSet(childNodeID)
	childStatus.ReportedBy = common.NewPeerSet(childNodeID)
	childStatus.RaftStatus = &common.RaftStatus{
		Lead:   childNodeID,
		Voters: common.NewPeerSet(childNodeID),
	}
	writeShardStatus(t, db, childStatus)

	writeStoreStatus(t, db, parentNodeID, true)
	writeStoreStatus(t, db, childNodeID, true)

	err = ms.forwardBatchToShard(
		context.Background(),
		parentShardID,
		[][2][]byte{{[]byte("mango"), []byte(`{"content":"value"}`)}},
		nil,
		nil,
		storedb.Op_SyncLevelWrite,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, parentRPC.batchHit)
	assert.Equal(t, 1, childRPC.batchHit)
}

func TestForwardBatchToShard_ReroutesToParentDuringRollback(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	tableName := "test_table"
	table, err := ms.tm.CreateTable(tableName, tablemgr.TableConfig{
		NumShards: 1,
		StartID:   100,
	})
	require.NoError(t, err)
	require.Len(t, table.Shards, 1)

	var parentShardID types.ID
	for shardID := range table.Shards {
		parentShardID = shardID
	}
	childShardID := types.ID(101)
	parentNodeID := types.ID(1)
	childNodeID := types.ID(2)
	splitKey := []byte("m:\x00")

	parentRPC := &stubStoreRPC{
		id: parentNodeID,
		batchFn: func(shardID types.ID, writes [][2][]byte) error {
			assert.Equal(t, parentShardID, shardID)
			require.Len(t, writes, 1)
			assert.Equal(t, []byte("mango"), writes[0][0])
			return nil
		},
	}
	childRPC := &stubStoreRPC{
		id: childNodeID,
		batchFn: func(shardID types.ID, writes [][2][]byte) error {
			assert.Equal(t, childShardID, shardID)
			require.Len(t, writes, 1)
			assert.Equal(t, []byte("mango"), writes[0][0])
			return &client.ResponseError{
				StatusCode: http.StatusBadRequest,
				Body:       "key 6d616e676f out of range [6d3a00,ff)",
			}
		},
	}
	ms.tm.SetStoreClientFactory(func(_ *http.Client, id types.ID, _ string) client.StoreRPC {
		switch id {
		case parentNodeID:
			return parentRPC
		case childNodeID:
			return childRPC
		default:
			return &stubStoreRPC{id: id}
		}
	})

	parentStatus, err := ms.tm.GetShardStatus(parentShardID)
	require.NoError(t, err)
	parentStatus.Peers = common.NewPeerSet(parentNodeID)
	parentStatus.ReportedBy = common.NewPeerSet(parentNodeID)
	parentStatus.RaftStatus = &common.RaftStatus{
		Lead:   parentNodeID,
		Voters: common.NewPeerSet(parentNodeID),
	}
	writeShardStatus(t, db, parentStatus)

	_, _, err = ms.tm.ReassignShardsForSplit(tablemgr.SplitTransition{
		ShardID:      parentShardID,
		SplitShardID: childShardID,
		SplitKey:     splitKey,
		TableName:    tableName,
	})
	require.NoError(t, err)

	parentStatus, err = ms.tm.GetShardStatus(parentShardID)
	require.NoError(t, err)
	parentStatus.State = store.ShardState_Splitting
	parentStatus.SplitState = &storedb.SplitState{}
	parentStatus.SplitState.SetPhase(storedb.SplitState_PHASE_ROLLING_BACK)
	parentStatus.SplitState.SetSplitKey(splitKey)
	parentStatus.SplitState.SetNewShardId(uint64(childShardID))
	parentStatus.SplitState.SetOriginalRangeEnd([]byte{0xff})
	parentStatus.Peers = common.NewPeerSet(parentNodeID)
	parentStatus.ReportedBy = common.NewPeerSet(parentNodeID)
	parentStatus.RaftStatus = &common.RaftStatus{
		Lead:   parentNodeID,
		Voters: common.NewPeerSet(parentNodeID),
	}
	writeShardStatus(t, db, parentStatus)

	childStatus, err := ms.tm.GetShardStatus(childShardID)
	require.NoError(t, err)
	childStatus.State = store.ShardState_SplitOffPreSnap
	childStatus.HasSnapshot = true
	childStatus.Initializing = false
	childStatus.SplitReplayRequired = true
	childStatus.SplitReplayCaughtUp = true
	childStatus.SplitCutoverReady = true
	childStatus.Peers = common.NewPeerSet(childNodeID)
	childStatus.ReportedBy = common.NewPeerSet(childNodeID)
	childStatus.RaftStatus = &common.RaftStatus{
		Lead:   childNodeID,
		Voters: common.NewPeerSet(childNodeID),
	}
	writeShardStatus(t, db, childStatus)

	writeStoreStatus(t, db, parentNodeID, true)
	writeStoreStatus(t, db, childNodeID, true)

	err = ms.forwardBatchToShard(
		context.Background(),
		childShardID,
		[][2][]byte{{[]byte("mango"), []byte(`{"content":"value"}`)}},
		nil,
		nil,
		storedb.Op_SyncLevelWrite,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, childRPC.batchHit)
	assert.Equal(t, 1, parentRPC.batchHit)
}

func TestForwardBatchToShard_DoesNotRetryWhenRerouteStaysMultiShard(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	tableName := "test_table"
	table, err := ms.tm.CreateTable(tableName, tablemgr.TableConfig{
		NumShards: 2,
		StartID:   100,
	})
	require.NoError(t, err)
	require.Len(t, table.Shards, 2)

	var shardIDs []types.ID
	for shardID := range table.Shards {
		shardIDs = append(shardIDs, shardID)
	}
	require.Len(t, shardIDs, 2)
	slices.Sort(shardIDs)

	leftShardID := shardIDs[0]
	rightShardID := shardIDs[1]
	leftNodeID := types.ID(1)
	rightNodeID := types.ID(2)

	leftRPC := &stubStoreRPC{
		id: leftNodeID,
		batchFn: func(shardID types.ID, writes [][2][]byte) error {
			assert.Equal(t, leftShardID, shardID)
			return &client.ResponseError{
				StatusCode: http.StatusBadRequest,
				Body:       "key out of range",
			}
		},
	}
	rightRPC := &stubStoreRPC{id: rightNodeID}

	ms.tm.SetStoreClientFactory(func(_ *http.Client, id types.ID, _ string) client.StoreRPC {
		switch id {
		case leftNodeID:
			return leftRPC
		case rightNodeID:
			return rightRPC
		default:
			return &stubStoreRPC{id: id}
		}
	})

	leftStatus, err := ms.tm.GetShardStatus(leftShardID)
	require.NoError(t, err)
	leftStatus.Peers = common.NewPeerSet(leftNodeID)
	leftStatus.ReportedBy = common.NewPeerSet(leftNodeID)
	leftStatus.RaftStatus = &common.RaftStatus{
		Lead:   leftNodeID,
		Voters: common.NewPeerSet(leftNodeID),
	}
	writeShardStatus(t, db, leftStatus)

	rightStatus, err := ms.tm.GetShardStatus(rightShardID)
	require.NoError(t, err)
	rightStatus.Peers = common.NewPeerSet(rightNodeID)
	rightStatus.ReportedBy = common.NewPeerSet(rightNodeID)
	rightStatus.RaftStatus = &common.RaftStatus{
		Lead:   rightNodeID,
		Voters: common.NewPeerSet(rightNodeID),
	}
	writeShardStatus(t, db, rightStatus)

	writeStoreStatus(t, db, leftNodeID, true)
	writeStoreStatus(t, db, rightNodeID, true)

	err = ms.forwardBatchToShard(
		context.Background(),
		leftShardID,
		[][2][]byte{
			{[]byte("apple"), []byte(`{"content":"left"}`)},
			{[]byte("zebra"), []byte(`{"content":"right"}`)},
		},
		nil,
		nil,
		storedb.Op_SyncLevelWrite,
	)
	require.Error(t, err)
	assert.Equal(t, 1, leftRPC.batchHit)
	assert.Equal(t, 0, rightRPC.batchHit)
}

func TestDirectLeaderClientForShardBypassesSplitFallback_PrefersSelfReportedLeader(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	shardID := types.ID(101)
	leaderNodeID := types.ID(1)
	followerNodeID := types.ID(2)

	leaderRPC := &stubStoreRPC{id: leaderNodeID}
	followerRPC := &stubStoreRPC{id: followerNodeID}
	ms.tm.SetStoreClientFactory(func(_ *http.Client, id types.ID, _ string) client.StoreRPC {
		switch id {
		case leaderNodeID:
			return leaderRPC
		case followerNodeID:
			return followerRPC
		default:
			return &stubStoreRPC{id: id}
		}
	})

	childStatus := splitShardStatus(
		shardID,
		store.ShardState_SplitOffPreSnap,
		[2][]byte{[]byte("m:\x00"), {0xff}},
		true,
		false,
		followerNodeID, // stale merged leader
	)
	childStatus.SplitReplayRequired = true
	childStatus.SplitReplayCaughtUp = true
	childStatus.SplitCutoverReady = true
	childStatus.Peers = common.NewPeerSet(leaderNodeID, followerNodeID)
	childStatus.ReportedBy = common.NewPeerSet(leaderNodeID, followerNodeID)
	writeShardStatus(t, db, childStatus)
	reloadedShardStatus, err := ms.tm.GetShardStatus(shardID)
	require.NoError(t, err)
	assert.True(t, reloadedShardStatus.ReportedBy.Contains(leaderNodeID))
	assert.True(t, reloadedShardStatus.ReportedBy.Contains(followerNodeID))

	leaderLocalInfo := childStatus.ShardInfo.DeepCopy()
	leaderLocalInfo.HasSnapshot = true
	leaderLocalInfo.Initializing = false
	leaderLocalInfo.RaftStatus = &common.RaftStatus{
		Lead:   leaderNodeID,
		Voters: common.NewPeerSet(leaderNodeID, followerNodeID),
	}
	followerLocalInfo := childStatus.ShardInfo.DeepCopy()
	followerLocalInfo.HasSnapshot = true
	followerLocalInfo.Initializing = false
	followerLocalInfo.RaftStatus = &common.RaftStatus{
		Lead:   leaderNodeID,
		Voters: common.NewPeerSet(leaderNodeID, followerNodeID),
	}

	writeStoreStatusWithShards(t, db, leaderNodeID, true, map[types.ID]*store.ShardInfo{
		shardID: leaderLocalInfo,
	})
	writeStoreStatusWithShards(t, db, followerNodeID, true, map[types.ID]*store.ShardInfo{
		shardID: followerLocalInfo,
	})
	leaderStoreStatus, err := ms.tm.GetStoreStatus(context.Background(), leaderNodeID)
	require.NoError(t, err)
	require.NotNil(t, leaderStoreStatus.Shards[shardID])
	assert.Equal(t, leaderNodeID, leaderStoreStatus.Shards[shardID].RaftStatus.Lead)
	assert.True(t, leaderStoreStatus.Shards[shardID].CanInitiateSplitCutover())
	followerStoreStatus, err := ms.tm.GetStoreStatus(context.Background(), followerNodeID)
	require.NoError(t, err)
	require.NotNil(t, followerStoreStatus.Shards[shardID])
	assert.Equal(t, leaderNodeID, followerStoreStatus.Shards[shardID].RaftStatus.Lead)
	leaderClient, leaderReachable, err := ms.tm.GetStoreClient(context.Background(), leaderNodeID)
	require.NoError(t, err)
	require.True(t, leaderReachable)
	require.NotNil(t, leaderClient)
	assert.Equal(t, leaderNodeID, leaderClient.ID())
	liveLeaderClient, err := ms.selfReportedLeaderClientForShard(context.Background(), shardID, splitChildIsFullyCutOver)
	require.NoError(t, err)
	require.NotNil(t, liveLeaderClient)
	assert.Equal(t, leaderNodeID, liveLeaderClient.ID())

	_, gotClient, err := ms.directLeaderClientForShardBypassesSplitFallback(context.Background(), shardID)
	require.NoError(t, err)
	require.NotNil(t, gotClient)
	assert.Equal(t, leaderNodeID, gotClient.ID())
}

func TestLeaderClientForShardNoFallback_SplitChildUsesLiveLeaderOnceCaughtUp(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	shardID := types.ID(101)
	leaderNodeID := types.ID(1)
	followerNodeID := types.ID(2)

	leaderRPC := &stubStoreRPC{id: leaderNodeID}
	followerRPC := &stubStoreRPC{id: followerNodeID}
	ms.tm.SetStoreClientFactory(func(_ *http.Client, id types.ID, _ string) client.StoreRPC {
		switch id {
		case leaderNodeID:
			return leaderRPC
		case followerNodeID:
			return followerRPC
		default:
			return &stubStoreRPC{id: id}
		}
	})

	childStatus := splitShardStatus(
		shardID,
		store.ShardState_SplitOffPreSnap,
		[2][]byte{[]byte("m:\x00"), {0xff}},
		true,
		false,
		followerNodeID, // stale merged leader
	)
	childStatus.SplitReplayRequired = true
	childStatus.SplitReplayCaughtUp = true
	childStatus.SplitCutoverReady = false
	childStatus.Peers = common.NewPeerSet(leaderNodeID, followerNodeID)
	childStatus.ReportedBy = common.NewPeerSet(leaderNodeID, followerNodeID)
	writeShardStatus(t, db, childStatus)

	leaderLocalInfo := childStatus.ShardInfo.DeepCopy()
	leaderLocalInfo.HasSnapshot = true
	leaderLocalInfo.Initializing = false
	leaderLocalInfo.RaftStatus = &common.RaftStatus{
		Lead:   leaderNodeID,
		Voters: common.NewPeerSet(leaderNodeID, followerNodeID),
	}
	followerLocalInfo := childStatus.ShardInfo.DeepCopy()
	followerLocalInfo.HasSnapshot = true
	followerLocalInfo.Initializing = false
	followerLocalInfo.RaftStatus = &common.RaftStatus{
		Lead:   leaderNodeID,
		Voters: common.NewPeerSet(leaderNodeID, followerNodeID),
	}

	writeStoreStatusWithShards(t, db, leaderNodeID, true, map[types.ID]*store.ShardInfo{
		shardID: leaderLocalInfo,
	})
	writeStoreStatusWithShards(t, db, followerNodeID, true, map[types.ID]*store.ShardInfo{
		shardID: followerLocalInfo,
	})

	_, gotClient, err := ms.leaderClientForShardNoFallback(context.Background(), shardID)
	require.NoError(t, err)
	require.NotNil(t, gotClient)
	assert.Equal(t, leaderNodeID, gotClient.ID())
}

func TestResolveWriteShardID_FinalizingParentRoutesToChild(t *testing.T) {
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
					ByteRange: [2][]byte{{0x00}, splitKey},
				},
				SplitState: finalizingState,
			},
			Table: "test_table",
			State: store.ShardState_Splitting,
		},
		childID: {
			ShardInfo: storedb.ShardInfo{
				ShardConfig: storedb.ShardConfig{
					ByteRange: [2][]byte{splitKey, {0xff}},
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

	resolved, err := resolveWriteShardID(statuses, childID)
	require.NoError(t, err)
	assert.Equal(t, childID, resolved)
}

func TestResolveWriteShardID_PreSplitParentWithoutSplitStateRoutesToChild(t *testing.T) {
	parentID := types.ID(30)
	childID := types.ID(31)
	splitKey := []byte("m:\x00")

	statuses := map[types.ID]*store.ShardStatus{
		parentID: {
			ShardInfo: storedb.ShardInfo{
				ShardConfig: storedb.ShardConfig{
					ByteRange: [2][]byte{{0x00}, splitKey},
				},
			},
			Table: "test_table",
			State: store.ShardState_PreSplit,
		},
		childID: {
			ShardInfo: storedb.ShardInfo{
				ShardConfig: storedb.ShardConfig{
					ByteRange: [2][]byte{splitKey, {0xff}},
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

	resolved, err := resolveWriteShardID(statuses, childID)
	require.NoError(t, err)
	assert.Equal(t, childID, resolved)
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

func TestShouldRouteWriteToParent_SplitChildUsesCaughtUpFence(t *testing.T) {
	status := &store.ShardStatus{
		State: store.ShardState_SplitOffPreSnap,
		ShardInfo: storedb.ShardInfo{
			HasSnapshot:         true,
			Initializing:        false,
			RaftStatus:          &common.RaftStatus{Lead: 1},
			SplitReplayRequired: true,
			SplitReplayCaughtUp: false,
			SplitCutoverReady:   false,
		},
	}

	assert.True(t, shouldRouteWriteToParent(status))

	status.SplitReplayCaughtUp = true
	assert.False(t, shouldRouteWriteToParent(status))
}

func TestFindParentShardForSplitOffStatus_NoParent(t *testing.T) {
	childStatus := &store.ShardStatus{
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{[]byte("m:\x00"), {0xff}},
			},
		},
		Table: "test_table",
		State: store.ShardState_SplitOffPreSnap,
	}
	_, err := findParentShardForSplitOffStatus(map[types.ID]*store.ShardStatus{}, childStatus)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parent shard not found")
}

func TestFindParentShardForSplitOffStatus_FindsFinalizingParent(t *testing.T) {
	parentShardID := types.ID(100)
	childShardID := types.ID(101)
	splitKey := []byte("m:\x00")

	finalizingState := &storedb.SplitState{}
	finalizingState.SetPhase(storedb.SplitState_PHASE_FINALIZING)
	finalizingState.SetSplitKey(splitKey)
	finalizingState.SetNewShardId(uint64(childShardID))

	parentStatus := &store.ShardStatus{
		ID:    parentShardID,
		Table: "test_table",
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{{0x00}, splitKey},
			},
			SplitState: finalizingState,
		},
	}
	childStatus := &store.ShardStatus{
		ID:    childShardID,
		Table: "test_table",
		State: store.ShardState_SplitOffPreSnap,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xff}},
			},
			SplitReplayRequired: true,
			SplitReplayCaughtUp: true,
			SplitCutoverReady:   false,
		},
	}

	parentID, err := findParentShardForSplitOffStatus(map[types.ID]*store.ShardStatus{
		parentShardID: parentStatus,
		childShardID:  childStatus,
	}, childStatus)
	require.NoError(t, err)
	assert.Equal(t, parentShardID, parentID)
}

func TestFindParentShardForSplitOffStatus_FindsRollingBackParent(t *testing.T) {
	parentShardID := types.ID(100)
	childShardID := types.ID(101)
	splitKey := []byte("m:\x00")

	rollingBackState := &storedb.SplitState{}
	rollingBackState.SetPhase(storedb.SplitState_PHASE_ROLLING_BACK)
	rollingBackState.SetSplitKey(splitKey)
	rollingBackState.SetNewShardId(uint64(childShardID))

	parentStatus := &store.ShardStatus{
		ID:    parentShardID,
		Table: "test_table",
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{{0x00}, splitKey},
			},
			SplitState: rollingBackState,
		},
	}
	childStatus := &store.ShardStatus{
		ID:    childShardID,
		Table: "test_table",
		State: store.ShardState_SplitOffPreSnap,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xff}},
			},
			SplitReplayRequired: true,
			SplitReplayCaughtUp: true,
			SplitCutoverReady:   true,
		},
	}

	parentID, err := findParentShardForSplitOffStatus(map[types.ID]*store.ShardStatus{
		parentShardID: parentStatus,
		childShardID:  childStatus,
	}, childStatus)
	require.NoError(t, err)
	assert.Equal(t, parentShardID, parentID)
}

func TestFindParentShardForSplitOffStatus_UsesExplicitSplitParentID(t *testing.T) {
	parentShardID := types.ID(100)
	childShardID := types.ID(101)
	splitKey := []byte("m:\x00")

	parentStatus := &store.ShardStatus{
		ID:    parentShardID,
		Table: "test_table",
		State: store.ShardState_Default,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{{0x00}, {0xff}},
			},
		},
	}
	childStatus := &store.ShardStatus{
		ID:    childShardID,
		Table: "test_table",
		State: store.ShardState_SplitOffPreSnap,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xff}},
			},
			SplitReplayRequired: true,
			SplitReplayCaughtUp: true,
			SplitCutoverReady:   false,
			SplitParentShardID:  parentShardID,
		},
	}

	parentID, err := findParentShardForSplitOffStatus(map[types.ID]*store.ShardStatus{
		parentShardID: parentStatus,
		childShardID:  childStatus,
	}, childStatus)
	require.NoError(t, err)
	assert.Equal(t, parentShardID, parentID)
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

func TestLeaderClientForShard_LeaderExplicitlyMissingShard_FallsBackToReporter(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	shardID := types.ID(100)
	leaderNode := types.ID(1)
	follower := types.ID(2)

	status := newShardStatus(shardID, leaderNode, []types.ID{follower}, []types.ID{leaderNode, follower})
	writeShardStatus(t, db, status)
	writeStoreStatusWithShards(t, db, leaderNode, true, map[types.ID]*store.ShardInfo{
		types.ID(999): {},
	})
	writeStoreStatusWithShards(t, db, follower, true, map[types.ID]*store.ShardInfo{
		shardID: {},
	})

	gotClient, err := ms.leaderClientForShard(context.Background(), shardID)
	require.NoError(t, err)
	assert.NotNil(t, gotClient)
	assert.Equal(t, follower, gotClient.ID())
}

func TestLeaderClientForShard_InitializingLeaderFallsBackToReadyReporter(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	shardID := types.ID(100)
	leaderNode := types.ID(1)
	follower := types.ID(2)

	status := newShardStatus(shardID, leaderNode, []types.ID{leaderNode, follower}, []types.ID{leaderNode, follower})
	writeShardStatus(t, db, status)
	writeStoreStatusWithShards(t, db, leaderNode, true, map[types.ID]*store.ShardInfo{
		shardID: {
			Initializing: true,
		},
	})
	writeStoreStatusWithShards(t, db, follower, true, map[types.ID]*store.ShardInfo{
		shardID: {
			Initializing: false,
		},
	})

	gotClient, err := ms.leaderClientForShard(context.Background(), shardID)
	require.NoError(t, err)
	assert.NotNil(t, gotClient)
	assert.Equal(t, follower, gotClient.ID())
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

func TestShouldFallbackToParentShard_WhenParentIsRollingBack(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentID := types.ID(10)
	childID := types.ID(11)
	splitKey := []byte("m:\x00")

	rollingBackState := &storedb.SplitState{}
	rollingBackState.SetPhase(storedb.SplitState_PHASE_ROLLING_BACK)
	rollingBackState.SetSplitKey(splitKey)
	rollingBackState.SetNewShardId(uint64(childID))

	writeShardStatus(t, db, &store.ShardStatus{
		ID:    parentID,
		Table: "test_table",
		State: store.ShardState_Default,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{{0x00}, splitKey},
			},
			SplitState: rollingBackState,
		},
	})

	childStatus := &store.ShardStatus{
		ID:    childID,
		Table: "test_table",
		State: store.ShardState_Default,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xff}},
			},
			HasSnapshot:         true,
			Initializing:        false,
			SplitReplayRequired: true,
			SplitReplayCaughtUp: true,
			SplitCutoverReady:   true,
			RaftStatus:          &common.RaftStatus{Lead: 1},
		},
	}
	writeShardStatus(t, db, childStatus)

	assert.True(t, ms.shouldFallbackToParentShard(childStatus))
}

func TestShouldFallbackToParentShard_DefaultChildUsesExplicitParentLink(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentShardID := types.ID(100)
	childShardID := types.ID(101)
	splitKey := []byte("m:\x00")

	parentStatus := &store.ShardStatus{
		ID:    parentShardID,
		Table: "test_table",
		State: store.ShardState_Default,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{{0x00}, {0xff}},
			},
		},
	}
	childStatus := &store.ShardStatus{
		ID:    childShardID,
		Table: "test_table",
		State: store.ShardState_Default,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xff}},
			},
			HasSnapshot:         true,
			Initializing:        false,
			SplitReplayRequired: true,
			SplitReplayCaughtUp: true,
			SplitCutoverReady:   false,
			SplitParentShardID:  parentShardID,
			RaftStatus:          &common.RaftStatus{Lead: 1},
		},
	}

	writeShardStatus(t, db, parentStatus)
	writeShardStatus(t, db, childStatus)

	assert.True(t, ms.shouldFallbackToParentShard(childStatus))
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

func TestForwardLookupToShardWithVersion_SplitParentMissFallsBackToWriteReadyChild(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentShardID := types.ID(100)
	childShardID := types.ID(101)
	parentNodeID := types.ID(1)
	childNodeID := types.ID(2)
	splitKey := []byte("m")
	key := "mango"
	expectedDoc := []byte(`{"key":"mango"}`)
	const expectedVersion = 42

	parentRPC := &stubStoreRPC{
		id: parentNodeID,
		lookupWithVersionFn: func(shardID types.ID, gotKey string) ([]byte, uint64, error) {
			assert.Equal(t, parentShardID, shardID)
			assert.Equal(t, key, gotKey)
			return nil, 0, client.ErrNotFound
		},
	}
	childRPC := &stubStoreRPC{
		id: childNodeID,
		lookupWithVersionFn: func(shardID types.ID, gotKey string) ([]byte, uint64, error) {
			assert.Equal(t, childShardID, shardID)
			assert.Equal(t, key, gotKey)
			return expectedDoc, expectedVersion, nil
		},
	}
	ms.tm.SetStoreClientFactory(func(_ *http.Client, id types.ID, _ string) client.StoreRPC {
		switch id {
		case parentNodeID:
			return parentRPC
		case childNodeID:
			return childRPC
		default:
			return &stubStoreRPC{id: id}
		}
	})

	parentStatus := &store.ShardStatus{
		ID:    parentShardID,
		Table: "test_table",
		State: store.ShardState_Splitting,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{{0x00}, splitKey},
			},
			Peers:      common.NewPeerSet(parentNodeID),
			ReportedBy: common.NewPeerSet(parentNodeID),
			RaftStatus: &common.RaftStatus{
				Lead:   parentNodeID,
				Voters: common.NewPeerSet(parentNodeID),
			},
		},
	}
	parentStatus.SplitState = &storedb.SplitState{}
	parentStatus.SplitState.SetPhase(storedb.SplitState_PHASE_SPLITTING)
	parentStatus.SplitState.SetSplitKey(splitKey)
	parentStatus.SplitState.SetNewShardId(uint64(childShardID))

	childStatus := &store.ShardStatus{
		ID:    childShardID,
		Table: "test_table",
		State: store.ShardState_SplitOffPreSnap,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xff}},
			},
			Peers:               common.NewPeerSet(childNodeID),
			ReportedBy:          common.NewPeerSet(childNodeID),
			HasSnapshot:         true,
			Initializing:        false,
			SplitReplayRequired: true,
			SplitReplayCaughtUp: true,
			SplitCutoverReady:   false,
			RaftStatus: &common.RaftStatus{
				Lead:   childNodeID,
				Voters: common.NewPeerSet(childNodeID),
			},
		},
	}

	writeShardStatus(t, db, parentStatus)
	writeShardStatus(t, db, childStatus)
	writeStoreStatus(t, db, parentNodeID, true)
	writeStoreStatus(t, db, childNodeID, true)

	doc, version, err := ms.forwardLookupToShardWithVersion(context.Background(), childShardID, key)
	require.NoError(t, err)
	assert.Equal(t, expectedDoc, doc)
	assert.Equal(t, uint64(expectedVersion), version)
	assert.Equal(t, 1, parentRPC.lookupHit)
	assert.Equal(t, 1, childRPC.lookupHit)
}

func TestForwardLookupToShardWithVersion_SplitParentOutOfRangeFallsBackToWriteReadyChild(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentShardID := types.ID(100)
	childShardID := types.ID(101)
	parentNodeID := types.ID(1)
	childNodeID := types.ID(2)
	splitKey := []byte("m")
	key := "mango"
	expectedDoc := []byte(`{"key":"mango"}`)
	const expectedVersion = 42

	parentRPC := &stubStoreRPC{
		id: parentNodeID,
		lookupWithVersionFn: func(shardID types.ID, gotKey string) ([]byte, uint64, error) {
			assert.Equal(t, parentShardID, shardID)
			assert.Equal(t, key, gotKey)
			return nil, 0, client.ErrKeyOutOfRange
		},
	}
	childRPC := &stubStoreRPC{
		id: childNodeID,
		lookupWithVersionFn: func(shardID types.ID, gotKey string) ([]byte, uint64, error) {
			assert.Equal(t, childShardID, shardID)
			assert.Equal(t, key, gotKey)
			return expectedDoc, expectedVersion, nil
		},
	}
	ms.tm.SetStoreClientFactory(func(_ *http.Client, id types.ID, _ string) client.StoreRPC {
		switch id {
		case parentNodeID:
			return parentRPC
		case childNodeID:
			return childRPC
		default:
			return &stubStoreRPC{id: id}
		}
	})

	parentStatus := &store.ShardStatus{
		ID:    parentShardID,
		Table: "test_table",
		State: store.ShardState_Splitting,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{{0x00}, splitKey},
			},
			Peers:      common.NewPeerSet(parentNodeID),
			ReportedBy: common.NewPeerSet(parentNodeID),
			RaftStatus: &common.RaftStatus{
				Lead:   parentNodeID,
				Voters: common.NewPeerSet(parentNodeID),
			},
		},
	}
	parentStatus.SplitState = &storedb.SplitState{}
	parentStatus.SplitState.SetPhase(storedb.SplitState_PHASE_SPLITTING)
	parentStatus.SplitState.SetSplitKey(splitKey)
	parentStatus.SplitState.SetNewShardId(uint64(childShardID))

	childStatus := &store.ShardStatus{
		ID:    childShardID,
		Table: "test_table",
		State: store.ShardState_SplitOffPreSnap,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xff}},
			},
			Peers:               common.NewPeerSet(childNodeID),
			ReportedBy:          common.NewPeerSet(childNodeID),
			HasSnapshot:         true,
			Initializing:        false,
			SplitReplayRequired: true,
			SplitReplayCaughtUp: true,
			SplitCutoverReady:   false,
			RaftStatus: &common.RaftStatus{
				Lead:   childNodeID,
				Voters: common.NewPeerSet(childNodeID),
			},
		},
	}

	writeShardStatus(t, db, parentStatus)
	writeShardStatus(t, db, childStatus)
	writeStoreStatus(t, db, parentNodeID, true)
	writeStoreStatus(t, db, childNodeID, true)

	doc, version, err := ms.forwardLookupToShardWithVersion(context.Background(), childShardID, key)
	require.NoError(t, err)
	assert.Equal(t, expectedDoc, doc)
	assert.Equal(t, uint64(expectedVersion), version)
	assert.Equal(t, 1, parentRPC.lookupHit)
	assert.Equal(t, 1, childRPC.lookupHit)
}

func TestForwardLookupToShardWithVersion_StaleParentOutOfRangeFallsBackToReadReadyChild(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	parentShardID := types.ID(100)
	childShardID := types.ID(101)
	parentNodeID := types.ID(1)
	childNodeID := types.ID(2)
	splitKey := []byte("m")
	key := "mango"
	expectedDoc := []byte(`{"key":"mango"}`)
	const expectedVersion = 99

	parentRPC := &stubStoreRPC{
		id: parentNodeID,
		lookupWithVersionFn: func(shardID types.ID, gotKey string) ([]byte, uint64, error) {
			assert.Equal(t, parentShardID, shardID)
			assert.Equal(t, key, gotKey)
			return nil, 0, client.ErrKeyOutOfRange
		},
	}
	childRPC := &stubStoreRPC{
		id: childNodeID,
		lookupWithVersionFn: func(shardID types.ID, gotKey string) ([]byte, uint64, error) {
			assert.Equal(t, childShardID, shardID)
			assert.Equal(t, key, gotKey)
			return expectedDoc, expectedVersion, nil
		},
	}
	ms.tm.SetStoreClientFactory(func(_ *http.Client, id types.ID, _ string) client.StoreRPC {
		switch id {
		case parentNodeID:
			return parentRPC
		case childNodeID:
			return childRPC
		default:
			return &stubStoreRPC{id: id}
		}
	})
	writeTable(t, db, &store.Table{
		Name: "test_table",
		Shards: map[types.ID]*store.ShardConfig{
			parentShardID: {ByteRange: [2][]byte{{0x00}, {0xff}}},
		},
	})

	parentStatus := &store.ShardStatus{
		ID:    parentShardID,
		Table: "test_table",
		State: store.ShardState_Default,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{{0x00}, splitKey},
			},
			Peers:      common.NewPeerSet(parentNodeID),
			ReportedBy: common.NewPeerSet(parentNodeID),
			RaftStatus: &common.RaftStatus{
				Lead:   parentNodeID,
				Voters: common.NewPeerSet(parentNodeID),
			},
		},
	}
	parentStatus.SplitState = &storedb.SplitState{}
	parentStatus.SplitState.SetPhase(storedb.SplitState_PHASE_FINALIZING)
	parentStatus.SplitState.SetSplitKey(splitKey)
	parentStatus.SplitState.SetNewShardId(uint64(childShardID))

	childStatus := &store.ShardStatus{
		ID:    childShardID,
		Table: "test_table",
		State: store.ShardState_Default,
		ShardInfo: storedb.ShardInfo{
			ShardConfig: storedb.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xff}},
			},
			Peers:               common.NewPeerSet(childNodeID),
			ReportedBy:          common.NewPeerSet(childNodeID),
			HasSnapshot:         true,
			Initializing:        false,
			SplitReplayRequired: true,
			SplitReplayCaughtUp: true,
			SplitCutoverReady:   true,
			SplitParentShardID:  parentShardID,
			RaftStatus: &common.RaftStatus{
				Lead:   childNodeID,
				Voters: common.NewPeerSet(childNodeID),
			},
		},
	}

	writeShardStatus(t, db, parentStatus)
	writeShardStatus(t, db, childStatus)
	parentStoreInfo := parentStatus.ShardInfo.DeepCopy()
	parentStoreInfo.HasSnapshot = parentStatus.ShardInfo.HasSnapshot
	parentStoreInfo.Initializing = parentStatus.ShardInfo.Initializing
	childStoreInfo := childStatus.ShardInfo.DeepCopy()
	childStoreInfo.HasSnapshot = childStatus.ShardInfo.HasSnapshot
	childStoreInfo.Initializing = childStatus.ShardInfo.Initializing
	writeStoreStatusWithShards(t, db, parentNodeID, true, map[types.ID]*store.ShardInfo{
		parentShardID: parentStoreInfo,
	})
	writeStoreStatusWithShards(t, db, childNodeID, true, map[types.ID]*store.ShardInfo{
		childShardID: childStoreInfo,
	})

	table, err := ms.tm.GetTable("test_table")
	require.NoError(t, err)
	reroutedShardID, err := findReadShardForKey(ms.tm, table, key)
	require.NoError(t, err)
	assert.Equal(t, childShardID, reroutedShardID)
	reroutedEffectiveShardID, reroutedClient, err := ms.leaderClientForShardWithEffectiveID(context.Background(), childShardID)
	require.NoError(t, err)
	assert.Equal(t, childShardID, reroutedEffectiveShardID)
	assert.Equal(t, childNodeID, reroutedClient.ID())

	doc, version, err := ms.forwardLookupToShardWithVersion(context.Background(), parentShardID, key)
	require.NoError(t, err)
	assert.Equal(t, expectedDoc, doc)
	assert.Equal(t, uint64(expectedVersion), version)
	assert.Equal(t, 1, parentRPC.lookupHit)
	assert.Equal(t, 1, childRPC.lookupHit)
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

func TestLeaderClientForShard_LeaderNotServingReturnsTransientError(t *testing.T) {
	ms, db := setupTestMetadataStore(t)

	shardID := types.ID(500)
	leaderNode := types.ID(101)
	followerNode := types.ID(102)

	status := newShardStatus(shardID, leaderNode, []types.ID{leaderNode, followerNode}, []types.ID{leaderNode, followerNode})
	writeShardStatus(t, db, status)
	writeStoreStatusWithShards(t, db, leaderNode, true, map[types.ID]*store.ShardInfo{
		shardID: {
			ShardConfig: store.ShardConfig{
				ByteRange: [2][]byte{{0x00}, {0xff}},
			},
			Initializing: true,
		},
	})
	writeStoreStatusWithShards(t, db, followerNode, true, map[types.ID]*store.ShardInfo{
		shardID: {
			ShardConfig: store.ShardConfig{
				ByteRange: [2][]byte{{0x00}, {0xff}},
			},
			Initializing: true,
		},
	})

	_, err := ms.leaderClientForShard(context.Background(), shardID)
	require.Error(t, err)
	assert.ErrorIs(t, err, client.ErrShardInitializing)
	assert.True(t, isTransientShardError(err))
}
