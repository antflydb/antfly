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

package tablemgr

import (
	"bytes"
	"context"
	"maps"
	"math"
	"slices"
	"sort"
	"testing"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/antflydb/antfly/src/store"
	storedb "github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateShardConfig_Coverage checks if the generated shard ranges cover the entire
// byte space without gaps or overlaps.
func TestCreateShardConfig_Coverage(t *testing.T) {
	testCases := []struct {
		name      string
		numShards uint
		startID   types.ID
	}{
		{"SingleShard", 1, 100},
		{"TwoShards", 2, 200},
		{"ThreeShards", 3, 250},
		{"FiveShards", 5, 300},
		{"TenShards", 10, 400},
		{"49Shards", 49, 500},
		{"256Shards", 256, 1000},
		{"1024Shards", 1024, 10000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)

			config := TableConfig{
				NumShards: tc.numShards,
				StartID:   tc.startID,
			}
			shards := createShardConfig(config)

			require.Len(shards, int(tc.numShards), "Unexpected number of shards generated")

			// Extract and sort ranges by start key
			ranges := make([]types.Range, 0, len(shards))
			for _, shardConf := range shards {
				ranges = append(ranges, shardConf.ByteRange)
			}
			sort.Slice(ranges, func(i, j int) bool {
				// Handle empty start key (should come first)
				if len(ranges[i][0]) == 0 {
					return true
				}
				if len(ranges[j][0]) == 0 {
					return false
				}
				return bytes.Compare(ranges[i][0], ranges[j][0]) < 0
			})

			// Check first range starts at the beginning (empty byte slice)
			assert.Empty(ranges[0][0], "First shard range start should be empty")

			// Check last range ends at the maximum value
			expectedEnd := []byte{math.MaxUint8}
			assert.Equal(
				expectedEnd,
				ranges[len(ranges)-1][1],
				"Last shard range end should be MaxUint8",
			)

			// Check for gaps or overlaps between consecutive ranges
			for i := range len(ranges) - 1 {
				currentEnd := ranges[i][1]
				nextStart := ranges[i+1][0]
				assert.Equal(
					currentEnd,
					nextStart,
					"Gap or overlap found between shard %d and %d: end=%v, start=%v",
					i,
					i+1,
					currentEnd,
					nextStart,
				)
			}
		})
	}
}

// TestCreateShardConfig_ZeroShards checks behavior with zero shards requested.
func TestCreateShardConfig_ZeroShards(t *testing.T) {
	assert := assert.New(t)
	config := TableConfig{
		NumShards: 0,
		StartID:   100,
	}
	shards := createShardConfig(config)
	assert.Empty(shards, "Expected 0 shards for NumShards=0")
}

// Helper function to set up a test PebbleDB instance
func setupTestDB(t *testing.T) kv.DB {
	t.Helper()
	// path/filepath is needed for this, ensure it's imported
	// For now, let's assume t.TempDir() is sufficient and does not require explicit import here
	// if used directly for pebble.Open path.
	dbPath := t.TempDir() // pebble.Open can take this directly
	opts := pebbleutils.NewMemPebbleOpts()
	db, err := pebble.Open(dbPath, opts)
	require.NoError(t, err, "Failed to open pebble DB")

	t.Cleanup(func() {
		err := db.Close()
		require.NoError(t, err, "Failed to close pebble DB")
	})
	return &kv.PebbleDB{DB: db}
}

func TestTableManager_NewTableManager_LoadEmpty(t *testing.T) {
	db := setupTestDB(t)

	tm, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)
	require.NotNil(t, tm)

	loadedTables, err := tm.Tables(nil, nil)
	require.NoError(t, err)
	assert.Empty(t, loadedTables, "Expected no tables in a new manager with empty DB")

	loadedShardStatuses, err := tm.GetShardStatuses()
	require.NoError(t, err)
	assert.Empty(
		t,
		loadedShardStatuses,
		"Expected no shard statuses in a new manager with empty DB",
	)
}

func TestTableManager_CreateTable_GetTable_RemoveTable(t *testing.T) {
	db := setupTestDB(t)

	tm, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)

	tableName := "testTable1"
	tc := TableConfig{NumShards: 2, StartID: 1000}
	defaultType := "default"
	tc.Schema = &schema.TableSchema{
		DefaultType: defaultType,
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"data": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	// Create Table
	table, err := tm.CreateTable(tableName, tc)
	require.NoError(t, err)
	require.NotNil(t, table)
	assert.Equal(t, tableName, table.Name)
	assert.Equal(t, tc.Schema, table.Schema)
	assert.Len(t, table.Shards, int(tc.NumShards), "Unexpected number of shards in created table")

	allShardStatuses, err := tm.GetShardStatuses()
	require.NoError(t, err)
	assert.Len(
		t,
		allShardStatuses,
		int(tc.NumShards),
		"Unexpected number of shard statuses after table creation",
	)

	// Verify shard statuses
	for shardID, shardConf := range table.Shards {
		status, err := tm.GetShardStatus(shardID)
		require.NoError(t, err, "Shard status not found for shard %d", shardID)
		assert.Equal(t, shardID, status.ID)
		assert.Equal(t, tableName, status.Table)
		assert.Equal(t, *shardConf, status.ShardConfig)
		assert.NotNil(t, status.Peers)
	}

	// Get Table
	retrievedTable, err := tm.GetTable(tableName)
	require.NoError(t, err)
	assert.Equal(t, table, retrievedTable, "Retrieved table does not match created table")

	// Try to create existing table
	_, err = tm.CreateTable(tableName, tc)
	assert.ErrorIs(t, err, ErrTableExists, "Expected ErrTableExists when creating duplicate table")

	// Remove Table
	err = tm.RemoveTable(tableName)
	require.NoError(t, err)

	// Try to get removed table
	_, err = tm.GetTable(tableName)
	assert.ErrorIs(t, err, ErrNotFound, "Expected ErrNotFound when getting removed table")

	// Verify shard statuses are removed
	allShardStatusesAfterRemove, err := tm.GetShardStatuses()
	require.NoError(t, err)
	assert.Empty(
		t,
		allShardStatusesAfterRemove,
		"Shard statuses not cleaned up after table removal",
	)

	// Try to remove non-existent table
	err = tm.RemoveTable("nonExistentTable")
	assert.ErrorIs(t, err, ErrNotFound, "Expected ErrNotFound for non-existent table")
}

func TestTableManager_IndexOperations(t *testing.T) {
	db := setupTestDB(t)

	tm, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)

	tableName := "indexTestTable"
	_, err = tm.CreateTable(
		tableName,
		TableConfig{NumShards: 1, StartID: 1, Schema: &schema.TableSchema{}},
	)
	require.NoError(t, err)

	indexName1 := "idx1"
	indexConfig1 := indexes.EmbeddingsIndexConfig{Field: "data"}
	ic1 := *indexes.NewEmbeddingsConfig("idx1", indexConfig1)

	// Create Index
	table, err := tm.CreateIndex(tableName, indexName1, ic1)
	require.NoError(t, err)
	require.Contains(t, table.Indexes, indexName1)
	assert.Equal(t, indexName1, table.Indexes[indexName1].Name)
	assert.Equal(t, ic1, table.Indexes[indexName1])

	// Verify shard status reflects new index
	for shardID := range table.Shards {
		status, err := tm.GetShardStatus(shardID)
		require.NoError(t, err)
		require.Contains(t, status.Indexes, indexName1, "Index not updated in shard status")
	}

	// Get Index
	retrievedIndex, err := tm.GetIndex(tableName, indexName1)
	require.NoError(t, err)
	// assert.Equal(t, table.Indexes[indexName1], retrievedIndex.IndexConfig)
	assert.True(
		t,
		retrievedIndex.Equal(table.Indexes[indexName1]),
		"Retrieved index config does not match",
	)
	assert.Equal(t, indexName1, retrievedIndex.Name, "GetIndex should populate Name from map key")

	// Create another index
	indexName2 := "idx2"
	indexConfig2 := indexes.EmbeddingsIndexConfig{Field: "value"}
	ic2 := *indexes.NewEmbeddingsConfig("idx2", indexConfig2)
	_, err = tm.CreateIndex(tableName, indexName2, ic2)
	require.NoError(t, err)

	// Get All Indexes
	allIndexes, err := tm.Indexes(tableName)
	require.NoError(t, err)
	assert.Len(t, allIndexes, 2)
	assert.Contains(t, allIndexes, indexName1)
	assert.Contains(t, allIndexes, indexName2)
	for name, idx := range allIndexes {
		assert.Equal(t, name, idx.Name, "Indexes() should populate Name from map key")
	}

	// Try to create existing index
	_, err = tm.CreateIndex(tableName, indexName1, ic1)
	assert.Error(
		t,
		err,
		"Expected error when creating existing index",
	) // Specific error (e.g., ErrIndexExists)

	// Drop Index
	table, err = tm.DropIndex(tableName, indexName1)
	require.NoError(t, err)
	assert.NotContains(t, table.Indexes, indexName1)
	assert.Contains(t, table.Indexes, indexName2) // Ensure other index is still there

	// Verify shard status reflects dropped index
	for shardID := range table.Shards {
		status, err := tm.GetShardStatus(shardID)
		require.NoError(t, err)
		assert.NotContains(
			t,
			status.Indexes,
			indexName1,
			"Dropped index not removed from shard status",
		)
		require.Contains(t, status.Indexes, indexName2)
	}

	// Try to get dropped index
	_, err = tm.GetIndex(tableName, indexName1)
	assert.Error(t, err, "Expected error when getting dropped index")

	// Try to drop non-existent index
	_, err = tm.DropIndex(tableName, "nonExistentIndex")
	require.NoError(t, err) // Should not error if index doesn't exist, or have a specific error

	// Test on non-existent table
	_, err = tm.CreateIndex("badTable", "idx", indexes.IndexConfig{})
	assert.Error(t, err)
	_, err = tm.GetIndex("badTable", "idx")
	assert.Error(t, err)
	_, err = tm.Indexes("badTable")
	assert.Error(t, err)
	_, err = tm.DropIndex("badTable", "idx")
	assert.Error(t, err)
}

// TestCreateIndex_SetsNameFromParameter verifies that CreateIndex populates
// config.Name even when the caller passes a config with Name=="".
// This reproduces the bug where the HTTP API constructs configs from JSON map
// keys (Name comes from the key, not the struct) and the reconciler later fails
// with "index name cannot be empty".
func TestCreateIndex_SetsNameFromParameter(t *testing.T) {
	db := setupTestDB(t)

	tm, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)

	tableName := "nameFromParamTable"
	_, err = tm.CreateTable(
		tableName,
		TableConfig{NumShards: 1, StartID: 1, Schema: &schema.TableSchema{}},
	)
	require.NoError(t, err)

	// Simulate the HTTP API path: config arrives from JSON with Name=""
	config := indexes.IndexConfig{
		Type: indexes.IndexTypeEmbeddings,
	}
	_ = config.FromEmbeddingsIndexConfig(indexes.EmbeddingsIndexConfig{
		Field: "data",
	})

	indexName := "my_index"
	table, err := tm.CreateIndex(tableName, indexName, config)
	require.NoError(t, err)

	// The stored config MUST have Name set, even though the caller didn't set it
	assert.Equal(t, indexName, table.Indexes[indexName].Name,
		"CreateIndex must populate config.Name from the indexName parameter")

	// Also verify shard statuses carry the name
	for shardID := range table.Shards {
		status, err := tm.GetShardStatus(shardID)
		require.NoError(t, err)
		assert.Equal(t, indexName, status.Indexes[indexName].Name,
			"Shard status must carry the index name")
	}
}

func TestTableManager_ShardStatusOperations(t *testing.T) {
	db := setupTestDB(t)

	tm, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)

	tableName := "statusTestTable"
	_, err = tm.CreateTable(
		tableName,
		TableConfig{NumShards: 1, StartID: 500, Schema: &schema.TableSchema{}},
	)
	require.NoError(t, err)

	shardID := types.ID(500)
	originalStatus, err := tm.GetShardStatus(shardID)
	require.NoError(t, err)

	// Simulate an update via UpdateStatuses
	newPeers := common.NewPeerSet(10, 20)
	newStats := &store.ShardStats{Storage: &store.StorageStats{DiskSize: 1024}}
	newRaftStatus := &common.RaftStatus{Lead: 10}

	err = tm.UpdateStatuses(context.Background(), map[types.ID]*StoreStatus{
		types.ID(10): { // Dummy store ID
			StoreInfo: store.StoreInfo{ID: types.ID(1)}, // StoreInfo is needed by UpdateStatuses
			Shards: map[types.ID]*store.ShardInfo{
				shardID: {
					ShardConfig: originalStatus.ShardConfig, // Preserve original config
					Peers:       newPeers,
					ShardStats:  newStats,
					RaftStatus:  newRaftStatus,
				},
			},
		},
	})
	require.NoError(t, err)

	updatedStatus, err := tm.GetShardStatus(shardID)
	require.NoError(t, err)
	assert.Equal(t, newPeers, updatedStatus.Peers)
	// Compare Storage.DiskSize specifically since UpdateStatuses sets ShardStats.Updated to time.Now()
	assert.Equal(t, newStats.Storage.DiskSize, updatedStatus.ShardStats.Storage.DiskSize)
	assert.Equal(t, newRaftStatus, updatedStatus.RaftStatus)
	assert.Equal(
		t,
		store.ShardState_Default,
		updatedStatus.State,
	) // Assuming initial state update goes to Default

	// Test state transition
	// Set the shard's state to StateSplitting directly in the DB for this test
	currentStatusToModify, err := tm.GetShardStatus(shardID)
	require.NoError(t, err)
	currentStatusToModify.State = store.ShardState_SplittingOff
	err = tm.saveStoreAndShardStatuses(
		nil,
		map[types.ID]*store.ShardStatus{shardID: currentStatusToModify},
	)
	require.NoError(t, err)

	// Trigger UpdateStatuses again. This should see StateSplitting and transition it.
	// The ShardInfo provided here simulates a store reporting its status.
	err = tm.UpdateStatuses(context.Background(), map[types.ID]*StoreStatus{
		types.ID(1): { // Dummy store ID
			StoreInfo: store.StoreInfo{ID: types.ID(1)},
			Shards: map[types.ID]*store.ShardInfo{
				shardID: {
					ShardConfig: currentStatusToModify.ShardConfig, // Use current config
					Peers:       currentStatusToModify.Peers,       // Use current peers
					ShardStats:  currentStatusToModify.ShardStats,  // Use current stats
					RaftStatus:  currentStatusToModify.RaftStatus,  // Use current raft status
				},
			},
		},
	})
	require.NoError(t, err)

	updatedStatusAfterSplit, err := tm.GetShardStatus(shardID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardState_SplitOffPreSnap, updatedStatusAfterSplit.State)

	// GetShardStatusAll
	allStatuses, err := tm.GetShardStatuses()
	require.NoError(t, err)
	require.Len(t, allStatuses, 1)
	assert.Equal(t, updatedStatusAfterSplit, allStatuses[shardID])

	// Attempt to update status for a non-existent shard via UpdateStatuses
	// This should not error but also not create a new shard status if the shard isn't known from a table
	err = tm.UpdateStatuses(context.Background(), map[types.ID]*StoreStatus{
		types.ID(2): { // Another dummy store ID
			StoreInfo: store.StoreInfo{ID: types.ID(2)},
			Shards: map[types.ID]*store.ShardInfo{
				types.ID(999): {Peers: common.NewPeerSet()},
			},
		},
	})
	require.NoError(t, err)
	_, err = tm.GetShardStatus(types.ID(999))
	assert.ErrorIs(t, err, ErrNotFound, "Expected ErrNotFound for non-existent shard status")
}

// TestUpdateStatuses_NilShardStatsPreservesExisting verifies that when a storage
// node reports a heartbeat without ShardStats (nil), previously known stats are
// preserved rather than being clobbered.
func TestUpdateStatuses_NilShardStatsPreservesExisting(t *testing.T) {
	db := setupTestDB(t)

	tm, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)

	tableName := "statsPreserveTable"
	_, err = tm.CreateTable(
		tableName,
		TableConfig{NumShards: 1, StartID: 600, Schema: &schema.TableSchema{}},
	)
	require.NoError(t, err)

	shardID := types.ID(600)
	originalStatus, err := tm.GetShardStatus(shardID)
	require.NoError(t, err)

	// First update: set ShardStats with known disk size
	existingStats := &store.ShardStats{Storage: &store.StorageStats{DiskSize: 5000}}
	err = tm.UpdateStatuses(context.Background(), map[types.ID]*StoreStatus{
		types.ID(10): {
			StoreInfo: store.StoreInfo{ID: types.ID(10)},
			Shards: map[types.ID]*store.ShardInfo{
				shardID: {
					ShardConfig: originalStatus.ShardConfig,
					Peers:       common.NewPeerSet(10),
					ShardStats:  existingStats,
					RaftStatus:  &common.RaftStatus{Lead: 10},
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify stats were set
	status, err := tm.GetShardStatus(shardID)
	require.NoError(t, err)
	require.NotNil(t, status.ShardStats)
	require.NotNil(t, status.ShardStats.Storage)
	assert.Equal(t, uint64(5000), status.ShardStats.Storage.DiskSize)

	// Second update: report with nil ShardStats (simulates a heartbeat without stats)
	err = tm.UpdateStatuses(context.Background(), map[types.ID]*StoreStatus{
		types.ID(10): {
			StoreInfo: store.StoreInfo{ID: types.ID(10)},
			Shards: map[types.ID]*store.ShardInfo{
				shardID: {
					ShardConfig: status.ShardConfig,
					Peers:       common.NewPeerSet(10),
					ShardStats:  nil, // No stats in this heartbeat
					RaftStatus:  &common.RaftStatus{Lead: 10},
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify previous stats are preserved (not clobbered to nil)
	statusAfter, err := tm.GetShardStatus(shardID)
	require.NoError(t, err)
	require.NotNil(t, statusAfter.ShardStats, "ShardStats should be preserved when node reports nil")
	require.NotNil(t, statusAfter.ShardStats.Storage, "Storage stats should be preserved")
	assert.Equal(t, uint64(5000), statusAfter.ShardStats.Storage.DiskSize,
		"DiskSize should be preserved from previous update")
}

func TestTable_FindShardForKey(t *testing.T) {
	table := &store.Table{
		Name: "findKeyTable",
		Shards: map[types.ID]*store.ShardConfig{
			1: {ByteRange: [2][]byte{[]byte("A"), []byte("M")}}, // Shard for A-L
			2: {ByteRange: [2][]byte{[]byte("M"), []byte("Z")}}, // Shard for M-Y
			3: {
				ByteRange: [2][]byte{[]byte("Z"), {0xFF, 0xFF}},
			}, // Shard for Z onwards (example, might need precise MaxByte)
		},
	}
	// Tweak shard 3's end range for better testing if {0xFF,0xFF} causes issues.
	// For KeyInByteRange: k < end. A single 0xFF often used for "until very end".
	table.Shards[3].ByteRange[1] = []byte{0xFF}

	testCases := []struct {
		key         string
		expectedID  types.ID
		expectError bool
	}{
		{"Apple", 1, false},
		{"Mango", 2, false},
		{"Zebra", 3, false},
		{"A", 1, false},           // Boundary start
		{"M", 2, false},           // Boundary start
		{"Lz", 1, false},          // Near boundary end for shard 1
		{"Yzzz", 2, false},        // Near boundary end for shard 2
		{"\x00", 0, true},         // Before first shard
		{"\xFF\xFF\xFF", 0, true}, // After last shard (should be 3, but 0 is returned if not found)
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			shardID, err := table.FindShardForKey(tc.key)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedID, shardID)
			}
		})
	}
}

func TestTable_PartitionKeysByShard(t *testing.T) {
	table := &store.Table{
		Name: "partitionKeyTable",
		Shards: map[types.ID]*store.ShardConfig{
			10: {ByteRange: types.Range{[]byte("a"), []byte("g")}}, // a-f
			20: {ByteRange: types.Range{[]byte("g"), []byte("m")}}, // g-l
			30: {ByteRange: types.Range{[]byte("m"), []byte("s")}}, // m-r
		},
	}

	keys := []string{"apple", "banana", "grape", "kiwi", "mango", "orange", "pear", "zebra", "000"}
	expectedPartitions := map[types.ID][]string{
		10: {"apple", "banana"},
		20: {"grape", "kiwi"},
		30: {"mango", "orange", "pear"},
	}
	expectedUnfound := []string{"zebra", "000"}

	partitions, unfoundKeys := table.PartitionKeysByShard(keys)

	// Sort slices within maps for consistent comparison
	for _, pKeys := range partitions {
		slices.Sort(pKeys)
	}
	for _, pKeys := range expectedPartitions {
		slices.Sort(pKeys)
	}
	slices.Sort(unfoundKeys)
	slices.Sort(expectedUnfound)

	assert.Equal(t, expectedPartitions, partitions)
	assert.Equal(t, expectedUnfound, unfoundKeys)

	// Test with empty keys
	partitions, unfoundKeys = table.PartitionKeysByShard([]string{})
	assert.Empty(t, partitions)
	assert.Empty(t, unfoundKeys)

	// Test with all keys unfound
	partitions, unfoundKeys = table.PartitionKeysByShard([]string{"1", "2", "3"})
	assert.Empty(t, partitions)
	assert.Equal(t, []string{"1", "2", "3"}, unfoundKeys)
}

func TestTableManager_ReassignShardsForSplit(t *testing.T) {
	db := setupTestDB(t)

	tm, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)

	tableName := "splitTestTable"
	initialPeers := common.NewPeerSet(1, 2, 3)
	// Create a table with one shard: "" to "Z"
	table, err := tm.CreateTable(
		tableName,
		TableConfig{NumShards: 1, StartID: 100, Schema: &schema.TableSchema{}},
	)
	require.NoError(t, err)

	originalShardID := types.ID(100)
	newShardID := types.ID(101)

	// Manually set peers for the initial shard for testing peer propagation
	status, err := tm.GetShardStatus(originalShardID)
	require.NoError(t, err)
	status.Peers = initialPeers
	// Save the updated shard status directly
	err = tm.saveTableAndShardStatus(
		table,
		map[types.ID]*store.ShardStatus{originalShardID: status},
	)
	require.NoError(t, err)
	splitKey := []byte("M")

	transition := SplitTransition{
		ShardID:      originalShardID,
		SplitShardID: newShardID,
		SplitKey:     splitKey,
		TableName:    tableName,
	}

	originalEnd := table.Shards[originalShardID].ByteRange[1]
	returnedPeerIDs, newConf, err := tm.ReassignShardsForSplit(transition)
	require.NoError(t, err)
	require.NotNil(t, newConf)

	// Verify peers are returned correctly
	assert.ElementsMatch(t, initialPeers.IDSlice(), returnedPeerIDs)

	assert.Equal(t, splitKey, newConf.ByteRange[0])
	assert.Equal(t, originalEnd, newConf.ByteRange[1])

	// Fetch the table again to get the latest state of its Shards map
	table, err = tm.GetTable(tableName)
	require.NoError(t, err)

	originalStatus, err := tm.GetShardStatus(originalShardID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardState_PreSplit, originalStatus.State)

	// Verify new shard is created
	createdNewShard, ok := table.Shards[newShardID]
	require.True(t, ok)
	assert.Equal(t, splitKey, createdNewShard.ByteRange[0]) // New start
	assert.Equal(
		t,
		originalEnd,
		createdNewShard.ByteRange[1],
		"new shard end should match original shard's previous end",
	)
	// The CreateTable with NumShards:1 creates a range of {} to {0xFF}.
	assert.Equal(t, []byte{0xFF}, createdNewShard.ByteRange[1])

	newStatus, err := tm.GetShardStatus(newShardID)
	require.NoError(t, err)
	assert.Equal(t, *createdNewShard, newStatus.ShardConfig)
	assert.Equal(t, initialPeers, newStatus.Peers, "New shard should inherit peers")
	assert.Equal(t, store.ShardState_SplittingOff, newStatus.State)

	// Test persistence
	tm2, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)
	table2, err := tm2.GetTable(tableName)
	require.NoError(t, err)
	assert.Len(t, table2.Shards, 2)
	assert.Contains(t, table2.Shards, originalShardID)
	assert.Contains(t, table2.Shards, newShardID)

	_, err = tm2.GetShardStatus(newShardID)
	require.NoError(t, err, "New shard status not persisted")
}

func TestTableManager_ReassignShardsForMerge(t *testing.T) {
	db := setupTestDB(t)

	tm, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)

	tableName := "mergeTestTable"
	// Create a table with two shards: Shard1: [A-M), Shard2: [M-Z)
	// IDs 200, 201 for predictability
	tc := TableConfig{
		NumShards: 2,
		StartID:   200, // This will create shards 200 (empty-"M") and 201 ("M"-0xFF)
		Schema:    &schema.TableSchema{},
	}
	table, err := tm.CreateTable(tableName, tc)
	require.NoError(t, err)

	shardToKeepID := types.ID(200)   // Range: "" - "M"
	shardToRemoveID := types.ID(201) // Range: "M" - 0xFF

	originalShardToKeepConf := *table.Shards[shardToKeepID] // Make a copy
	originalShardToRemoveConf := *table.Shards[shardToRemoveID]

	transition := MergeTransition{
		ShardID:      shardToKeepID,
		MergeShardID: shardToRemoveID,
		TableName:    tableName,
	}

	mergedConf, err := tm.ReassignShardsForMerge(transition)
	require.NoError(t, err)
	require.NotNil(t, mergedConf)

	// Verify the kept shard is updated
	assert.Equal(
		t,
		originalShardToKeepConf.ByteRange[0],
		mergedConf.ByteRange[0],
		"Merged shard start key mismatch",
	)
	assert.Equal(
		t,
		originalShardToRemoveConf.ByteRange[1],
		mergedConf.ByteRange[1],
		"Merged shard end key mismatch",
	)

	// Fetch the table again to get the latest state of its Shards map
	_, err = tm.GetTable(tableName)
	require.NoError(t, err)

	keptStatus, err := tm.GetShardStatus(shardToKeepID)
	require.NoError(t, err)
	assert.Equal(t, mergedConf, &keptStatus.ShardConfig)
	assert.Equal(t, store.ShardState_PreMerge, keptStatus.State)

	// Verify the other shard is removed from table config
	table, err = tm.GetTable(tableName)
	require.NoError(t, err)
	_, ok := table.Shards[shardToRemoveID]
	assert.False(t, ok, "Merged shard was not removed from table config")
	// Verify the other shard status is removed (or marked as gone, depending on impl - current impl deletes)
	_, err = tm.GetShardStatus(shardToRemoveID)
	assert.ErrorIs(t, err, ErrNotFound, "Merged shard status was not removed")

	// Test persistence
	tm2, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)
	table2, err := tm2.GetTable(tableName)
	require.NoError(t, err)
	assert.Len(t, table2.Shards, 1, "Merged table should have only one shard after reload")
	assert.Contains(t, table2.Shards, shardToKeepID)
	assert.NotContains(t, table2.Shards, shardToRemoveID)

	_, err = tm2.GetShardStatus(shardToKeepID)
	require.NoError(t, err, "Kept shard status not persisted")
	_, err = tm2.GetShardStatus(shardToRemoveID)
	assert.ErrorIs(t, err, ErrNotFound, "Removed shard status should not be found")

	// Test merging non-adjacent shards (should error) - need to set up a specific scenario
	// For now, this requires more intricate setup of ranges.
}

func TestTableManager_Persistence(t *testing.T) {
	dbPath := t.TempDir()
	opts := pebbleutils.NewMemPebbleOpts()

	// First instance: Create table, add index
	db1, err := pebble.Open(dbPath, opts)
	require.NoError(t, err)
	tm1, err := NewTableManager(&kv.PebbleDB{DB: db1}, nil, 0)
	require.NoError(t, err)

	tableName := "persistentTable"
	schema := &schema.TableSchema{}
	tc := TableConfig{NumShards: 1, StartID: 1, Schema: schema}
	_, err = tm1.CreateTable(tableName, tc)
	require.NoError(t, err)
	indexConfig1 := indexes.EmbeddingsIndexConfig{Field: "data"}
	ic1 := *indexes.NewEmbeddingsConfig("idx_persist", indexConfig1)
	_, err = tm1.CreateIndex(tableName, "idx_persist", ic1)
	require.NoError(t, err)

	// Update shard status for coverage using UpdateStatuses
	originalShardStatus, err := tm1.GetShardStatus(1)
	require.NoError(t, err)

	err = tm1.UpdateStatuses(context.Background(), map[types.ID]*StoreStatus{
		types.ID(99): { // Dummy store ID
			StoreInfo: store.StoreInfo{ID: types.ID(99)}, // StoreInfo is needed
			Shards: map[types.ID]*store.ShardInfo{
				types.ID(1): { // ShardID
					ShardConfig: originalShardStatus.ShardConfig, // Preserve original config
					Peers:       common.NewPeerSet(1, 2, 99),
					ShardStats:  &store.ShardStats{Storage: &store.StorageStats{DiskSize: 100}},
					RaftStatus:  &common.RaftStatus{Lead: 1},
				},
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, db1.Close())

	// Second instance: Load and verify
	db2, err := pebble.Open(dbPath, opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, db2.Close()) }()

	tm2, err := NewTableManager(&kv.PebbleDB{DB: db2}, nil, 0)
	require.NoError(t, err)

	// Verify table
	loadedTable, err := tm2.GetTable(tableName)
	require.NoError(t, err)
	assert.Equal(t, tableName, loadedTable.Name)
	assert.Len(t, loadedTable.Shards, 1)
	assert.Contains(t, loadedTable.Indexes, "idx_persist")

	// Verify shard status
	status, err := tm2.GetShardStatus(1)
	require.NoError(t, err)
	require.NotNil(t, status.RaftStatus, "RaftStatus should not be nil")
	assert.Equal(t, types.ID(1), status.RaftStatus.Lead)
	require.NotNil(t, status.ShardStats, "ShardStats should not be nil")
	require.NotNil(t, status.ShardStats.Storage, "ShardStats.Storage should not be nil")
	assert.Equal(t, uint64(100), status.ShardStats.Storage.DiskSize)
	require.NotNil(t, status.Peers, "Peers should not be nil")
	assert.True(t, status.Peers.Contains(2))
}

func TestTableManager_UpdateSchema(t *testing.T) {
	db := setupTestDB(t)

	tm, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)

	tableName := "schemaUpdateTestTable"
	initialSchema := &schema.TableSchema{
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{"type": "string"},
			},
		},
		Version: 0,
	}

	tc := TableConfig{
		NumShards: 1,
		StartID:   1,
		Schema:    initialSchema,
		Indexes:   map[string]indexes.IndexConfig{},
	}

	// Add a full text index to test versioning
	fullTextIndexName := "full_text_index_v0"
	fullTextIndexConfig := indexes.IndexConfig{
		Name: fullTextIndexName,
		Type: indexes.IndexTypeFullTextV0,
	}
	tc.Indexes[fullTextIndexName] = fullTextIndexConfig

	_, err = tm.CreateTable(tableName, tc)
	require.NoError(t, err)

	// First schema update
	newSchemaV1 := &schema.TableSchema{
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{"type": "number"},
			},
		},
	}

	var table *store.Table
	table, err = tm.UpdateSchema(tableName, newSchemaV1)
	require.NoError(t, err)

	// Check schema versions
	assert.Equal(t, uint32(1), table.Schema.Version)
	assert.Equal(t, initialSchema, table.ReadSchema)
	// The passed schema is mutated, so compare its contents.
	assert.Equal(t, newSchemaV1.DocumentSchemas, table.Schema.DocumentSchemas)

	// Check index versioning
	assert.Contains(t, table.Indexes, fullTextIndexName)
	assert.Contains(t, table.Indexes, "full_text_index_v1")
	assert.Equal(t, "full_text_index_v1", table.Indexes["full_text_index_v1"].Name)

	// Second schema update (migration in progress)
	newSchemaV2 := &schema.TableSchema{
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{"type": "boolean"},
			},
		},
	}

	table, err = tm.UpdateSchema(tableName, newSchemaV2)
	require.NoError(t, err)

	// Check schema versions.
	require.Equal(t, uint32(2), table.Schema.Version)
	assert.Equal(t, initialSchema, table.ReadSchema) // ReadSchema doesn't change during migration
	assert.Equal(t, newSchemaV2.DocumentSchemas, table.Schema.DocumentSchemas)

	// Check index versioning
	assert.Contains(t, table.Indexes, fullTextIndexName)
	assert.Contains(t, table.Indexes, "full_text_index_v2")
	assert.Equal(t, "full_text_index_v2", table.Indexes["full_text_index_v2"].Name)

	// Check persistence by reloading the manager and table
	tm2, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)
	table2, err := tm2.GetTable(tableName)
	require.NoError(t, err)
	assert.Equal(t, table.Schema, table2.Schema)
	assert.Equal(t, table.ReadSchema, table2.ReadSchema)
	assert.Equal(t, table.Name, table2.Name)
	assert.True(t, maps.EqualFunc(table.Shards, table2.Shards, func(a, b *store.ShardConfig) bool {
		return a.Equal(b)
	}))
	assert.True(
		t,
		maps.EqualFunc(table.Indexes, table2.Indexes, func(a, b indexes.IndexConfig) bool {
			return a.Equal(b)
		}),
	)
	assert.Equal(t, uint32(2), table2.Schema.Version)
	assert.Contains(t, table2.Indexes, "full_text_index_v2")
}

// TestTableManager_UpdateSchema_UpdatesShardStatuses verifies that UpdateSchema
// persists the new schema and indexes into the shard status entries, not just
// the table definition. Without this, the reconciler sees stale shard configs
// and the Indexes() API never reports stats for newly created versioned indexes.
func TestTableManager_UpdateSchema_UpdatesShardStatuses(t *testing.T) {
	db := setupTestDB(t)
	tm, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)

	tableName := "shardStatusSchemaTest"
	initialSchema := &schema.TableSchema{
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {Schema: map[string]any{"type": "string"}},
		},
		Version: 0,
	}
	tc := TableConfig{
		NumShards: 2,
		StartID:   100,
		Schema:    initialSchema,
		Indexes: map[string]indexes.IndexConfig{
			"full_text_index_v0": *indexes.NewFullTextIndexConfig("full_text_index_v0", false),
		},
	}

	table, err := tm.CreateTable(tableName, tc)
	require.NoError(t, err)

	// Verify initial shard statuses have v0 index and schema version 0
	for shardID := range table.Shards {
		status, err := tm.GetShardStatus(shardID)
		require.NoError(t, err)
		assert.Contains(t, status.Indexes, "full_text_index_v0")
		assert.NotContains(t, status.Indexes, "full_text_index_v1")
		assert.Equal(t, uint32(0), status.Schema.Version)
	}

	// Update schema — this should create full_text_index_v1
	newSchema := &schema.TableSchema{
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {Schema: map[string]any{"type": "number"}},
		},
	}
	table, err = tm.UpdateSchema(tableName, newSchema)
	require.NoError(t, err)
	require.Contains(t, table.Indexes, "full_text_index_v1")

	// Verify shard statuses were updated with the new indexes and schema
	for shardID := range table.Shards {
		status, err := tm.GetShardStatus(shardID)
		require.NoError(t, err)
		assert.Contains(t, status.Indexes, "full_text_index_v0",
			"shard status should still have v0 index")
		assert.Contains(t, status.Indexes, "full_text_index_v1",
			"shard status should have the new v1 index after UpdateSchema")
		assert.Equal(t, uint32(1), status.Schema.Version,
			"shard status schema version should be updated to 1")
	}

	// Verify DropReadSchema also updates shard statuses
	err = tm.DropReadSchema(tableName)
	require.NoError(t, err)

	table, err = tm.GetTable(tableName)
	require.NoError(t, err)
	assert.NotContains(t, table.Indexes, "full_text_index_v0",
		"v0 index should be removed after DropReadSchema")

	for shardID := range table.Shards {
		status, err := tm.GetShardStatus(shardID)
		require.NoError(t, err)
		assert.NotContains(t, status.Indexes, "full_text_index_v0",
			"shard status should no longer have v0 index after DropReadSchema")
		assert.Contains(t, status.Indexes, "full_text_index_v1",
			"shard status should still have v1 index after DropReadSchema")
		assert.Equal(t, uint32(1), status.Schema.Version,
			"shard status schema version should remain at 1 after DropReadSchema")
	}
}

// TestSplittingStateWaitsForSplitOffShardReady tests that a parent shard in Splitting
// state does not transition to Default until its split-off shard has HasSnapshot=true.
// This ensures continuous data availability during shard splits by keeping the parent
// shard serving the split-off range until the new shard has loaded its data.
func TestSplittingStateWaitsForSplitOffShardReady(t *testing.T) {
	db := setupTestDB(t)

	tm, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)

	tableName := "splitTestTable"
	parentShardID := types.ID(1)
	splitOffShardID := types.ID(2)
	splitKey := []byte("M") // Split at "M"

	// Set up the parent shard in Splitting state with its range ending at splitKey
	parentStatus := &store.ShardStatus{
		ID:    parentShardID,
		Table: tableName,
		State: store.ShardState_Splitting,
		ShardInfo: store.ShardInfo{
			ShardConfig: store.ShardConfig{
				ByteRange: [2][]byte{{0x00}, splitKey}, // Parent range: [0x00, M)
			},
			Peers:     common.NewPeerSet(1),
			Splitting: false, // Split operation completed
		},
	}

	// Set up the split-off shard in SplittingOff state, not ready yet
	splitOffStatus := &store.ShardStatus{
		ID:    splitOffShardID,
		Table: tableName,
		State: store.ShardState_SplittingOff,
		ShardInfo: store.ShardInfo{
			ShardConfig: store.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xFF}}, // Split-off range: [M, 0xFF)
			},
			Peers:       common.NewPeerSet(1),
			HasSnapshot: false, // Not ready yet
		},
	}

	// Save both shard statuses
	err = tm.saveStoreAndShardStatuses(nil, map[types.ID]*store.ShardStatus{
		parentShardID:   parentStatus,
		splitOffShardID: splitOffStatus,
	})
	require.NoError(t, err)

	// Simulate a status update from the store node - the parent should stay in Splitting
	// because the split-off shard is not ready yet
	err = tm.UpdateStatuses(context.Background(), map[types.ID]*StoreStatus{
		types.ID(10): {
			StoreInfo: store.StoreInfo{ID: types.ID(10)},
			Shards: map[types.ID]*store.ShardInfo{
				parentShardID: {
					ShardConfig: parentStatus.ShardConfig,
					Peers:       parentStatus.Peers,
					Splitting:   false, // Split operation completed
				},
				splitOffShardID: {
					ShardConfig: splitOffStatus.ShardConfig,
					Peers:       splitOffStatus.Peers,
					HasSnapshot: false,
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify the parent shard is still in Splitting state (not Default)
	updatedParent, err := tm.GetShardStatus(parentShardID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardState_Splitting, updatedParent.State,
		"Parent shard should remain in Splitting state while split-off shard is not ready")

	// Now update the split-off shard to have HasSnapshot=true
	// First, update just the split-off shard so its status is persisted
	err = tm.UpdateStatuses(context.Background(), map[types.ID]*StoreStatus{
		types.ID(10): {
			StoreInfo: store.StoreInfo{ID: types.ID(10)},
			Shards: map[types.ID]*store.ShardInfo{
				splitOffShardID: {
					ShardConfig:  splitOffStatus.ShardConfig,
					Peers:        splitOffStatus.Peers,
					HasSnapshot:  true, // Data loaded
					Initializing: false,
					RaftStatus:   &common.RaftStatus{Lead: 1},
				},
			},
		},
	})
	require.NoError(t, err)

	// Then update the parent shard - it should now see the split-off shard is ready
	err = tm.UpdateStatuses(context.Background(), map[types.ID]*StoreStatus{
		types.ID(10): {
			StoreInfo: store.StoreInfo{ID: types.ID(10)},
			Shards: map[types.ID]*store.ShardInfo{
				parentShardID: {
					ShardConfig: parentStatus.ShardConfig,
					Peers:       parentStatus.Peers,
					Splitting:   false,
				},
			},
		},
	})
	require.NoError(t, err)

	// Now the parent shard should transition to Default
	updatedParent, err = tm.GetShardStatus(parentShardID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardState_Default, updatedParent.State,
		"Parent shard should transition to Default once split-off shard is ready")
}

func TestPreSplitStateTransitionsToSplittingWhenSplitStateActive(t *testing.T) {
	db := setupTestDB(t)

	tm, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)

	tableName := "splitStateTable"
	parentShardID := types.ID(10)
	splitKey := []byte("M")

	parentStatus := &store.ShardStatus{
		ID:    parentShardID,
		Table: tableName,
		State: store.ShardState_PreSplit,
		ShardInfo: store.ShardInfo{
			ShardConfig: store.ShardConfig{
				ByteRange: [2][]byte{{0x00}, splitKey},
			},
			Peers: common.NewPeerSet(1),
		},
	}

	err = tm.saveStoreAndShardStatuses(nil, map[types.ID]*store.ShardStatus{
		parentShardID: parentStatus,
	})
	require.NoError(t, err)

	activeSplitState := &storedb.SplitState{}
	activeSplitState.SetPhase(storedb.SplitState_PHASE_SPLITTING)
	activeSplitState.SetSplitKey(splitKey)
	activeSplitState.SetNewShardId(uint64(types.ID(11)))

	err = tm.UpdateStatuses(context.Background(), map[types.ID]*StoreStatus{
		types.ID(10): {
			StoreInfo: store.StoreInfo{ID: types.ID(10)},
			Shards: map[types.ID]*store.ShardInfo{
				parentShardID: {
					ShardConfig: parentStatus.ShardConfig,
					Peers:       parentStatus.Peers,
					Splitting:   false,
					SplitState:  activeSplitState,
					RaftStatus:  &common.RaftStatus{Lead: 10},
				},
			},
		},
	})
	require.NoError(t, err)

	updatedParent, err := tm.GetShardStatus(parentShardID)
	require.NoError(t, err)
	assert.Equal(t, store.ShardState_Splitting, updatedParent.State,
		"Parent shard should transition to Splitting when an active split state is reported")
}

// TestSplitOffShardIsReady tests the helper function that checks if a split-off
// shard is ready to serve traffic.
func TestSplitOffShardIsReady(t *testing.T) {
	db := setupTestDB(t)

	tm, err := NewTableManager(db, nil, 0)
	require.NoError(t, err)

	tableName := "readyTestTable"
	parentShardID := types.ID(1)
	splitOffShardID := types.ID(2)
	splitKey := []byte("M")

	// Test case 1: No split-off shard exists - should return true (allow transition)
	parentStatus := &store.ShardStatus{
		ID:    parentShardID,
		Table: tableName,
		State: store.ShardState_Splitting,
		ShardInfo: store.ShardInfo{
			ShardConfig: store.ShardConfig{
				ByteRange: [2][]byte{{0x00}, splitKey},
			},
		},
	}
	err = tm.saveStoreAndShardStatuses(nil, map[types.ID]*store.ShardStatus{
		parentShardID: parentStatus,
	})
	require.NoError(t, err)

	assert.True(t, tm.splitOffShardIsReady(parentStatus),
		"Should return true when no split-off shard exists")

	// Test case 2: Split-off shard exists in SplittingOff state with HasSnapshot=false
	splitOffStatus := &store.ShardStatus{
		ID:    splitOffShardID,
		Table: tableName,
		State: store.ShardState_SplittingOff,
		ShardInfo: store.ShardInfo{
			ShardConfig: store.ShardConfig{
				ByteRange: [2][]byte{splitKey, {0xFF}},
			},
			HasSnapshot:         false,
			SplitReplayRequired: true,
		},
	}
	err = tm.saveStoreAndShardStatuses(nil, map[types.ID]*store.ShardStatus{
		splitOffShardID: splitOffStatus,
	})
	require.NoError(t, err)

	assert.False(t, tm.splitOffShardIsReady(parentStatus),
		"Should return false when split-off shard is in SplittingOff with HasSnapshot=false")

	// Test case 3: Split-off shard has HasSnapshot=true but no leader - not ready
	splitOffStatus.HasSnapshot = true
	splitOffStatus.Initializing = false
	splitOffStatus.RaftStatus = nil
	err = tm.saveStoreAndShardStatuses(nil, map[types.ID]*store.ShardStatus{
		splitOffShardID: splitOffStatus,
	})
	require.NoError(t, err)

	assert.False(t, tm.splitOffShardIsReady(parentStatus),
		"Should return false when split-off shard has HasSnapshot=true but no leader")

	// Test case 4: Split-off shard has HasSnapshot=true AND an elected leader - ready
	splitOffStatus.HasSnapshot = true
	splitOffStatus.RaftStatus = &common.RaftStatus{Lead: 1}
	splitOffStatus.SplitCutoverReady = true
	err = tm.saveStoreAndShardStatuses(nil, map[types.ID]*store.ShardStatus{
		splitOffShardID: splitOffStatus,
	})
	require.NoError(t, err)

	assert.True(t, tm.splitOffShardIsReady(parentStatus),
		"Should return true when split-off shard has HasSnapshot=true and a leader")

	// Test case 5: Split-off shard is in Default state (already fully ready)
	splitOffStatus.State = store.ShardState_Default
	splitOffStatus.HasSnapshot = true
	splitOffStatus.SplitCutoverReady = false
	err = tm.saveStoreAndShardStatuses(nil, map[types.ID]*store.ShardStatus{
		splitOffShardID: splitOffStatus,
	})
	require.NoError(t, err)

	assert.False(t, tm.splitOffShardIsReady(parentStatus),
		"Should return false when split-off shard is in Default state but not cutover-ready")

	splitOffStatus.SplitCutoverReady = true
	err = tm.saveStoreAndShardStatuses(nil, map[types.ID]*store.ShardStatus{
		splitOffShardID: splitOffStatus,
	})
	require.NoError(t, err)

	assert.True(t, tm.splitOffShardIsReady(parentStatus),
		"Should return true when split-off shard is in Default state and cutover-ready")

	// Test case 6: Split-off shard in SplitOffPreSnap with HasSnapshot=false
	splitOffStatus.State = store.ShardState_SplitOffPreSnap
	splitOffStatus.HasSnapshot = false
	splitOffStatus.RaftStatus = nil
	err = tm.saveStoreAndShardStatuses(nil, map[types.ID]*store.ShardStatus{
		splitOffShardID: splitOffStatus,
	})
	require.NoError(t, err)

	assert.False(t, tm.splitOffShardIsReady(parentStatus),
		"Should return false when split-off shard is in SplitOffPreSnap with HasSnapshot=false")

	// Test case 7: Split-off shard in SplitOffPreSnap with HasSnapshot=true but Lead=0 - not ready
	// (leader is required for the shard to actually serve reads)
	splitOffStatus.HasSnapshot = true
	splitOffStatus.Initializing = false
	splitOffStatus.RaftStatus = &common.RaftStatus{Lead: 0}
	splitOffStatus.SplitCutoverReady = false
	err = tm.saveStoreAndShardStatuses(nil, map[types.ID]*store.ShardStatus{
		splitOffShardID: splitOffStatus,
	})
	require.NoError(t, err)

	assert.False(t, tm.splitOffShardIsReady(parentStatus),
		"Should return false when split-off shard has HasSnapshot=true but Lead=0")

	// Test case 8: Split-off shard has leader but is not cutover-ready yet
	splitOffStatus.RaftStatus = &common.RaftStatus{Lead: 1}
	splitOffStatus.SplitCutoverReady = false
	err = tm.saveStoreAndShardStatuses(nil, map[types.ID]*store.ShardStatus{
		splitOffShardID: splitOffStatus,
	})
	require.NoError(t, err)

	assert.False(t, tm.splitOffShardIsReady(parentStatus),
		"Should return false when split-off shard has leader but is not cutover-ready")

	// Test case 9: Parent shard has empty splitKey (not a real split parent)
	parentWithNoSplitKey := &store.ShardStatus{
		ID:    types.ID(99),
		Table: tableName,
		State: store.ShardState_Splitting,
		ShardInfo: store.ShardInfo{
			ShardConfig: store.ShardConfig{
				ByteRange: [2][]byte{{0x00}, {}}, // Empty end key
			},
		},
	}

	assert.True(t, tm.splitOffShardIsReady(parentWithNoSplitKey),
		"Should return true when parent has empty split key (not a real split)")
}
