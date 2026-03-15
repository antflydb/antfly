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
	"fmt"
	"slices"
	"testing"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Helper Functions
// ============================================================================

// makeNodeIDs creates a PeerSet from node IDs
func makeNodeIDs(ids ...uint64) common.PeerSet {
	nodeIDs := make(common.PeerSet, len(ids))
	for _, id := range ids {
		nodeIDs.Add(types.ID(id))
	}
	return nodeIDs
}

// makeShardInfos creates ShardInfo map from assignments
func makeShardInfos(assignments map[uint64][]uint64) map[types.ID]*store.ShardInfo {
	infos := make(map[types.ID]*store.ShardInfo)
	shardIndex := 0
	for shardIDVal, peerIDsVal := range assignments {
		shardID := types.ID(shardIDVal)
		peerIDs := make(common.PeerSet, len(peerIDsVal))
		for _, peerIDVal := range peerIDsVal {
			peerIDs.Add(types.ID(peerIDVal))
		}
		infos[shardID] = &store.ShardInfo{
			ShardConfig: store.ShardConfig{
				ByteRange: [2][]byte{{byte(shardIndex)}, {byte(shardIndex + 1)}},
			},
			Peers:      peerIDs,
			ReportedBy: peerIDs.Copy(), // Nodes that actually reported having the shard
		}
		shardIndex++
	}
	return infos
}

// makeNodeIDSlice creates a slice of node IDs
func makeNodeIDSlice(count int, startID uint64) []types.ID {
	nodes := make([]types.ID, count)
	for i := range count {
		nodes[i] = types.ID(startID + uint64(i))
	}
	return nodes
}

// makeV2ShardStatus creates a ShardStatus for testing
func makeV2ShardStatus(id types.ID, state store.ShardState, peers ...uint64) *store.ShardStatus {
	ps := common.NewPeerSet()
	for _, p := range peers {
		ps.Add(types.ID(p))
	}
	byteRangeStart := fmt.Appendf(nil, "shard%d_start", id)
	byteRangeEnd := fmt.Appendf(nil, "shard%d_end", id)

	return &store.ShardStatus{
		ID:    id,
		State: state,
		ShardInfo: store.ShardInfo{
			Peers: ps,
			ShardConfig: store.ShardConfig{
				ByteRange: types.Range{byteRangeStart, byteRangeEnd},
			},
		},
	}
}

// makeV2CurrentShardInfo creates a ShardInfo for testing
func makeV2CurrentShardInfo(id types.ID, peers ...uint64) *store.ShardInfo {
	ps := common.NewPeerSet()
	for _, p := range peers {
		ps.Add(types.ID(p))
	}
	byteRangeStart := fmt.Appendf(nil, "shard%d_start", id)
	byteRangeEnd := fmt.Appendf(nil, "shard%d_end", id)
	return &store.ShardInfo{
		Peers: ps,
		ShardConfig: store.ShardConfig{
			ByteRange: types.Range{byteRangeStart, byteRangeEnd},
		},
	}
}

func TestIdealShardAssignmentsV2(t *testing.T) {
	assert := assert.New(t)

	nodes123 := makeNodeIDSlice(3, 1)     // Nodes 1, 2, 3
	nodes1234 := makeNodeIDSlice(4, 1)    // Nodes 1, 2, 3, 4
	nodes12 := makeNodeIDSlice(2, 1)      // Nodes 1, 2
	nodesNoNodes := makeNodeIDSlice(0, 1) // No nodes

	tests := []struct {
		name           string
		nodes          []types.ID
		desiredShards  map[types.ID]*store.ShardStatus
		currentShards  map[types.ID]*store.ShardInfo
		expectedCounts map[types.ID]int        // Expected number of shards per node
		expectedPeers  map[types.ID][]types.ID // Expected specific peers for some shards (sorted)
	}{
		{
			name:  "initial assignment - 3 nodes, 3 shards",
			nodes: nodes123,
			desiredShards: map[types.ID]*store.ShardStatus{
				101: makeV2ShardStatus(101, store.ShardState_Default),
				102: makeV2ShardStatus(102, store.ShardState_Default),
				103: makeV2ShardStatus(103, store.ShardState_Default),
			},
			currentShards: make(map[types.ID]*store.ShardInfo),
			expectedCounts: map[types.ID]int{
				1: 3, 2: 3, 3: 3,
			},
		},
		{
			name:  "initial assignment - 3 nodes, 1 shard",
			nodes: nodes123,
			desiredShards: map[types.ID]*store.ShardStatus{
				101: makeV2ShardStatus(101, store.ShardState_Default),
			},
			currentShards: make(map[types.ID]*store.ShardInfo),
			expectedCounts: map[types.ID]int{
				1: 1, 2: 1, 3: 1,
			},
			expectedPeers: map[types.ID][]types.ID{
				101: {1, 2, 3},
			},
		},
		{
			name:  "minimal disruption - already balanced",
			nodes: nodes123,
			desiredShards: map[types.ID]*store.ShardStatus{
				101: makeV2ShardStatus(
					101,
					store.ShardState_Default,
					1,
					2,
					3,
				), // Desired uses its peers for ByteRange
				102: makeV2ShardStatus(102, store.ShardState_Default, 1, 2, 3),
			},
			currentShards: map[types.ID]*store.ShardInfo{
				101: makeV2CurrentShardInfo(101, 1, 2, 3),
				102: makeV2CurrentShardInfo(102, 1, 2, 3),
			},
			expectedCounts: map[types.ID]int{1: 2, 2: 2, 3: 2},
			expectedPeers: map[types.ID][]types.ID{
				101: {1, 2, 3},
				102: {1, 2, 3},
			},
		},
		{
			name:  "rebalance overloaded node - 3 nodes, 2 shards, node 1 has one peer of each",
			nodes: nodes123, // 1, 2, 3
			desiredShards: map[types.ID]*store.ShardStatus{
				101: makeV2ShardStatus(101, store.ShardState_Default),
				102: makeV2ShardStatus(102, store.ShardState_Default),
			},
			currentShards: map[types.ID]*store.ShardInfo{
				101: makeV2CurrentShardInfo(101, 1),    // Currently only on node 1
				102: makeV2CurrentShardInfo(102, 1, 2), // Currently on node 1, 2
			},
			// Shard 101: current [1]. Add [2,3]. Ideal: [1,2,3]. Node loads: N1:1, N2:1, N3:1
			// Shard 102: current [1,2]. Add [3]. Ideal: [1,2,3]. Node loads: N1:2, N2:2, N3:2
			expectedCounts: map[types.ID]int{1: 2, 2: 2, 3: 2},
		},
		{
			name:  "handle splitting shard - preserve peers",
			nodes: nodes123,
			desiredShards: map[types.ID]*store.ShardStatus{
				101: makeV2ShardStatus(
					101,
					store.ShardState_SplittingOff,
					1,
					2,
				), // Splitting, current peers 1,2
				102: makeV2ShardStatus(102, store.ShardState_Default),
			},
			currentShards: map[types.ID]*store.ShardInfo{
				101: makeV2CurrentShardInfo(101, 1, 2), // Current actual peers
				102: makeV2CurrentShardInfo(102, 1, 3), // Arbitrary current for non-splitting
			},
			// Shard 101 (splitting): peers [1,2] preserved. Loads: N1:1, N2:1, N3:0
			// Shard 102 (running): current [1,3]. Add [2]. Ideal [1,3,2]. Loads: N1:2, N2:2, N3:1
			expectedPeers: map[types.ID][]types.ID{
				101: {1, 2}, // Must be preserved
			},
			expectedCounts: map[types.ID]int{1: 2, 2: 2, 3: 1},
		},
		{
			name:  "add new node - rebalance from 3 to 4 nodes",
			nodes: nodes1234, // 1,2,3,4
			desiredShards: map[types.ID]*store.ShardStatus{
				201: makeV2ShardStatus(201, store.ShardState_Default),
				202: makeV2ShardStatus(202, store.ShardState_Default),
				203: makeV2ShardStatus(203, store.ShardState_Default),
				204: makeV2ShardStatus(204, store.ShardState_Default),
			},
			currentShards: map[types.ID]*store.ShardInfo{ // All currently on 1,2,3
				201: makeV2CurrentShardInfo(201, 1, 2, 3),
				202: makeV2CurrentShardInfo(202, 1, 2, 3),
				203: makeV2CurrentShardInfo(203, 1, 2, 3),
				204: makeV2CurrentShardInfo(204, 1, 2, 3),
			},
			// 4 shards, 4 nodes, replication 3. Total 12 slots. Each node gets 12/4 = 3 shards.
			expectedCounts: map[types.ID]int{1: 3, 2: 3, 3: 3, 4: 3},
		},
		{
			name:  "node removal - rebalance from 3 to 2 nodes (node 3 removed)",
			nodes: nodes12, // Nodes 1, 2. Node 3 is gone from 'nodes' list.
			desiredShards: map[types.ID]*store.ShardStatus{
				101: makeV2ShardStatus(101, store.ShardState_Default),
				102: makeV2ShardStatus(102, store.ShardState_Default),
			},
			currentShards: map[types.ID]*store.ShardInfo{
				101: makeV2CurrentShardInfo(
					101,
					1,
					2,
					3,
				), // Peer 3 is invalid as it's not in 'nodes'
				102: makeV2CurrentShardInfo(102, 1, 3), // Peer 3 is invalid
			},
			// Shard 101: current valid [1,2]. Needs 1 more. Nodes are [1,2]. Picks [1,2] (effectively picks one to fill the third slot if possible, but cannot).
			// Shard 102: current valid [1]. Needs 2 more. Adds [2].
			// Result: 101 on {1,2}, 102 on {1,2}.
			expectedCounts: map[types.ID]int{1: 2, 2: 2},
			expectedPeers: map[types.ID][]types.ID{
				101: {1, 2}, // Tries for 3, but only 2 nodes available
				102: {1, 2},
			},
		},
		{
			name:  "insufficient nodes - 2 nodes, 3 shards (replication 3)",
			nodes: nodes12, // 1, 2
			desiredShards: map[types.ID]*store.ShardStatus{
				101: makeV2ShardStatus(101, store.ShardState_Default),
				102: makeV2ShardStatus(102, store.ShardState_Default),
				103: makeV2ShardStatus(103, store.ShardState_Default),
			},
			currentShards:  make(map[types.ID]*store.ShardInfo), // No current assignments
			expectedCounts: map[types.ID]int{1: 3, 2: 3},
			expectedPeers: map[types.ID][]types.ID{
				101: {1, 2},
				102: {1, 2},
				103: {1, 2},
			},
		},
		{
			name:  "no nodes available",
			nodes: nodesNoNodes,
			desiredShards: map[types.ID]*store.ShardStatus{
				101: makeV2ShardStatus(101, store.ShardState_Default),
			},
			currentShards:  make(map[types.ID]*store.ShardInfo),
			expectedCounts: map[types.ID]int{},
			expectedPeers: map[types.ID][]types.ID{
				101: {},
			},
		},
		{
			name:  "complex scenario - mix of states and loads",
			nodes: nodes1234, // 1,2,3,4
			desiredShards: map[types.ID]*store.ShardStatus{
				301: makeV2ShardStatus(301, store.ShardState_Default),
				302: makeV2ShardStatus(302, store.ShardState_Default),
				303: makeV2ShardStatus(303, store.ShardState_Default),
				304: makeV2ShardStatus(
					304,
					store.ShardState_SplittingOff,
					2,
					3,
				), // Desired for splitting implies these peers
				305: makeV2ShardStatus(305, store.ShardState_Default),
			},
			currentShards: map[types.ID]*store.ShardInfo{
				302: makeV2CurrentShardInfo(302, 1, 2, 3), // Balanced on 1,2,3
				303: makeV2CurrentShardInfo(
					303,
					1,
					1,
					1,
				), // Overloaded on 1 (currentShards has 1 as only peer)
				304: makeV2CurrentShardInfo(304, 2, 3), // Actual current peers for splitting
			},
			// Shard 304 (Splitting): Peers [2,3] preserved from desiredShards.Peers. Node loads: N2:1, N3:1. Others:0.
			// Shard 301 (New): No current. Nodes by load: N1(0),N4(0),N2(1),N3(1). Assigns [1,4,2]. Node loads: N1:1, N4:1, N2:2, N3:1
			// Shard 302 (Existing, Balanced): Current [1,2,3]. Valid. Nodes by load (current): N1(1),N4(1),N3(1),N2(2). Prefer [1,3,2]. Node loads: N1:2, N4:1, N2:3, N3:2
			// Shard 303 (Existing, Needs Rebalance): Current [1]. Valid. Nodes by load (current): N4(1),N1(2),N3(2),N2(3). Prefer [1]. Add [4,3]. Peers for 303: [1,4,3]. Node loads: N1:3, N4:2, N2:3, N3:3
			// Shard 305 (New): No current. Nodes by load: N4(2),N1(3),N2(3),N3(3). Assigns [4,1,2]. Node loads: N1:4, N4:3, N2:4, N3:3
			expectedCounts: map[types.ID]int{1: 4, 2: 4, 3: 3, 4: 3},
			expectedPeers: map[types.ID][]types.ID{
				304: {2, 3}, // Splitting shard must preserve its peers
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IdealShardAssignmentsV2(3, tt.nodes, tt.desiredShards, tt.currentShards)

			// Verify shard config (ByteRange) is preserved from desiredShards.Info
			for shardID, desiredStatus := range tt.desiredShards {
				assignedInfo, ok := result[shardID]
				assert.True(ok, "shard %d not found in result", shardID)
				if desiredStatus != nil {
					assert.Equal(desiredStatus.ByteRange, assignedInfo.ByteRange,
						"ByteRange mismatch for shard %d", shardID)
				}
			}

			nodeCounts := make(map[types.ID]int)
			for _, shardInfo := range result {
				for peerID := range shardInfo.Peers {
					nodeCounts[peerID]++
				}
			}

			if tt.expectedCounts != nil {
				assert.Equal(tt.expectedCounts, nodeCounts, "Node load counts mismatch")
			}

			if tt.expectedPeers != nil {
				for shardID, expectedPeersForShard := range tt.expectedPeers {
					assignedShardInfo, ok := result[shardID]
					assert.True(ok, "Shard %d expected but not found in result", shardID)
					if ok {
						assignedPeersSlice := assignedShardInfo.Peers.IDSlice() // Already sorted
						// expectedPeersForShard is already sorted if defined in test case
						assert.Equal(types.IDSlice(expectedPeersForShard), assignedPeersSlice,
							"Peer assignment mismatch for shard %d", shardID)
					}
				}
			}

			// General validation for non-splitting shards (should have replicationFactor peers if enough nodes)
			const replicationFactor = 3
			for shardID, shardStatus := range tt.desiredShards {
				if shardStatus.State == store.ShardState_SplittingOff {
					// Splitting shards preserve peers from desiredShards.Info.Peers or currentShards.Peers
					// If specific peers are expected, they are checked by tt.expectedPeers.
					// Otherwise, we check the number of peers.
					if _, hasSpecificExpectation := tt.expectedPeers[shardID]; !hasSpecificExpectation {
						assignedShardInfo := result[shardID]
						expectedNumSplittingPeers := 0
						if shardStatus != nil && len(shardStatus.Peers) > 0 {
							expectedNumSplittingPeers = len(shardStatus.Peers)
						} else if curInfo, ok := tt.currentShards[shardID]; ok && curInfo != nil && len(curInfo.Peers) > 0 {
							expectedNumSplittingPeers = len(curInfo.Peers)
						}
						// If still 0, it means no peers were defined in desired/current for splitting
						assert.Len(
							assignedShardInfo.Peers,
							expectedNumSplittingPeers,
							"Incorrect number of peers for splitting shard %d (initial peers: %d)",
							shardID,
							expectedNumSplittingPeers,
						)
					}
					continue
				}

				// Non-splitting shards:
				assignedShardInfo := result[shardID]
				expectedNumPeers := min(len(tt.nodes), replicationFactor)
				if len(tt.nodes) == 0 {
					expectedNumPeers = 0
				}
				assert.Len(
					assignedShardInfo.Peers,
					expectedNumPeers,
					"Incorrect number of peers for non-splitting shard %d (available_nodes: %d)",
					shardID,
					len(tt.nodes),
				)
			}
		})
	}
}

func TestCreateShardTransitionPlan_NoChanges(t *testing.T) {
	assert := assert.New(t)

	currentShards := makeShardInfos(map[uint64][]uint64{
		1: {101, 102, 103},
		2: {101, 102, 103},
	})
	idealShards := makeShardInfos(map[uint64][]uint64{
		1: {101, 102, 103},
		2: {101, 102, 103},
	})

	plan := CreateShardTransitionPlan(currentShards, idealShards)

	assert.Empty(plan.Starts, "Expected 0 starts")
	assert.Empty(plan.Transitions, "Expected 0 transitions")
}

func TestCreateShardTransitionPlan_AddNewShard(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	currentShards := makeShardInfos(map[uint64][]uint64{
		1: {101, 102, 103},
	})
	idealShards := makeShardInfos(map[uint64][]uint64{
		1: {101, 102, 103},
		2: {101, 102, 103}, // New shard 2
	})

	plan := CreateShardTransitionPlan(currentShards, idealShards)

	assert.Empty(plan.Transitions, "Expected 0 transitions")
	require.Len(plan.Starts, 1, "Expected 1 start")

	expectedStart := tablemgr.ShardTransition{
		ShardID:  types.ID(2),
		AddPeers: makeNodeIDs(101, 102, 103).IDSlice(),
	}
	// Sort peers for comparison
	// Sort peers for comparison to ensure order doesn't affect equality check
	slices.Sort(plan.Starts[0].AddPeers)
	slices.Sort(expectedStart.AddPeers)

	assert.Equal(expectedStart, plan.Starts[0], "Start plan mismatch")
}

func TestCreateShardTransitionPlan_AddPeer(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	currentShards := makeShardInfos(map[uint64][]uint64{
		1: {101, 102}, // Missing 103
		2: {101, 102, 103},
	})
	idealShards := makeShardInfos(map[uint64][]uint64{
		1: {101, 102, 103}, // Add 103
		2: {101, 102, 103},
	})

	plan := CreateShardTransitionPlan(currentShards, idealShards)

	assert.Empty(plan.Starts, "Expected 0 starts")
	require.Len(plan.Transitions, 1, "Expected 1 transition")

	expectedTransition := tablemgr.ShardTransition{
		ShardID:     types.ID(1),
		AddPeers:    makeNodeIDs(103).IDSlice(),
		RemovePeers: nil,
	}

	// Sort peers for comparison
	// Sort peers for comparison
	slices.Sort(plan.Transitions[0].AddPeers)
	slices.Sort(expectedTransition.AddPeers)

	assert.Equal(expectedTransition, plan.Transitions[0], "Transition plan mismatch")
}

func TestCreateShardTransitionPlan_RemovePeer(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	currentShards := makeShardInfos(map[uint64][]uint64{
		1: {101, 102, 103, 104}, // Extra peer 104
		2: {101, 102, 103},
	})
	idealShards := makeShardInfos(map[uint64][]uint64{
		1: {101, 102, 103}, // Remove 104
		2: {101, 102, 103},
	})

	plan := CreateShardTransitionPlan(currentShards, idealShards)

	assert.Empty(plan.Starts, "Expected 0 starts")
	require.Len(plan.Transitions, 1, "Expected 1 transition")

	expectedTransition := tablemgr.ShardTransition{
		ShardID:     types.ID(1),
		AddPeers:    nil,
		RemovePeers: makeNodeIDs(104).IDSlice(),
	}

	// Sort peers for comparison
	// Sort peers for comparison
	slices.Sort(plan.Transitions[0].RemovePeers)
	slices.Sort(expectedTransition.RemovePeers)

	assert.Equal(expectedTransition, plan.Transitions[0], "Transition plan mismatch")
}

func TestCreateShardTransitionPlan_AddAndRemovePeer(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	// Plan should prioritize Add
	currentShards := makeShardInfos(map[uint64][]uint64{
		1: {101, 102, 104}, // Missing 103, Extra 104
		2: {101, 102, 103},
	})
	idealShards := makeShardInfos(map[uint64][]uint64{
		1: {101, 102, 103}, // Add 103, Remove 104
		2: {101, 102, 103},
	})

	plan := CreateShardTransitionPlan(currentShards, idealShards)

	assert.Empty(plan.Starts)
	require.Lenf(plan.Transitions, 1, "Expected 1 transition: got %v", plan.Transitions)

	// Expecting only the addition in this plan (add gets priority)
	expectedTransition := tablemgr.ShardTransition{
		ShardID:     types.ID(1),
		AddPeers:    makeNodeIDs(103).IDSlice(),
		RemovePeers: nil,
	}

	// Sort peers for comparison
	// Sort peers for comparison
	slices.Sort(plan.Transitions[0].AddPeers)
	slices.Sort(expectedTransition.AddPeers)

	assert.Equal(
		expectedTransition,
		plan.Transitions[0],
		"Transition plan mismatch (should prioritize add)",
	)
}

func TestCreateShardTransitionPlan_MultipleChanges(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	// Plan should create one transition per shard with a change, prioritizing adds
	currentShards := makeShardInfos(map[uint64][]uint64{
		1: {101, 102},      // Missing 103
		2: {101, 102, 104}, // Extra 104
		3: {101, 102, 103}, // No change
	})
	idealShards := makeShardInfos(map[uint64][]uint64{
		1: {101, 102, 103}, // Add 103
		2: {101, 102, 103}, // Add 103, Remove 104 (add gets priority)
		3: {101, 102, 103},
	})

	plan := CreateShardTransitionPlan(currentShards, idealShards)

	assert.Empty(plan.Starts, "Expected 0 starts")
	require.Len(plan.Transitions, 2, "Expected 2 transitions")

	// Expecting addition for shard 1 and addition for shard 2 (add gets priority)
	expectedTransitions := []tablemgr.ShardTransition{
		{ // Shard 1: Add 103
			ShardID:     types.ID(1),
			AddPeers:    makeNodeIDs(103).IDSlice(),
			RemovePeers: nil, // Empty slice
		},
		{ // Shard 2: Add 103 (priority over removing 104)
			ShardID:     types.ID(2),
			AddPeers:    makeNodeIDs(103).IDSlice(),
			RemovePeers: nil, // Empty slice
		},
	}

	// Sort actual and expected transitions by ShardID for deterministic comparison
	slices.SortFunc(plan.Transitions, func(a, b tablemgr.ShardTransition) int {
		if a.ShardID < b.ShardID {
			return -1
		}
		if a.ShardID > b.ShardID {
			return 1
		}
		return 0
	})
	slices.SortFunc(expectedTransitions, func(a, b tablemgr.ShardTransition) int {
		if a.ShardID < b.ShardID {
			return -1
		}
		if a.ShardID > b.ShardID {
			return 1
		}
		return 0
	})

	// Sort peers within each transition for comparison
	for i := range plan.Transitions {
		slices.Sort(plan.Transitions[i].AddPeers)
		slices.Sort(plan.Transitions[i].RemovePeers)
		slices.Sort(expectedTransitions[i].AddPeers)
		slices.Sort(expectedTransitions[i].RemovePeers)
		// Ensure nil slices are treated consistently if necessary, though assert.Equal usually handles nil vs empty slice correctly.
		if expectedTransitions[i].AddPeers == nil {
			expectedTransitions[i].AddPeers = []types.ID{}
		}
		if expectedTransitions[i].RemovePeers == nil {
			expectedTransitions[i].RemovePeers = []types.ID{}
		}
		if plan.Transitions[i].AddPeers == nil {
			plan.Transitions[i].AddPeers = []types.ID{}
		}
		if plan.Transitions[i].RemovePeers == nil {
			plan.Transitions[i].RemovePeers = []types.ID{}
		}
	}

	assert.Equal(expectedTransitions, plan.Transitions, "Transition plan mismatch")
}

// TestCreateShardTransitionPlan_NodeInRaftButNotReported tests the scenario where
// a node is listed as a peer in the Raft voter config (Peers) but didn't actually
// report having the shard (not in ReportedBy). This simulates the bug where a shard
// failed to start on a node but the metadata still thinks it should be there.
func TestCreateShardTransitionPlan_NodeInRaftButNotReported(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	// Simulate: metadata says shard 1 should be on nodes 101, 102, 103
	// But only nodes 101 and 102 actually have the shard (node 103 never started it)
	currentShards := map[types.ID]*store.ShardInfo{
		types.ID(1): {
			ShardConfig: store.ShardConfig{
				ByteRange: [2][]byte{{0}, {1}},
			},
			// Peers includes all nodes from Raft voter config (includes 103)
			Peers: makeNodeIDs(101, 102, 103),
			// ReportedBy only includes nodes that actually reported having the shard
			ReportedBy: makeNodeIDs(101, 102), // Node 103 is missing!
		},
	}
	idealShards := makeShardInfos(map[uint64][]uint64{
		1: {101, 102, 103},
	})

	plan := CreateShardTransitionPlan(currentShards, idealShards)

	// Should detect that node 103 doesn't actually have the shard and needs to start it
	assert.Empty(plan.Starts, "Expected 0 starts (shard already exists)")
	require.Len(plan.Transitions, 1, "Expected 1 transition to add node 103")

	expectedTransition := tablemgr.ShardTransition{
		ShardID:     types.ID(1),
		AddPeers:    makeNodeIDs(103).IDSlice(),
		RemovePeers: nil,
	}

	assert.Equal(expectedTransition.ShardID, plan.Transitions[0].ShardID)
	assert.Equal(expectedTransition.AddPeers, plan.Transitions[0].AddPeers)
}

// TestCreateShardTransitionPlan_NodeInVotersButNotReported tests the scenario where
// a node is listed as a Raft voter (RaftStatus.Voters) but hasn't heartbeated yet
// (not in ReportedBy). This happens when a ConfChange was recently committed but
// the node hasn't sent a heartbeat yet. We should NOT try to re-add it.
func TestCreateShardTransitionPlan_NodeInVotersButNotReported(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	// Simulate: shard 1 has Raft voters {101, 102, 103}
	// But only nodes 101 and 102 have sent heartbeats (node 103 just joined, hasn't heartbeated)
	// The ideal state also wants 101, 102, 103
	// We should NOT try to re-add node 103 since it's already a voter
	currentShards := map[types.ID]*store.ShardInfo{
		types.ID(1): {
			ShardConfig: store.ShardConfig{
				ByteRange: [2][]byte{{0}, {1}},
			},
			// Peers includes all voters
			Peers: makeNodeIDs(101, 102, 103),
			// ReportedBy only includes nodes that actually heartbeated
			ReportedBy: makeNodeIDs(101, 102), // Node 103 hasn't heartbeated yet
			// RaftStatus.Voters includes node 103 (ConfChange was committed)
			RaftStatus: &common.RaftStatus{
				Voters: makeNodeIDs(101, 102, 103),
			},
		},
	}
	idealShards := makeShardInfos(map[uint64][]uint64{
		1: {101, 102, 103},
	})

	plan := CreateShardTransitionPlan(currentShards, idealShards)

	// Should NOT try to add node 103 since it's already in Voters
	assert.Empty(plan.Starts, "Expected 0 starts")
	require.Empty(plan.Transitions, "Expected 0 transitions - node 103 is already a voter")
}
