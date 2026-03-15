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
	"cmp"
	"maps"
	"slices"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/tablemgr"
)

// ShardTransitionPlan represents a plan for transitioning shards between states
type ShardTransitionPlan struct {
	Transitions []tablemgr.ShardTransition
	Starts      []tablemgr.ShardTransition
	Stops       []tablemgr.ShardTransition
}

// IdealShardAssignmentsV2 computes the ideal shard assignments based on current state
func IdealShardAssignmentsV2(
	replicationFactor uint64,
	nodes []types.ID,
	desiredShards map[types.ID]*store.ShardStatus,
	currentShards map[types.ID]*store.ShardInfo,
) map[types.ID]*store.ShardInfo {
	ideal := make(map[types.ID]*store.ShardInfo)
	nodeLoad := make(map[types.ID]int) // Stores number of peer slots taken on each node

	// Sort nodes by ID for deterministic behavior
	slices.Sort(nodes)

	// Get a sorted list of shard IDs for deterministic processing
	sortedShardIDs := slices.Collect(maps.Keys(desiredShards))
	slices.Sort(sortedShardIDs)

	// Phase 1: Initialize ShardInfo in ideal map and handle splitting shards
	for _, shardID := range sortedShardIDs {
		desiredShardStatus := desiredShards[shardID]

		// Use ShardConfig from desiredShards.Info. This is a struct copy.
		// Maps/slices within ShardConfig (like ByteRange, Indexes) are shared.
		ideal[shardID] = &store.ShardInfo{
			ShardConfig: desiredShardStatus.ShardConfig,
		}

		// For transitioning shards, try to get peers from desiredShardStatus.Info first
		if desiredShardStatus.State.Transitioning() {
			if desiredShardStatus.Peers != nil {
				ideal[shardID].Peers = desiredShardStatus.Peers.Copy()
			} else if curShardInfo, ok := currentShards[shardID]; ok && curShardInfo != nil {
				// If peers for transitioning shard weren't in desiredShardStatus.Info, try currentShards
				ideal[shardID].Peers = curShardInfo.Peers.Copy()
			}
			for peerID := range ideal[shardID].Peers {
				nodeLoad[peerID]++
			}
		}
	}

	// Phase 2: Assign peers for non-transitioning shards
	for _, shardID := range sortedShardIDs {
		desiredShardStatus := desiredShards[shardID]
		if desiredShardStatus.State.Transitioning() {
			continue // Already handled in Phase 1
		}

		// Get current peers for this shard, if any, to help with tie-breaking for minimal disruption.
		var currentPeersOfThisShard common.PeerSet
		if curShardInfo, ok := currentShards[shardID]; ok && curShardInfo != nil {
			currentPeersOfThisShard = curShardInfo.Peers
		} else {
			currentPeersOfThisShard = common.NewPeerSet()
		}

		// Don't overreplicate the shard
		if uint64(len(currentPeersOfThisShard)) > replicationFactor {
			candidateNodes := currentPeersOfThisShard.IDSlice()
			slices.SortFunc(candidateNodes, func(a, b types.ID) int {
				return cmp.Compare(nodeLoad[a], nodeLoad[b]) // Prefer lower load
			})
			candidateNodes = candidateNodes[:replicationFactor]
			ideal[shardID].Peers = common.NewPeerSet(candidateNodes...)
			continue
		}
		ideal[shardID].Peers = common.NewPeerSet() // Start with empty peers

		// Prepare a list of all active nodes to consider for this shard.
		candidateNodes := slices.Clone(nodes)

		// Sort candidateNodes to select the best peers:
		// 1. Primary sort by current global nodeLoad (ascending - prefer less loaded).
		// 2. Secondary sort by whether the node is an existing peer for this shard
		//    (prefer current peers if loads are equal - for minimal disruption).
		// 3. Tertiary sort by node ID (ascending - for deterministic tie-breaking).
		slices.SortFunc(candidateNodes, func(a, b types.ID) int {
			loadA := nodeLoad[a]
			loadB := nodeLoad[b]
			if loadA != loadB {
				return cmp.Compare(loadA, loadB) // Prefer lower load
			}

			// Loads are equal, consider if nodes are current peers for this shard.
			aIsCurrentPeer := currentPeersOfThisShard.Contains(a)
			bIsCurrentPeer := currentPeersOfThisShard.Contains(b)
			if aIsCurrentPeer != bIsCurrentPeer {
				if aIsCurrentPeer { // 'a' is current, 'b' is not; prefer 'a'
					return -1
				}
				return 1 // 'b' is current, 'a' is not; prefer 'b'
			}

			// Both are current peers or both are not; tie-break by node ID.
			return cmp.Compare(a, b)
		})

		// Assign the top 'replicationFactor' nodes from the sorted list.
		// ideal[shardID].Peers is already initialized as an empty PeerSet in Phase 1.
		count := uint64(0)
		for _, nodeID := range candidateNodes {
			if count >= replicationFactor {
				break
			}
			ideal[shardID].Peers.Add(nodeID)
			count++
		}

		// After all peers for *this* shard are finalized, update nodeLoad.
		// This ensures that the load calculation for the *next* shard considers assignments for *this* shard.
		for peerAssignedToThisShard := range ideal[shardID].Peers {
			nodeLoad[peerAssignedToThisShard]++
		}
	} // End loop for non-splitting shards
	return ideal
}

// CreateShardTransitionPlan creates a plan for transitioning shards from current to ideal state
func CreateShardTransitionPlan(
	currentShards, idealShards map[types.ID]*store.ShardInfo,
) *ShardTransitionPlan {
	plan := &ShardTransitionPlan{
		Transitions: []tablemgr.ShardTransition{},
		Starts:      []tablemgr.ShardTransition{},
		Stops:       []tablemgr.ShardTransition{},
	}

	// Sort shard IDs for deterministic ordering
	for shardID := range idealShards {
		if shard, ok := currentShards[shardID]; !ok ||
			(!shard.Initializing && len(shard.Peers) == 0) {
			plan.Starts = append(plan.Starts, tablemgr.ShardTransition{
				ShardID:  shardID,
				AddPeers: idealShards[shardID].Peers.IDSlice(),
			})
		}
	}
	slices.SortFunc(plan.Starts, func(a, b tablemgr.ShardTransition) int {
		return cmp.Compare(a.ShardID, b.ShardID)
	})

	// Sort shard IDs for deterministic ordering
	shardIDs := make([]types.ID, 0, len(currentShards))
	for shardID := range currentShards {
		shardIDs = append(shardIDs, shardID)
	}
	slices.Sort(shardIDs)

	for _, shardID := range shardIDs {
		currentShard := currentShards[shardID]
		idealShard, ok := idealShards[shardID]
		if !ok {
			// ln.logger.Info("Shard not found in ideal shards, removing", zap.Stringer("shardID", shardID))
			plan.Stops = append(plan.Stops, tablemgr.ShardTransition{
				ShardID:     shardID,
				RemovePeers: currentShard.Peers.IDSlice(),
			})
			// FIXME (ajr) Add a state for merges of the shard so this is atomic and handled within split/merge execution
			// for _, peer := range status.Info.Peers {
			// 	if err := ln.stopShardOnNode(transition.MergeShardID, peer); err != nil {
			// 		ln.logger.Warningf("Failed to stop shard %s on node %s: %v", transition.MergeShardID, peerID, err)
			// 		continue
			// 	}
			// }
			continue
		}

		// Convert current and ideal peers to sets for easy comparison.
		// Use ReportedBy (nodes that actually reported having the shard) instead of
		// Peers (which includes nodes from Raft voter config that may not have the shard).
		// This ensures we detect shards missing on nodes even if Raft thinks they're voters.
		currentReportedBySet := make(map[types.ID]bool)
		for peer := range currentShard.ReportedBy {
			currentReportedBySet[peer] = true
		}

		// Also track current Voters to avoid re-adding peers that have a pending
		// ConfChange but haven't heartbeated yet (not in ReportedBy).
		currentVotersSet := make(map[types.ID]bool)
		if currentShard.RaftStatus != nil {
			for voter := range currentShard.RaftStatus.Voters {
				currentVotersSet[voter] = true
			}
		}

		idealPeersSet := make(map[types.ID]bool)
		for peer := range idealShard.Peers {
			idealPeersSet[peer] = true
		}

		// Find differences
		var addPeers, removePeers []types.ID

		// Add peers that are in ideal but not in ReportedBy AND not already a Voter.
		// A peer in Voters but not in ReportedBy means a ConfChange was recently
		// committed and the node is starting up - we should not re-add it.
		for peer := range idealPeersSet {
			if !currentReportedBySet[peer] && !currentVotersSet[peer] {
				addPeers = append(addPeers, peer)
			}
		}

		// Remove peers that reported having the shard but shouldn't (not in ideal)
		for peer := range currentReportedBySet {
			if !idealPeersSet[peer] {
				removePeers = append(removePeers, peer)
			}
		}

		// Sort for determinism
		slices.Sort(addPeers)
		slices.Sort(removePeers)

		// For this shard, we'll only make one change in this plan
		// We prioritize additions over removals
		if len(addPeers) > 0 {
			plan.Transitions = append(plan.Transitions, tablemgr.ShardTransition{
				ShardID:     shardID,
				AddPeers:    []types.ID{addPeers[0]},
				RemovePeers: nil,
			})
		} else if len(removePeers) > 0 {
			plan.Transitions = append(plan.Transitions, tablemgr.ShardTransition{
				ShardID:     shardID,
				AddPeers:    nil,
				RemovePeers: []types.ID{removePeers[0]},
			})
		}
	}

	return plan
}
