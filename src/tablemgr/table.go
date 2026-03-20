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
	"errors"
	"fmt"
	"log"
	"maps"
	"net/http"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble/v2"
	"github.com/goccy/go-json"
	"google.golang.org/protobuf/proto"
)

const (
	tablePrefix                = "tm:t:"
	storeStatusPrefix          = "tm:sts:"
	shardStatusPrefix          = "tm:shs:"
	storeTombstonePrefix       = "tm:stb:"
	tableReallocationReqPrefix = "tm:rar:"
)

// TableManager handles operations related to tables including creation and shard assignment
type TableManager struct {
	sync.Mutex

	db kv.DB

	// client is used for making HTTP requests using the store client
	client        *http.Client
	clientFactory StoreClientFactory

	// maxShardSizeBytes is the threshold for triggering shard splits.
	// When a shard's disk size crosses this threshold, we persist the ShardStats
	// so the reconciler can trigger a split.
	maxShardSizeBytes uint64
}

type StoreClientFactory func(client *http.Client, id types.ID, url string) client.StoreRPC

func defaultStoreClientFactory(c *http.Client, id types.ID, url string) client.StoreRPC {
	return client.NewStoreClient(c, id, url)
}

// NewTableManager creates a new TableManager instance
// maxShardSizeBytes is used to determine when to persist ShardStats for split detection
// (pass 0 to disable threshold-based persistence)
func NewTableManager(db kv.DB, httpClient *http.Client, maxShardSizeBytes uint64) (*TableManager, error) {
	tm := &TableManager{
		db:                db,
		client:            httpClient,
		clientFactory:     defaultStoreClientFactory,
		maxShardSizeBytes: maxShardSizeBytes,
	}
	return tm, nil
}

func (tm *TableManager) HttpClient() *http.Client {
	if tm.client == nil {
		tm.client = &http.Client{
			Timeout: 10 * time.Second,
		}
	}
	return tm.client
}

func (tm *TableManager) SetStoreClientFactory(factory StoreClientFactory) {
	tm.Lock()
	defer tm.Unlock()
	if factory == nil {
		tm.clientFactory = defaultStoreClientFactory
		return
	}
	tm.clientFactory = factory
}

func (tm *TableManager) newStoreClient(id types.ID, url string) client.StoreRPC {
	factory := tm.clientFactory
	if factory == nil {
		factory = defaultStoreClientFactory
	}
	return factory(tm.client, id, url)
}

func (tm *TableManager) UpdateStatuses(
	ctx context.Context,
	newStatuses map[types.ID]*StoreStatus,
) error {
	if len(newStatuses) == 0 {
		return nil // No new statuses to set
	}
	tm.Lock()
	defer tm.Unlock()
	currentShards := make(map[types.ID]*store.ShardInfo)
	updatedStores := make(map[types.ID]*StoreStatus)
	for nodeID, status := range newStatuses {
		// FIXME (ajr) If a node disappears, we should remove it from the shard
		//if !status.Healthy {
		//	continue // Skip unhealthy nodes
		//}
		oldStoreStatus, err := tm.GetStoreStatus(ctx, nodeID)
		// If the store status is not found i.e. ErrNotFound, we treat it as a new store
		if errors.Is(err, ErrNotFound) {
			updatedStores[nodeID] = status
		} else if err != nil {
			return fmt.Errorf("getting store status for %s: %w", nodeID, err)
		} else if !oldStoreStatus.Equivalent(status) {
			updatedStores[nodeID] = status
		}

		for shardID := range status.Shards {
			if _, ok := currentShards[shardID]; !ok {
				currentShards[shardID] = store.NewShardInfo()
			}
			// if status.Shards[shardID].Splitting {
			// 	// Debug log to help track down split issues
			// 	log.Printf("Node %s reports shard %s is splitting", nodeID, shardID)
			// }
			currentShards[shardID].Merge(nodeID, status.Shards[shardID])
		}
	}
	updatedShards := make(map[types.ID]*store.ShardStatus, len(currentShards))
	for shardID := range currentShards {
		shardStatus, err := tm.GetShardStatus(shardID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return fmt.Errorf("getting shard status for %s: %w", shardID, err)
		}
		if errors.Is(err, ErrNotFound) {
			log.Printf("shard status on peer but not in metadata: %s", shardID)
			continue
		}
		newStatus, needsUpdate := tm.needsUpdates(shardStatus, currentShards[shardID])
		if needsUpdate {
			updatedShards[shardID] = newStatus
		}
	}
	if err := tm.saveStoreAndShardStatuses(updatedStores, updatedShards); err != nil {
		return fmt.Errorf("saving store and shard statuses: %w", err)
	}
	return nil
}

func (tm *TableManager) needsUpdates(
	oldShardStatus *store.ShardStatus,
	newShardInfo *store.ShardInfo,
) (*store.ShardStatus, bool) {
	if newShardInfo == nil {
		return nil, false
	}
	if len(newShardInfo.Peers) == 0 {
		// If there are no peers, we don't need to update the status
		return nil, false
	}
	// Preserve old ShardStats when the reporting node omits them (nil).
	// Otherwise the previously-known stats would be clobbered on every heartbeat.
	shardStats := newShardInfo.ShardStats
	if shardStats == nil {
		shardStats = oldShardStatus.ShardStats
	}
	newShardStatus := &store.ShardStatus{
		ID:    oldShardStatus.ID,
		Table: oldShardStatus.Table,
		State: oldShardStatus.State,
		ShardInfo: store.ShardInfo{
			ShardConfig:         oldShardStatus.ShardConfig,
			ShardStats:          shardStats,
			Peers:               oldShardStatus.Peers,
			ReportedBy:          oldShardStatus.ReportedBy,
			RaftStatus:          oldShardStatus.RaftStatus,
			HasSnapshot:         oldShardStatus.HasSnapshot,
			Initializing:        oldShardStatus.Initializing,
			SplitReplayRequired: oldShardStatus.SplitReplayRequired,
			SplitReplayCaughtUp: oldShardStatus.SplitReplayCaughtUp,
			SplitCutoverReady:   oldShardStatus.SplitCutoverReady,
			SplitReplaySeq:      oldShardStatus.SplitReplaySeq,
			SplitParentShardID:  oldShardStatus.SplitParentShardID,
		},
	}
	needsPersist := false
	if !oldShardStatus.Peers.Equal(newShardInfo.Peers) {
		// log.Printf("Updating shard %s peers from %s to %s", shardID, status.Info.Peers, newShardInfo.Peers)
		newShardStatus.Peers = newShardInfo.Peers
		needsPersist = true

	}
	if !oldShardStatus.ReportedBy.Equal(newShardInfo.ReportedBy) {
		newShardStatus.ReportedBy = newShardInfo.ReportedBy
		needsPersist = true
	}
	// Note: We intentionally do NOT update ShardConfig from storage node reports.
	// The metadata is the source of truth for ShardConfig (ByteRange, Schema, Indexes).
	// Storage nodes inherit their ShardConfig from metadata, not the other way around.
	// This is critical during shard splits when storage nodes may report stale ByteRanges.
	if oldShardStatus.Initializing != newShardInfo.Initializing {
		newShardStatus.Initializing = newShardInfo.Initializing
		needsPersist = true
	}
	if oldShardStatus.Splitting != newShardInfo.Splitting {
		newShardStatus.Splitting = newShardInfo.Splitting
		needsPersist = true
	}
	if oldShardStatus.SplitReplayRequired != newShardInfo.SplitReplayRequired {
		newShardStatus.SplitReplayRequired = newShardInfo.SplitReplayRequired
		needsPersist = true
	}
	if oldShardStatus.SplitReplayCaughtUp != newShardInfo.SplitReplayCaughtUp {
		newShardStatus.SplitReplayCaughtUp = newShardInfo.SplitReplayCaughtUp
		needsPersist = true
	}
	if oldShardStatus.SplitCutoverReady != newShardInfo.SplitCutoverReady {
		newShardStatus.SplitCutoverReady = newShardInfo.SplitCutoverReady
		needsPersist = true
	}
	if oldShardStatus.SplitReplaySeq != newShardInfo.SplitReplaySeq {
		newShardStatus.SplitReplaySeq = newShardInfo.SplitReplaySeq
		needsPersist = true
	}
	if oldShardStatus.SplitParentShardID != newShardInfo.SplitParentShardID {
		newShardStatus.SplitParentShardID = newShardInfo.SplitParentShardID
		needsPersist = true
	}
	// Copy SplitState from storage node reports so reconciler can see it.
	// SplitState is the Raft-replicated split phase from the shard leader.
	// Preserve the richer version when heartbeats briefly report a partial state
	// during failover, but still persist upgrades within the same phase such as
	// the child shard ID appearing after metadata already recorded PHASE_SPLITTING.
	if oldShardStatus.SplitState != nil {
		newShardStatus.SplitState = proto.Clone(oldShardStatus.SplitState).(*db.SplitState)
	}
	leaderClearedSplitState := oldShardStatus.SplitState != nil &&
		oldShardStatus.SplitState.GetPhase() != db.SplitState_PHASE_NONE &&
		newShardInfo.SplitState == nil &&
		newShardInfo.RaftStatus != nil &&
		newShardInfo.RaftStatus.Lead != 0
	if leaderClearedSplitState {
		newShardStatus.SplitState = nil
		needsPersist = true
	} else if mergedSplitState, splitStateChanged := mergeSplitStates(
		oldShardStatus.SplitState,
		newShardInfo.SplitState,
	); mergedSplitState != nil {
		newShardStatus.SplitState = mergedSplitState
		needsPersist = needsPersist || splitStateChanged
	}
	if newShardInfo.RaftStatus != nil {
		newShardStatus.RaftStatus = newShardInfo.RaftStatus
		if !oldShardStatus.RaftStatus.Equal(newShardInfo.RaftStatus) {
			needsPersist = true
		}
	}
	if newShardInfo.ShardStats != nil {
		newStorage := newShardInfo.ShardStats.Storage
		if oldShardStatus.ShardStats == nil ||
			oldShardStatus.ShardStats.Storage == nil ||
			(newStorage != nil && newStorage.Empty != oldShardStatus.ShardStats.Storage.Empty) ||
			(newStorage != nil && newStorage.DiskSize > 3*oldShardStatus.ShardStats.Storage.DiskSize) {
			needsPersist = true
		}
		// Always persist if disk size is above the split threshold so the reconciler
		// can trigger splits. The reconciler checks ShardStats.Updated and skips shards
		// with stats older than 1 minute, so we need to keep persisting to keep stats fresh.
		if tm.maxShardSizeBytes > 0 && newStorage != nil && newStorage.DiskSize >= tm.maxShardSizeBytes {
			needsPersist = true
		} else if oldShardStatus.ShardStats != nil && time.Since(oldShardStatus.ShardStats.Updated) > 30*time.Second {
			// Only force a refresh for shards approaching the split threshold.
			// The reconciler skips shards with stats older than 1 minute, but that
			// only matters for split decisions which require fresh size data.
			if tm.maxShardSizeBytes > 0 && newStorage != nil && newStorage.DiskSize >= tm.maxShardSizeBytes/2 {
				needsPersist = true
			}
		}
		if oldShardStatus.ShardStats == nil {
			needsPersist = true
		} else if len(newShardInfo.ShardStats.Indexes) != len(oldShardStatus.ShardStats.Indexes) {
			needsPersist = true
		} else if len(newShardInfo.ShardStats.Indexes) > 0 {
			indexStatsChanged := false
			for idxName, stats := range newShardInfo.ShardStats.Indexes {
				oldStats, exists := oldShardStatus.ShardStats.Indexes[idxName]
				if !exists {
					needsPersist = true
					break
				}
				if strings.HasPrefix(idxName, "full_text_index") {
					bs, _ := stats.AsFullTextIndexStats()
					os, _ := oldStats.AsFullTextIndexStats()
					if !bs.Equal(os) {
						indexStatsChanged = true
						if bs.Rebuilding != os.Rebuilding || bs.Error != os.Error ||
							diffExceedsPercent(bs.TotalIndexed, os.TotalIndexed, 10) ||
							diffExceedsPercent(bs.DiskUsage, os.DiskUsage, 10) {
							needsPersist = true
							break
						}
					}
				} else {
					bs, _ := stats.AsEmbeddingsIndexStats()
					os, _ := oldStats.AsEmbeddingsIndexStats()
					if !bs.Equal(os) {
						indexStatsChanged = true
						if bs.Error != os.Error ||
							diffExceedsPercent(bs.TotalIndexed, os.TotalIndexed, 10) ||
							diffExceedsPercent(bs.TotalNodes, os.TotalNodes, 10) {
							needsPersist = true
							break
						}
					}
				}
			}
			// Time-based fallback: persist minor index stats changes every 30s
			// so stats don't go completely stale.
			if !needsPersist && indexStatsChanged &&
				oldShardStatus.ShardStats != nil &&
				time.Since(oldShardStatus.ShardStats.Updated) > 30*time.Second {
				needsPersist = true
			}
		}
	}
	if newShardInfo.HasSnapshot != oldShardStatus.HasSnapshot {
		needsPersist = true
		newShardStatus.HasSnapshot = newShardInfo.HasSnapshot
	}
	// When persisting, ensure ShardStats.Updated is set to current time
	// so the reconciler knows the stats are fresh (it skips shards with stats older than 1 minute)
	if needsPersist && newShardStatus.ShardStats != nil {
		newShardStatus.ShardStats.Updated = time.Now()
	}
	if newShardInfo.HasSnapshot && oldShardStatus.RestoreConfig != nil {
		// If the shard has a snapshot, we can clear the restore config
		newShardStatus.RestoreConfig = nil
	}
	switch oldShardStatus.State {
	case store.ShardState_Initializing:
		if !newShardInfo.Initializing {
			log.Printf("SPLIT_STATE: Shard %s transitioning Initializing -> Default", oldShardStatus.ID)
			newShardStatus.State = store.ShardState_Default
			needsPersist = true
		}
	case store.ShardState_PreMerge:
		if newShardInfo.ByteRange.Equal(oldShardStatus.ByteRange) {
			// If the byte range is correct, we can assume the merge is complete
			log.Printf("SPLIT_STATE: Shard %s transitioning PreMerge -> Default", oldShardStatus.ID)
			newShardStatus.State = store.ShardState_Default
			needsPersist = true
		}
	case store.ShardState_PreSplit:
		if newShardInfo.ByteRange.Equal(oldShardStatus.ByteRange) {
			splitStateActive := newShardInfo.SplitState != nil &&
				newShardInfo.SplitState.GetPhase() != db.SplitState_PHASE_NONE
			if newShardInfo.Splitting || splitStateActive {
				log.Printf("SPLIT_STATE: Shard %s transitioning PreSplit -> Splitting", oldShardStatus.ID)
				newShardStatus.State = store.ShardState_Splitting
				needsPersist = true
			} else {
				// Don't transition to Default unless the split-off shard is ready.
				// This handles a race condition where the storage node reports Splitting=false
				// before the split-off shard has loaded its data. Without this check, the parent
				// shard would stop serving the split-off range (via fallback) prematurely.
				if tm.splitOffShardIsReady(oldShardStatus) {
					log.Printf("SPLIT_STATE: Shard %s transitioning PreSplit -> Default (splitOffShardIsReady=true)", oldShardStatus.ID)
					newShardStatus.State = store.ShardState_Default
					needsPersist = true
				}
			}
		} else {
		}
	case store.ShardState_Splitting:
		if !newShardInfo.Splitting {
			// Only transition to Default if the split-off shard is ready to serve traffic.
			// This prevents a gap in availability when the parent shard stops serving the
			// split-off range before the new shard has elected a leader and loaded its data.
			// The fallback logic in metadata.go routes requests to the parent shard while
			// it remains in Splitting state, so keeping this state ensures continuous availability.
			if tm.splitOffShardIsReady(oldShardStatus) {
				log.Printf("SPLIT_STATE: Shard %s transitioning Splitting -> Default (splitOffShardIsReady=true)", oldShardStatus.ID)
				newShardStatus.State = store.ShardState_Default
				needsPersist = true
			}
		}
	case store.ShardState_SplittingOff:
		if !newShardInfo.Initializing {
			// We we're splitting but now we've seen a node with the shard so set the state to pre-snapshot
			// TODO (ajr) Should we wait until all three nodes (or one in swarm mode) have checked in before switching states?
			log.Printf("SPLIT_STATE: Shard %s transitioning SplittingOff -> SplitOffPreSnap", oldShardStatus.ID)
			newShardStatus.State = store.ShardState_SplitOffPreSnap
			needsPersist = true
		}
	case store.ShardState_SplitOffPreSnap:
		// FIXME (ajr) Store the last snapshot in the KVStoreStats
		// to transition a node into default state  if a snapshot
		// has completed for the shard since being in this state
		//
		// FIXME (ajr) We should explicitly trigger a snapshot in the leader
		// for shards in this state, also need to trigger a snapshot for restored
		// shards too
		if newShardInfo.IsReadyForSplitReads() {
			// Only transition once the shard is fully cutover-ready, not merely bootstrapped.
			// This prevents metadata from routing reads to the child before it has replayed
			// through the parent's final fence.
			log.Printf("SPLIT_STATE: Shard %s transitioning SplitOffPreSnap -> Default (cutoverReady=true)", oldShardStatus.ID)
			newShardStatus.State = store.ShardState_Default
		}
	case store.ShardState_Default:
	default:
		if newShardInfo.Initializing {
			newShardStatus.State = store.ShardState_Initializing
		} else {
			newShardStatus.State = store.ShardState_Default
			needsPersist = true
		}
	}
	if newShardStatus.State == store.ShardState_Default &&
		newShardStatus.SplitState != nil &&
		newShardStatus.SplitState.GetPhase() != db.SplitState_PHASE_NONE &&
		newShardInfo.SplitState == nil &&
		!newShardInfo.Splitting {
		newShardStatus.SplitState = nil
		needsPersist = true
	}
	if newShardStatus.State == store.ShardState_Default &&
		newShardStatus.SplitState != nil &&
		newShardStatus.SplitState.GetPhase() != db.SplitState_PHASE_NONE &&
		newShardStatus.SplitReplayRequired &&
		newShardStatus.SplitCutoverReady &&
		newShardStatus.SplitParentShardID != 0 {
		newShardStatus.SplitState = nil
		needsPersist = true
	}
	return newShardStatus, needsPersist
}

func mergeSplitStates(oldState, newState *db.SplitState) (*db.SplitState, bool) {
	switch {
	case oldState == nil && newState == nil:
		return nil, false
	case newState == nil:
		return proto.Clone(oldState).(*db.SplitState), false
	case oldState == nil:
		return proto.Clone(newState).(*db.SplitState), true
	}

	merged := proto.Clone(newState).(*db.SplitState)
	if oldState.GetPhase() == merged.GetPhase() {
		if merged.GetNewShardId() == 0 && oldState.GetNewShardId() != 0 {
			merged.SetNewShardId(oldState.GetNewShardId())
		}
		if len(merged.GetSplitKey()) == 0 && len(oldState.GetSplitKey()) > 0 {
			merged.SetSplitKey(oldState.GetSplitKey())
		}
		if len(merged.GetOriginalRangeEnd()) == 0 && len(oldState.GetOriginalRangeEnd()) > 0 {
			merged.SetOriginalRangeEnd(oldState.GetOriginalRangeEnd())
		}
		if merged.GetStartedAtUnixNanos() == 0 && oldState.GetStartedAtUnixNanos() != 0 {
			merged.SetStartedAtUnixNanos(oldState.GetStartedAtUnixNanos())
		}
	}

	return merged, !proto.Equal(oldState, merged)
}

// diffExceedsPercent reports whether new differs from old by more than pct percent.
func diffExceedsPercent(new, old uint64, pct uint64) bool {
	if old == 0 {
		return new > 0
	}
	diff := new - old
	if new < old {
		diff = old - new
	}
	return diff*100 > old*pct
}

// splitOffShardIsReady checks if the split-off shard corresponding to a parent
// shard in Splitting state is ready to serve traffic. This is used to gate the
// parent shard's transition from Splitting to Default state, ensuring continuous
// data availability during splits.
//
// The split-off shard is considered ready when it is read-ready according to
// ShardInfo.IsReadyForSplitReads(): it has a snapshot, is no longer initializing,
// has an elected Raft leader, and if replay is required, has crossed the final
// cutover fence. Until then, the metadata layer falls back to the parent shard
// for the split-off shard's key range (see shouldFallbackToParentShard).
func (tm *TableManager) splitOffShardIsReady(parentStatus *store.ShardStatus) bool {
	// The split-off shard's byte range starts at the parent's byte range end (the split key)
	splitKey := parentStatus.ByteRange[1]
	if len(splitKey) == 0 {
		// No split key means this isn't a split parent, allow transition
		return true
	}

	// Find all shard statuses to look for the split-off shard
	allStatuses, err := tm.GetShardStatuses()
	if err != nil {
		// If we can't check, be conservative and don't transition yet
		return false
	}

	for shardID, status := range allStatuses {
		if status.Table != parentStatus.Table || shardID == parentStatus.ID {
			continue
		}
		// The split-off shard has ByteRange[0] == splitKey (parent's end = child's start)
		if bytes.Equal(status.ByteRange[0], splitKey) {
			// Found the split-off shard, check if it's ready
			switch status.State {
			case store.ShardState_SplittingOff, store.ShardState_SplitOffPreSnap:
				return status.IsReadyForSplitReads()
			case store.ShardState_Default:
				return status.IsReadyForSplitReads()
			default:
				// Unexpected state, be conservative
				return false
			}
		}
	}

	// If we can't find the split-off shard, allow the transition.
	// This handles cases where the split was aborted or the split-off shard
	// was cleaned up for some reason.
	return true
}

type StoreStatuses struct {
	Statuses map[types.ID]*StoreStatus `json:"statuses,omitzero,omitempty"`
	Error    error                     `json:"error,omitzero,omitempty"`
}
type ShardStatuses struct {
	Statuses map[types.ID]*store.ShardStatus `json:"statuses,omitzero,omitempty"`
	Error    error                           `json:"error,omitzero,omitempty"`
}

func (tm *TableManager) Status() (*StoreStatuses, *ShardStatuses) {
	sh, err1 := tm.loadShardStatuses()
	st, err2 := tm.loadStoreStatuses()

	// Filter out tombstoned stores from the status response.
	// Tombstoned stores are in the process of being removed from the cluster
	// and should not be included in health checks.
	tombstones, err3 := tm.GetStoreTombstones(context.TODO())
	if err3 == nil && len(tombstones) > 0 {
		for _, id := range tombstones {
			delete(st, id)
		}
	}

	return &StoreStatuses{
			Statuses: st,
			Error:    err1,
		}, &ShardStatuses{
			Statuses: sh,
			Error:    err2,
		}
}

func (tm *TableManager) GetStoreStatus(ctx context.Context, peerID types.ID) (*StoreStatus, error) {
	b, closer, err := tm.db.Get(ctx, []byte(storeStatusPrefix+peerID.String()))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("getting store status for %s: %w", peerID, err)
	}
	defer func() { _ = closer.Close() }()
	status := &StoreStatus{}
	if err := json.Unmarshal(b, status); err != nil {
		return nil, fmt.Errorf("unmarshalling store status for %s: %w", peerID, err)
	}
	status.StoreClient = tm.newStoreClient(peerID, status.ApiURL)
	return status, nil
}

func (tm *TableManager) RangeStoreStatuses(
	fn func(peerID types.ID, status *StoreStatus) bool,
) error {
	resp, err := tm.loadStoreStatuses()
	if err != nil {
		return fmt.Errorf("loading store statuses: %w", err)
	}
	for peerID, status := range resp {
		if !fn(peerID, status) {
			break
		}
	}
	return nil
}

func (tm *TableManager) GetStoreClient(
	ctx context.Context,
	peerID types.ID,
) (client.StoreRPC, bool, error) {
	status, err := tm.GetStoreStatus(ctx, peerID)
	if err != nil {
		return nil, false, err
	}
	return status.StoreClient, status.IsReachable(), nil
}

func (tm *TableManager) GetShardStatuses() (map[types.ID]*store.ShardStatus, error) {
	resp, err := tm.loadShardStatuses()
	if err != nil {
		return resp, fmt.Errorf("loading shard statuses: %w", err)
	}
	return resp, nil
}

var ErrNotFound = errors.New("not found")

func (tm *TableManager) GetShardStatus(shardID types.ID) (*store.ShardStatus, error) {
	b, closer, err := tm.db.Get(context.Background(), []byte(shardStatusPrefix+shardID.String()))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("getting shard status for %s: %w", shardID, err)
	}
	defer func() { _ = closer.Close() }()
	status := &store.ShardStatus{}
	if err := json.Unmarshal(b, status); err != nil {
		return nil, fmt.Errorf("unmarshalling shard status for %s: %w", shardID, err)
	}
	return status, nil
}

// UpdateShardSplitState updates the SplitState field of a shard status.
// This is used to immediately propagate the SplitState after PrepareSplit succeeds,
// without waiting for the next heartbeat cycle to update the status.
func (tm *TableManager) UpdateShardSplitState(ctx context.Context, shardID types.ID, splitState *db.SplitState) error {
	tm.Lock()
	defer tm.Unlock()

	status, err := tm.GetShardStatus(shardID)
	if err != nil {
		return fmt.Errorf("getting shard status for %s: %w", shardID, err)
	}

	status.SplitState = splitState

	data, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("marshalling shard status for %s: %w", shardID, err)
	}

	writes := [][2][]byte{{[]byte(shardStatusPrefix + shardID.String()), data}}
	return tm.db.Batch(ctx, writes, nil)
}

func (tm *TableManager) EnqueueReallocationRequest(ctx context.Context) error {
	writes := [][2][]byte{{[]byte(tableReallocationReqPrefix), []byte{0x00}}}
	return tm.db.Batch(ctx, writes, nil)
}

func (tm *TableManager) ClearReallocationRequest(ctx context.Context) error {
	return tm.db.Batch(ctx, nil, [][]byte{[]byte(tableReallocationReqPrefix)})
}

func (tm *TableManager) HasReallocationRequest(ctx context.Context) (bool, error) {
	b, closer, err := tm.db.Get(ctx, []byte(tableReallocationReqPrefix))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("checking reallocation request: %w", err)
	}
	defer func() { _ = closer.Close() }()
	return len(b) > 0, nil
}

// TODO (ajr) In the leader we should periodically clean up tombstones for stores that have been removed
// once the node is no longer part of the cluster and all shards have been reassigned
func (tm *TableManager) DeleteTombstones(ctx context.Context, ids []types.ID) error {
	if len(ids) == 0 {
		return nil
	}
	deletes := make([][]byte, 0, len(ids)*2)
	for _, id := range ids {
		deletes = append(deletes, []byte(storeStatusPrefix+id.String()))
		deletes = append(deletes, []byte(storeTombstonePrefix+id.String()))
	}
	return tm.db.Batch(ctx, nil, deletes)
}

func (tm *TableManager) TombstoneStore(ctx context.Context, id types.ID) error {
	// TODO (ajr) Remove a shard altogether after reconciliation has happened
	writes := [][2][]byte{{[]byte(storeTombstonePrefix + id.String()), nil}}

	// Also update store state to Terminating so IsReachable() immediately returns false.
	// This prevents routing requests to a store that is being removed, avoiding
	// "connection refused" errors during the window between deregistration and
	// Raft peer removal.
	if status, err := tm.GetStoreStatus(ctx, id); err == nil {
		status.State = store.StoreState_Terminating
		if data, err := json.Marshal(status); err == nil {
			writes = append(writes, [2][]byte{[]byte(storeStatusPrefix + id.String()), data})
		}
	}

	return tm.db.Batch(ctx, writes, nil)
}

func (tm *TableManager) GetStoreTombstones(ctx context.Context) ([]types.ID, error) {
	var tombstones []types.ID
	iter, err := tm.db.NewIter(ctx, &pebble.IterOptions{
		LowerBound: []byte(storeTombstonePrefix),
		UpperBound: []byte(storeTombstonePrefix + "~"),
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for store tombstones: %w", err)
	}
	defer func() { _ = iter.Close() }()
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, []byte(storeTombstonePrefix)) {
			break
		}
		id, err := types.IDFromString(string(bytes.TrimPrefix(key, []byte(storeTombstonePrefix))))
		if err != nil {
			// TODO (ajr) Should we just skip?
			return nil, fmt.Errorf("invalid store tombstone id %s: %v", key, err)
		}
		tombstones = append(tombstones, types.ID(id))
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating store tombstones: %w", err)
	}
	return tombstones, nil
}

func (tm *TableManager) RegisterStore(
	ctx context.Context,
	req *store.StoreRegistrationRequest,
) error {
	storeStatus := &StoreStatus{
		StoreInfo: req.StoreInfo,
		State:     store.StoreState_Healthy,
		LastSeen:  time.Now(),
		Shards:    req.Shards,
	}
	newStatus := map[types.ID]*StoreStatus{
		req.ID: storeStatus,
	}
	// If the request contains shards, we need to update the shard status
	if len(req.Shards) > 0 {
		return tm.UpdateStatuses(ctx, newStatus)
	}
	if err := tm.saveStoreStatuses(newStatus); err != nil {
		return fmt.Errorf("saving node status: %w", err)
	}
	return nil
}

type TableConfig struct {
	NumShards          uint
	StartID            types.ID
	Description        string              // Optional description of the table
	Schema             *schema.TableSchema // Schema for the table
	Indexes            map[string]indexes.IndexConfig
	ReplicationSources []store.ReplicationSourceConfig
}

var (
	ErrTableExists = errors.New("table already exists")
	ErrIndexExists = errors.New("index already exists")
)

// CreateTable creates a new table with the given name and schema
func (tm *TableManager) CreateTable(name string, tc TableConfig) (*store.Table, error) {
	if tc.StartID == 0 {
		tc.StartID = types.ID(xxhash.Sum64String(name))
	}

	if table, err := tm.GetTable(name); err == nil {
		return table, ErrTableExists
	} else if !errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("checking for existing table %s: %w", name, err)
	}

	shards := createShardConfig(tc)
	table := &store.Table{
		Name:               name,
		Description:        tc.Description,
		Shards:             shards,
		Schema:             tc.Schema,
		Indexes:            tc.Indexes,
		ReplicationSources: tc.ReplicationSources,
	}

	newShardStatuses := make(map[types.ID]*store.ShardStatus, len(table.Shards))
	// Add shards to the table
	for id, conf := range table.Shards {
		newShardStatuses[id] = &store.ShardStatus{
			ID: id,
			ShardInfo: store.ShardInfo{
				Peers:       common.NewPeerSet(),
				ShardConfig: *conf,
			},
			Table: name,
		}
	}

	// Persist the updated table definition
	if err := tm.saveTableAndShardStatus(table, newShardStatuses); err != nil {
		return nil, fmt.Errorf("failed to save table with new shards: %w", err)
	}
	return table, nil
}

func (tm *TableManager) RestoreTable(
	table *store.Table,
	restoreConfig *common.BackupConfig,
) error {
	if _, err := tm.GetTable(table.Name); err == nil {
		return ErrTableExists
	} else if !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("checking for existing table %s: %w", table.Name, err)
	}

	newShardStatuses := make(map[types.ID]*store.ShardStatus, len(table.Shards))
	for id, conf := range table.Shards {
		conf.Indexes = table.Indexes
		conf.Schema = table.Schema
		conf.RestoreConfig = restoreConfig
		newShardStatuses[id] = &store.ShardStatus{
			ID: id,
			ShardInfo: store.ShardInfo{
				Peers:       common.NewPeerSet(),
				ShardConfig: *conf,
			},
			Table: table.Name,
		}
	}

	// Persist the updated table definition
	if err := tm.saveTableAndShardStatus(table, newShardStatuses); err != nil {
		return fmt.Errorf("failed to save table with new shards: %w", err)
	}
	return nil
}

func (tm *TableManager) Tables(prefix, pattern *string) ([]*store.Table, error) {
	tables, err := tm.loadTables(prefix, pattern)
	if err != nil {
		return nil, fmt.Errorf("loading tables: %w", err)
	}
	return slices.Collect(maps.Values(tables)), nil
}

func (tm *TableManager) TablesMap() (map[string]*store.Table, error) {
	tables, err := tm.loadTables(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("loading tables: %w", err)
	}
	return tables, nil
}

// GetTable retrieves a table by name
func (tm *TableManager) GetTable(name string) (*store.Table, error) {
	b, closer, err := tm.db.Get(context.Background(), []byte(tablePrefix+name))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("getting table %s: %w", name, err)
	}
	defer func() { _ = closer.Close() }()
	table := &store.Table{}
	if err := json.Unmarshal(b, table); err != nil {
		return nil, fmt.Errorf("decoding table: %w", err)
	}

	return table, nil
}

func (tm *TableManager) GetTableWithShardStatuses(
	name string,
) (*store.Table, map[types.ID]*store.ShardStatus, error) {
	table, err := tm.GetTable(name)
	if err != nil {
		return nil, nil, err
	}

	statuses := make(map[types.ID]*store.ShardStatus, len(table.Shards))
	for id := range table.Shards {
		statuses[id], err = tm.GetShardStatus(id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				// If the shard status is not found, we can create a new one
				statuses[id] = &store.ShardStatus{
					ID:    id,
					Table: name,
					ShardInfo: store.ShardInfo{
						Peers: common.NewPeerSet(),
					},
				}
			} else {
				return nil, nil, fmt.Errorf("getting shard status for %s: %w", id, err)
			}
		}
	}

	return table, statuses, nil
}

func (tm *TableManager) Indexes(tableName string) (map[string]*store.IndexStatus, error) {
	table, err := tm.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("getting table %s: %w", tableName, err)
	}
	if table.Indexes == nil {
		table.Indexes = map[string]indexes.IndexConfig{}
	}
	idxs := make(map[string]*store.IndexStatus, len(table.Indexes))
	for name, index := range table.Indexes {
		index.Name = name
		indexStatus := &store.IndexStatus{
			IndexConfig: index,
			ShardStatus: map[types.ID]indexes.IndexStats{},
		}
		idxs[name] = indexStatus
	}
	for id := range table.Shards {
		shardStatus, err := tm.GetShardStatus(id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("getting status for shard %s: %w", id, err)
		}
		if shardStatus != nil && len(shardStatus.Indexes) != 0 {
			if shardStatus.ShardStats != nil {
				m := shardStatus.ShardStats.Indexes
				for indexName := range shardStatus.Indexes {
					if _, exists := idxs[indexName]; !exists {
						continue
					}
					idxs[indexName].ShardStatus[id] = m[indexName]
					indexes.MergeIndexStats(&idxs[indexName].Status, m[indexName])
				}
			}
		}
	}
	return idxs, nil
}

func (tm *TableManager) GetIndex(tableName, indexName string) (*store.IndexStatus, error) {
	table, err := tm.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("getting table %s: %w", tableName, err)
	}
	index := &store.IndexStatus{
		ShardStatus: map[types.ID]indexes.IndexStats{},
	}
	if len(table.Indexes) == 0 {
		return nil, ErrNotFound
	}
	tableIndex, ok := table.Indexes[indexName]
	if !ok {
		return nil, ErrNotFound
	}
	tableIndex.Name = indexName
	index.IndexConfig = tableIndex
	for id := range table.Shards {
		shardStatus, err := tm.GetShardStatus(id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("getting status for shard %s: %w", id, err)
		}
		if shardStatus != nil && len(shardStatus.Indexes) != 0 {
			if shardStatus.ShardStats != nil && shardStatus.ShardStats.Storage != nil {
				m := shardStatus.ShardStats.Indexes
				index.ShardStatus[id] = m[indexName]
				indexes.MergeIndexStats(&index.Status, m[indexName])
			}
		}
	}
	return index, nil
}

func (tm *TableManager) DropIndex(tableName, indexName string) (*store.Table, error) {
	tm.Lock()
	defer tm.Unlock()
	table, err := tm.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("getting table %s: %w", tableName, err)
	}
	delete(table.Indexes, indexName)
	newShardStatuses := make(map[types.ID]*store.ShardStatus, len(table.Shards))
	for id := range table.Shards {
		newShardStatuses[id], err = tm.GetShardStatus(id)
		if errors.Is(err, ErrNotFound) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("getting shard status for %s: %w", id, err)
		}
		newShardStatuses[id].Indexes = table.Indexes
	}
	if err := tm.saveTableAndShardStatus(table, newShardStatuses); err != nil {
		return nil, fmt.Errorf("failed to save table with new index: %w", err)
	}
	return table, nil
}

func (tm *TableManager) CreateIndex(
	tableName, indexName string,
	config indexes.IndexConfig,
) (*store.Table, error) {
	tm.Lock()
	defer tm.Unlock()
	table, err := tm.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("getting table %s: %w", tableName, err)
	}
	if table.Indexes == nil {
		table.Indexes = map[string]indexes.IndexConfig{}
	}
	if _, exists := table.Indexes[indexName]; exists {
		return nil, fmt.Errorf("index %s already exists", indexName)
	}
	config.Name = indexName
	table.Indexes[indexName] = config

	newShardStatuses := make(map[types.ID]*store.ShardStatus, len(table.Shards))
	for id := range table.Shards {
		newShardStatuses[id], err = tm.GetShardStatus(id)
		if errors.Is(err, ErrNotFound) {
			return nil, fmt.Errorf("shard %s not found in shard status", id)
		} else if err != nil {
			return nil, fmt.Errorf("getting shard status for %s: %w", id, err)
		}
		newShardStatuses[id].Indexes = table.Indexes
		table.Shards[id].Indexes = table.Indexes
	}
	if err := tm.saveTableAndShardStatus(table, newShardStatuses); err != nil {
		return nil, fmt.Errorf("failed to save table with new index: %w", err)
	}
	return table, nil
}

func (tm *TableManager) DropReadSchema(tableName string) error {
	tm.Lock()
	defer tm.Unlock()
	table, err := tm.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("getting table %s: %w", tableName, err)
	}
	if table.ReadSchema == nil {
		return fmt.Errorf("no read schema to drop for table %s", tableName)
	}
	versionSuffix := fmt.Sprintf("_v%d", table.ReadSchema.Version)
	for idxName, idx := range table.Indexes {
		if idx.Type != indexes.IndexTypeFullTextV0 && idx.Type != indexes.IndexTypeFullText {
			continue
		}
		if strings.HasSuffix(idxName, versionSuffix) {
			delete(table.Indexes, idxName)
		}
		if table.ReadSchema.Version == 0 {
			if idxName == "full_text_index" {
				delete(table.Indexes, idxName)
			}
		}
	}
	updatedShardStatuses := make(map[types.ID]*store.ShardStatus, len(table.Shards))
	for id := range table.Shards {
		table.Shards[id].Indexes = table.Indexes
		table.Shards[id].Schema = table.Schema

		shardStatus, err := tm.GetShardStatus(id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return fmt.Errorf("getting shard status for %s: %w", id, err)
		}
		shardStatus.ShardConfig = *table.Shards[id]
		updatedShardStatuses[id] = shardStatus
	}
	table.ReadSchema = nil
	if err := tm.saveTableAndShardStatus(table, updatedShardStatuses); err != nil {
		return fmt.Errorf("failed to save table with new index: %w", err)
	}
	return nil
}

func (tm *TableManager) UpdateSchema(
	tableName string,
	tableSchema *schema.TableSchema,
) (*store.Table, error) {
	tm.Lock()
	defer tm.Unlock()
	table, err := tm.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("getting table %s: %w", tableName, err)
	}

	prevVersion := uint32(0)
	if table.Schema != nil {
		prevVersion = table.Schema.Version
	}

	// Only bump version if document schemas changed (not just templates)
	// Template changes only affect future documents and don't require index rebuilds
	if documentSchemasChanged(table.Schema, tableSchema) {
		tableSchema.Version = prevVersion + 1
		if table.ReadSchema != nil {
			// Migration in progress, replacing target schema. Cleanup old versioned indexes.
			indexesToDrop := []string{}
			versionSuffix := fmt.Sprintf("_v%d", table.Schema.Version)
			for name := range table.Indexes {
				if strings.HasSuffix(name, versionSuffix) {
					indexesToDrop = append(indexesToDrop, name)
				}
			}
			for _, name := range indexesToDrop {
				delete(table.Indexes, name)
			}
		} else {
			// Start new migration.
			table.ReadSchema = table.Schema
			if table.ReadSchema == nil {
				table.ReadSchema = &schema.TableSchema{Version: 0}
			}
		}

		// Create new versioned indexes for the new schema.
		newlyCreatedIndexes := make(map[string]indexes.IndexConfig)
		newName := fmt.Sprintf("full_text_index_v%d", tableSchema.Version)
		for _, config := range table.Indexes {
			if strings.HasPrefix(config.Name, "full_text_index") {
				newConfig := config
				newConfig.Name = newName
				newlyCreatedIndexes[newName] = newConfig
				break
			}
		}

		if table.Indexes == nil {
			table.Indexes = map[string]indexes.IndexConfig{}
		}
		maps.Copy(table.Indexes, newlyCreatedIndexes)
	} else {
		// Template-only change: keep same version, no new indexes created
		tableSchema.Version = prevVersion
	}

	table.Schema = tableSchema

	// Update shard configs and their persisted statuses so the reconciler
	// and API see the new indexes and schema version immediately.
	updatedShardStatuses := make(map[types.ID]*store.ShardStatus, len(table.Shards))
	for id := range table.Shards {
		table.Shards[id].Indexes = table.Indexes
		table.Shards[id].Schema = table.Schema

		shardStatus, err := tm.GetShardStatus(id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("getting shard status for %s: %w", id, err)
		}
		shardStatus.ShardConfig = *table.Shards[id]
		updatedShardStatuses[id] = shardStatus
	}
	if err := tm.saveTableAndShardStatus(table, updatedShardStatuses); err != nil {
		return nil, fmt.Errorf("failed to save table with new index: %w", err)
	}
	return table, nil
}

// documentSchemasChanged checks if the document schemas have changed between old and new.
// It compares DocumentSchemas, DefaultType, EnforceTypes, TtlField, and TtlDuration.
// DynamicTemplates are intentionally excluded - template changes don't require version bumps.
func documentSchemasChanged(old, new *schema.TableSchema) bool {
	if old == nil && new == nil {
		return false
	}
	if old == nil || new == nil {
		return true
	}
	// Compare document-related fields (excluding DynamicTemplates and Version)
	if !reflect.DeepEqual(old.DocumentSchemas, new.DocumentSchemas) {
		return true
	}
	if old.DefaultType != new.DefaultType {
		return true
	}
	if old.EnforceTypes != new.EnforceTypes {
		return true
	}
	if old.TtlField != new.TtlField {
		return true
	}
	if old.TtlDuration != new.TtlDuration {
		return true
	}
	return false
}

func (tm *TableManager) ReassignShardsForMerge(merge MergeTransition) (*store.ShardConfig, error) {
	tm.Lock()
	defer tm.Unlock()
	tableName := merge.TableName
	table, err := tm.GetTable(tableName)
	if err != nil {
		return nil, err
	}

	conf, ok := table.Shards[merge.ShardID]
	if !ok {
		return nil, fmt.Errorf("shard %s does not exist", merge.ShardID)
	}
	mergeConf, ok := table.Shards[merge.MergeShardID]
	if !ok {
		return nil, fmt.Errorf("shard %s does not exist", merge.MergeShardID)
	}
	if !bytes.Equal(conf.ByteRange[1], mergeConf.ByteRange[0]) &&
		!bytes.Equal(mergeConf.ByteRange[1], conf.ByteRange[0]) {
		return nil, fmt.Errorf(
			"shard %s cannot be merged with %s: shardRange: %s mergeRange: %s",
			merge.ShardID,
			merge.MergeShardID,
			conf.ByteRange,
			mergeConf.ByteRange,
		)
	}
	newRange := [2][]byte{conf.ByteRange[0], mergeConf.ByteRange[1]}
	if bytes.Equal(mergeConf.ByteRange[1], conf.ByteRange[0]) {
		newRange = [2][]byte{mergeConf.ByteRange[0], conf.ByteRange[1]}
	}
	newConfig := &store.ShardConfig{
		ByteRange: newRange,
		Indexes:   conf.Indexes,
		Schema:    conf.Schema,
	}
	table.Shards[merge.ShardID] = newConfig
	newShardStatus, err := tm.GetShardStatus(merge.ShardID)
	if err != nil {
		return nil, fmt.Errorf("cannot get shard %d from shard status: %w", merge.ShardID, err)
	}
	newShardStatus.ShardConfig = *newConfig
	newShardStatus.State = store.ShardState_PreMerge
	// Persist the updated table definition
	delete(table.Shards, merge.MergeShardID)
	if err := tm.saveTableAndShardStatus(table, map[types.ID]*store.ShardStatus{
		merge.ShardID: newShardStatus,
	}, merge.MergeShardID); err != nil {
		return nil, fmt.Errorf("saving table: %w", err)
	}
	return newConfig, nil
}

func (tm *TableManager) ReassignShardsForSplit(
	split SplitTransition,
) ([]types.ID, *store.ShardConfig, error) {
	tm.Lock()
	defer tm.Unlock()
	if split.ShardID == split.SplitShardID {
		return nil, nil, errors.New("shard ID and split shard ID cannot be the same")
	}
	tableName := split.TableName
	table, err := tm.GetTable(tableName)
	if err != nil {
		return nil, nil, err
	}

	newShards := make(map[types.ID]*store.ShardConfig)
	conf, ok := table.Shards[split.ShardID]
	if !ok {
		return nil, nil, fmt.Errorf("shard %s does not exist", split.ShardID)
	}
	newShards[split.ShardID] = &store.ShardConfig{
		ByteRange:     [2][]byte{conf.ByteRange[0], split.SplitKey},
		Indexes:       conf.Indexes,
		Schema:        conf.Schema,
		RestoreConfig: conf.RestoreConfig,
	}
	newShards[split.SplitShardID] = &store.ShardConfig{
		ByteRange: [2][]byte{split.SplitKey, conf.ByteRange[1]},
		Indexes:   conf.Indexes,
		Schema:    conf.Schema,
	}

	shardStatus, err := tm.GetShardStatus(split.ShardID)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot get shard %d from shard status: %w", split.ShardID, err)
	}
	shardStatus.ShardConfig = *newShards[split.ShardID]
	shardStatus.State = store.ShardState_PreSplit
	newStatuses := map[types.ID]*store.ShardStatus{
		split.ShardID: shardStatus,
		split.SplitShardID: {
			ID: split.SplitShardID,
			ShardInfo: store.ShardInfo{
				ShardConfig: *newShards[split.SplitShardID],
				Peers:       shardStatus.Peers,
			},
			Table: tableName,
			State: store.ShardState_SplittingOff,
		},
	}
	maps.Copy(table.Shards, newShards)
	// Persist the updated table definition
	if err := tm.saveTableAndShardStatus(table, newStatuses); err != nil {
		return nil, nil, fmt.Errorf("saving table: %w", err)
	}
	return shardStatus.Peers.IDSlice(), newShards[split.SplitShardID], nil
}

// RemoveTable deletes a table by name
func (tm *TableManager) RemoveTable(name string) error {
	tm.Lock()
	defer tm.Unlock()
	table, err := tm.GetTable(name)
	if err != nil {
		return err
	}

	shards := slices.Collect(maps.Keys(table.Shards))
	// After deleting the table, we should also persist the modified shardStatus map.
	if err := tm.commitRmTableAndShardStatus(name, shards); err != nil {
		return fmt.Errorf("failed to save shard status after deleting table %s: %w", name, err)
	}
	return nil
}

// commitRmTableAndShardStatus persists a table removal and shard status to the database
func (tm *TableManager) commitRmTableAndShardStatus(tableName string, shards []types.ID) error {
	deletes := make([][]byte, 0, len(shards)+1)
	for _, shardID := range shards {
		deletes = append(deletes, []byte(shardStatusPrefix+shardID.String()))
	}
	deletes = append(deletes, []byte(tablePrefix+tableName))
	return tm.db.Batch(context.TODO(), nil, deletes)
}

func (tm *TableManager) saveStoreStatuses(storeStatuses map[types.ID]*StoreStatus) error {
	writes := make([][2][]byte, 0, len(storeStatuses))
	for id, status := range storeStatuses {
		data, err := json.Marshal(status)
		if err != nil {
			return fmt.Errorf("marshaling node status: %w", err)
		}
		writes = append(writes, [2][]byte{[]byte(storeStatusPrefix + id.String()), data})
	}
	return tm.db.Batch(context.TODO(), writes, nil)
}

func (tm *TableManager) saveStoreAndShardStatuses(
	storeStatuses map[types.ID]*StoreStatus,
	shardStatus map[types.ID]*store.ShardStatus,
) error {
	writes := make([][2][]byte, 0, len(storeStatuses)+len(shardStatus))
	if cap(writes) == 0 {
		return nil
	}
	for id, status := range storeStatuses {
		data, err := json.Marshal(status)
		if err != nil {
			return fmt.Errorf("marshaling node status: %w", err)
		}
		writes = append(writes, [2][]byte{[]byte(storeStatusPrefix + id.String()), data})
	}
	for id, status := range shardStatus {
		data, err := json.Marshal(status)
		if err != nil {
			return fmt.Errorf("marshaling shard status: %w", err)
		}
		writes = append(writes, [2][]byte{[]byte(shardStatusPrefix + id.String()), data})
	}
	return tm.db.Batch(context.TODO(), writes, nil)
}

// saveTableAndShardStatus persists a table definition and shard status to the database
func (tm *TableManager) saveTableAndShardStatus(
	table *store.Table,
	shardStatuses map[types.ID]*store.ShardStatus,
	deleteShardStatuses ...types.ID,
) error {
	writes := make([][2][]byte, 0, len(shardStatuses)+1)
	data, err := json.Marshal(table)
	if err != nil {
		return fmt.Errorf("marshalling table: %w", err)
	}
	writes = append(writes, [2][]byte{[]byte(tablePrefix + table.Name), data})
	for id, shardStatus := range shardStatuses {
		data, err := json.Marshal(shardStatus)
		if err != nil {
			return fmt.Errorf("failed to marshal shard status: %w", err)
		}
		writes = append(writes, [2][]byte{[]byte(shardStatusPrefix + id.String()), data})
	}

	deletes := make([][]byte, 0, len(deleteShardStatuses))
	for _, id := range deleteShardStatuses {
		deletes = append(deletes, []byte(shardStatusPrefix+id.String()))
	}

	return tm.db.Batch(context.TODO(), writes, deletes)
}

func (tm *TableManager) loadStoreStatuses() (map[types.ID]*StoreStatus, error) {
	resp := map[types.ID]*StoreStatus{}
	iterOpt := &pebble.IterOptions{
		LowerBound: []byte(storeStatusPrefix),
		UpperBound: []byte(storeStatusPrefix + "~"), // '~' is a common sentinel for "end of prefix"
	}
	iter, err := tm.db.NewIter(context.TODO(), iterOpt)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for tables: %w", err)
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		var storeStatus StoreStatus
		if err := json.Unmarshal(iter.Value(), &storeStatus); err != nil {
			return nil, fmt.Errorf("unmarshalling table: %w", err)
		}
		storeStatus.StoreClient = tm.newStoreClient(
			storeStatus.ID,
			storeStatus.ApiURL,
		)
		resp[storeStatus.ID] = &storeStatus
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error while loading store statuses: %w", err)
	}
	return resp, nil
}

func (tm *TableManager) loadShardStatuses() (map[types.ID]*store.ShardStatus, error) {
	resp := map[types.ID]*store.ShardStatus{}
	iterOpt := &pebble.IterOptions{
		LowerBound: []byte(shardStatusPrefix),
		UpperBound: []byte(shardStatusPrefix + "~"), // '~' is a common sentinel for "end of prefix"
	}
	iter, err := tm.db.NewIter(context.TODO(), iterOpt)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for tables: %w", err)
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		var shardStatus store.ShardStatus
		if err := json.Unmarshal(iter.Value(), &shardStatus); err != nil {
			return nil, fmt.Errorf("unmarshalling table: %w", err)
		}
		resp[shardStatus.ID] = &shardStatus
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error while loading shard statuses: %w", err)
	}
	return resp, nil
}

// loadTables loads table definitions from the database, optionally filtered by prefix and/or pattern
func (tm *TableManager) loadTables(prefix, pattern *string) (map[string]*store.Table, error) {
	resp := make(map[string]*store.Table)

	// Optimize iterator bounds if prefix is provided
	lowerBound := tablePrefix
	upperBound := tablePrefix + "~"
	if prefix != nil && *prefix != "" {
		lowerBound = tablePrefix + *prefix
		upperBound = tablePrefix + *prefix + "~"
	}

	iterOpt := &pebble.IterOptions{
		LowerBound: []byte(lowerBound),
		UpperBound: []byte(upperBound),
	}
	iter, err := tm.db.NewIter(context.TODO(), iterOpt)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for tables: %w", err)
	}
	defer func() { _ = iter.Close() }()

	// Compile regex pattern if provided
	var re *regexp.Regexp
	if pattern != nil && *pattern != "" {
		re, err = regexp.Compile(*pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern: %w", err)
		}
	}

	for iter.First(); iter.Valid(); iter.Next() {
		var table store.Table
		if err := json.Unmarshal(iter.Value(), &table); err != nil {
			return nil, fmt.Errorf("unmarshalling table: %w", err)
		}

		// Apply regex filter if pattern is provided
		if re != nil && !re.MatchString(table.Name) {
			continue
		}

		resp[table.Name] = &table
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error while loading tables: %w", err)
	}

	return resp, nil
}

// Get retrieves a value from the metadata store
func (tm *TableManager) Get(key string) ([]byte, error) {
	ctx := context.Background()
	value, closer, err := tm.db.Get(ctx, []byte(key))
	if closer != nil {
		defer func() { _ = closer.Close() }()
	}
	if err != nil {
		return nil, err
	}
	// Make a copy since the value is only valid until closer is called
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

// Put stores a key-value pair in the metadata store
func (tm *TableManager) Put(key string, value []byte) error {
	ctx := context.Background()
	writes := [][2][]byte{{[]byte(key), value}}
	return tm.db.Batch(ctx, writes, nil)
}

// Delete removes a key from the metadata store
func (tm *TableManager) Delete(key string) error {
	ctx := context.Background()
	deletes := [][]byte{[]byte(key)}
	return tm.db.Batch(ctx, nil, deletes)
}

// IteratePrefix iterates over all keys with the given prefix
func (tm *TableManager) IteratePrefix(prefix string, fn func(key, value []byte) error) error {
	ctx := context.Background()
	iter, err := tm.db.NewIter(ctx, &pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: func() []byte {
			// Create upper bound by incrementing the last byte
			ub := []byte(prefix)
			ub = append(ub, 0xff)
			return ub
		}(),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := fn(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}
