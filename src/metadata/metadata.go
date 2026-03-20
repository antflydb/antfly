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
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/clock"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/workerpool"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/antflydb/antfly/src/metadata/reconciler"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/antflydb/antfly/src/usermgr"
	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// MetadataStore implements the metadata HTTP API and runs background tasks to monitor nodes, shards
type MetadataStore struct {
	logger *zap.Logger

	metadataStore *kv.MetadataStore

	config *common.Config
	tm     *tablemgr.TableManager
	um     *usermgr.UserManager
	pool   *workerpool.Pool

	reconciler          *reconciler.Reconciler
	prevDebugShardsHash uint64

	embeddingCache *ttlcache.Cache[string, []float32]

	runHealthCheckC  chan struct{}
	reconcileShardsC chan struct{}

	// HLC for transaction timestamp allocation
	hlc   *HLC
	clock clock.Clock

	txnIDGenerator func() uuid.UUID
}

func (ms *MetadataStore) clockOrReal() clock.Clock {
	if ms.clock == nil {
		return clock.RealClock{}
	}
	return ms.clock
}

// handleForwardResolveIntent forwards ResolveIntent requests from coordinator shards to participant shards
func (ms *MetadataStore) handleForwardResolveIntent(w http.ResponseWriter, r *http.Request) {
	shardIDStr := r.PathValue("shardID")
	shardID, err := types.IDFromString(shardIDStr)
	if err != nil {
		errorResponse(w, "Invalid shard ID", http.StatusBadRequest)
		return
	}

	// Parse request body - expecting protobuf ResolveIntentsOp
	body, err := io.ReadAll(r.Body)
	if err != nil {
		errorResponse(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var resolveOp db.ResolveIntentsOp
	if err := proto.Unmarshal(body, &resolveOp); err != nil {
		errorResponse(w, "Failed to unmarshal resolve intent op", http.StatusBadRequest)
		return
	}

	// Forward to the leader of the target shard
	if err := ms.forwardResolveIntentToShard(r.Context(), shardID, resolveOp.GetTxnId(), resolveOp.GetStatus(), resolveOp.GetCommitVersion()); err != nil {
		ms.logger.Error("Failed to forward resolve intent",
			zap.Stringer("shardID", shardID),
			zap.Binary("txnID", resolveOp.GetTxnId()),
			zap.Error(err))
		errorResponse(w, fmt.Sprintf("Failed to forward resolve intent: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleStoreDeregistration handles requests from nodes to deregister with the leader
func (ms *MetadataStore) handleStoreDeregistration(w http.ResponseWriter, r *http.Request) {
	storeID := r.PathValue("store")

	// Extract node ID from URL path
	id, err := types.IDFromString(storeID)
	if err != nil {
		errorResponse(w, "Invalid store ID", http.StatusBadRequest)
		return
	}
	// FIXME (ajr) This has a race condition with health checks
	// Maybe we should use a different tombstone mechanism to mark
	// a node as deregistered or forward the request to the leader?
	//
	// This means you can't find it as a leader for removing it from the shard.
	if err := ms.tm.TombstoneStore(r.Context(), id); err != nil {
		ms.logger.Error("Failed to register node", zap.Stringer("storeID", id), zap.Error(err))
		errorResponse(w, fmt.Errorf("registering node: %w", err).Error(), http.StatusBadRequest)
		return
	}
	// 1. Remove shards from the deregistered store
	// 2. This causes the store to also step down if it's a leader

	ms.TriggerReconciliation()
	ms.logger.Info("Node deregistered", zap.Stringer("storeID", id))

	// Return success
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status": "deregistered",
	}); err != nil {
		ms.logger.Error("Error encoding response", zap.Error(err))
	}
}

// handleReallocateShards manually triggers shard reallocation (splits/merges) for a specific table
func (ms *MetadataStore) handleReallocateShards(w http.ResponseWriter, r *http.Request) {
	// FIXME (ajr) This should enqueue a reallocation request instead of processing immediately
	// the leader can then process the reallocation request
	if err := ms.tm.EnqueueReallocationRequest(r.Context()); err != nil {
		ms.logger.Error("Failed to enqueue reallocation request", zap.Error(err))
		errorResponse(w, "Failed to enqueue reallocation request", http.StatusInternalServerError)
		return
	}
	// Return success
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status": "reallocation enqueued",
	}); err != nil {
		ms.logger.Error("Error encoding response", zap.Error(err))
	}
}

// handleStoreRegistration handles requests from nodes to register with the leader
func (ms *MetadataStore) handleStoreRegistration(w http.ResponseWriter, r *http.Request) {
	var req store.StoreRegistrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, "Failed to parse registration request", http.StatusBadRequest)
		return
	}
	if err := ms.tm.RegisterStore(r.Context(), &req); err != nil {
		ms.logger.Error("Failed to register node", zap.Stringer("nodeID", req.ID), zap.Error(err))
		errorResponse(w, "Failed to register store", http.StatusInternalServerError)
		return
	}
	ms.TriggerReconciliation()

	ms.logger.Info("Node registered",
		zap.Stringer("nodeID", req.ID),
		zap.String("raftURL", req.RaftURL),
		zap.String("apiURL", req.ApiURL),
	)

	// Return success
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status": "registered",
	}); err != nil {
		ms.logger.Error("Error encoding response", zap.Error(err))
	}
}

// checkAllNodes queries all nodes for their status and stats (used for load balancing shards)
func (ms *MetadataStore) runHealthCheck(ctx context.Context) {
	// Check context before accessing Pebble to avoid logging errors after shutdown
	select {
	case <-ctx.Done():
		return
	default:
	}

	// Get tombstoned stores to skip them during health checks.
	// This prevents a race condition where the health check writes back
	// an unhealthy status for a store that's being removed, which would
	// resurrect the store status after DeleteTombstones deletes it.
	tombstones, _ := ms.tm.GetStoreTombstones(ctx)
	tombstoneSet := make(map[types.ID]struct{}, len(tombstones))
	for _, id := range tombstones {
		tombstoneSet[id] = struct{}{}
	}

	g, _ := errgroup.WithContext(ctx)

	results := make(map[types.ID]*tablemgr.StoreStatus)
	var localmu sync.Mutex

	err := ms.tm.RangeStoreStatuses(func(id types.ID, status *tablemgr.StoreStatus) bool {
		// Skip tombstoned stores - they're being removed from the cluster
		if _, tombstoned := tombstoneSet[id]; tombstoned {
			return true
		}

		g.Go(func() error {
			defer func() {
				if r := recover(); r != nil {
					fmt.Println("Recovered from panic:", r)              // Print the panic value
					fmt.Println("Stack trace:\n", string(debug.Stack())) // Print the stack trace
					panic(r)
				}
			}()
			storeStatus, err := status.Status(ctx)
			if err != nil {
				// TODO (ajr) This log gets spammy if a node goes away
				// ln.logger.Warn(
				// 	"Failed to fetch status for node",
				// 	zap.Stringer("nodeID", id),
				// 	zap.Error(err),
				// )
				//
				// FIXME (ajr) A node that has been unreachable for 10 minutes should be removed
				// from the cluster. If the shard loses all it's replicas at once though we should
				// not remove them.
				// Mark node as unhealthy if we can't reach it
				status.State = store.StoreState_Unhealthy
				localmu.Lock()
				defer localmu.Unlock()
				results[id] = status
				// Continue processing other nodes even if this one failed
				// But we should mark this node as unhealthy
				return nil
			}

			if storeStatus.ID != id {
				ms.logger.Warn("Node ID mismatch on status check",
					zap.Any("expectedInfo", status),
					zap.Stringer("expected", id), zap.Stringer("actual", storeStatus.ID))
				status.State = store.StoreState_Unhealthy
				localmu.Lock()
				defer localmu.Unlock()
				results[id] = status
				return nil
			}

			newStatus := &tablemgr.StoreStatus{
				StoreClient: status.StoreClient,
				StoreInfo:   status.StoreInfo,
				State:       store.StoreState_Healthy,
				LastSeen:    ms.clockOrReal().Now(),
				Shards:      storeStatus.Shards,
			}
			// Store successful result
			localmu.Lock()
			defer localmu.Unlock()
			results[id] = newStatus
			return nil
		})
		return true
	})
	if err != nil {
		// Don't log errors if context is cancelled or Pebble is closed during shutdown
		select {
		case <-ctx.Done():
			return
		default:
			// Check if error is due to closed database (common during shutdown)
			if errors.Is(err, pebble.ErrClosed) {
				return
			}
			ms.logger.Warn("Error iterating over store statuses", zap.Error(err))
		}
		return
	}

	// Wait for all goroutines to complete
	if err := g.Wait(); err != nil {
		ms.logger.Warn("Error during node status checks", zap.Error(err))
	}

	select {
	case <-ctx.Done():
		ms.logger.Info("Context done, stopping health check")
		return
	default:
	}

	// Update the node status map with all results
	// MVP (ajr) Contains side-effects for raft log
	if err := ms.tm.UpdateStatuses(ctx, results); err != nil {
		// Check if error is due to closed database (common during shutdown)
		if errors.Is(err, pebble.ErrClosed) {
			return
		}
		ms.logger.Warn("failed to set node status", zap.Error(err))
	}
}

// reconcileShards checks if nodes are running the correct shards
func (ms *MetadataStore) reconcileShards(ctx context.Context) {
	defaultTick := 1 * time.Second
	clk := ms.clockOrReal()
	ticker := clk.NewTimer(defaultTick)
	defer ticker.Stop()

	var i int
	run := func(force bool) {
		// Check context before running to avoid operations during shutdown
		select {
		case <-ctx.Done():
			return
		default:
		}
		ms.runHealthCheck(ctx)
		if i%10 == 0 || force {
			ms.checkShardAssignments(ctx)
		}
		i++
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ms.reconcileShardsC:
			select {
			// Wait so we can dedupe autoscaling events
			case <-clk.After(1 * time.Second):
			case <-ctx.Done():
				return
			}
			run(true)
			ticker.Reset(defaultTick)
		case <-ticker.C():
			run(false)
			ticker.Reset(defaultTick)
		}
	}
}

func (ms *MetadataStore) TriggerReconciliation() {
	// TODO (ajr) Make this an endpoint and forward to the leader
	select {
	case ms.reconcileShardsC <- struct{}{}:
	default:
	}
}

type (
	ShardDebugList = []*ShardDebugItem
	ShardDebugItem struct {
		ShardID types.ID         `json:"shard_id"`
		Info    *store.ShardInfo `json:"shard_info"`
	}
)

// checkShardAssignments ensures all shards are properly assigned to nodes
func (ms *MetadataStore) checkShardAssignments(ctx context.Context) {
	// Check context before accessing Pebble to avoid logging errors after shutdown
	select {
	case <-ctx.Done():
		return
	default:
	}

	desiredShards, err := ms.tm.GetShardStatuses()
	if err != nil {
		// Don't log errors if context is cancelled or Pebble is closed during shutdown
		select {
		case <-ctx.Done():
			return
		default:
			// Check if error is due to closed database (common during shutdown)
			if errors.Is(err, pebble.ErrClosed) {
				return
			}
			ms.logger.Error("Failed to get shard statuses", zap.Error(err))
		}
		return
	}

	// Collect current shard assignments from node statuses
	currentStores := []types.ID{}
	removedStores, err := ms.tm.GetStoreTombstones(ctx)
	if err != nil {
		ms.logger.Warn("Failed to get tombstoned stores", zap.Error(err))
	}
	removedStoresSet := make(map[types.ID]struct{}, len(removedStores))
	for _, id := range removedStores {
		removedStoresSet[id] = struct{}{}
	}

	tablesMap, err := ms.tm.TablesMap()
	if err != nil {
		ms.logger.Warn("Failed to get tables", zap.Error(err))
	}
	currentShards := make(map[types.ID]*store.ShardInfo)
	err = ms.tm.RangeStoreStatuses(func(peerID types.ID, storeStatus *tablemgr.StoreStatus) bool {
		if _, ok := removedStoresSet[peerID]; !ok {
			currentStores = append(currentStores, peerID)
		}
		for shardID, shardInfo := range storeStatus.Shards {
			if _, ok := currentShards[shardID]; !ok {
				// Build the reconciler's current view from live store heartbeats, not
				// the desired metadata record. Seeding from desired state can preserve
				// stale split-readiness fields such as SplitReplayCaughtUp /
				// SplitCutoverReady and prevent split finalization from observing that
				// the child shard is actually ready.
				currentShards[shardID] = shardInfo.DeepCopy()
				currentShards[shardID].ReportedBy = make(map[types.ID]struct{})
				currentShards[shardID].Merge(peerID, shardInfo)
				continue
			}
			currentShards[shardID].Merge(peerID, shardInfo)
			if shardInfo.RaftStatus != nil && types.ID(shardInfo.RaftStatus.Lead) == peerID {
				// Split finalization is gated on the split-child leader being able to
				// serve traffic after replay. Using replica-wide AND semantics for these
				// fields can leave the parent stuck behind lagging followers even though
				// the serving leader is ready to take over. Keep the live reconciler view
				// aligned to leader-observed split readiness while still merging the rest
				// of the shard state conservatively.
				currentShards[shardID].HasSnapshot = shardInfo.HasSnapshot
				currentShards[shardID].Initializing = shardInfo.Initializing
				currentShards[shardID].SplitReplayRequired = shardInfo.SplitReplayRequired
				currentShards[shardID].SplitReplayCaughtUp = shardInfo.SplitReplayCaughtUp
				currentShards[shardID].SplitCutoverReady = shardInfo.SplitCutoverReady
				currentShards[shardID].SplitReplaySeq = shardInfo.SplitReplaySeq
				currentShards[shardID].SplitParentShardID = shardInfo.SplitParentShardID
			}
		}
		return true
	})
	if err != nil {
		ms.logger.Error("Failed to get store statuses", zap.Error(err))
		return
	}

	slices.Sort(currentStores)
	if len(currentStores) == 0 {
		ms.logger.Warn("No nodes available")
		return
	}

	select {
	case <-ctx.Done():
		ms.logger.Warn("Context cancelled while checking shard assignments")
		return
	default:
	}

	// Cleanup expired cooldowns and stale split-ready tracking before reconciliation
	ms.reconciler.CleanupExpiredCooldowns()
	ms.reconciler.CleanupStaleSplitReadyTracking(desiredShards)

	// Build state structs for reconciler
	current := reconciler.CurrentClusterState{
		Stores:        currentStores,
		Shards:        currentShards,
		RemovedStores: removedStoresSet,
		Tables:        tablesMap,
	}

	desired := reconciler.DesiredClusterState{
		Shards: desiredShards,
	}

	// Execute reconciliation
	if err := ms.reconciler.Reconcile(ctx, current, desired); err != nil {
		ms.logger.Error("Reconciliation failed", zap.Error(err))
		// Continue - reconciler logs individual operation failures
	}

	// Debug logging (with hash-based deduplication)
	ms.reconciler.LogDebugState(current, desired, &ms.prevDebugShardsHash)
}

func (ms *MetadataStore) getHealthyShardPeers(
	shardStatuses map[types.ID]*store.ShardStatus,
) (map[types.ID][]string, error) {
	// Add peer information for each shard in the table
	shardPeers := make(map[types.ID][]string, len(shardStatuses))
	for shardID, shardStatus := range shardStatuses {
		peerURLs := make([]string, 0, len(shardStatus.Peers))
		for _, peerID := range shardStatus.Peers.IDSlice() {
			if nodeStatus, err := ms.tm.GetStoreStatus(context.TODO(), peerID); err == nil &&
				nodeStatus.IsReachable() {
				peerURLs = append(peerURLs, nodeStatus.ApiURL)
			}
		}
		if len(peerURLs) == 0 {
			return nil, fmt.Errorf("no healthy peers found for shard ID: %v", shardID)
		}

		shardPeers[shardID] = peerURLs
	}

	return shardPeers, nil
}
