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
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/workerpool"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/sethvargo/go-retry"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// innerFanOutLimit caps goroutine concurrency for errgroup-based fan-outs
// that run inside pool tasks. Using the shared workerpool.Pool for these inner
// loops risks deadlock (outer workers block waiting for inner tasks that cannot
// start because the pool is full). A plain errgroup with this limit avoids
// both deadlock and unbounded goroutine creation for large shard counts.
const innerFanOutLimit = 24

func (ms *MetadataStore) leaderClientForShard(
	ctx context.Context,
	shardID types.ID,
) (client.StoreRPC, error) {
	// FIXME (ajr) This whole function should be moved to the table manager

	status, err := ms.tm.GetShardStatus(shardID)
	if err != nil || status == nil {
		return nil, fmt.Errorf("no status info available: %w", err)
	}
	if ms.shouldFallbackToMergeReceiver(status) {
		receiverShardID := types.ID(status.MergeState.GetReceiverShardId())
		return ms.leaderClientForShard(ctx, receiverShardID)
	}
	var nodeID types.ID
	if !ms.config.SwarmMode {
		// If this is a split-off shard that doesn't have data yet,
		// try to find the parent shard and route to that instead.
		// During a split, the parent shard still has all the data until
		// the split-off shard's snapshot transfer completes.
		// Check this first, before checking raft status, because a split-off
		// shard may have a raft group but no leader elected yet.
		if ms.shouldFallbackToParentShard(status) {
			parentShardID, err := ms.findParentShardForSplitOff(status)
			if err == nil {
				return ms.leaderClientForShard(ctx, parentShardID)
			}
		}
		if status.RaftStatus == nil {
			return nil, fmt.Errorf("no raft status available for shard: %w", client.ErrNoRaftStatus)
		} else if status.RaftStatus.Lead == 0 {
			return nil, ErrNoLeaderElected
		}
		nodeID = status.RaftStatus.Lead

		// Validate that the leader is still serving the shard. During shard
		// rebalancing, the stored Raft leader can point to a node whose current
		// store status no longer contains the shard (or never finished starting it).
		// In that case, fall back to another healthy replica that does report it.
		if len(status.ReportedBy) > 0 && !status.ReportedBy.Contains(nodeID) {
			for reportedNode := range status.ReportedBy {
				reportingClient, ok := ms.storeClientIfServingShard(ctx, reportedNode, shardID)
				if ok {
					return reportingClient, nil
				}
			}
			return nil, fmt.Errorf("leader %s has not reported shard %s and no other healthy node found", nodeID, shardID)
		}
	} else {
		// In swarm mode, prefer nodes that have actually reported the shard (ReportedBy)
		// over nodes that are just in the Raft voter config (Peers). This prevents
		// routing to nodes that haven't finished loading the shard yet.
		// We iterate through all candidates and return the first healthy one.
		peersToCheck := status.ReportedBy
		if len(peersToCheck) == 0 {
			peersToCheck = status.Peers
		}
		for peerID := range peersToCheck {
			reportingClient, ok := ms.storeClientIfServingShard(ctx, peerID, shardID)
			if ok {
				return reportingClient, nil
			}
		}
		// If no peers are known yet (e.g., table just created and store hasn't
		// reported the shard), fall back to any healthy registered store.
		// This handles the race condition between table creation and store registration.
		if len(peersToCheck) == 0 {
			var foundClient client.StoreRPC
			_ = ms.tm.RangeStoreStatuses(func(storeID types.ID, _ *tablemgr.StoreStatus) bool {
				c, reachable, err := ms.tm.GetStoreClient(ctx, storeID)
				if err == nil && reachable {
					foundClient = c
					return false // stop iteration
				}
				return true // continue
			})
			if foundClient != nil {
				return foundClient, nil
			}
		}
		return nil, fmt.Errorf("no healthy peer found for shard %s: %w", shardID, client.ErrNoHealthyPeer)
	}
	reportingClient, ok := ms.storeClientIfServingShard(ctx, nodeID, shardID)
	if ok {
		return reportingClient, nil
	}

	// Leader is not available (removed or unreachable). Fall back to any other
	// node that has reported having the shard.
	for reportedNode := range status.ReportedBy {
		if reportedNode == nodeID {
			continue // Already tried the leader
		}
		reportingClient, ok := ms.storeClientIfServingShard(ctx, reportedNode, shardID)
		if ok {
			return reportingClient, nil
		}
	}

	// Also try Peers if ReportedBy didn't work
	for peerID := range status.Peers {
		if peerID == nodeID || status.ReportedBy.Contains(peerID) {
			continue // Already tried
		}
		reportingClient, ok := ms.storeClientIfServingShard(ctx, peerID, shardID)
		if ok {
			return reportingClient, nil
		}
	}

	return nil, fmt.Errorf("leader %s is not currently serving shard %s and no fallback available", nodeID, shardID)
}

func (ms *MetadataStore) storeClientIfServingShard(
	ctx context.Context,
	nodeID types.ID,
	shardID types.ID,
) (client.StoreRPC, bool) {
	storeStatus, err := ms.tm.GetStoreStatus(ctx, nodeID)
	if err != nil || !storeStatus.IsReachable() {
		return nil, false
	}
	// Treat an explicit shard map as authoritative. If the node currently
	// reports some shards and this shard is absent, don't route to it.
	if len(storeStatus.Shards) > 0 {
		shardInfo, ok := storeStatus.Shards[shardID]
		if !ok {
			return nil, false
		}
		if !shardInfoCanServeRead(shardInfo) {
			return nil, false
		}
	}
	return storeStatus.StoreClient, true
}

func shardInfoCanServeRead(shardInfo *store.ShardInfo) bool {
	if shardInfo == nil || shardInfo.Initializing {
		return false
	}
	// Split children must remain behind parent fallback until they are fully
	// read-ready. Routing to a node that merely reports the shard but has not
	// crossed the split replay fence can surface transient misses after failover.
	if shardInfo.SplitParentShardID != 0 || shardInfo.SplitReplayRequired {
		return shardInfo.IsReadyForSplitReads()
	}
	return true
}

// isTransientShardError returns true if the error is a transient condition that
// may resolve with a brief retry (e.g., leader election in progress, shard initializing).
func isTransientShardError(err error) bool {
	if err == nil {
		return false
	}
	// Typed error checks via errors.Is. These match both:
	// - ResponseError from store client (via ResponseError.Is body/status matching)
	// - Local errors wrapped with the corresponding sentinel (e.g., ErrNoRaftStatus)
	return errors.Is(err, client.ErrNotLeader) ||
		errors.Is(err, ErrNoLeaderElected) ||
		errors.Is(err, client.ErrProposalDropped) ||
		errors.Is(err, client.ErrNoRaftStatus) ||
		errors.Is(err, client.ErrNoHealthyPeer) ||
		errors.Is(err, client.ErrShardNotReady) ||
		errors.Is(err, client.ErrNotFound) ||
		errors.Is(err, client.ErrShardInitializing) ||
		errors.Is(err, ErrShardInitializing) ||
		errors.Is(err, client.ErrKeyOutOfRange)
}

// shardRetryBackoff returns the standard backoff policy used by read-style shard-forwarding
// functions: exponential backoff starting at 100ms, capped at 1s, up to 10 retries.
func shardRetryBackoff() retry.Backoff {
	b := retry.WithMaxRetries(10, retry.NewExponential(100*time.Millisecond))
	return retry.WithCappedDuration(1*time.Second, b)
}

// writeShardRetryBackoff gives write paths more time than reads to ride out split
// cutover churn (parent out-of-range, child leader startup, transient proposal drops)
// without surfacing a 500 to the client.
func writeShardRetryBackoff() retry.Backoff {
	b := retry.WithMaxRetries(20, retry.NewExponential(100*time.Millisecond))
	return retry.WithCappedDuration(1*time.Second, b)
}

func (ms *MetadataStore) retryableWriteShardError(ctx context.Context, err error) error {
	if !isTransientShardError(err) {
		return err
	}
	if ms.pool != nil {
		ms.runHealthCheck(ctx)
	}
	return retry.RetryableError(err)
}

// leaderClientForShardWithEffectiveID returns the leader client and the effective shard ID
// to use for requests. For SplittingOff shards with no Raft status yet, this falls back
// to the parent PreSplit shard and returns the parent's shard ID. The effective shard ID
// should be used in all requests to the client.
// NOTE: This function falls back to parent for reads during splits. For writes, use
// leaderClientForShardNoFallback instead to avoid writing to a shard that will reject
// writes to the split-off range.
func (ms *MetadataStore) leaderClientForShardWithEffectiveID(
	ctx context.Context,
	shardID types.ID,
) (effectiveShardID types.ID, c client.StoreRPC, err error) {
	status, err := ms.tm.GetShardStatus(shardID)
	if err != nil || status == nil {
		return 0, nil, fmt.Errorf("no status info available: %w", err)
	}

	// Check if this is a shard in split transition that doesn't have data yet.
	// During split, the new shard transitions: SplittingOff -> SplitOffPreSnap -> Default
	// Until HasSnapshot is true (data has been transferred), reads should go to parent shard.
	// The parent shard is in PreSplit or Splitting state and still has all the data.
	if ms.shouldFallbackToMergeReceiver(status) {
		receiverShardID := types.ID(status.MergeState.GetReceiverShardId())
		return ms.leaderClientForShardWithEffectiveID(ctx, receiverShardID)
	}
	if ms.shouldFallbackToParentShard(status) {
		parentShardID, err := ms.findParentShardForSplitOff(status)
		if err == nil {
			ms.logger.Debug("SPLIT_ROUTING: Falling back to parent shard",
				zap.Stringer("originalShardID", shardID),
				zap.Stringer("parentShardID", parentShardID),
				zap.String("state", status.State.String()),
				zap.Bool("hasSnapshot", status.HasSnapshot))
			// Recursively get client for parent shard, returning parent's shard ID
			return ms.leaderClientForShardWithEffectiveID(ctx, parentShardID)
		}
		// If we can't find parent, fall through to try the shard directly
		// This can happen during state transitions
		ms.logger.Warn("SPLIT_ROUTING: Failed to find parent shard for fallback, trying shard directly",
			zap.Stringer("shardID", shardID),
			zap.String("state", status.State.String()),
			zap.Bool("hasSnapshot", status.HasSnapshot),
			zap.Error(err))
	}

	// Get client using regular logic
	c, err = ms.leaderClientForShard(ctx, shardID)
	if err != nil {
		// No leader elected yet (e.g., during re-election after node removal).
		// Since this is a read path, fall back to any node that has reported
		// having the shard — reads can be served by any replica with the data.
		// Note: these reads may be slightly stale if the follower is behind.
		// This fallback only applies in non-SwarmMode; in SwarmMode,
		// leaderClientForShard uses peer-any routing and never returns
		// ErrNoLeaderElected.
		// The status snapshot used here was fetched at the top of this function
		// and may be slightly stale relative to leaderClientForShard's inner
		// fetch, but this is safe — unhealthy nodes fail the reachable check.
		if errors.Is(err, ErrNoLeaderElected) {
			// Try ReportedBy nodes first (confirmed to have the shard data)
			for reportedNode := range status.ReportedBy {
				if c, ok := ms.storeClientIfServingShard(ctx, reportedNode, shardID); ok {
					return shardID, c, nil
				}
			}
			// Fall back to Raft voter peers if no ReportedBy node is available
			for peerID := range status.Peers {
				if status.ReportedBy.Contains(peerID) {
					continue // Already tried above
				}
				if c, ok := ms.storeClientIfServingShard(ctx, peerID, shardID); ok {
					return shardID, c, nil
				}
			}
		}
		return 0, nil, err
	}
	return shardID, c, nil
}

// ErrNoLeaderElected is returned when a shard's Raft group has no leader (e.g., during
// re-election after node removal). Callers should retry after a delay.
var ErrNoLeaderElected = errors.New("no leader elected for shard")

// strictLeaderClientForShardWithEffectiveID is like leaderClientForShardWithEffectiveID
// but does NOT fall back to follower nodes. It contacts the actual Raft leader or
// returns an error. Use this for operations that require leader consistency, such as
// OCC version validation where a stale read from a follower could cause a missed conflict.
func (ms *MetadataStore) strictLeaderClientForShardWithEffectiveID(
	ctx context.Context,
	shardID types.ID,
) (effectiveShardID types.ID, c client.StoreRPC, err error) {
	status, err := ms.tm.GetShardStatus(shardID)
	if err != nil || status == nil {
		return 0, nil, fmt.Errorf("no status info available: %w", err)
	}

	// Handle split fallback (same as leaderClientForShardWithEffectiveID)
	if ms.shouldFallbackToParentShard(status) {
		parentShardID, err := ms.findParentShardForSplitOff(status)
		if err == nil {
			return ms.strictLeaderClientForShardWithEffectiveID(ctx, parentShardID)
		}
		// If we can't find parent, fall through to try the shard directly.
		// This can happen during state transitions.
		ms.logger.Warn("SPLIT_ROUTING: strict path failed to find parent shard, trying shard directly",
			zap.Stringer("shardID", shardID),
			zap.String("state", status.State.String()),
			zap.Bool("hasSnapshot", status.HasSnapshot),
			zap.Error(err))
	}

	// Strict leader lookup: only return the actual Raft leader, never a follower.
	// Unlike leaderClientForShard, this has NO fallback to ReportedBy/Peers when
	// the leader is unreachable or hasn't reported the shard.
	if status.RaftStatus == nil {
		return 0, nil, fmt.Errorf("no raft status available for shard: %w", client.ErrNoRaftStatus)
	}
	if status.RaftStatus.Lead == 0 {
		return 0, nil, ErrNoLeaderElected
	}
	c, reachable, err := ms.tm.GetStoreClient(ctx, status.RaftStatus.Lead)
	if err != nil {
		return 0, nil, fmt.Errorf("leader %s not found: %w", status.RaftStatus.Lead, err)
	}
	if !reachable {
		return 0, nil, ErrNoLeaderElected
	}
	return shardID, c, nil
}

// ErrShardInitializing is returned when a shard is still initializing and can't accept writes yet.
// The caller should retry after a delay.
var ErrShardInitializing = errors.New("shard is initializing, retry later")

func splitChildCanServeWrites(info *store.ShardInfo) bool {
	return info != nil && info.CanInitiateSplitCutover()
}

func splitChildIsFullyCutOver(info *store.ShardInfo) bool {
	return info != nil && info.IsReadyForSplitReads()
}

// leaderClientForShardNoFallback returns the leader client for write operations.
// Unlike leaderClientForShardWithEffectiveID, this does NOT fall back to the parent shard
// during splits. Instead, it returns ErrShardInitializing if the shard isn't ready.
// This prevents writes from going to a parent shard that has narrowed its range.
func (ms *MetadataStore) leaderClientForShardNoFallback(
	ctx context.Context,
	shardID types.ID,
) (effectiveShardID types.ID, c client.StoreRPC, err error) {
	status, err := ms.tm.GetShardStatus(shardID)
	if err != nil || status == nil {
		return 0, nil, fmt.Errorf("no status info available: %w", err)
	}

	// If shard is in split transition without data, return error to trigger retry
	// Don't fall back to parent because parent's range has been narrowed and will reject
	if ms.shouldFallbackToMergeReceiver(status) {
		receiverShardID := types.ID(status.MergeState.GetReceiverShardId())
		return ms.leaderClientForShardNoFallback(ctx, receiverShardID)
	}
	isSplitChild := status.State == store.ShardState_SplittingOff ||
		status.State == store.ShardState_SplitOffPreSnap
	if isSplitChild {
		if !status.CanInitiateSplitCutover() {
			ms.logger.Debug("SPLIT_ROUTING: Split child not ready for writes yet, returning retry error",
				zap.Stringer("shardID", shardID),
				zap.String("state", status.State.String()),
				zap.Bool("hasSnapshot", status.HasSnapshot))
			return 0, nil, ErrShardInitializing
		}
	}

	// For shards in split-related states, also check if leader is elected.
	// A shard may have snapshot but no leader yet during the window between
	// snapshot transfer and leader election. For non-split shards, let
	// leaderClientForShard handle the leader lookup (it has fallback logic).
	if isSplitChild && (status.RaftStatus == nil || status.RaftStatus.Lead == 0) {
		ms.logger.Debug("SPLIT_ROUTING: Split shard not ready for writes (no leader), returning retry error",
			zap.Stringer("shardID", shardID),
			zap.String("state", status.State.String()),
			zap.Bool("hasSnapshot", status.HasSnapshot),
			zap.Bool("hasRaftStatus", status.RaftStatus != nil))
		return 0, nil, ErrShardInitializing
	}
	if isSplitChild {
		if leaderClient, leaderErr := ms.selfReportedLeaderClientForShard(ctx, shardID, splitChildCanServeWrites); leaderErr == nil {
			return shardID, leaderClient, nil
		}

		var reachable bool
		c, reachable, err = ms.tm.GetStoreClient(ctx, status.RaftStatus.Lead)
		if err == nil && reachable {
			return shardID, c, nil
		}
		return 0, nil, ErrNoLeaderElected
	}

	// Get client using regular logic
	c, err = ms.leaderClientForShard(ctx, shardID)
	if err != nil {
		return 0, nil, err
	}
	return shardID, c, nil
}

// directLeaderClientForShardBypassesSplitFallback returns the shard's own leader
// without applying split-parent fallback. This is only used as a recovery path
// when stale metadata routed a write to the parent shard and that parent now
// rejects the key as out of range.
//
// The child must still be fully cut over before we write to it directly. If we
// bypass readiness checks and write into a child that has not yet caught up to
// the parent's current replay fence, that write can sit outside the parent's
// replay stream and disappear when the split finishes.
func (ms *MetadataStore) directLeaderClientForShardBypassesSplitFallback(
	ctx context.Context,
	shardID types.ID,
) (effectiveShardID types.ID, c client.StoreRPC, err error) {
	status, err := ms.tm.GetShardStatus(shardID)
	if err != nil || status == nil {
		return 0, nil, fmt.Errorf("no status info available: %w", err)
	}
	isSplitChild := status.State == store.ShardState_SplittingOff ||
		status.State == store.ShardState_SplitOffPreSnap
	if isSplitChild && !status.IsReadyForSplitReads() {
		return 0, nil, ErrShardInitializing
	}
	if status.RaftStatus == nil {
		return 0, nil, fmt.Errorf("no raft status available for shard: %w", client.ErrNoRaftStatus)
	}
	if status.RaftStatus.Lead == 0 {
		return 0, nil, ErrShardInitializing
	}
	readinessFilter := splitChildIsFullyCutOver
	if !isSplitChild {
		readinessFilter = nil
	}
	if leaderClient, leaderErr := ms.selfReportedLeaderClientForShard(ctx, shardID, readinessFilter); leaderErr == nil {
		return shardID, leaderClient, nil
	}

	var reachable bool
	c, reachable, err = ms.tm.GetStoreClient(ctx, status.RaftStatus.Lead)
	if err == nil && reachable {
		return shardID, c, nil
	}
	return 0, nil, ErrNoLeaderElected
}

func (ms *MetadataStore) selfReportedLeaderClientForShard(
	ctx context.Context,
	shardID types.ID,
	readinessFilter func(*store.ShardInfo) bool,
) (client.StoreRPC, error) {
	status, err := ms.tm.GetShardStatus(shardID)
	if err != nil || status == nil {
		return nil, fmt.Errorf("no status info available: %w", err)
	}

	candidates := status.ReportedBy
	if len(candidates) == 0 {
		candidates = status.Peers
	}
	for peerID := range candidates {
		storeStatus, err := ms.tm.GetStoreStatus(ctx, peerID)
		if err != nil || storeStatus == nil || !storeStatus.IsReachable() {
			continue
		}
		shardInfo := storeStatus.Shards[shardID]
		if shardInfo == nil || shardInfo.RaftStatus == nil || shardInfo.RaftStatus.Lead != peerID {
			continue
		}
		if readinessFilter != nil && !readinessFilter(shardInfo) {
			continue
		}
		c, reachable, err := ms.tm.GetStoreClient(ctx, peerID)
		if err == nil && reachable {
			return c, nil
		}
	}
	return nil, ErrNoLeaderElected
}

// shouldFallbackToParentShard returns true if reads to this shard should be
// redirected to its parent shard because the split-off shard is not fully ready.
// Readiness is determined by ShardInfo.IsReadyForSplitReads(), the single source
// of truth shared with tablemgr and the reconciler's FinalizeSplit guard.
func (ms *MetadataStore) shouldFallbackToParentShard(status *store.ShardStatus) bool {
	allStatuses, err := ms.tm.GetShardStatuses()
	if err == nil {
		if parentShardID, parentErr := findParentShardForSplitOffStatus(allStatuses, status); parentErr == nil {
			parentStatus := allStatuses[parentShardID]
			if parentStatus != nil &&
				parentStatus.SplitState != nil &&
				parentStatus.SplitState.GetPhase() == db.SplitState_PHASE_ROLLING_BACK {
				return true
			}
		}
	}
	switch status.State {
	case store.ShardState_SplittingOff, store.ShardState_SplitOffPreSnap:
		return !status.IsReadyForSplitReads()
	case store.ShardState_Default:
		if status.IsReadyForSplitReads() {
			return false
		}
		if err != nil {
			return false
		}
		_, err = findParentShardForSplitOffStatus(allStatuses, status)
		return err == nil
	default:
		return false
	}
}

func (ms *MetadataStore) shouldFallbackToMergeReceiver(status *store.ShardStatus) bool {
	if status == nil || status.MergeState == nil {
		return false
	}
	receiverShardID := types.ID(status.MergeState.GetReceiverShardId())
	if status.MergeState.GetPhase() != db.MergeState_PHASE_FINALIZING ||
		receiverShardID == 0 ||
		receiverShardID == status.ID {
		return false
	}
	allStatuses, err := ms.tm.GetShardStatuses()
	if err != nil {
		return false
	}
	receiverStatus := allStatuses[receiverShardID]
	return receiverStatus != nil && receiverStatus.IsReadyForMergeCutover()
}

// findParentShardForSplitOff finds the parent shard for a shard in SplittingOff or SplitOffPreSnap state.
// During a split, the parent shard's range ends at the split key, which is the same as
// the split-off shard's range start. The parent shard (in PreSplit or Splitting state) still
// has all the data until the split completes, so it can serve reads/writes for the split-off shard's range.
func (ms *MetadataStore) findParentShardForSplitOff(splitOffStatus *store.ShardStatus) (types.ID, error) {
	allStatuses, err := ms.tm.GetShardStatuses()
	if err != nil {
		return 0, err
	}
	return findParentShardForSplitOffStatus(allStatuses, splitOffStatus)
}

func (ms *MetadataStore) healthyPeerForShard(shardID types.ID) (client.StoreRPC, error) {
	// FIXME (ajr) This whole function should be moved to the table manager
	// Find a healthy peer for this shard to get the median key
	status, err := ms.tm.GetShardStatus(shardID)
	if err != nil || status == nil {
		return nil, fmt.Errorf("no status info available: %w", err)
	}
	// Prefer nodes that have actually reported the shard (ReportedBy)
	// over nodes that are just in the Raft voter config (Peers).
	peersToCheck := status.ReportedBy
	if len(peersToCheck) == 0 {
		peersToCheck = status.Peers
	}
	for peerID := range peersToCheck {
		client, reachable, err := ms.tm.GetStoreClient(context.TODO(), peerID)
		if reachable && err == nil {
			return client, nil
		}
	}
	return nil, fmt.Errorf("no healthy peer found: %w", client.ErrNoHealthyPeer)
}

func (ms *MetadataStore) rerouteBatchShardOnOutOfRange(
	shardID types.ID,
	writes [][2][]byte,
	deletes [][]byte,
	transforms []*db.Transform,
) (types.ID, error) {
	status, err := ms.tm.GetShardStatus(shardID)
	if err != nil || status == nil {
		return 0, fmt.Errorf("loading shard %s status: %w", shardID, err)
	}
	table, err := ms.tm.GetTable(status.Table)
	if err != nil {
		return 0, fmt.Errorf("loading table %s for shard %s: %w", status.Table, shardID, err)
	}
	shardStatuses, err := ms.tm.GetShardStatuses()
	if err != nil {
		return 0, fmt.Errorf("loading shard statuses for shard %s: %w", shardID, err)
	}

	var reroutedShardID types.ID
	assignShard := func(key []byte) error {
		targetShardID, err := findWriteShardForKeyWithStatuses(table, shardStatuses, string(key))
		if err != nil {
			return fmt.Errorf("resolving write owner for key %q: %w", string(key), err)
		}
		if reroutedShardID == 0 {
			reroutedShardID = targetShardID
			return nil
		}
		if reroutedShardID != targetShardID {
			return fmt.Errorf(
				"write batch for shard %s now spans shards %s and %s after reroute",
				shardID,
				reroutedShardID,
				targetShardID,
			)
		}
		return nil
	}

	for _, write := range writes {
		if err := assignShard(write[0]); err != nil {
			return 0, err
		}
	}
	for _, key := range deletes {
		if err := assignShard(key); err != nil {
			return 0, err
		}
	}
	for _, transform := range transforms {
		if err := assignShard(transform.GetKey()); err != nil {
			return 0, err
		}
	}

	if reroutedShardID == 0 {
		return shardID, nil
	}
	return reroutedShardID, nil
}

func (ms *MetadataStore) rerouteReadShardOnLookupMiss(
	shardID types.ID,
	key string,
) (types.ID, error) {
	status, err := ms.tm.GetShardStatus(shardID)
	if err != nil || status == nil {
		return 0, fmt.Errorf("loading shard %s status: %w", shardID, err)
	}
	table, err := ms.tm.GetTable(status.Table)
	if err != nil {
		return 0, fmt.Errorf("loading table %s for shard %s: %w", status.Table, shardID, err)
	}
	shardStatuses, err := ms.tm.GetShardStatuses()
	if err != nil {
		return 0, fmt.Errorf("loading shard statuses for shard %s: %w", shardID, err)
	}
	return findReadShardForKeyWithStatuses(table, shardStatuses, key)
}

// forwardBatchToShard sends a batch request to the leader node of a specific shard.
// Retries on transient errors (leader election, shard initialization) for both
// client lookup and the actual RPC call.
func (ms *MetadataStore) forwardBatchToShard(
	ctx context.Context,
	shardID types.ID,
	writes [][2][]byte,
	deletes [][]byte,
	transforms []*db.Transform,
	syncLevel db.Op_SyncLevel,
) error {
	backoff := writeShardRetryBackoff()
	targetShardID := shardID

	return retry.Do(ctx, backoff, func(ctx context.Context) error {
		// Use leaderClientForShardNoFallback for writes - don't fall back to parent
		// during splits since parent's byteRange has been narrowed
		effectiveShardID, leader, err := ms.leaderClientForShardNoFallback(ctx, targetShardID)
		if err != nil {
			return ms.retryableWriteShardError(ctx, err)
		}
		err = leader.Batch(ctx, effectiveShardID, writes, deletes, transforms, syncLevel)
		if errors.Is(err, client.ErrKeyOutOfRange) {
			reroutedShardID, rerouteErr := ms.rerouteBatchShardOnOutOfRange(targetShardID, writes, deletes, transforms)
			if rerouteErr == nil && reroutedShardID != targetShardID {
				targetShardID = reroutedShardID
			}
		}
		return ms.retryableWriteShardError(ctx, err)
	})
}

func (ms *MetadataStore) forwardBackupToShard(
	ctx context.Context,
	shardID types.ID,
	loc, id string,
) error {
	targetURL, err := ms.leaderClientForShard(ctx, shardID)
	if err != nil {
		return fmt.Errorf("failed to find leader for shard %s: %w", shardID, err)
	}
	return targetURL.Backup(ctx, shardID, loc, id)
}

// forwardLookupToShard sends a batch lookup request to the leader node of a specific shard.
// Retries on transient errors for both client lookup and the actual RPC call.
func (ms *MetadataStore) forwardLookupToShard(
	ctx context.Context,
	shardID types.ID,
	keys []string,
) (map[string][]byte, error) {
	backoff := shardRetryBackoff()

	var result map[string][]byte
	err := retry.Do(ctx, backoff, func(ctx context.Context) error {
		effectiveShardID, targetClient, err := ms.leaderClientForShardWithEffectiveID(ctx, shardID)
		if err != nil {
			if isTransientShardError(err) {
				return retry.RetryableError(err)
			}
			return err
		}
		result, err = targetClient.Lookup(ctx, effectiveShardID, keys)
		if err != nil && isTransientShardError(err) {
			return retry.RetryableError(err)
		}
		return err
	})
	return result, err
}

// forwardLookupToShardWithVersion sends a lookup request to the leader node of a specific shard
// and returns both the document bytes and the version timestamp from the X-Antfly-Version header.
func (ms *MetadataStore) forwardLookupToShardWithVersion(
	ctx context.Context,
	shardID types.ID,
	key string,
) ([]byte, uint64, error) {
	backoff := shardRetryBackoff()

	var result []byte
	var version uint64
	err := retry.Do(ctx, backoff, func(ctx context.Context) error {
		effectiveShardID, targetClient, err := ms.leaderClientForShardWithEffectiveID(ctx, shardID)
		if err != nil {
			if isTransientShardError(err) {
				return retry.RetryableError(err)
			}
			return err
		}
		result, version, err = targetClient.LookupWithVersion(ctx, effectiveShardID, key)
		// During split cutover, writes can start routing to the child as soon as it
		// is write-ready, while reads may still temporarily fall back to the parent
		// until the child crosses the final replay fence. If the parent reports a
		// miss or an out-of-range due to a narrowed parent byte range in that
		// window, probe the child directly before surfacing the error.
		if (errors.Is(err, client.ErrNotFound) || errors.Is(err, client.ErrKeyOutOfRange)) &&
			effectiveShardID != shardID {
			childShardID, childClient, childErr := ms.leaderClientForShardNoFallback(ctx, shardID)
			if childErr == nil {
				childResult, childVersion, childLookupErr := childClient.LookupWithVersion(ctx, childShardID, key)
				if childLookupErr == nil {
					result = childResult
					version = childVersion
					return nil
				}
				if isTransientShardError(childLookupErr) {
					return retry.RetryableError(childLookupErr)
				}
			} else if isTransientShardError(childErr) {
				return retry.RetryableError(childErr)
			}
		}
		// If the caller started on a stale parent shard, re-resolve the current
		// read owner from live split metadata before surfacing a miss or out-of-range.
		if (errors.Is(err, client.ErrNotFound) || errors.Is(err, client.ErrKeyOutOfRange)) &&
			effectiveShardID == shardID {
			reroutedShardID, rerouteErr := ms.rerouteReadShardOnLookupMiss(shardID, key)
			if rerouteErr == nil && reroutedShardID != shardID {
				reroutedEffectiveShardID, reroutedClient, reroutedClientErr := ms.leaderClientForShardWithEffectiveID(ctx, reroutedShardID)
				if reroutedClientErr == nil {
					reroutedResult, reroutedVersion, reroutedLookupErr := reroutedClient.LookupWithVersion(ctx, reroutedEffectiveShardID, key)
					if reroutedLookupErr == nil {
						result = reroutedResult
						version = reroutedVersion
						return nil
					}
					if isTransientShardError(reroutedLookupErr) {
						return retry.RetryableError(reroutedLookupErr)
					}
				} else if isTransientShardError(reroutedClientErr) {
					return retry.RetryableError(reroutedClientErr)
				}
			}
		}
		if err != nil && isTransientShardError(err) {
			return retry.RetryableError(err)
		}
		return err
	})
	return result, version, err
}

// forwardStrictLookupToShardWithVersion is like forwardLookupToShardWithVersion but
// requires leader consistency — it does NOT fall back to followers when no leader is
// elected. Use this for OCC version validation where a stale version from a follower
// could cause a missed conflict.
//
// Retry budget: at most 10 retries with exponential backoff (100ms base, 1s cap),
// bounding worst-case latency to ~10s per call. The caller's context provides an
// additional upper bound.
func (ms *MetadataStore) forwardStrictLookupToShardWithVersion(
	ctx context.Context,
	shardID types.ID,
	key string,
) ([]byte, uint64, error) {
	backoff := shardRetryBackoff()

	var result []byte
	var version uint64
	err := retry.Do(ctx, backoff, func(ctx context.Context) error {
		effectiveShardID, targetClient, err := ms.strictLeaderClientForShardWithEffectiveID(ctx, shardID)
		if err != nil {
			if isTransientShardError(err) {
				return retry.RetryableError(err)
			}
			return err
		}
		result, version, err = targetClient.LookupWithVersion(ctx, effectiveShardID, key)
		if err != nil && isTransientShardError(err) {
			return retry.RetryableError(err)
		}
		return err
	})
	return result, version, err
}

// forwardScanToShard sends a Scan request to the leader node of a specific shard.
// Retries on transient errors for both client lookup and the actual RPC call.
func (ms *MetadataStore) forwardScanToShard(
	ctx context.Context,
	shardID types.ID,
	fromKey []byte,
	toKey []byte,
	inclusiveFrom bool,
	exclusiveTo bool,
	includeDocuments bool,
	filterQuery json.RawMessage,
	limit int,
) (*db.ScanResult, error) {
	backoff := shardRetryBackoff()

	var result *db.ScanResult
	err := retry.Do(ctx, backoff, func(ctx context.Context) error {
		effectiveShardID, targetClient, err := ms.leaderClientForShardWithEffectiveID(ctx, shardID)
		if err != nil {
			if isTransientShardError(err) {
				return retry.RetryableError(err)
			}
			return fmt.Errorf("failed to find leader for shard %s: %w", shardID, err)
		}
		result, err = targetClient.Scan(ctx, effectiveShardID, fromKey, toKey, client.ScanOptions{
			InclusiveFrom:    inclusiveFrom,
			ExclusiveTo:      exclusiveTo,
			IncludeDocuments: includeDocuments,
			FilterQuery:      filterQuery,
			Limit:            limit,
		})
		if err != nil && isTransientShardError(err) {
			return retry.RetryableError(err)
		}
		return err
	})
	return result, err
}

// forwardResolveIntentToShard sends a ResolveIntent request to the leader node of a specific shard.
// Retries on transient errors for both client lookup and the actual RPC call.
func (ms *MetadataStore) forwardResolveIntentToShard(
	ctx context.Context,
	shardID types.ID,
	txnID []byte,
	status int32,
	commitVersion uint64,
) error {
	backoff := writeShardRetryBackoff()

	return retry.Do(ctx, backoff, func(ctx context.Context) error {
		// Use leaderClientForShardNoFallback for writes - don't fall back to parent
		// during splits since parent's byteRange has been narrowed
		effectiveShardID, targetClient, err := ms.leaderClientForShardNoFallback(ctx, shardID)
		if err != nil {
			err = fmt.Errorf("failed to find leader for shard %s: %w", shardID, err)
			return ms.retryableWriteShardError(ctx, err)
		}
		err = targetClient.ResolveIntent(ctx, effectiveShardID, txnID, status, commitVersion)
		return ms.retryableWriteShardError(ctx, err)
	})
}

// forwardInsertToShard sends an insert request to the leader node of a specific shard.
// Re-routes on each retry to pick up routing table changes during splits.
func (ms *MetadataStore) forwardInsertToShard(
	ctx context.Context,
	tableName string,
	key string,
	value map[string]any,
	syncLevel db.Op_SyncLevel,
) error {
	table, err := ms.tm.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("getting table %s: %w", tableName, err)
	}
	if err := validateDocumentInsertKey(table, key); err != nil {
		return fmt.Errorf("invalid document id %q: %w", key, err)
	}

	// Marshal value outside retry loop - this won't change between retries
	e, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshalling value for insert: %w", err)
	}
	writes := [][2][]byte{{[]byte(key), e}}

	backoff := writeShardRetryBackoff()

	return retry.Do(ctx, backoff, func(ctx context.Context) error {
		// Re-fetch table and route on each retry to pick up routing table changes during splits
		table, err := ms.tm.GetTable(tableName)
		if err != nil {
			return fmt.Errorf("getting table %s: %w", tableName, err)
		}
		shardID, err := findWriteShardForKey(ms.tm, table, key)
		if err != nil {
			if isTransientShardError(err) {
				return retry.RetryableError(err)
			}
			return fmt.Errorf("finding shard for key %s: %w", key, err)
		}

		// Use leaderClientForShardNoFallback for writes - don't fall back to parent
		// during splits since parent's byteRange has been narrowed
		effectiveShardID, targetClient, err := ms.leaderClientForShardNoFallback(ctx, shardID)
		if err != nil {
			return ms.retryableWriteShardError(ctx, err)
		}
		err = targetClient.Batch(ctx, effectiveShardID, writes, nil, nil, syncLevel)
		return ms.retryableWriteShardError(ctx, err)
	})
}

// forwardRangeScanToShards scans keys in a range across relevant shards.
// Returns a map of user-facing document IDs to their content hashes.
// Retries on transient errors for both client lookup and the actual RPC call.
func (ms *MetadataStore) forwardRangeScanToShards(
	ctx context.Context,
	tableName string,
	fromKey string,
	toKey string,
) (map[string]uint64, error) {
	table, err := ms.tm.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("getting table %s: %w", tableName, err)
	}

	// Find which shard(s) own this key range
	// For simplicity, we'll find the shard for fromKey and toKey
	// In a multi-shard scan, we'd need to query all shards in the range
	startShardID, err := table.FindShardForKey(fromKey)
	if err != nil {
		return nil, fmt.Errorf("finding shard for start key %s: %w", fromKey, err)
	}

	endShardID, err := table.FindShardForKey(toKey)
	if err != nil {
		// toKey might be a shard boundary (e.g., \xff), which doesn't belong to any shard
		// In this case, scan to the boundary of the shard containing fromKey
		endShardID = startShardID
	}

	// Collect all unique shard IDs we need to query
	shardIDs := make(map[types.ID]bool)
	shardIDs[startShardID] = true
	if endShardID != startShardID {
		shardIDs[endShardID] = true
		// TODO: In a production implementation, we'd need to find all shards
		// between startShardID and endShardID. For now, we assume the range
		// doesn't cross shard boundaries or only crosses one boundary.
	}

	backoff := shardRetryBackoff()

	// Aggregate results from all relevant shards
	result := make(map[string]uint64)
	for shardID := range shardIDs {
		var scanResult *db.ScanResult
		err := retry.Do(ctx, backoff, func(ctx context.Context) error {
			effectiveShardID, leaderClient, err := ms.leaderClientForShardWithEffectiveID(ctx, shardID)
			if err != nil {
				if isTransientShardError(err) {
					return retry.RetryableError(err)
				}
				return fmt.Errorf("finding leader for shard %s: %w", shardID, err)
			}

			// Call Scan on the effective shard (may be parent during split)
			// Use default options: exclusive lower bound, inclusive upper bound, hashes only
			scanResult, err = leaderClient.Scan(
				ctx,
				effectiveShardID,
				[]byte(fromKey),
				[]byte(toKey),
				client.ScanOptions{}, // defaults: (fromKey, toKey], hashes only
			)
			if err != nil && isTransientShardError(err) {
				return retry.RetryableError(err)
			}
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("scanning shard %s: %w", shardID, err)
		}

		// Merge results
		maps.Copy(result, scanResult.Hashes)
	}

	return result, nil
}

// forwardGetEdgesToShard sends a GetEdges request to the leader node of a specific shard.
// Retries on transient errors for both client lookup and the actual RPC call.
func (ms *MetadataStore) forwardGetEdgesToShard(
	ctx context.Context,
	shardID types.ID,
	indexName string,
	key string,
	edgeType string,
	direction string,
) ([]indexes.Edge, error) {
	backoff := shardRetryBackoff()

	var result []indexes.Edge
	err := retry.Do(ctx, backoff, func(ctx context.Context) error {
		effectiveShardID, targetClient, err := ms.leaderClientForShardWithEffectiveID(ctx, shardID)
		if err != nil {
			if isTransientShardError(err) {
				return retry.RetryableError(err)
			}
			return fmt.Errorf("failed to find leader for shard %s: %w", shardID, err)
		}
		result, err = targetClient.GetEdges(ctx, effectiveShardID, indexName, key, edgeType, indexes.EdgeDirection(direction))
		if err != nil && isTransientShardError(err) {
			return retry.RetryableError(err)
		}
		return err
	})
	return result, err
}

// broadcastGetIncomingEdgesToAllShards sends GetEdges request with direction=in to ALL shards
// and merges the results. This enables cross-shard incoming edge queries.
func (ms *MetadataStore) broadcastGetIncomingEdgesToAllShards(
	ctx context.Context,
	table *store.Table,
	indexName string,
	key string,
	edgeType string,
) ([]indexes.Edge, error) {
	type shardResult struct {
		shardID types.ID
		edges   []indexes.Edge
		err     error
	}
	// Use errgroup (not the shared pool) to avoid deadlock: this function is
	// called from within pool tasks during graph traversal (e.g.,
	// getEdgesAcrossShards → broadcastGetIncomingEdgesToAllShards).
	shardIDs := make([]types.ID, 0, len(table.Shards))
	for shardID := range table.Shards {
		shardIDs = append(shardIDs, shardID)
	}
	shardResults := make([]shardResult, len(shardIDs))
	eg := new(errgroup.Group)
	eg.SetLimit(innerFanOutLimit)
	for i, shardID := range shardIDs {
		eg.Go(func() error {
			edges, err := ms.forwardGetEdgesToShard(
				ctx, shardID, indexName, key, edgeType, "in",
			)
			shardResults[i] = shardResult{shardID: shardID, edges: edges, err: err}
			return nil
		})
	}
	_ = eg.Wait()

	// Merge results from all shards
	allEdges := []indexes.Edge{}
	var errs []error
	failedShards := []types.ID{}

	for _, result := range shardResults {
		if result.err != nil {
			errs = append(errs, result.err)
			failedShards = append(failedShards, result.shardID)
			continue
		}
		allEdges = append(allEdges, result.edges...)
	}

	// Log partial failures but don't fail the request if some shards succeeded
	if len(errs) > 0 {
		ms.logger.Warn("Partial failure in broadcast incoming edge query",
			zap.Int("failed_shards", len(errs)),
			zap.Int("successful_shards", len(table.Shards)-len(errs)),
			zap.String("key", key),
			zap.String("index", indexName),
			zap.Any("failed_shard_ids", failedShards),
			zap.Errors("errors", errs))

		// If ALL shards failed, return error
		if len(allEdges) == 0 {
			return nil, fmt.Errorf("all %d shards failed to query incoming edges: %w",
				len(table.Shards), errors.Join(errs...))
		}
	}

	// Deduplicate edges (in case of inconsistencies during edge updates)
	return deduplicateEdges(allEdges), nil
}

// deduplicateEdges removes duplicate edges based on (source, target, type) tuple.
// This is needed because during edge updates, the edge index might temporarily have
// inconsistencies that could result in duplicate results from different shards.
func deduplicateEdges(edges []indexes.Edge) []indexes.Edge {
	if len(edges) == 0 {
		return edges
	}

	// Use map to track unique edges
	seen := make(map[string]bool, len(edges))
	deduplicated := make([]indexes.Edge, 0, len(edges))

	for _, edge := range edges {
		// Create unique key from source, target, and type
		key := string(edge.Source) + "\x00" + string(edge.Target) + "\x00" + edge.Type
		if !seen[key] {
			seen[key] = true
			deduplicated = append(deduplicated, edge)
		}
	}

	return deduplicated
}

// findCrossShardShortestPath finds the shortest path between two nodes across multiple shards.
// It coordinates pathfinding across the distributed graph by iteratively expanding the frontier
// and querying edges from the appropriate shards.
func (ms *MetadataStore) findCrossShardShortestPath(
	ctx context.Context,
	table *store.Table,
	indexName string,
	source, target []byte,
	edgeTypes []string,
	direction indexes.EdgeDirection,
	weightMode indexes.PathWeightMode,
	maxDepth int,
	minWeight, maxWeight float64,
) (*indexes.Path, error) {
	// Validate inputs
	if len(source) == 0 || len(target) == 0 {
		return nil, fmt.Errorf("source and target must not be empty")
	}
	if maxDepth <= 0 {
		maxDepth = 50 // default max depth
	}

	// Early exit if source == target
	if string(source) == string(target) {
		return &indexes.Path{
			Nodes:       []string{base64.StdEncoding.EncodeToString(source)},
			Edges:       []indexes.PathEdge{},
			TotalWeight: 0.0,
			Length:      0,
		}, nil
	}

	// Choose algorithm based on weight mode
	switch weightMode {
	case indexes.PathWeightModeMinHops:
		return ms.bfsCrossShardShortestPath(ctx, table, indexName, source, target, edgeTypes, direction, maxDepth, minWeight, maxWeight)
	case indexes.PathWeightModeMaxWeight:
		return ms.dijkstraCrossShardMaxWeight(ctx, table, indexName, source, target, edgeTypes, direction, maxDepth, minWeight, maxWeight)
	case indexes.PathWeightModeMinWeight:
		return ms.dijkstraCrossShardMinWeight(ctx, table, indexName, source, target, edgeTypes, direction, maxDepth, minWeight, maxWeight)
	default:
		return nil, fmt.Errorf("invalid weight_mode: %s (must be min_hops, max_weight, or min_weight)", weightMode)
	}
}

// pathNode represents a node in the pathfinding algorithm with parent tracking
type pathNode struct {
	key        []byte
	distance   float64
	hops       int
	parent     *pathNode
	parentEdge indexes.Edge
}

// bfsCrossShardShortestPath implements cross-shard BFS for minimum hop count path
func (ms *MetadataStore) bfsCrossShardShortestPath(
	ctx context.Context,
	table *store.Table,
	indexName string,
	source, target []byte,
	edgeTypes []string,
	direction indexes.EdgeDirection,
	maxDepth int,
	minWeight, maxWeight float64,
) (*indexes.Path, error) {
	visited := make(map[string]bool)
	visited[string(source)] = true

	// Parent tracking for path reconstruction
	parent := make(map[string]*pathNode)
	parent[string(source)] = &pathNode{
		key:      source,
		distance: 0,
		hops:     0,
	}

	// BFS frontier
	frontier := [][]byte{source}
	targetStr := string(target)

	for depth := 0; depth < maxDepth && len(frontier) > 0; depth++ {
		// Check if target is in current frontier
		for _, node := range frontier {
			if string(node) == targetStr {
				return ms.reconstructPath(parent, target), nil
			}
		}

		// Expand frontier: get edges for all nodes in current frontier
		nextFrontier := make([][]byte, 0)

		for _, node := range frontier {
			// Get edges from appropriate shards based on direction
			edges, err := ms.getEdgesAcrossShards(ctx, table, indexName, string(node), "", direction)
			if err != nil {
				ms.logger.Warn("Failed to get edges during cross-shard BFS",
					zap.String("key", string(node)),
					zap.String("index", indexName),
					zap.Error(err))
				continue
			}

			// Process each edge
			for _, edge := range edges {
				// Filter by edge type
				if len(edgeTypes) > 0 && !slices.Contains(edgeTypes, edge.Type) {
					continue
				}

				// Filter by weight
				if edge.Weight < minWeight || edge.Weight > maxWeight {
					continue
				}

				// Determine neighbor based on direction
				var neighborKey []byte
				if direction == indexes.EdgeDirectionOut || direction == indexes.EdgeDirectionBoth {
					neighborKey = edge.Target
				} else {
					neighborKey = edge.Source
				}

				neighborStr := string(neighborKey)
				if !visited[neighborStr] {
					visited[neighborStr] = true
					parent[neighborStr] = &pathNode{
						key:        neighborKey,
						distance:   float64(depth + 1),
						hops:       depth + 1,
						parent:     parent[string(node)],
						parentEdge: edge,
					}
					nextFrontier = append(nextFrontier, neighborKey)
				}
			}
		}

		frontier = nextFrontier
	}

	return nil, fmt.Errorf("no path found between nodes")
}

// dijkstraCrossShardMinWeight implements cross-shard Dijkstra for minimum weight path
func (ms *MetadataStore) dijkstraCrossShardMinWeight(
	ctx context.Context,
	table *store.Table,
	indexName string,
	source, target []byte,
	edgeTypes []string,
	direction indexes.EdgeDirection,
	maxDepth int,
	minWeight, maxWeight float64,
) (*indexes.Path, error) {
	distances := make(map[string]float64)
	distances[string(source)] = 0

	visited := make(map[string]bool)
	parent := make(map[string]*pathNode)
	parent[string(source)] = &pathNode{
		key:      source,
		distance: 0,
		hops:     0,
	}

	// Priority queue: track nodes by distance
	type queueItem struct {
		key      []byte
		distance float64
		hops     int
	}
	pq := []queueItem{{key: source, distance: 0, hops: 0}}

	targetStr := string(target)

	for len(pq) > 0 {
		// Pop min distance node (simple linear scan, can optimize with heap)
		minIdx := 0
		for i := range pq {
			if pq[i].distance < pq[minIdx].distance {
				minIdx = i
			}
		}
		current := pq[minIdx]
		pq = append(pq[:minIdx], pq[minIdx+1:]...)

		currentStr := string(current.key)

		// Skip if already visited
		if visited[currentStr] {
			continue
		}
		visited[currentStr] = true

		// Check if we've reached the target
		if currentStr == targetStr {
			return ms.reconstructPath(parent, target), nil
		}

		// Check max depth
		if current.hops >= maxDepth {
			continue
		}

		// Get edges
		edges, err := ms.getEdgesAcrossShards(ctx, table, indexName, string(current.key), "", direction)
		if err != nil {
			ms.logger.Warn("Failed to get edges during cross-shard Dijkstra",
				zap.String("key", currentStr),
				zap.String("index", indexName),
				zap.Error(err))
			continue
		}

		// Process each edge
		for _, edge := range edges {
			// Filter by edge type
			if len(edgeTypes) > 0 && !slices.Contains(edgeTypes, edge.Type) {
				continue
			}

			// Filter by weight
			if edge.Weight < minWeight || edge.Weight > maxWeight {
				continue
			}

			// Determine neighbor
			var neighborKey []byte
			if direction == indexes.EdgeDirectionOut || direction == indexes.EdgeDirectionBoth {
				neighborKey = edge.Target
			} else {
				neighborKey = edge.Source
			}

			neighborStr := string(neighborKey)
			if visited[neighborStr] {
				continue
			}

			// Calculate new distance
			newDistance := current.distance + edge.Weight
			oldDistance, exists := distances[neighborStr]

			if !exists || newDistance < oldDistance {
				distances[neighborStr] = newDistance
				parent[neighborStr] = &pathNode{
					key:        neighborKey,
					distance:   newDistance,
					hops:       current.hops + 1,
					parent:     parent[currentStr],
					parentEdge: edge,
				}
				pq = append(pq, queueItem{
					key:      neighborKey,
					distance: newDistance,
					hops:     current.hops + 1,
				})
			}
		}
	}

	return nil, fmt.Errorf("no path found between nodes")
}

// dijkstraCrossShardMaxWeight implements cross-shard Dijkstra for maximum weight path
func (ms *MetadataStore) dijkstraCrossShardMaxWeight(
	ctx context.Context,
	table *store.Table,
	indexName string,
	source, target []byte,
	edgeTypes []string,
	direction indexes.EdgeDirection,
	maxDepth int,
	minWeight, maxWeight float64,
) (*indexes.Path, error) {
	// For max weight (product), we use negative log to convert to min problem
	distances := make(map[string]float64)
	distances[string(source)] = 0

	visited := make(map[string]bool)
	parent := make(map[string]*pathNode)
	parent[string(source)] = &pathNode{
		key:      source,
		distance: 0,
		hops:     0,
	}

	type queueItem struct {
		key      []byte
		distance float64
		hops     int
	}
	pq := []queueItem{{key: source, distance: 0, hops: 0}}

	targetStr := string(target)

	for len(pq) > 0 {
		// Pop min distance node
		minIdx := 0
		for i := range pq {
			if pq[i].distance < pq[minIdx].distance {
				minIdx = i
			}
		}
		current := pq[minIdx]
		pq = append(pq[:minIdx], pq[minIdx+1:]...)

		currentStr := string(current.key)

		if visited[currentStr] {
			continue
		}
		visited[currentStr] = true

		// Check if we've reached the target
		if currentStr == targetStr {
			return ms.reconstructPath(parent, target), nil
		}

		// Check max depth
		if current.hops >= maxDepth {
			continue
		}

		// Get edges
		edges, err := ms.getEdgesAcrossShards(ctx, table, indexName, string(current.key), "", direction)
		if err != nil {
			ms.logger.Warn("Failed to get edges during cross-shard max-weight Dijkstra",
				zap.String("key", currentStr),
				zap.String("index", indexName),
				zap.Error(err))
			continue
		}

		// Process each edge
		for _, edge := range edges {
			// Filter by edge type
			if len(edgeTypes) > 0 && !slices.Contains(edgeTypes, edge.Type) {
				continue
			}

			// Filter by weight (skip zero/negative for log)
			if edge.Weight < minWeight || edge.Weight > maxWeight || edge.Weight <= 0 {
				continue
			}

			// Determine neighbor
			var neighborKey []byte
			if direction == indexes.EdgeDirectionOut || direction == indexes.EdgeDirectionBoth {
				neighborKey = edge.Target
			} else {
				neighborKey = edge.Source
			}

			neighborStr := string(neighborKey)
			if visited[neighborStr] {
				continue
			}

			// Calculate new distance using -log(weight)
			newDistance := current.distance + (-math.Log(edge.Weight))
			oldDistance, exists := distances[neighborStr]

			if !exists || newDistance < oldDistance {
				distances[neighborStr] = newDistance
				parent[neighborStr] = &pathNode{
					key:        neighborKey,
					distance:   newDistance,
					hops:       current.hops + 1,
					parent:     parent[currentStr],
					parentEdge: edge,
				}
				pq = append(pq, queueItem{
					key:      neighborKey,
					distance: newDistance,
					hops:     current.hops + 1,
				})
			}
		}
	}

	return nil, fmt.Errorf("no path found between nodes")
}

// getEdgesAcrossShards retrieves edges for a given key, querying the appropriate shard(s)
// based on the edge direction
func (ms *MetadataStore) getEdgesAcrossShards(
	ctx context.Context,
	table *store.Table,
	indexName string,
	key string,
	edgeType string,
	direction indexes.EdgeDirection,
) ([]indexes.Edge, error) {
	switch direction {
	case indexes.EdgeDirectionIn:
		// Broadcast to all shards for incoming edges
		return ms.broadcastGetIncomingEdgesToAllShards(ctx, table, indexName, key, edgeType)
	case indexes.EdgeDirectionBoth:
		// Get outgoing from source shard + broadcast for incoming
		shardID, err := table.FindShardForKey(key)
		if err != nil {
			return nil, fmt.Errorf("finding shard for key: %w", err)
		}

		outgoing, err := ms.forwardGetEdgesToShard(ctx, shardID, indexName, key, edgeType, "out")
		if err != nil {
			return nil, fmt.Errorf("getting outgoing edges: %w", err)
		}

		incoming, err := ms.broadcastGetIncomingEdgesToAllShards(ctx, table, indexName, key, edgeType)
		if err != nil {
			return nil, fmt.Errorf("getting incoming edges: %w", err)
		}

		return append(outgoing, incoming...), nil
	default:
		// Direction is "out": query only the shard containing the source key
		shardID, err := table.FindShardForKey(key)
		if err != nil {
			return nil, fmt.Errorf("finding shard for key: %w", err)
		}

		return ms.forwardGetEdgesToShard(ctx, shardID, indexName, key, edgeType, "out")
	}
}

// reconstructPath builds the Path result from parent tracking map
func (ms *MetadataStore) reconstructPath(parent map[string]*pathNode, target []byte) *indexes.Path {
	// Trace back from target to source
	var nodes []string
	var edges []indexes.PathEdge

	current := parent[string(target)]
	for current != nil {
		nodes = append(nodes, base64.StdEncoding.EncodeToString(current.key))
		// Only add edge if this node has a parent (i.e., not the source node)
		if current.parent != nil {
			edges = append(edges, indexes.PathEdge{
				Source:   base64.StdEncoding.EncodeToString(current.parentEdge.Source),
				Target:   base64.StdEncoding.EncodeToString(current.parentEdge.Target),
				Type:     current.parentEdge.Type,
				Weight:   current.parentEdge.Weight,
				Metadata: current.parentEdge.Metadata,
			})
		}
		current = current.parent
	}

	// Reverse to get source -> target order
	slices.Reverse(nodes)
	slices.Reverse(edges)

	// Calculate total weight
	totalWeight := 0.0
	for _, edge := range edges {
		totalWeight += edge.Weight
	}

	return &indexes.Path{
		Nodes:       nodes,
		Edges:       edges,
		TotalWeight: totalWeight,
		Length:      len(edges),
	}
}

func (ms *MetadataStore) dropIndexFromTable(
	ctx context.Context,
	tableName, indexName string,
) error {
	table, err := ms.tm.DropIndex(tableName, indexName)
	if err != nil {
		return err
	}
	g, _ := workerpool.NewGroup(ctx, ms.pool)
	for shardID := range table.Shards {
		g.Go(func(ctx context.Context) error {
			if err := retry.Fibonacci(ctx, 5*time.Second, func(ctx context.Context) error {
				leaderClient, err := ms.leaderClientForShard(ctx, shardID)
				if err != nil {
					return fmt.Errorf("finding leader: %w", err)
				}
				return leaderClient.DropIndex(ctx, shardID, indexName)
			}); err != nil {
				if !errors.Is(err, context.Canceled) {
					ms.logger.Error(
						"failed retrying dropping index to shard",
						zap.String("index", indexName),
						zap.String("shardID", shardID.String()),
						zap.Error(err),
					)
				}
				return nil
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		if !errors.Is(err, context.Canceled) {
			ms.logger.Error(
				"failed to drop index from table",
				zap.String("tableName", tableName),
				zap.String("indexName", indexName),
				zap.Error(err),
			)
		}
		return fmt.Errorf("dropping index from table: %w", err)
	}
	return nil
}

func (ms *MetadataStore) addIndexToTable(
	ctx context.Context,
	tableName, indexName string,
	config indexes.IndexConfig,
) error {
	table, err := ms.tm.CreateIndex(tableName, indexName, config)
	if err != nil {
		return err
	}

	// Get the updated config from the table (includes populated chunking_config)
	updatedConfig, ok := table.Indexes[indexName]
	if !ok {
		return fmt.Errorf("index %s not found in table after creation", indexName)
	}

	g, _ := workerpool.NewGroup(ctx, ms.pool)
	for shardID := range table.Shards {
		g.Go(func(ctx context.Context) error {
			if err := retry.Fibonacci(ctx, 5*time.Second, func(ctx context.Context) error {
				leaderClient, err := ms.leaderClientForShard(ctx, shardID)
				if err != nil {
					return fmt.Errorf("finding leader: %w", err)
				}
				return leaderClient.AddIndex(ctx, shardID, indexName, &updatedConfig)
			}); err != nil {
				if !errors.Is(err, context.Canceled) {
					ms.logger.Error(
						"failed retrying adding index to shard",
						zap.String("index", indexName),
						zap.String("shardID", shardID.String()),
						zap.Error(err),
					)
				}
				return nil
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		if !errors.Is(err, context.Canceled) {
			ms.logger.Error(
				"failed to add index to table",
				zap.String("tableName", tableName),
				zap.String("indexName", indexName),
				zap.Error(err),
			)
		}
		return fmt.Errorf("adding index to table: %w", err)
	}
	return nil
}
