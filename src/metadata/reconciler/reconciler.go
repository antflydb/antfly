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
	"bytes"
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antflydb/antfly/lib/types"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/cespare/xxhash/v2"
	"go.uber.org/zap"
)

// ============================================================================
// RECONCILER - Main orchestrator
// ============================================================================

// Reconciler orchestrates cluster reconciliation
type Reconciler struct {
	shardOps ShardOperations
	tableOps TableOperations
	storeOps StoreOperations
	config   ReconciliationConfig
	logger   *zap.Logger

	// Cooldown tracking to prevent rapid changes
	cooldownMu           sync.RWMutex
	shardCooldown        map[types.ID]time.Time
	shardForNodeCooldown map[string]time.Time

	// splitReadySince tracks when a split-off shard first became ready
	// (HasSnapshot && !Initializing && hasLeader). FinalizeSplit is deferred
	// until the shard has been continuously ready for SplitFinalizeGracePeriod,
	// ensuring the new shard's leader is stable before parent data is deleted.
	splitReadySince map[types.ID]time.Time

	// Time abstraction for testing
	timeProvider TimeProvider
}

// NewReconciler creates a new reconciler with real time provider
func NewReconciler(
	shardOps ShardOperations,
	tableOps TableOperations,
	storeOps StoreOperations,
	config ReconciliationConfig,
	logger *zap.Logger,
) *Reconciler {
	return NewReconcilerWithTimeProvider(
		shardOps,
		tableOps,
		storeOps,
		config,
		logger,
		RealTimeProvider{},
	)
}

// NewReconcilerWithTimeProvider creates a new reconciler with custom time provider (for testing)
func NewReconcilerWithTimeProvider(
	shardOps ShardOperations,
	tableOps TableOperations,
	storeOps StoreOperations,
	config ReconciliationConfig,
	logger *zap.Logger,
	timeProvider TimeProvider,
) *Reconciler {
	return &Reconciler{
		shardOps:             shardOps,
		tableOps:             tableOps,
		storeOps:             storeOps,
		config:               config,
		logger:               logger,
		shardCooldown:        make(map[types.ID]time.Time),
		shardForNodeCooldown: make(map[string]time.Time),
		splitReadySince:      make(map[types.ID]time.Time),
		timeProvider:         timeProvider,
	}
}

// getCooldownDuration returns the configured cooldown period, or a default of 1 minute
func (r *Reconciler) getCooldownDuration() time.Duration {
	if r.config.ShardCooldownPeriod > 0 {
		return r.config.ShardCooldownPeriod
	}
	return time.Minute // default
}

// SetShardCooldown sets a cooldown period for a shard
func (r *Reconciler) SetShardCooldown(shardID types.ID, duration time.Duration) {
	r.cooldownMu.Lock()
	defer r.cooldownMu.Unlock()
	r.shardCooldown[shardID] = r.timeProvider.Now().Add(duration)
}

// IsShardInCooldown checks if a shard is in cooldown
func (r *Reconciler) IsShardInCooldown(shardID types.ID) bool {
	r.cooldownMu.Lock()
	defer r.cooldownMu.Unlock()
	if cooldownEnd, ok := r.shardCooldown[shardID]; ok {
		if r.timeProvider.Now().Before(cooldownEnd) {
			return true
		}
		delete(r.shardCooldown, shardID) // Clean up expired entry
	}
	return false
}

// SetShardForNodeCooldown sets a cooldown period for a shard on a specific node
func (r *Reconciler) SetShardForNodeCooldown(
	shardID types.ID,
	nodeID types.ID,
	duration time.Duration,
) {
	r.cooldownMu.Lock()
	defer r.cooldownMu.Unlock()
	key := shardID.String() + nodeID.String()
	r.shardForNodeCooldown[key] = r.timeProvider.Now().Add(duration)
}

// IsShardForNodeInCooldown checks if a shard is in cooldown for a specific node
func (r *Reconciler) IsShardForNodeInCooldown(shardID types.ID, nodeID types.ID) bool {
	r.cooldownMu.Lock()
	defer r.cooldownMu.Unlock()
	key := shardID.String() + nodeID.String()
	if cooldownEnd, ok := r.shardForNodeCooldown[key]; ok {
		if r.timeProvider.Now().Before(cooldownEnd) {
			return true
		}
		delete(r.shardForNodeCooldown, key) // Clean up expired entry
	}
	return false
}

// CleanupExpiredCooldowns removes all expired cooldown entries
// Call this periodically to prevent memory growth
func (r *Reconciler) CleanupExpiredCooldowns() {
	r.cooldownMu.Lock()
	defer r.cooldownMu.Unlock()
	now := r.timeProvider.Now()

	for shardID, expiry := range r.shardCooldown {
		if now.After(expiry) {
			delete(r.shardCooldown, shardID)
		}
	}

	for key, expiry := range r.shardForNodeCooldown {
		if now.After(expiry) {
			delete(r.shardForNodeCooldown, key)
		}
	}
}

// CleanupStaleSplitReadyTracking removes splitReadySince entries for shard IDs
// that no longer appear in the desired shard set. Call this periodically alongside
// CleanupExpiredCooldowns to prevent unbounded map growth from abandoned splits.
func (r *Reconciler) CleanupStaleSplitReadyTracking(activeShards map[types.ID]*store.ShardStatus) {
	for shardID := range r.splitReadySince {
		if _, exists := activeShards[shardID]; !exists {
			delete(r.splitReadySince, shardID)
		}
	}
}

// Reconcile performs a full reconciliation cycle
func (r *Reconciler) Reconcile(
	ctx context.Context,
	current CurrentClusterState,
	desired DesiredClusterState,
) error {
	// Compute what needs to be done
	plan := r.ComputePlan(ctx, current, desired)

	// Execute the plan
	return r.ExecutePlan(ctx, plan, current, desired)
}

// ComputePlan computes a reconciliation plan (pure decision-making)
func (r *Reconciler) ComputePlan(
	ctx context.Context,
	current CurrentClusterState,
	desired DesiredClusterState,
) *ReconciliationPlan {
	plan := &ReconciliationPlan{}

	// 1. Identify tombstoned stores that still need peer removal
	plan.RemovedStorePeerRemovals = r.computeRemovedStorePeerRemovals(current)

	// 2. If we need to remove peers from tombstoned stores, do that first
	if len(plan.RemovedStorePeerRemovals) > 0 {
		plan.SkipRemainingSteps = true
		return plan
	}

	// 3. Identify tombstones that can be deleted (no longer voters)
	plan.TombstoneDeletions = r.computeTombstoneDeletions(current)

	// 3. Compute raft voter fixes
	plan.RaftVoterFixes = r.computeRaftVoterFixes(desired)

	// 4. Compute split/merge state advancements
	plan.SplitStateActions = r.computeSplitStateActions(current, desired)
	if len(plan.SplitStateActions) > 0 {
		plan.HasUnreconciledSplitOrMerge = true
		plan.SkipRemainingSteps = true
		return plan
	}

	// 5. Compute index operations
	plan.IndexOperations = r.computeIndexOperations(current, desired)

	// 5.5. Check for completed index rebuilds
	plan.IndexRebuildCompletions = r.computeIndexRebuildCompletions(current)

	// 6. Check for manual reallocation request
	hasReallocReq, err := r.tableOps.HasReallocationRequest(ctx)
	if err != nil {
		r.logger.Warn("Failed to check for reallocation request", zap.Error(err))
		hasReallocReq = false
	}

	// 7. Compute split and merge transitions if shard allocation is enabled or forced
	if !r.config.DisableShardAlloc || hasReallocReq {
		plan.SplitTransitions, plan.MergeTransitions = r.computeSplitAndMergeTransitions(
			ctx,
			desired,
		)
		if hasReallocReq {
			plan.ForcedReallocation = true
			plan.ClearReallocationRequest = true
		}
		if len(plan.SplitTransitions) > 0 || len(plan.MergeTransitions) > 0 {
			plan.SkipRemainingSteps = true
			return plan
		}
	}

	// 8. Compute ideal shard assignments
	idealShards := IdealShardAssignmentsV2(
		r.config.ReplicationFactor,
		current.Stores,
		desired.Shards,
		current.Shards,
	)

	// 8. Compute shard placement transitions
	plan.ShardTransitions = CreateShardTransitionPlan(current.Shards, idealShards)

	return plan
}

// ExecutePlan executes a reconciliation plan
// Implementation is in executor.go

// ============================================================================
// PURE DECISION FUNCTIONS - No I/O, fully testable
// ============================================================================

// computeRemovedStorePeerRemovals identifies tombstoned stores that are still voters in shards
// These stores need to be removed from shard peer lists before deletion
func (r *Reconciler) computeRemovedStorePeerRemovals(current CurrentClusterState) []types.ID {
	removalsNeeded := []types.ID{}

	for removedStore := range current.RemovedStores {
		// Check if this tombstoned store is still a voter in any shard
		for _, shard := range current.Shards {
			if shard != nil && shard.RaftStatus != nil &&
				shard.RaftStatus.Voters.Contains(removedStore) {
				removalsNeeded = append(removalsNeeded, removedStore)
				break // Only add once per store
			}
		}
	}

	return removalsNeeded
}

// computeTombstoneDeletions identifies tombstones that can be safely deleted
// These are tombstoned stores that are no longer voters in any shard
func (r *Reconciler) computeTombstoneDeletions(current CurrentClusterState) []types.ID {
	deletions := []types.ID{}

	for removedStore := range current.RemovedStores {
		hasVoters := false

		// Check if this tombstoned store is still in any raft voter groups
		for _, shard := range current.Shards {
			if shard != nil && shard.RaftStatus != nil &&
				shard.RaftStatus.Voters.Contains(removedStore) {
				hasVoters = true
				break
			}
		}

		if !hasVoters {
			deletions = append(deletions, removedStore)
		}
	}

	return deletions
}

// computeRaftVoterFixes computes fixes needed for raft voter membership
func (r *Reconciler) computeRaftVoterFixes(desired DesiredClusterState) []RaftVoterFix {
	fixes := []RaftVoterFix{}

	for shardID, shardStatus := range desired.Shards {
		if shardStatus == nil || shardStatus.RaftStatus == nil ||
			len(shardStatus.RaftStatus.Voters) == 0 {
			continue
		}

		// Check if voters match peers
		if !shardStatus.RaftStatus.Voters.Equal(shardStatus.Peers) {
			for peer := range shardStatus.Peers {
				if !shardStatus.RaftStatus.Voters.Contains(peer) {
					// Peer is running but not in voters - should be stopped
					fixes = append(fixes, RaftVoterFix{
						ShardID: shardID,
						PeerID:  peer,
						Action:  RaftVoterActionStopShard,
					})
				}
			}
		}
	}

	return fixes
}

// getSplitTimeout returns the configured split timeout or the default (5 minutes)
func (r *Reconciler) getSplitTimeout() time.Duration {
	if r.config.SplitTimeout > 0 {
		return r.config.SplitTimeout
	}
	return 5 * time.Minute
}

func (r *Reconciler) getSplitFinalizeGracePeriod() time.Duration {
	if r.config.SplitFinalizeGracePeriod > 0 {
		return r.config.SplitFinalizeGracePeriod
	}
	return 15 * time.Second
}

func (r *Reconciler) getMinShardSizeBytes() uint64 {
	if r.config.MinShardSizeBytes > 0 {
		return r.config.MinShardSizeBytes
	}
	if r.config.MaxShardSizeBytes == 0 {
		return 0
	}
	return max(r.config.MaxShardSizeBytes/4, 1)
}

func (r *Reconciler) getMinShardsPerTable() uint64 {
	if r.config.MinShardsPerTable > 0 {
		return r.config.MinShardsPerTable
	}
	return 1
}

func (r *Reconciler) getAutoRangeTransitionPerTableLimit() int {
	if r.config.AutoRangeTransitionPerTableLimit > 0 {
		return r.config.AutoRangeTransitionPerTableLimit
	}
	return 1
}

func (r *Reconciler) getAutoRangeTransitionClusterLimit() int {
	if r.config.AutoRangeTransitionClusterLimit > 0 {
		return r.config.AutoRangeTransitionClusterLimit
	}
	return 1
}

// trackAndCheckSplitFinalizeReady updates readiness tracking for the split-off shard
// and returns true if it has been continuously ready for the grace period.
// Mutates splitReadySince: records first-ready time, clears it if readiness is lost.
func (r *Reconciler) trackAndCheckSplitFinalizeReady(splitOffShardID types.ID, isReady bool) bool {
	if !isReady {
		delete(r.splitReadySince, splitOffShardID)
		return false
	}

	now := r.timeProvider.Now()
	firstReady, tracked := r.splitReadySince[splitOffShardID]
	if !tracked {
		r.splitReadySince[splitOffShardID] = now
		return false
	}

	return now.Sub(firstReady) >= r.getSplitFinalizeGracePeriod()
}

// isSplitTimedOut checks if a split operation has exceeded the timeout
func (r *Reconciler) isSplitTimedOut(splitState *db.SplitState) bool {
	if splitState == nil || splitState.GetStartedAtUnixNanos() == 0 {
		return false
	}
	startedAt := time.Unix(0, splitState.GetStartedAtUnixNanos())
	return r.timeProvider.Now().Sub(startedAt) > r.getSplitTimeout()
}

func (r *Reconciler) isMergeTimedOut(mergeState *db.MergeState) bool {
	if mergeState == nil || mergeState.GetStartedAtUnixNanos() == 0 {
		return false
	}
	startedAt := time.Unix(0, mergeState.GetStartedAtUnixNanos())
	return r.timeProvider.Now().Sub(startedAt) > r.getSplitTimeout()
}

// computeSplitStateActions computes split/merge state advancement actions
func (r *Reconciler) computeSplitStateActions(current CurrentClusterState, desired DesiredClusterState) []SplitStateAction {
	actions := []SplitStateAction{}
	currentShardStatuses := make(map[types.ID]*store.ShardStatus, len(current.Shards))
	for shardID, shardInfo := range current.Shards {
		if shardInfo == nil {
			continue
		}
		currentShardStatuses[shardID] = &store.ShardStatus{
			ID:        shardID,
			ShardInfo: *shardInfo.DeepCopy(),
		}
	}

	// Sort shards by state priority (PreMerge > PreSplit > SplittingOff)
	sortedShardIDs := r.sortShardsByStatePriority(desired.Shards)

	for _, shardID := range sortedShardIDs {
		shardStatus := desired.Shards[shardID]

		// Check for zero-downtime split state transitions (based on Raft-replicated SplitState)
		if mergeState := shardStatus.MergeState; mergeState != nil {
			action := r.computeMergeStatePhaseActionWithDesired(
				shardID,
				shardStatus,
				mergeState,
				currentShardStatuses,
				desired.Shards,
			)
			if action != nil {
				actions = append(actions, *action)
			}
			if mergeState.GetPhase() != db.MergeState_PHASE_NONE {
				continue
			}
		}

		// Check for zero-downtime split state transitions (based on Raft-replicated SplitState)
		if splitState := shardStatus.SplitState; splitState != nil {
			action := r.computeSplitStatePhaseActionWithDesired(
				shardID,
				shardStatus,
				splitState,
				currentShardStatuses,
				desired.Shards,
			)
			if action != nil {
				actions = append(actions, *action)
			}
			// Active SplitState takes precedence over ShardState - skip the old
			// state-based path even when no action is needed (e.g., waiting for
			// new shard to be ready in PHASE_SPLITTING). Without this, the old
			// PreSplit handler would try to re-split with the already-narrowed
			// ByteRange[1], causing "key out of range" errors.
			if splitState.GetPhase() != db.SplitState_PHASE_NONE {
				continue
			}
		}

		switch shardStatus.State {
		case store.ShardState_SplittingOff:
			if shardStatus.Initializing {
				continue // Skip shards that are still initializing
			}

			// NOTE (ajr): I don't think we need to wait for the pre-split to be fully done
			// Check if there's a corresponding PreSplit shard
			hasPendingPreSplit := r.hasPendingPreSplitShard(shardID, shardStatus, desired.Shards)
			if hasPendingPreSplit {
				continue // Wait for PreSplit to be resolved first
			}

			actions = append(actions, SplitStateAction{
				ShardID:      shardID,
				State:        shardStatus.State,
				Action:       SplitStateActionStartShard,
				PeersToStart: shardStatus.Peers.IDSlice(),
				ShardConfig:  &shardStatus.ShardConfig,
			})

		case store.ShardState_PreMerge:
			continue

		case store.ShardState_PreSplit:
			// Find the corresponding splitting shard
			newShardID, found := r.findCorrespondingSplittingShard(
				shardID,
				shardStatus,
				desired.Shards,
			)
			if !found {
				r.logger.Error("Found PreSplit shard without corresponding SplittingOff shard",
					zap.Stringer("shardID", shardID))
				continue
			}

			newShardConfig := desired.Shards[newShardID]
			actions = append(actions, SplitStateAction{
				ShardID:      shardID,
				State:        shardStatus.State,
				Action:       SplitStateActionSplit,
				NewShardID:   newShardID,
				SplitKey:     shardStatus.ByteRange[1],
				PeersToStart: shardStatus.Peers.IDSlice(),
				ShardConfig:  &newShardConfig.ShardConfig,
			})

		case store.ShardState_Splitting:
			// Parent shard is in Splitting state - check if the split-off shard is ready
			// for us to finalize (delete split-off data and update byte range)
			splitOffShardID, found := r.findCorrespondingSplittingShard(
				shardID,
				shardStatus,
				desired.Shards,
			)
			if !found {
				// No corresponding split-off shard found - this can happen if the split
				// was aborted or the split-off shard was cleaned up. Allow transition.
				r.logger.Debug("Splitting shard without corresponding split-off shard, may finalize",
					zap.Stringer("shardID", shardID))
				continue
			}

			splitOffShard := desired.Shards[splitOffShardID]
			if r.trackAndCheckSplitFinalizeReady(splitOffShardID, splitOffShard.CanInitiateSplitCutover()) {
				r.logger.Info("Split-off shard is stable, finalizing parent shard",
					zap.Stringer("parentShardID", shardID),
					zap.Stringer("splitOffShardID", splitOffShardID))
				delete(r.splitReadySince, splitOffShardID)
				actions = append(actions, SplitStateAction{
					ShardID:     shardID,
					State:       shardStatus.State,
					Action:      SplitStateActionFinalizeSplit,
					NewShardID:  splitOffShardID,
					SplitKey:    shardStatus.ByteRange[1], // The split key becomes the new range end
					TargetRange: [2][]byte{shardStatus.ByteRange[0], shardStatus.ByteRange[1]},
				})
			}
		}
	}

	return actions
}

func (r *Reconciler) computeMergeStatePhaseActionWithDesired(
	shardID types.ID,
	shardStatus *store.ShardStatus,
	mergeState *db.MergeState,
	currentShards map[types.ID]*store.ShardStatus,
	desiredShards map[types.ID]*store.ShardStatus,
) *SplitStateAction {
	if mergeState == nil {
		return nil
	}
	receiverShardID := types.ID(mergeState.GetReceiverShardId())
	if receiverShardID == 0 {
		return nil
	}
	donorShardID := types.ID(mergeState.GetDonorShardId())
	if donorShardID == 0 {
		return nil
	}
	if receiverShardID != shardID {
		// Donor shards replicate merge metadata so they can enforce write gates,
		// but only the receiver should drive the copy/catchup/finalize workflow.
		return nil
	}
	if mergeState.GetPhase() != db.MergeState_PHASE_NONE && r.isMergeTimedOut(mergeState) {
		return &SplitStateAction{
			ShardID:      shardID,
			MergeShardID: donorShardID,
			State:        shardStatus.State,
			Action:       SplitStateActionRollbackMerge,
		}
	}
	donorStatus := desiredShards[donorShardID]
	if donorStatus == nil {
		return nil
	}

	switch mergeState.GetPhase() {
	case db.MergeState_PHASE_PREPARE, db.MergeState_PHASE_COPYING:
		if mergeState.GetAcceptDonorRange() && !mergeState.GetCopyCompleted() {
			return &SplitStateAction{
				ShardID:      shardID,
				MergeShardID: donorShardID,
				State:        shardStatus.State,
				Action:       SplitStateActionCopyMerge,
				TargetRange:  [2][]byte{mergeState.GetReceiverRangeStart(), mergeState.GetDonorRangeEnd()},
			}
		}
		return &SplitStateAction{
			ShardID:      shardID,
			MergeShardID: donorShardID,
			State:        shardStatus.State,
			Action:       SplitStateActionCatchUpMerge,
		}

	case db.MergeState_PHASE_CATCHUP:
		if mergeState.GetReplaySeq() < donorStatus.MergeDeltaSeq {
			return &SplitStateAction{
				ShardID:      shardID,
				MergeShardID: donorShardID,
				State:        shardStatus.State,
				Action:       SplitStateActionCatchUpMerge,
			}
		}
		return &SplitStateAction{
			ShardID:      shardID,
			MergeShardID: donorShardID,
			State:        shardStatus.State,
			Action:       SplitStateActionSealMerge,
		}

	case db.MergeState_PHASE_FINALIZING:
		if shardStatus.IsReadyForMergeCutover() {
			return &SplitStateAction{
				ShardID:      shardID,
				MergeShardID: donorShardID,
				State:        shardStatus.State,
				Action:       SplitStateActionFinalizeMerge,
				TargetRange:  [2][]byte{mergeState.GetReceiverRangeStart(), mergeState.GetDonorRangeEnd()},
			}
		}
		if mergeState.GetFinalSeq() > 0 && mergeState.GetReplaySeq() < mergeState.GetFinalSeq() {
			return &SplitStateAction{
				ShardID:      shardID,
				MergeShardID: donorShardID,
				State:        shardStatus.State,
				Action:       SplitStateActionCatchUpMerge,
			}
		}
		return nil

	case db.MergeState_PHASE_ROLLING_BACK:
		return &SplitStateAction{
			ShardID:      shardID,
			MergeShardID: donorShardID,
			State:        shardStatus.State,
			Action:       SplitStateActionRollbackMerge,
		}
	default:
		_ = currentShards
		return nil
	}
}

// computeSplitStatePhaseAction computes the action needed for a shard based on its SplitState.Phase.
// This handles the zero-downtime split state machine transitions.
func (r *Reconciler) computeSplitStatePhaseAction(
	shardID types.ID,
	shardStatus *store.ShardStatus,
	splitState *db.SplitState,
	currentShards map[types.ID]*store.ShardStatus,
) *SplitStateAction {
	return r.computeSplitStatePhaseActionWithDesired(
		shardID,
		shardStatus,
		splitState,
		currentShards,
		currentShards,
	)
}

func (r *Reconciler) computeSplitStatePhaseActionWithDesired(
	shardID types.ID,
	shardStatus *store.ShardStatus,
	splitState *db.SplitState,
	currentShards map[types.ID]*store.ShardStatus,
	desiredShards map[types.ID]*store.ShardStatus,
) *SplitStateAction {
	phase := splitState.GetPhase()

	// Check for timeout in any active phase
	if phase != db.SplitState_PHASE_NONE && r.isSplitTimedOut(splitState) {
		newShardID := types.ID(splitState.GetNewShardId())
		delete(r.splitReadySince, newShardID)
		r.logger.Warn("Split operation timed out, triggering rollback",
			zap.Stringer("shardID", shardID),
			zap.Stringer("phase", phase),
			zap.Duration("timeout", r.getSplitTimeout()))
		return &SplitStateAction{
			ShardID:     shardID,
			State:       shardStatus.State,
			Action:      SplitStateActionRollback,
			SplitKey:    splitState.GetSplitKey(),
			TargetRange: [2][]byte{shardStatus.ByteRange[0], splitState.GetOriginalRangeEnd()},
		}
	}

	switch phase {
	case db.SplitState_PHASE_NONE:
		// No active split - nothing to do
		return nil

	case db.SplitState_PHASE_PREPARE:
		// Shadow index is being built and dual-write is active.
		// Check if shadow index backfill is complete to transition to SPLITTING.
		// For now, we'll check if the shard is no longer "Splitting" which indicates
		// the shadow index backfill is complete.
		if !shardStatus.Splitting {
			r.logger.Info("Shadow index ready, transitioning to SPLITTING phase",
				zap.Stringer("shardID", shardID))
			return &SplitStateAction{
				ShardID:    shardID,
				State:      shardStatus.State,
				Action:     SplitStateActionTransitionToSplit,
				NewShardID: types.ID(splitState.GetNewShardId()),
				SplitKey:   splitState.GetSplitKey(),
			}
		}
		// Still preparing, no action needed yet
		return nil

	case db.SplitState_PHASE_SPLITTING:
		// Archive created and new shard is starting.
		// Check if new shard has been continuously ready for the grace period.
		newShardID := types.ID(splitState.GetNewShardId())
		if newShardID == 0 {
			if inferredShardID, found := r.findCorrespondingSplittingShard(
				shardID,
				shardStatus,
				desiredShards,
			); found {
				newShardID = inferredShardID
			}
		}
		if newShardID == 0 {
			return nil
		}
		currentShard, currentExists := currentShards[newShardID]
		desiredShard, desiredExists := desiredShards[newShardID]
		isReady := currentExists && currentShard.CanInitiateSplitCutover() ||
			desiredExists && desiredShard.CanInitiateSplitCutover()
		if r.trackAndCheckSplitFinalizeReady(newShardID, isReady) {
			r.logger.Info("New shard is stable, transitioning to FINALIZING phase",
				zap.Stringer("parentShardID", shardID),
				zap.Stringer("newShardID", newShardID))
			delete(r.splitReadySince, newShardID)
			return &SplitStateAction{
				ShardID:     shardID,
				State:       shardStatus.State,
				Action:      SplitStateActionFinalizeSplit,
				NewShardID:  newShardID,
				SplitKey:    splitState.GetSplitKey(),
				TargetRange: [2][]byte{shardStatus.ByteRange[0], splitState.GetSplitKey()},
			}
		}
		if (currentExists || desiredExists) && !isReady {
			shardForLog := currentShard
			if shardForLog == nil {
				shardForLog = desiredShard
			}
			hasLeader := shardForLog != nil && shardForLog.RaftStatus != nil && shardForLog.RaftStatus.Lead != 0
			r.logger.Debug("New shard not fully ready yet, waiting before finalize",
				zap.Stringer("parentShardID", shardID),
				zap.Stringer("newShardID", newShardID),
				zap.Bool("hasSnapshot", shardForLog != nil && shardForLog.HasSnapshot),
				zap.Bool("initializing", shardForLog != nil && shardForLog.Initializing),
				zap.Bool("hasLeader", hasLeader))
		}
		return nil

	case db.SplitState_PHASE_FINALIZING:
		// Final fence is already persisted and the parent is frozen. If the initial
		// finalize attempt timed out waiting for the child to catch up, retry once
		// the child can serve reads directly.
		newShardID := types.ID(splitState.GetNewShardId())
		currentShard, currentExists := currentShards[newShardID]
		desiredShard, desiredExists := desiredShards[newShardID]
		if currentExists && currentShard.IsReadyForSplitReads() ||
			desiredExists && desiredShard.IsReadyForSplitReads() {
			return &SplitStateAction{
				ShardID:     shardID,
				State:       shardStatus.State,
				Action:      SplitStateActionFinalizeSplit,
				NewShardID:  newShardID,
				SplitKey:    splitState.GetSplitKey(),
				TargetRange: [2][]byte{shardStatus.ByteRange[0], splitState.GetSplitKey()},
			}
		}
		return nil

	case db.SplitState_PHASE_ROLLING_BACK:
		// Rollback is in progress
		r.logger.Info("Split rollback in progress",
			zap.Stringer("shardID", shardID))
		return &SplitStateAction{
			ShardID:     shardID,
			State:       shardStatus.State,
			Action:      SplitStateActionRollback,
			SplitKey:    splitState.GetSplitKey(),
			TargetRange: [2][]byte{shardStatus.ByteRange[0], splitState.GetOriginalRangeEnd()},
		}
	}

	return nil
}

// computeIndexRebuildCompletions checks for completed index rebuilds
func (r *Reconciler) computeIndexRebuildCompletions(
	current CurrentClusterState,
) []IndexRebuildCompletion {
	completions := []IndexRebuildCompletion{}

	// Get all tables with read schemas (indicating an index rebuild is in progress)
	tables, err := r.tableOps.GetTablesWithReadSchemas()
	if err != nil {
		r.logger.Warn("Failed to get tables with read schemas", zap.Error(err))
		return completions
	}

	for _, table := range tables {
		if table.ReadSchema == nil {
			continue // Skip tables without read schemas
		}

		// Check if all shards have completed rebuilding the new index.
		// Use Schema.Version (the new version being built), not ReadSchema.Version (the old version).
		newIndexName := fmt.Sprintf("full_text_index_v%d", table.Schema.Version)
		indexRebuildComplete := true

		for shardID := range table.Shards {
			currentShard, ok := current.Shards[shardID]
			if !ok {
				indexRebuildComplete = false
				break
			}

			if currentShard.ShardStats == nil || currentShard.ShardStats.Indexes == nil {
				indexRebuildComplete = false
				break
			}

			indexStats, ok := currentShard.ShardStats.Indexes[newIndexName]
			if !ok {
				indexRebuildComplete = false
				break
			}

			// Check if the index is still rebuilding
			bleveStats, err := indexStats.AsFullTextIndexStats()
			if err == nil && bleveStats.Rebuilding {
				indexRebuildComplete = false
				break
			}
		}

		if indexRebuildComplete {
			r.logger.Info("Index rebuild completed for table",
				zap.String("table", table.Name),
				zap.String("index", newIndexName))
			completions = append(completions, IndexRebuildCompletion{
				TableName: table.Name,
				IndexName: newIndexName,
			})
		}
	}

	return completions
}

// computeIndexOperations computes index operations needed
func (r *Reconciler) computeIndexOperations(
	current CurrentClusterState,
	desired DesiredClusterState,
) []IndexOperation {
	operations := []IndexOperation{}

	for shardID, currentShard := range current.Shards {
		desiredShard, ok := desired.Shards[shardID]
		if !ok {
			continue
		}

		if desiredShard.RaftStatus != nil && desiredShard.RaftStatus.Lead == 0 {
			continue // Skip shards where raft exists but no leader is elected
		}

		table := current.Tables[desiredShard.Table]
		if table == nil {
			continue
		}

		// Check schema
		if !table.Schema.Equal(currentShard.Schema) {
			operations = append(operations, IndexOperation{
				ShardID:   shardID,
				Operation: IndexOpUpdateSchema,
				Schema:    table.Schema,
			})
		}

		// Check for missing indexes
		for idxName, desiredIndex := range table.Indexes {
			actualIndex, exists := currentShard.Indexes[idxName]
			if !exists {
				operations = append(operations, IndexOperation{
					ShardID:   shardID,
					IndexName: idxName,
					Operation: IndexOpAdd,
					Config:    &desiredIndex,
				})
			} else if !desiredIndex.Equal(actualIndex) {
				// NOTE (ajr): For now, we just log a warning if there's a mismatch
				// but maybe we want to delete the index and recreate it?
				// Index mismatch - would need reconciliation logic
				r.logger.Warn("Index mismatch detected",
					zap.Stringer("shardID", shardID),
					zap.String("indexName", idxName))
			}
		}

		// Check for extra indexes
		for idxName := range currentShard.Indexes {
			if _, exists := table.Indexes[idxName]; !exists {
				operations = append(operations, IndexOperation{
					ShardID:   shardID,
					IndexName: idxName,
					Operation: IndexOpDrop,
				})
			}
		}
	}

	return operations
}

// computeSplitAndMergeTransitions computes split and merge transitions
func (r *Reconciler) computeSplitAndMergeTransitions(
	ctx context.Context,
	desired DesiredClusterState,
) ([]tablemgr.SplitTransition, []tablemgr.MergeTransition) {
	// Delegate to the pure computeSplitTransitions function
	return computeSplitTransitions(
		ctx,
		r.config.ReplicationFactor,
		desired.Shards,
		r.config.MaxShardSizeBytes,
		r.getMinShardSizeBytes(),
		r.getMinShardsPerTable(),
		r.config.MaxShardsPerTable,
		r.shardCooldown,
		r.shardOps.GetMedianKey,
		r.timeProvider,
		r.getAutoRangeTransitionPerTableLimit(),
		r.getAutoRangeTransitionClusterLimit(),
	)
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// Helper functions for split state computation
func (r *Reconciler) sortShardsByStatePriority(shards map[types.ID]*store.ShardStatus) []types.ID {
	// Returns shard IDs sorted by priority: PreMerge > PreSplit > SplittingOff > others
	// This ensures state transitions happen in the correct order
	result := make([]types.ID, 0, len(shards))
	for id := range shards {
		result = append(result, id)
	}

	slices.SortFunc(result, func(a, b types.ID) int {
		stateA := shards[a].State
		stateB := shards[b].State

		// If both have the same state, sort by ID
		if stateA == stateB {
			return cmp.Compare(a, b)
		}

		// Define state priorities (lower number = higher priority, processed first)
		priority := func(state store.ShardState) int {
			switch state {
			case store.ShardState_PreMerge:
				return 1 // Highest priority
			case store.ShardState_PreSplit:
				return 2
			case store.ShardState_SplittingOff:
				return 3
			default:
				return 4 // Lowest priority (all other states)
			}
		}

		priorityA := priority(stateA)
		priorityB := priority(stateB)

		// Sort by priority (lower priority number comes first)
		if priorityA != priorityB {
			return cmp.Compare(priorityA, priorityB)
		}

		// If same priority level, sort by ID
		return cmp.Compare(a, b)
	})

	return result
}

func (r *Reconciler) hasPendingPreSplitShard(
	shardID types.ID,
	shardStatus *store.ShardStatus,
	allShards map[types.ID]*store.ShardStatus,
) bool {
	// Check if there's a PreSplit shard that would create this splitting shard
	for _, other := range allShards {
		if shardStatus.Table == other.Table && other.State == store.ShardState_PreSplit &&
			bytes.Equal(other.ByteRange[1], shardStatus.ByteRange[0]) {
			return true
		}
	}
	return false
}

func (r *Reconciler) findCorrespondingSplittingShard(
	shardID types.ID,
	shardStatus *store.ShardStatus,
	allShards map[types.ID]*store.ShardStatus,
) (types.ID, bool) {
	if splitState := shardStatus.SplitState; splitState != nil && splitState.GetNewShardId() != 0 {
		newShardID := types.ID(splitState.GetNewShardId())
		if other, ok := allShards[newShardID]; ok && other != nil && other.Table == shardStatus.Table {
			return newShardID, true
		}
	}

	// Find the split-off shard that corresponds to this PreSplit/Splitting shard.
	// The split-off shard may be in SplittingOff (just created) or SplitOffPreSnap
	// (initialized, waiting for snapshot + leader election before transitioning to Default).
	// Once the child is fully ready it may already be in Default while the parent metadata
	// is still in PreSplit/Splitting due to heartbeat ordering, so include Default only
	// when the parent is actively splitting (prevents false-matching an unrelated adjacent
	// shard in steady state if this helper is ever reused outside the split code path).
	parentIsActivelySplitting := shardStatus.State == store.ShardState_PreSplit ||
		shardStatus.State == store.ShardState_Splitting
	for otherID, other := range allShards {
		isSplitOff := other.State == store.ShardState_SplittingOff ||
			other.State == store.ShardState_SplitOffPreSnap ||
			(other.State == store.ShardState_Default && parentIsActivelySplitting)
		if shardStatus.Table == other.Table && isSplitOff &&
			bytes.Equal(other.ByteRange[0], shardStatus.ByteRange[1]) {
			return otherID, true
		}
	}
	return 0, false
}

// LogDebugState logs current cluster state for debugging (with deduplication)
// This can be called from metadata.go's checkShardAssignments
func (r *Reconciler) LogDebugState(
	current CurrentClusterState,
	desired DesiredClusterState,
	prevHash *atomic.Uint64,
) {
	// Build debug representation of current shards
	type ShardDebugItem struct {
		ShardID types.ID         `json:"shard_id"`
		Info    *store.ShardInfo `json:"shard_info"`
	}

	debugShards := []*ShardDebugItem{}
	debugStates := map[types.ID]*store.ShardStatus{}

	for shardID, shardInfo := range current.Shards {
		// Create a copy for logging, removing verbose fields
		shardInfoCopy := shardInfo.DeepCopy()
		shardInfoCopy.Indexes = nil // Avoid logging indexes for brevity
		shardInfoCopy.Schema = nil  // Avoid logging schema for brevity

		if shardInfoCopy.ShardStats != nil {
			shardInfoCopy.ShardStats.Created = time.Time{}
			shardInfoCopy.ShardStats.Updated = time.Time{}
		}

		debugShards = append(debugShards, &ShardDebugItem{
			ShardID: shardID,
			Info:    shardInfoCopy,
		})

		// Track shards in transitioning states
		if desiredShard, ok := desired.Shards[shardID]; ok && desiredShard.State.Transitioning() {
			debugStates[shardID] = desiredShard
		}
	}

	// Sort for consistent output
	slices.SortFunc(debugShards, func(a, b *ShardDebugItem) int {
		return cmp.Compare(a.ShardID, b.ShardID)
	})

	// Hash the state to deduplicate logs
	s, err := json.Marshal(debugShards)
	if err != nil {
		r.logger.Warn("Failed to encode debug shards", zap.Error(err))
		return
	}

	newHash := xxhash.Sum64(s)
	if prevHash != nil && prevHash.Load() != 0 && newHash == prevHash.Load() {
		// State hasn't changed, skip logging
		return
	}

	// Update the hash and log
	if prevHash != nil {
		prevHash.Store(newHash)
	}

	r.logger.Debug("Checking shard assignments...",
		zap.Any("nonDefaultShardStates", debugStates),
		zap.Any("currentShards", debugShards))
}

// computeSplitTransitions identifies shards that exceed the size threshold and generates split plans.
// This is extracted from the original code in metadata.go (lines 878-1012) to be more testable.
//
// The getMedianKey parameter is a callback to fetch median keys for splits, allowing this function
// to remain testable by injecting a mock function.
func computeSplitTransitions(
	ctx context.Context,
	replicationFactor uint64,
	shards map[types.ID]*store.ShardStatus,
	maxShardSizeBytes uint64,
	minShardSizeBytes uint64,
	minShardsPerTable uint64,
	maxShardsPerTable uint64,
	shardCooldown map[types.ID]time.Time,
	getMedianKey MedianKeyGetter,
	timeProvider TimeProvider,
	autoRangeTransitionPerTableLimit int,
	autoRangeTransitionClusterLimit int,
) ([]tablemgr.SplitTransition, []tablemgr.MergeTransition) {
	splits := []tablemgr.SplitTransition{}
	merges := []tablemgr.MergeTransition{}

	// Only do merges 1 at a time per table
	tablesWithMerges := map[string]int{}
	plannedRangeTransitionsByShard := make(map[types.ID]struct{})

	// Track shards per table for split limiting
	shardsPerTable := make(map[string]int)
	for _, status := range shards {
		shardsPerTable[status.Table]++
	}
	if minShardsPerTable == 0 {
		minShardsPerTable = 1
	}
	if minShardSizeBytes == 0 && maxShardSizeBytes > 0 {
		minShardSizeBytes = max(maxShardSizeBytes/4, 1)
	}
	if autoRangeTransitionPerTableLimit <= 0 {
		autoRangeTransitionPerTableLimit = 1
	}
	if autoRangeTransitionClusterLimit <= 0 {
		autoRangeTransitionClusterLimit = 1
	}
	activeTransitionCount := countActiveRangeTransitionWork(shards)
	remainingAutoTransitionBudget := max(autoRangeTransitionClusterLimit-activeTransitionCount, 0)
	autoTransitionsPerTable := make(map[string]int)

	// Build list of candidate shards for split/merge
	shardIDs := make([]types.ID, 0, len(shards))
	now := timeProvider.Now()
	for shardID, status := range shards {
		// Skip shards in cooldown
		if cooldownEnd, ok := shardCooldown[shardID]; ok {
			if timeProvider.Now().Before(cooldownEnd) {
				continue
			}
		}

		// Skip shards without proper raft status
		if status.RaftStatus == nil || status.RaftStatus.Voters == nil ||
			uint64(len(status.RaftStatus.Voters)) != replicationFactor {
			continue
		}

		// Skip shards without an elected leader
		if status.RaftStatus.Lead == 0 {
			continue
		}

		// Skip shards that are currently transitioning
		if status.State.Transitioning() {
			continue
		}

		// Skip shards that have an active SplitState (handled by computeSplitStateActions)
		if status.SplitState != nil && status.SplitState.GetPhase() != db.SplitState_PHASE_NONE {
			continue
		}
		if status.MergeState != nil && status.MergeState.GetPhase() != db.MergeState_PHASE_NONE {
			continue
		}

		// Skip split children that are still replaying parent deltas or are
		// otherwise in split-only serving states. These shards are mid-cutover
		// and must not be selected for another size-based split yet.
		if status.SplitReplayRequired && !status.SplitCutoverReady {
			continue
		}
		if status.State == store.ShardState_SplittingOff ||
			status.State == store.ShardState_SplitOffPreSnap {
			continue
		}

		// Skip shards without stats
		if status.ShardStats == nil || status.ShardStats.Storage == nil {
			continue
		}

		// Skip shards with stale stats (older than 1 minute). Empty-only merges are
		// still destructive, so they must also rely on fresh metadata.
		if now.Sub(status.ShardStats.Updated) > time.Minute {
			continue
		}

		shardIDs = append(shardIDs, shardID)
	}

	// Sort shard IDs for deterministic ordering (by table, then by byte range)
	slices.SortFunc(shardIDs, func(shardID1, shardID2 types.ID) int {
		if shards[shardID1].Table != shards[shardID2].Table {
			return cmp.Compare(shards[shardID1].Table, shards[shardID2].Table)
		}
		return bytes.Compare(shards[shardID1].ByteRange[0], shards[shardID2].ByteRange[0])
	})

	// Process each candidate shard
	for idx, shardID := range shardIDs {
		if _, alreadyPlanned := plannedRangeTransitionsByShard[shardID]; alreadyPlanned {
			continue
		}

		status := shards[shardID]
		size := status.ShardStats.Storage.DiskSize

		// Check for automatic merge conditions on the adjacent pair to the right.
		// The left/lower-range shard always survives so merge identity is deterministic.
		if idx+1 < len(shardIDs) &&
			remainingAutoTransitionBudget > 0 &&
			autoTransitionsPerTable[status.Table] < autoRangeTransitionPerTableLimit &&
			shardsPerTable[status.Table] > int(minShardsPerTable) { //nolint:gosec // G115: bounded value, cannot overflow in practice

			// Only allow 3 automatic merges at once per table
			if mergeCount, ok := tablesWithMerges[status.Table]; ok && mergeCount >= 3 {
			} else {
				rightShardID := shardIDs[idx+1]
				rightStatus := shards[rightShardID]
				if rightStatus != nil &&
					rightStatus.Table == status.Table &&
					bytes.Equal(status.ByteRange[1], rightStatus.ByteRange[0]) &&
					rightStatus.ShardStats != nil &&
					rightStatus.ShardStats.Storage != nil {
					if _, alreadyPlanned := plannedRangeTransitionsByShard[rightShardID]; !alreadyPlanned &&
						now.Sub(status.ShardStats.Created) > 5*time.Minute &&
						now.Sub(rightStatus.ShardStats.Created) > 5*time.Minute {
						rightSize := rightStatus.ShardStats.Storage.DiskSize
						if (size < minShardSizeBytes || rightSize < minShardSizeBytes) &&
							size+rightSize < maxShardSizeBytes {
							merges = append(merges, tablemgr.MergeTransition{
								ShardID:      shardID,
								MergeShardID: rightShardID,
								TableName:    status.Table,
							})
							tablesWithMerges[status.Table]++
							autoTransitionsPerTable[status.Table]++
							remainingAutoTransitionBudget--
							plannedRangeTransitionsByShard[shardID] = struct{}{}
							plannedRangeTransitionsByShard[rightShardID] = struct{}{}
							shardsPerTable[status.Table]--
							continue
						}
					}
				}
			}
		}

		// Check if we've reached max shards for this table
		if shardsPerTable[status.Table] >= int(maxShardsPerTable) { //nolint:gosec // G115: bounded value, cannot overflow in practice
			continue
		}

		// Skip zero-size shards that aren't empty (shouldn't happen, but be safe)
		if size == 0 {
			continue
		}

		// Check for split conditions (shard exceeds size threshold)
		if size > maxShardSizeBytes {
			if remainingAutoTransitionBudget <= 0 {
				continue
			}
			if autoTransitionsPerTable[status.Table] >= autoRangeTransitionPerTableLimit {
				continue
			}
			// Generate a new unique shard ID for the split
			// Note: This uses a hash of table name, shard ID, and byte range
			newShardID := types.ID(
				xxhash.Sum64(fmt.Append(nil, status.Table, status.ID, status.ByteRange)),
			)

			// Avoid ID collision with parent or existing child shard
			// (e.g., after metadata failover loses cooldown state)
			if shardID == newShardID {
				continue
			}
			if _, exists := shards[newShardID]; exists {
				continue
			}

			// Get median key for splitting
			medianKey, err := getMedianKey(ctx, shardID)
			if err != nil {
				// Skip this shard if we can't get median key
				continue
			}

			if len(medianKey) == 0 {
				// Skip if median key is empty (can't split)
				continue
			}

			// Increment shard count for this table
			shardsPerTable[status.Table]++
			autoTransitionsPerTable[status.Table]++
			remainingAutoTransitionBudget--
			plannedRangeTransitionsByShard[shardID] = struct{}{}

			// Create split transition
			splits = append(splits, tablemgr.SplitTransition{
				ShardID:      shardID,
				SplitShardID: newShardID,
				SplitKey:     medianKey,
				TableName:    status.Table,
			})
		}
	}

	return splits, merges
}

func countActiveRangeTransitionWork(shards map[types.ID]*store.ShardStatus) int {
	activeKeys := make(map[string]struct{})
	for _, status := range shards {
		if status == nil {
			continue
		}
		if status.SplitState != nil && status.SplitState.GetPhase() != db.SplitState_PHASE_NONE {
			key := fmt.Sprintf("split:%s:%d", status.ID, status.SplitState.GetNewShardId())
			if status.SplitState.GetNewShardId() != 0 {
				ids := []string{status.ID.String(), types.ID(status.SplitState.GetNewShardId()).String()}
				slices.Sort(ids)
				key = "splitpair:" + ids[0] + ":" + ids[1]
			}
			activeKeys[key] = struct{}{}
			continue
		}
		if status.MergeState != nil && status.MergeState.GetPhase() != db.MergeState_PHASE_NONE {
			ids := []string{status.ID.String(), types.ID(status.MergeState.GetDonorShardId()).String()}
			if status.MergeState.GetReceiverShardId() != 0 {
				ids[0] = types.ID(status.MergeState.GetReceiverShardId()).String()
			}
			slices.Sort(ids)
			activeKeys["mergepair:"+ids[0]+":"+ids[1]] = struct{}{}
			continue
		}
		switch status.State {
		case store.ShardState_PreMerge,
			store.ShardState_PreSplit,
			store.ShardState_Splitting,
			store.ShardState_SplittingOff,
			store.ShardState_SplitOffPreSnap:
			activeKeys[fmt.Sprintf("state:%s", status.ID)] = struct{}{}
		}
	}
	return len(activeKeys)
}
