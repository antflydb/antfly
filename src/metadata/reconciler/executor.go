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
	"errors"
	"fmt"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/sethvargo/go-retry"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// ExecutePlan executes a reconciliation plan
func (r *Reconciler) ExecutePlan(
	ctx context.Context,
	plan *ReconciliationPlan,
	current CurrentClusterState,
	desired DesiredClusterState,
) error {
	// Skip logging if there are no actions to perform
	if plan.Empty() {
		return nil
	}

	r.logger.Info("Executing reconciliation plan",
		zap.Int("removedStorePeerRemovals", len(plan.RemovedStorePeerRemovals)),
		zap.Int("tombstoneDeletions", len(plan.TombstoneDeletions)),
		zap.Int("raftVoterFixes", len(plan.RaftVoterFixes)),
		zap.Int("indexOperations", len(plan.IndexOperations)),
		zap.Int("splitStateActions", len(plan.SplitStateActions)),
		zap.Int("splitTransitions", len(plan.SplitTransitions)),
		zap.Int("mergeTransitions", len(plan.MergeTransitions)),
		zap.Bool("skipRemainingSteps", plan.SkipRemainingSteps),
	)

	// 1. Remove tombstoned stores from shards where they are still voters
	if len(plan.RemovedStorePeerRemovals) > 0 {
		if err := r.executeRemovedStorePeerRemovals(ctx, plan.RemovedStorePeerRemovals, current); err != nil {
			r.logger.Error("Failed to remove peers from tombstoned stores", zap.Error(err))
			// Continue despite errors
		}
	}

	// 2. Delete tombstone records for stores no longer in any shard
	if len(plan.TombstoneDeletions) > 0 {
		if err := r.tableOps.DeleteTombstones(ctx, plan.TombstoneDeletions); err != nil {
			r.logger.Error("Failed to delete tombstones", zap.Error(err))
			// Continue despite errors
		}
	}

	// Check for context cancellation
	select {
	case <-ctx.Done():
		r.logger.Info("Context cancelled after removed store cleanup")
		return ctx.Err()
	default:
	}

	// 2.5 Execute split and merge transitions BEFORE SkipRemainingSteps check
	// Split/merge transitions are triggered when size thresholds are met and should
	// be executed even when SkipRemainingSteps is true. The SkipRemainingSteps flag
	// is meant to prevent shard placement transitions during splits, not to skip
	// the split execution itself.
	if len(plan.MergeTransitions) > 0 || len(plan.SplitTransitions) > 0 {
		r.logger.Info("About to execute split/merge transitions",
			zap.Int("splits", len(plan.SplitTransitions)),
			zap.Int("merges", len(plan.MergeTransitions)))
		eg, egCtx := errgroup.WithContext(ctx)
		r.executeSplitAndMergeTransitions(
			egCtx,
			eg,
			current,
			plan.SplitTransitions,
			plan.MergeTransitions,
		)
		if err := eg.Wait(); err != nil {
			r.logger.Error("Failed to execute split/merge transitions", zap.Error(err))
		}
		r.logger.Info("Split/merge transitions execution completed")
	}

	// 2.6 Execute split state actions BEFORE SkipRemainingSteps check
	// Split state actions (FinalizeSplit, Prepare, TransitionToSplit, Rollback) are
	// part of the split lifecycle and must execute even when SkipRemainingSteps is true.
	// Without this, the split gets stuck: the reconciler keeps returning FinalizeSplit
	// actions but the executor skips them, preventing the split from completing.
	if len(plan.SplitStateActions) > 0 {
		if err := r.executeSplitStateActions(ctx, plan.SplitStateActions); err != nil {
			r.logger.Error("Failed to execute split state actions", zap.Error(err))
			// Continue despite errors
		}
	}

	// If we need to skip remaining steps (e.g., splits/merges in progress, removed stores still need reconciliation)
	if plan.SkipRemainingSteps {
		// Clear reallocation request if this was a forced reallocation
		if plan.ClearReallocationRequest {
			if err := r.tableOps.ClearReallocationRequest(ctx); err != nil {
				r.logger.Warn("Failed to clear reallocation request", zap.Error(err))
			}
		}
		return nil
	}

	// 2. Execute raft voter fixes
	if len(plan.RaftVoterFixes) > 0 {
		if err := r.executeRaftVoterFixes(ctx, plan.RaftVoterFixes); err != nil {
			r.logger.Error("Failed to execute raft voter fixes", zap.Error(err))
			// Continue despite errors
		}
	}

	// Check for context cancellation
	select {
	case <-ctx.Done():
		r.logger.Info("Context cancelled after raft voter fixes")
		return ctx.Err()
	default:
	}

	// 3. Execute index operations
	if len(plan.IndexOperations) > 0 {
		if err := r.executeIndexOperations(ctx, plan.IndexOperations); err != nil {
			r.logger.Error("Failed to execute index operations", zap.Error(err))
			// Continue despite errors
		}
	}

	// Check for context cancellation
	select {
	case <-ctx.Done():
		r.logger.Info("Context cancelled after index operations")
		return ctx.Err()
	default:
	}

	// 3.5. Execute index rebuild completions (drop old read schemas)
	if len(plan.IndexRebuildCompletions) > 0 {
		if err := r.executeIndexRebuildCompletions(ctx, plan.IndexRebuildCompletions); err != nil {
			r.logger.Error("Failed to execute index rebuild completions", zap.Error(err))
			// Continue despite errors
		}
	}

	// 4. Execute shard transitions (if any)
	// Note: Split/merge transitions and split state actions are executed earlier (sections 2.5, 2.6)
	// before the SkipRemainingSteps check
	if plan.ShardTransitions != nil {
		eg, _ := errgroup.WithContext(ctx)
		r.executeShardTransitionPlan(ctx, eg, current, desired, plan.ShardTransitions)
		if err := eg.Wait(); err != nil {
			r.logger.Error("Failed to execute shard transitions", zap.Error(err))
		}
	}

	// Clear reallocation request if this was a forced reallocation
	if plan.ClearReallocationRequest {
		if err := r.tableOps.ClearReallocationRequest(ctx); err != nil {
			r.logger.Warn("Failed to clear reallocation request", zap.Error(err))
		}
	}

	return nil
}

// executeRemovedStorePeerRemovals removes tombstoned stores from shards where they are still voters
func (r *Reconciler) executeRemovedStorePeerRemovals(
	ctx context.Context,
	removedStores []types.ID,
	current CurrentClusterState,
) error {
	eg, egCtx := errgroup.WithContext(ctx)

	for _, removedStore := range removedStores {
		// Find all shards that still have this tombstoned store as a voter
		for shardID, shardInfo := range current.Shards {
			if shardInfo == nil || shardInfo.RaftStatus == nil ||
				!shardInfo.RaftStatus.Voters.Contains(removedStore) {
				continue
			}

			// Skip if removing this peer would leave the shard with 0 voters.
			// This can happen when replication factor is 1 and the only voter
			// is being removed. We need to let shard transitions add a new
			// replica first before we can remove this one.
			voterCount := len(shardInfo.RaftStatus.Voters)
			if voterCount <= 1 {
				r.logger.Info("Skipping removal of tombstoned store from shard - would leave 0 voters",
					zap.Stringer("shardID", shardID),
					zap.Stringer("removedStore", removedStore),
					zap.Int("voterCount", voterCount))
				continue
			}

			// Need to remove this peer from the shard
			eg.Go(func() error {
				leaderClient, err := r.storeOps.GetLeaderClientForShard(egCtx, shardID)
				if err != nil {
					r.logger.Warn(
						"Failed to find leader for shard for removal",
						zap.Stringer("removedStore", removedStore),
						zap.Stringer("shardID", shardID),
						zap.Error(err),
					)
					return nil
				}

				r.logger.Info("Attempting to remove store from shard",
					zap.Stringer("shardID", shardID),
					zap.Stringer("nodeID", leaderClient.ID()),
					zap.Stringer("removedStore", removedStore))

				// Use sync removal (async=false) to wait for the conf change to be applied.
				// This ensures the removed store is actually removed from the Raft voters
				// before we proceed, preventing races in reconciliation.
				if err := r.shardOps.RemovePeer(ctx, shardID, leaderClient, removedStore, false); err != nil {
					r.logger.Warn(
						"Failed to remove peer from shard",
						zap.Stringer("removedStore", removedStore),
						zap.Stringer("shardID", shardID),
						zap.Error(err),
					)
					return nil
				}
				return nil
			})
		}
	}

	return eg.Wait()
}

// executeRaftVoterFixes executes raft voter fixes
func (r *Reconciler) executeRaftVoterFixes(ctx context.Context, fixes []RaftVoterFix) error {
	eg, _ := errgroup.WithContext(ctx)

	for _, fix := range fixes {
		switch fix.Action {
		case RaftVoterActionStopShard:
			eg.Go(func() error {
				if err := r.shardOps.StopShard(ctx, fix.ShardID, fix.PeerID); err != nil {
					r.logger.Warn(
						"Failed to stop shard on peer that is not in raft voters",
						zap.Error(err),
						zap.Stringer("peerID", fix.PeerID),
						zap.Stringer("shardID", fix.ShardID),
					)
					return nil
				}
				return nil
			})
		}
	}

	return eg.Wait()
}

// executeIndexOperations executes index operations
func (r *Reconciler) executeIndexOperations(ctx context.Context, ops []IndexOperation) error {
	eg, _ := errgroup.WithContext(ctx)

	for _, op := range ops {
		switch op.Operation {
		case IndexOpAdd:
			eg.Go(func() error {
				if err := r.tableOps.AddIndex(ctx, op.ShardID, op.IndexName, op.Config); err != nil {
					r.logger.Warn(
						"Failed to add index",
						zap.Stringer("shardID", op.ShardID),
						zap.String("indexName", op.IndexName),
						zap.Error(err),
					)
					return nil
				}
				return nil
			})
		case IndexOpDrop:
			eg.Go(func() error {
				if err := r.tableOps.DropIndex(ctx, op.ShardID, op.IndexName); err != nil {
					r.logger.Warn(
						"Failed to drop index",
						zap.Stringer("shardID", op.ShardID),
						zap.String("indexName", op.IndexName),
						zap.Error(err),
					)
					return nil
				}
				return nil
			})
		case IndexOpUpdateSchema:
			eg.Go(func() error {
				if err := r.tableOps.UpdateSchema(ctx, op.ShardID, op.Schema); err != nil {
					r.logger.Warn(
						"Failed to update schema",
						zap.Stringer("shardID", op.ShardID),
						zap.Error(err),
					)
					return nil
				}
				return nil
			})
		}
	}

	return eg.Wait()
}

// executeIndexRebuildCompletions drops read schemas for completed index rebuilds
func (r *Reconciler) executeIndexRebuildCompletions(
	ctx context.Context,
	completions []IndexRebuildCompletion,
) error {
	for _, completion := range completions {
		r.logger.Info("Dropping read schema after index rebuild completion",
			zap.String("table", completion.TableName),
			zap.String("index", completion.IndexName))

		if err := r.tableOps.DropReadSchema(completion.TableName); err != nil {
			r.logger.Warn("Failed to drop read schema",
				zap.String("table", completion.TableName),
				zap.Error(err))
			// Continue despite errors - this is cleanup work
			continue
		}

		r.logger.Info("Successfully dropped read schema after index rebuild",
			zap.String("table", completion.TableName),
			zap.String("index", completion.IndexName))
	}

	return nil
}

// executeSplitStateActions executes split state advancement actions
func (r *Reconciler) executeSplitStateActions(
	ctx context.Context,
	actions []SplitStateAction,
) error {
	for _, action := range actions {
		switch action.Action {
		case SplitStateActionStartShard:
			r.logger.Debug("Found Splitting shard that needs start",
				zap.Stringer("shardID", action.ShardID),
				zap.Stringer("byteRange", action.ShardConfig.ByteRange),
			)
			if err := r.shardOps.StartShard(ctx, action.ShardID, action.PeersToStart, action.ShardConfig, false, true); err != nil {
				r.logger.Warn(
					"Failed to start shard",
					zap.Stringer("shardID", action.ShardID),
					zap.Error(err),
				)
			}

		case SplitStateActionSplit:
			if err := r.shardOps.SplitShard(ctx, action.ShardID, action.NewShardID, action.SplitKey); err != nil {
				r.logger.Warn(
					"Failed to split shard on node",
					zap.Stringer("shardID", action.ShardID),
					zap.Stringer("newShardID", action.NewShardID),
					zap.Error(err),
				)
				continue
			}
			// Immediately propagate the active split phase to metadata so write routing
			// stays aligned with the store while heartbeats catch up.
			splitState := &db.SplitState{}
			splitState.SetPhase(db.SplitState_PHASE_SPLITTING)
			splitState.SetSplitKey(action.SplitKey)
			splitState.SetNewShardId(uint64(action.NewShardID))
			if action.ShardConfig != nil {
				splitState.SetOriginalRangeEnd(action.ShardConfig.ByteRange[1])
			}
			if err := r.storeOps.UpdateShardSplitState(ctx, action.ShardID, splitState); err != nil {
				r.logger.Warn(
					"Failed to update ShardStatus with SplitState after SplitShard",
					zap.Stringer("shardID", action.ShardID),
					zap.Error(err),
				)
				// Continue anyway - the heartbeat will eventually propagate the state
			}

			// Start the new shard config on the peers
			if err := r.shardOps.StartShard(ctx, action.ShardID, action.PeersToStart, action.ShardConfig, false, true); err != nil {
				r.logger.Warn(
					"Failed to start shard after split",
					zap.Stringer("shardID", action.ShardID),
					zap.Error(err),
				)
			}

		case SplitStateActionMerge:
			if err := r.shardOps.MergeRange(ctx, action.ShardID, action.TargetRange); err != nil {
				r.logger.Warn(
					"Failed to complete merge shard",
					zap.Stringer("shardID", action.ShardID),
					zap.Error(err),
				)
				continue
			}
			r.SetShardCooldown(action.ShardID, r.getCooldownDuration())

		case SplitStateActionFinalizeSplit:
			// Finalize a split by cleaning up the parent shard's split-off data
			// This is the second phase of the two-phase split that ensures continuous
			// data availability during splits
			r.logger.Info("Finalizing split on parent shard",
				zap.Stringer("shardID", action.ShardID),
				zap.Binary("newRangeEnd", action.SplitKey))
			if err := r.shardOps.FinalizeSplit(ctx, action.ShardID, action.SplitKey); err != nil {
				r.logger.Warn(
					"Failed to finalize split on parent shard",
					zap.Stringer("shardID", action.ShardID),
					zap.Error(err),
				)
				continue
			}
			// FinalizeSplit only returns after the parent has deleted the split-off range
			// and cleared its local split state. Reflect that in metadata immediately so
			// we do not keep routing writes back to the old parent while heartbeats catch up.
			if err := r.storeOps.UpdateShardSplitState(ctx, action.ShardID, nil); err != nil {
				r.logger.Warn(
					"Failed to clear ShardStatus SplitState after FinalizeSplit",
					zap.Stringer("shardID", action.ShardID),
					zap.Error(err),
				)
				// Continue anyway - the heartbeat will eventually propagate the state
			}
			r.SetShardCooldown(action.ShardID, r.getCooldownDuration())

		case SplitStateActionPrepare:
			// Initiate the zero-downtime split prepare phase
			// This sets up the shadow index and dual-write routing
			r.logger.Info("Preparing split (zero-downtime)",
				zap.Stringer("shardID", action.ShardID),
				zap.Binary("splitKey", action.SplitKey))
			if err := r.shardOps.PrepareSplit(ctx, action.ShardID, action.SplitKey); err != nil {
				r.logger.Warn(
					"Failed to prepare split",
					zap.Stringer("shardID", action.ShardID),
					zap.Error(err),
				)
				continue
			}
			// Immediately update the ShardStatus with the SplitState so subsequent
			// reconciliation runs see it, rather than waiting for heartbeat propagation.
			splitState := &db.SplitState{}
			splitState.SetPhase(db.SplitState_PHASE_PREPARE)
			splitState.SetSplitKey(action.SplitKey)
			if err := r.storeOps.UpdateShardSplitState(ctx, action.ShardID, splitState); err != nil {
				r.logger.Warn(
					"Failed to update ShardStatus with SplitState after PrepareSplit",
					zap.Stringer("shardID", action.ShardID),
					zap.Error(err),
				)
				// Continue anyway - the heartbeat will eventually propagate the state
			}

		case SplitStateActionTransitionToSplit:
			// Transition from PREPARE to SPLITTING phase
			// This creates the archive and starts the new shard
			r.logger.Info("Transitioning to split phase",
				zap.Stringer("shardID", action.ShardID),
				zap.Stringer("newShardID", action.NewShardID),
				zap.Binary("splitKey", action.SplitKey))
			if err := r.shardOps.SplitShard(ctx, action.ShardID, action.NewShardID, action.SplitKey); err != nil {
				r.logger.Warn(
					"Failed to transition to split",
					zap.Stringer("shardID", action.ShardID),
					zap.Stringer("newShardID", action.NewShardID),
					zap.Error(err),
				)
				continue
			}

		case SplitStateActionRollback:
			// Rollback a stuck split operation
			// This restores the original state and cleans up shadow indexes
			r.logger.Warn("Rolling back split operation",
				zap.Stringer("shardID", action.ShardID),
				zap.Binary("splitKey", action.SplitKey))
			if err := r.shardOps.RollbackSplit(ctx, action.ShardID); err != nil {
				r.logger.Warn(
					"Failed to rollback split",
					zap.Stringer("shardID", action.ShardID),
					zap.Error(err),
				)
				continue
			}
			r.SetShardCooldown(action.ShardID, r.getCooldownDuration())
		}
	}

	return nil
}

// executeSplitAndMergeTransitions executes split and merge transitions
func (r *Reconciler) executeSplitAndMergeTransitions(
	ctx context.Context,
	eg *errgroup.Group,
	current CurrentClusterState,
	splits []tablemgr.SplitTransition,
	merges []tablemgr.MergeTransition,
) {
	// Execute merges first
	for _, transition := range merges {
		shardID := transition.MergeShardID
		if _, err := r.storeOps.GetLeaderClientForShard(ctx, shardID); err != nil {
			r.logger.Warn(
				"Skipping merge transition for shard that has no leader",
				zap.Stringer("shardID", shardID),
				zap.Error(err),
			)
			continue
		}

		status := current.Shards[shardID]
		r.logger.Info("Executing merge transition",
			zap.Stringers("peers", status.Peers.IDSlice()),
			zap.Stringer("transition", transition))

		eg.Go(func() error {
			newShardConfig, err := r.tableOps.ReassignShardsForMerge(transition)
			if err != nil {
				r.logger.Warn(
					"Failed to reassign shards for merge",
					zap.Stringer("shardID", shardID),
					zap.Error(err),
				)
				return nil
			}

			// Stop the merge shard on all peers
			stopEg, _ := errgroup.WithContext(ctx)
			for peer := range status.Peers {
				stopEg.Go(func() error {
					if err := r.shardOps.StopShard(ctx, transition.MergeShardID, peer); err != nil {
						if !errors.Is(err, client.ErrNotFound) {
							return fmt.Errorf("stopping shard on node for merge: %w", err)
						}
					}
					return nil
				})
			}
			if err := stopEg.Wait(); err != nil {
				r.logger.Warn(
					"Failed to stop shard on node",
					zap.Stringer("shardID", transition.MergeShardID),
					zap.Error(err),
				)
				return nil
			}

			// Merge the range into the target shard
			if err := r.shardOps.MergeRange(ctx, transition.ShardID, newShardConfig.ByteRange); err != nil {
				r.logger.Warn(
					"Failed to merge shard",
					zap.Stringer("mergeShardID", transition.MergeShardID),
					zap.Stringer("targetShardID", transition.ShardID),
					zap.Error(err),
				)
				return nil
			}

			r.SetShardCooldown(shardID, r.getCooldownDuration())
			return nil
		})
	}

	// Execute splits
	for _, transition := range splits {
		shardID := transition.ShardID
		r.logger.Debug("Processing split transition",
			zap.Stringer("shardID", shardID),
			zap.Stringer("splitShardID", transition.SplitShardID),
			zap.String("tableName", transition.TableName))
		if _, err := r.storeOps.GetLeaderClientForShard(ctx, shardID); err != nil {
			r.logger.Warn(
				"Skipping split transition for shard that has no leader",
				zap.Stringer("shardID", shardID),
				zap.Error(err),
			)
			continue
		}

		r.logger.Info("Executing shard split", zap.Object("transition", transition))
		eg.Go(func() error {
			newShardID := transition.SplitShardID

			// CRITICAL: Call SplitShard BEFORE ReassignShardsForSplit.
			// SplitShard sets pendingSplitKey AND proposes the split through Raft atomically.
			// This ensures pendingSplitKey is replicated to all nodes (via the split proposal).
			// ReassignShardsForSplit updates metadata routing, after which clients may
			// route writes to the parent shard via fallback (if new shard isn't ready).
			// If we updated metadata first, there would be a window where writes to the
			// split-off range are accepted by the parent shard, proposed to Raft, and
			// then dropped when applied after the split narrows byteRange.
			//
			// Note: PrepareSplit is NOT used here because pendingSplitKey is local state
			// that is not replicated through Raft. A leadership change between PrepareSplit
			// and SplitShard would cause the new leader to accept writes that get lost.
			r.logger.Info("Splitting shard",
				zap.Stringer("shardID", shardID),
				zap.Stringer("newShardID", newShardID),
				zap.ByteString("splitKey", transition.SplitKey))
			if err := r.shardOps.SplitShard(ctx, shardID, newShardID, transition.SplitKey); err != nil {
				r.logger.Warn(
					"Failed to split shard on node",
					zap.Stringer("shardID", shardID),
					zap.Stringer("newShardID", newShardID),
					zap.Error(err),
				)
				// Don't continue if split failed - metadata update would create
				// inconsistent state where new shard exists but has no data source
				return nil
			}

			// Immediately update ShardStatus with SplitState so subsequent reconciliation
			// runs see it and skip this shard, without waiting for heartbeat propagation.
			// After SplitShard, the shard is in PHASE_SPLITTING.
			splitState := &db.SplitState{}
			splitState.SetPhase(db.SplitState_PHASE_SPLITTING)
			splitState.SetSplitKey(transition.SplitKey)
			splitState.SetNewShardId(uint64(newShardID))
			if err := r.storeOps.UpdateShardSplitState(ctx, shardID, splitState); err != nil {
				r.logger.Warn(
					"Failed to update ShardStatus with SplitState after SplitShard",
					zap.Stringer("shardID", shardID),
					zap.Error(err),
				)
				// Continue anyway - the heartbeat will eventually propagate the state
			}

			// Now update metadata routing - pendingSplitKey is set so any writes
			// routed via fallback will be rejected at proposal time
			r.logger.Info("Reassigning shards for table", zap.Object("transition", transition))
			peers, newShardConfig, err := r.tableOps.ReassignShardsForSplit(transition)
			if err != nil {
				r.logger.Warn(
					"Failed to reassign shards for split",
					zap.Stringer("shardID", shardID),
					zap.Error(err),
				)
				return nil
			}

			// Now start the new shard on its peers - the archive should exist
			r.logger.Info("Starting shard on nodes", zap.Any("transition", transition))
			if err := r.shardOps.StartShard(ctx, newShardID, peers, newShardConfig, false, true); err != nil {
				r.logger.Warn(
					"Failed to start shard",
					zap.Stringer("shardID", newShardID),
					zap.Error(err),
				)
			}

			return nil
		})
	}
}

// executeShardTransitionPlan executes the transition plan for ideal shard assignments
func (r *Reconciler) executeShardTransitionPlan(
	ctx context.Context,
	eg *errgroup.Group,
	current CurrentClusterState,
	desired DesiredClusterState,
	plan *ShardTransitionPlan,
) {
	// Handle starts first
	for _, start := range plan.Starts {
		eg.Go(func() error {
			shardID := start.ShardID

			shardInfo, ok := desired.Shards[shardID]
			if !ok {
				r.logger.Warn(
					"Shard info not found, cannot start",
					zap.Stringer("shardID", shardID),
				)
				return nil
			}

			// Set a cooldown period so we don't split shards in the middle of recovering
			r.SetShardCooldown(shardID, r.getCooldownDuration())

			splitStart := false
			switch shardInfo.State {
			case store.ShardState_SplitOffPreSnap:
				splitStart = true
			case store.ShardState_SplittingOff:
				// should be handled by the split transitioner
				return nil
			}

			if err := r.shardOps.StartShard(ctx, shardID, start.AddPeers, &shardInfo.ShardConfig, false, splitStart); err != nil {
				r.logger.Warn(
					"Failed to start shard",
					zap.Stringer("shardID", shardID),
					zap.Error(err),
				)
				return nil
			}
			return nil
		})
	}

	// Handle stops
	for _, stop := range plan.Stops {
		shardID := stop.ShardID
		r.SetShardCooldown(shardID, r.getCooldownDuration())
		for _, nodeID := range stop.RemovePeers {
			eg.Go(func() error {
				if err := r.shardOps.StopShard(ctx, shardID, nodeID); err != nil {
					if !errors.Is(err, client.ErrNotFound) {
						r.logger.Warn(
							"Failed to stop shard",
							zap.Stringer("shardID", shardID),
							zap.Stringer("nodeID", nodeID),
							zap.Error(err),
						)
					}
					return nil
				}
				return nil
			})
		}
	}

	// Handle transitions (add/remove peers)
	for _, transition := range plan.Transitions {
		shardID := transition.ShardID

		shardInfo, ok := desired.Shards[shardID]
		if !ok {
			r.logger.Warn(
				"Shard info not found, cannot transition",
				zap.Stringer("shardID", shardID),
			)
			continue
		}

		if shardInfo.RaftStatus == nil || len(shardInfo.RaftStatus.Voters) == 0 {
			r.logger.Warn(
				"Shard has no raft status or voters, cannot transition",
				zap.Stringer("shardID", shardID),
			)
			continue
		}

		r.logger.Debug("Transitioning shard on peers",
			zap.Stringer("shardID", shardID),
			zap.Stringer("voters", shardInfo.RaftStatus.Voters.IDSlice()),
			zap.Any("transition", transition),
		)

		// Get the leader client for this shard
		leaderClient, err := r.storeOps.GetLeaderClientForShard(ctx, shardID)
		if err != nil {
			r.logger.Warn(
				"Error finding leader for shard",
				zap.Stringer("shardID", shardID),
				zap.Error(err),
			)
			continue
		}

		// Handle additions
		for _, peerToAdd := range transition.AddPeers {
			if r.IsShardForNodeInCooldown(shardID, peerToAdd) {
				continue // Skip adding if shard is in cooldown for this node
			}

			eg.Go(func() error {
				// Add the peer to the existing raft group
				if err := r.shardOps.AddPeer(ctx, shardID, leaderClient, peerToAdd); err != nil {
					r.logger.Warn(
						"Failed to add peer to shard",
						zap.Stringer("peerToAdd", peerToAdd),
						zap.Stringer("shardID", shardID),
						zap.Error(err),
					)
					return nil
				}

				r.logger.Info(
					"Added peer to shard",
					zap.Stringer("peerToAdd", peerToAdd),
					zap.Stringer("shardID", shardID),
				)

				// Build the peer list for the join request, including the peer we just added.
				// We include peerToAdd because:
				// 1. The AddPeer conf change has been proposed (or committed by now)
				// 2. The joining node needs to know it's a voter to avoid panic when
				//    processing any subsequent conf changes that remove other voters
				voters := make([]types.ID, 0, len(shardInfo.RaftStatus.Voters)+1)
				peerAlreadyInVoters := false
				for voter := range shardInfo.RaftStatus.Voters {
					voters = append(voters, voter)
					if voter == peerToAdd {
						peerAlreadyInVoters = true
					}
				}
				// Only add peerToAdd if not already in voters (avoids duplicate if conf change already committed)
				if !peerAlreadyInVoters {
					voters = append(voters, peerToAdd)
				}

				splitStart := false
				switch shardInfo.State {
				case store.ShardState_SplitOffPreSnap, store.ShardState_SplittingOff:
					splitStart = true
				}

				// Start the shard on the new peer, joining the existing raft group
				if err := r.shardOps.StartShardOnNode(ctx, peerToAdd, shardID, voters, &shardInfo.ShardConfig, splitStart); err != nil {
					r.logger.Warn(
						"Failed to start shard on node",
						zap.Stringer("shardID", shardID),
						zap.Stringer("nodeID", peerToAdd),
						zap.Error(err),
					)
					return nil
				}

				r.logger.Info(
					"Started shard on node",
					zap.Stringer("shardID", shardID),
					zap.Stringer("nodeID", peerToAdd),
				)

				// Set cooldown so we don't immediately try to re-add this peer
				// before it has a chance to send a heartbeat and appear in ReportedBy
				r.SetShardForNodeCooldown(shardID, peerToAdd, r.getCooldownDuration())

				return nil
			})
		}

		// Handle removals
		// Skip removals if we're also adding peers in this transition.
		// Adding peers involves Raft conf changes that must fully propagate before we can
		// safely remove other peers. Removing in the same cycle can cause a race where
		// a node tries to apply the remove conf change before it's caught up with the add.
		if len(transition.AddPeers) > 0 && len(transition.RemovePeers) > 0 {
			r.logger.Info("Skipping peer removals because peers are being added in same transition",
				zap.Stringer("shardID", shardID),
				zap.Any("addPeers", transition.AddPeers),
				zap.Any("removePeers", transition.RemovePeers))
			continue
		}

		for _, peerToRemove := range transition.RemovePeers {
			// Skip if this is a removed store - those are handled separately
			if _, removed := current.RemovedStores[peerToRemove]; removed {
				r.logger.Warn("Peer is in removed stores set, skipping for transition",
					zap.Stringer("shardID", shardID),
					zap.Stringer("peerID", peerToRemove))
				continue
			}

			eg.Go(func() error {
				r.logger.Info("Attempting to remove peer from shard",
					zap.Stringer("shardID", shardID),
					zap.Stringer("nodeID", leaderClient.ID()),
					zap.Stringer("peerToRemove", peerToRemove))

				// Use async removal here since we have retry logic below to verify
				if err := r.shardOps.RemovePeer(ctx, shardID, leaderClient, peerToRemove, true); err != nil {
					r.logger.Warn(
						"Failed to remove peer from shard",
						zap.Stringer("peerToRemove", peerToRemove),
						zap.Stringer("shardID", shardID),
						zap.Error(err),
					)
					return nil
				}

				r.logger.Info(
					"Removed peer from shard",
					zap.Stringer("peerToRemove", peerToRemove),
					zap.Stringer("shardID", shardID),
				)

				// Check if the peer was actually removed, then stop the shard
				// Use retry logic because Raft config changes are asynchronous
				removed := false
				err := retry.Do(
					ctx,
					retry.WithMaxRetries(5, retry.NewExponential(3*time.Second)),
					func(ctx context.Context) error {
						var err error
						removed, err = r.shardOps.IsIDRemoved(
							ctx,
							shardID,
							leaderClient,
							peerToRemove,
						)
						if err != nil {
							r.logger.Warn(
								"Failed to check if ID was removed from shard, retrying",
								zap.Stringer("peerToRemove", peerToRemove),
								zap.Stringer("leaderID", leaderClient.ID()),
								zap.Error(err),
							)
							return retry.RetryableError(err)
						}
						return nil
					},
				)
				if err != nil {
					r.logger.Error(
						"Failed to check if ID was removed from shard after retries",
						zap.Stringer("peerToRemove", peerToRemove),
						zap.Stringer("leaderID", leaderClient.ID()),
						zap.Error(err),
					)
					return nil
				}

				if removed {
					if err := r.shardOps.StopShard(ctx, shardID, peerToRemove); err != nil {
						if !errors.Is(err, client.ErrNotFound) {
							r.logger.Warn(
								"Failed to stop shard on node",
								zap.Stringer("shardID", shardID),
								zap.Stringer("nodeID", peerToRemove),
								zap.Error(err),
							)
							return nil
						}
						// "not found" error is acceptable - shard isn't running anyway
						// Continue to set cooldown
					}

					// Set cooldown so we don't immediately re-add this shard
					r.SetShardForNodeCooldown(shardID, peerToRemove, r.getCooldownDuration())

					r.logger.Info(
						"Stopped shard on node after removal",
						zap.Stringer("shardID", shardID),
						zap.Stringer("nodeID", peerToRemove),
					)
				}

				return nil
			})
		}
	}
}
