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
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/workerpool"
	storeclient "github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Distributed transaction control-plane RPCs can cross leader elections,
// store restarts, and simulator-induced proposal drops. Keep the retry
// budget long enough to ride out a couple of election intervals without
// forcing callers to build their own retry loops around ExecuteTransaction.
const maxTxnRetryAttempts = 20

// collectShards returns the union of shard IDs across writes, deletes, and transforms.
func collectShards(
	writes map[types.ID][][2][]byte,
	deletes map[types.ID][][]byte,
	transforms map[types.ID][]*db.Transform,
) map[types.ID]struct{} {
	set := make(map[types.ID]struct{})
	for id := range writes {
		set[id] = struct{}{}
	}
	for id := range deletes {
		set[id] = struct{}{}
	}
	for id := range transforms {
		set[id] = struct{}{}
	}
	return set
}

// ExecuteTransaction orchestrates distributed transaction across shards.
// If syncLevel is SyncLevelWrite or higher, waits for all intents to be resolved before returning.
// predicates is an optional map of version predicates per shard for OCC validation.
// Pass nil for non-OCC transactions.
func (ms *MetadataStore) ExecuteTransaction(
	ctx context.Context,
	writes map[types.ID][][2][]byte,
	deletes map[types.ID][][]byte,
	transforms map[types.ID][]*db.Transform,
	predicates map[types.ID][]*db.VersionPredicate,
	syncLevel db.Op_SyncLevel,
) error {
	// Generate transaction ID
	generateTxnID := ms.txnIDGenerator
	if generateTxnID == nil {
		generateTxnID = uuid.New
	}
	txnID := generateTxnID()

	// Allocate timestamp
	timestamp := ms.hlc.Now()

	// Compute participating shards once (used by all phases).
	allShards := collectShards(writes, deletes, transforms)

	// Pick coordinator shard
	coordinatorID := ms.pickCoordinator(txnID, allShards)

	ms.logger.Info("Starting distributed transaction",
		zap.String("txnID", txnID.String()),
		zap.Uint64("timestamp", timestamp),
		zap.Stringer("coordinator", coordinatorID),
		zap.Int("numShards", len(allShards)))

	// PHASE 1: Prepare

	// Step 1: Initialize transaction on coordinator
	if err := ms.initTransaction(ctx, coordinatorID, txnID, timestamp, allShards); err != nil {
		return fmt.Errorf("initializing transaction: %w", err)
	}

	// Step 2: Write intents to all participants (parallel)
	ms.traceCheckPredicates(txnID, allShards)
	if err := ms.writeIntents(ctx, txnID, timestamp, coordinatorID, allShards, writes, deletes, transforms, predicates); err != nil {
		// Abort transaction on coordinator. If this fails, the recovery loop
		// will eventually auto-abort the stale Pending transaction.
		if abortErr := ms.abortTransaction(ctx, coordinatorID, txnID); abortErr != nil {
			ms.logger.Warn("Failed to abort transaction after intent write error; recovery loop will handle cleanup",
				zap.String("txnID", txnID.String()),
				zap.Stringer("coordinator", coordinatorID),
				zap.Error(abortErr))
		}
		return fmt.Errorf("writing intents: %w", err)
	}

	// PHASE 2: Commit

	// Step 3: Commit transaction on coordinator (commit point!)
	commitVersion, err := ms.commitTransaction(ctx, coordinatorID, txnID)
	if err != nil {
		// If this fails, coordinator will retry notifications via recovery loop
		return fmt.Errorf("committing transaction: %w", err)
	}

	ms.logger.Info("Transaction committed successfully",
		zap.String("txnID", txnID.String()),
		zap.Uint64("commitVersion", commitVersion))

	// Resolve intents - synchronously if syncLevel >= Write, otherwise async
	if syncLevel >= db.Op_SyncLevelWrite {
		// Wait for all participants (and coordinator) to resolve their intents
		if err := ms.notifyParticipantsSync(ctx, coordinatorID, txnID, allShards, commitVersion); err != nil {
			// Log warning but don't fail - transaction is already committed
			ms.logger.Warn("Some shards failed to resolve intents synchronously",
				zap.String("txnID", txnID.String()),
				zap.Error(err))
		}
	} else {
		// Fire-and-forget async notification
		go ms.notifyParticipantsAsync(coordinatorID, txnID, allShards, commitVersion) //nolint:gosec // G118: intentional fire-and-forget async notification
	}

	return nil
}

// notifyParticipantsAsync sends resolve intent notifications to all participant shards
func (ms *MetadataStore) notifyParticipantsAsync(
	coordinatorID types.ID,
	txnID uuid.UUID,
	allShards map[types.ID]struct{},
	commitVersion uint64,
) {
	// Copy and remove coordinator — participants only.
	participantShards := make(map[types.ID]struct{}, len(allShards))
	for id := range allShards {
		participantShards[id] = struct{}{}
	}
	delete(participantShards, coordinatorID)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	g, _ := workerpool.NewGroup(ctx, ms.pool)

	for shardID := range participantShards {
		g.Go(func(ctx context.Context) error {
			storeClient, err := ms.leaderClientForShard(ctx, shardID)
			if err != nil {
				ms.logger.Warn("Failed to get client for participant notification",
					zap.Stringer("shardID", shardID),
					zap.String("txnID", txnID.String()),
					zap.Error(err))
				return nil // best-effort; recovery loop will retry
			}

			if err := storeClient.ResolveIntent(ctx, shardID, txnID[:], db.TxnStatusCommitted, commitVersion); err != nil {
				ms.logger.Warn("Failed to notify participant to resolve intents",
					zap.Stringer("shardID", shardID),
					zap.String("txnID", txnID.String()),
					zap.Error(err))
			} else {
				ms.logger.Debug("Notified participant to resolve intents",
					zap.Stringer("shardID", shardID),
					zap.String("txnID", txnID.String()))
			}
			return nil
		})
	}

	// Fire-and-forget: log but don't surface errors.
	go func() { _ = g.Wait() }()
}

// notifyParticipantsSync sends resolve intent notifications to all shards and
// waits for completion.
//
// The coordinator still participates in the SyncLevelWrite contract: the local
// resolve path started by commit is asynchronous, so this explicit wait keeps
// the API from returning before the coordinator's own intents are durable.
func (ms *MetadataStore) notifyParticipantsSync(
	ctx context.Context,
	coordinatorID types.ID,
	txnID uuid.UUID,
	allShards map[types.ID]struct{},
	commitVersion uint64,
) error {
	g, _ := workerpool.NewGroup(ctx, ms.pool)

	for shardID := range allShards {
		g.Go(func(ctx context.Context) error {
			if err := ms.retryTxnOp(ctx, "resolve intents", func(ctx context.Context) error {
				storeClient, err := ms.leaderClientForShard(ctx, shardID)
				if err != nil {
					return fmt.Errorf("getting client for shard %s: %w", shardID, err)
				}
				if err := storeClient.ResolveIntent(ctx, shardID, txnID[:], db.TxnStatusCommitted, commitVersion); err != nil {
					return fmt.Errorf("resolving intents on shard %s: %w", shardID, err)
				}
				return nil
			}); err != nil {
				return err
			}

			ms.logger.Debug("Resolved intents synchronously",
				zap.Stringer("shardID", shardID),
				zap.String("txnID", txnID.String()))
			return nil
		})
	}

	return g.Wait()
}

// pickCoordinator selects coordinator shard deterministically
func (ms *MetadataStore) pickCoordinator(
	txnID uuid.UUID,
	shardIDSet map[types.ID]struct{},
) types.ID {
	// Hash txnID to pick coordinator from participants
	hash := xxhash.Sum64(txnID[:])

	// Sort shard IDs for determinism
	shardIDs := make([]types.ID, 0, len(shardIDSet))
	for id := range shardIDSet {
		shardIDs = append(shardIDs, id)
	}
	slices.Sort(shardIDs)

	return shardIDs[hash%uint64(len(shardIDs))]
}

// initTransaction creates transaction record on coordinator
func (ms *MetadataStore) initTransaction(
	ctx context.Context,
	coordinatorID types.ID,
	txnID uuid.UUID,
	timestamp uint64,
	allShards map[types.ID]struct{},
) error {
	// Build participant list (all shards excluding coordinator).
	// Copy to avoid mutating the caller's map.
	participantSet := make(map[types.ID]struct{}, len(allShards))
	for id := range allShards {
		participantSet[id] = struct{}{}
	}
	delete(participantSet, coordinatorID)

	participants := make([][]byte, 0, len(participantSet))
	for shardID := range participantSet {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(shardID))
		participants = append(participants, b)
	}

	return ms.retryTxnOp(ctx, "init transaction", func(ctx context.Context) error {
		storeClient, err := ms.leaderClientForShard(ctx, coordinatorID)
		if err != nil {
			return fmt.Errorf("getting coordinator client: %w", err)
		}
		return storeClient.InitTransaction(ctx, coordinatorID, txnID[:], timestamp, participants)
	})
}

// writeIntents sends write intents to all participating shards.
// predicates is an optional map of version predicates per shard for OCC validation.
func (ms *MetadataStore) writeIntents(
	ctx context.Context,
	txnID uuid.UUID,
	timestamp uint64,
	coordinatorID types.ID,
	allShards map[types.ID]struct{},
	writes map[types.ID][][2][]byte,
	deletes map[types.ID][][]byte,
	transforms map[types.ID][]*db.Transform,
	predicates map[types.ID][]*db.VersionPredicate,
) error {
	g, _ := workerpool.NewGroup(ctx, ms.pool)

	// Convert coordinatorID to bytes once for all shards
	coordinatorBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(coordinatorBytes, uint64(coordinatorID))

	// Send intents to all shards
	for shardID := range allShards {
		kvPairs := writes[shardID]
		deleteKeys := deletes[shardID]
		shardTransforms := transforms[shardID]
		shardPredicates := predicates[shardID] // nil if no predicates for this shard

		g.Go(func(ctx context.Context) error {
			return ms.retryTxnOp(ctx, "write intents", func(ctx context.Context) error {
				// Get shard leader client. Uses leaderClientForShard (with follower fallback)
				// rather than the strict path because writes are rejected by followers with
				// "not leader", causing the transaction to abort — the fallback is harmless
				// and helps during rebalancing when Raft leadership moves but metadata is stale.
				storeClient, err := ms.leaderClientForShard(ctx, shardID)
				if err != nil {
					return fmt.Errorf("getting client for shard %s: %w", shardID, err)
				}
				return storeClient.WriteIntent(ctx, shardID, txnID[:], timestamp, coordinatorBytes, kvPairs, deleteKeys, shardTransforms, shardPredicates)
			})
		})
	}

	// All intents must succeed, otherwise abort
	return g.Wait()
}

// commitTransaction tells coordinator to commit
func (ms *MetadataStore) commitTransaction(
	ctx context.Context,
	coordinatorID types.ID,
	txnID uuid.UUID,
) (uint64, error) {
	return ms.retryTxnUint64Op(ctx, "commit transaction", func(ctx context.Context) (uint64, error) {
		storeClient, err := ms.leaderClientForShard(ctx, coordinatorID)
		if err != nil {
			return 0, fmt.Errorf("getting coordinator client: %w", err)
		}
		return storeClient.CommitTransaction(ctx, coordinatorID, txnID[:])
	})
}

// abortTransaction tells coordinator to abort
func (ms *MetadataStore) abortTransaction(
	ctx context.Context,
	coordinatorID types.ID,
	txnID uuid.UUID,
) error {
	return ms.retryTxnOp(ctx, "abort transaction", func(ctx context.Context) error {
		storeClient, err := ms.leaderClientForShard(ctx, coordinatorID)
		if err != nil {
			return fmt.Errorf("getting coordinator client: %w", err)
		}
		return storeClient.AbortTransaction(ctx, coordinatorID, txnID[:])
	})
}

func (ms *MetadataStore) retryTxnOp(ctx context.Context, label string, fn func(context.Context) error) error {
	var lastErr error
	for attempt := range maxTxnRetryAttempts {
		if err := fn(ctx); err == nil {
			return nil
		} else {
			lastErr = err
			if !isRetryableTxnError(err) || attempt == maxTxnRetryAttempts-1 {
				return err
			}
			// Refresh shard and store status before retrying so the next attempt
			// can re-route away from a stale leader view after elections or
			// proposal drops.
			ms.runHealthCheck(ctx)
			ms.clockOrReal().Sleep(ms.txnRetryDelay(attempt))
			ms.logger.Debug("Retrying transaction step",
				zap.String("step", label),
				zap.Int("attempt", attempt+2),
				zap.Error(err))
		}
	}
	return lastErr
}

func (ms *MetadataStore) retryTxnUint64Op(ctx context.Context, label string, fn func(context.Context) (uint64, error)) (uint64, error) {
	var lastErr error
	for attempt := range maxTxnRetryAttempts {
		value, err := fn(ctx)
		if err == nil {
			return value, nil
		}
		lastErr = err
		if !isRetryableTxnError(err) || attempt == maxTxnRetryAttempts-1 {
			return 0, err
		}
		// Refresh shard and store status before retrying so the next attempt
		// can re-route away from a stale leader view after elections or
		// proposal drops.
		ms.runHealthCheck(ctx)
		ms.clockOrReal().Sleep(ms.txnRetryDelay(attempt))
		ms.logger.Debug("Retrying transaction step",
			zap.String("step", label),
			zap.Int("attempt", attempt+2),
			zap.Error(err))
	}
	return 0, lastErr
}

func (ms *MetadataStore) txnRetryDelay(attempt int) time.Duration {
	return 250 * time.Millisecond
}

func isRetryableTxnError(err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, storeclient.ErrProposalDropped),
		errors.Is(err, storeclient.ErrNotLeader),
		errors.Is(err, storeclient.ErrNoHealthyPeer),
		errors.Is(err, storeclient.ErrShardInitializing),
		errors.Is(err, storeclient.ErrShardNotReady):
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "proposal dropped") ||
		strings.Contains(msg, "operation did not complete") ||
		strings.Contains(msg, "no leader") ||
		strings.Contains(msg, "not leader")
}
