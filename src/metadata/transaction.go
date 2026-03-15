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
	"fmt"
	"slices"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/workerpool"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

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
	txnID := uuid.New()

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
	if err := ms.commitTransaction(ctx, coordinatorID, txnID); err != nil {
		// If this fails, coordinator will retry notifications via recovery loop
		return fmt.Errorf("committing transaction: %w", err)
	}

	ms.logger.Info("Transaction committed successfully",
		zap.String("txnID", txnID.String()))

	// Resolve intents - synchronously if syncLevel >= Write, otherwise async
	if syncLevel >= db.Op_SyncLevelWrite {
		// Wait for all participants (and coordinator) to resolve their intents
		if err := ms.notifyParticipantsSync(ctx, coordinatorID, txnID, allShards); err != nil {
			// Log warning but don't fail - transaction is already committed
			ms.logger.Warn("Some shards failed to resolve intents synchronously",
				zap.String("txnID", txnID.String()),
				zap.Error(err))
		}
	} else {
		// Fire-and-forget async notification
		go ms.notifyParticipantsAsync(coordinatorID, txnID, allShards) //nolint:gosec // G118: intentional fire-and-forget async notification
	}

	return nil
}

// notifyParticipantsAsync sends resolve intent notifications to all participant shards
func (ms *MetadataStore) notifyParticipantsAsync(
	coordinatorID types.ID,
	txnID uuid.UUID,
	allShards map[types.ID]struct{},
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

			if err := storeClient.ResolveIntent(ctx, shardID, txnID[:], db.TxnStatusCommitted); err != nil {
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

// notifyParticipantsSync sends resolve intent notifications to all shards and waits for completion.
// Unlike notifyParticipantsAsync, this includes the coordinator shard and blocks until all resolve.
func (ms *MetadataStore) notifyParticipantsSync(
	ctx context.Context,
	coordinatorID types.ID,
	txnID uuid.UUID,
	allShards map[types.ID]struct{},
) error {
	g, _ := workerpool.NewGroup(ctx, ms.pool)

	for shardID := range allShards {
		g.Go(func(ctx context.Context) error {
			storeClient, err := ms.leaderClientForShard(ctx, shardID)
			if err != nil {
				return fmt.Errorf("getting client for shard %s: %w", shardID, err)
			}

			if err := storeClient.ResolveIntent(ctx, shardID, txnID[:], db.TxnStatusCommitted); err != nil {
				return fmt.Errorf("resolving intents on shard %s: %w", shardID, err)
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

	// Get shard leader client
	storeClient, err := ms.leaderClientForShard(ctx, coordinatorID)
	if err != nil {
		return fmt.Errorf("getting coordinator client: %w", err)
	}

	return storeClient.InitTransaction(ctx, coordinatorID, txnID[:], timestamp, participants)
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
	}

	// All intents must succeed, otherwise abort
	return g.Wait()
}

// commitTransaction tells coordinator to commit
func (ms *MetadataStore) commitTransaction(
	ctx context.Context,
	coordinatorID types.ID,
	txnID uuid.UUID,
) error {
	// Get shard leader client
	storeClient, err := ms.leaderClientForShard(ctx, coordinatorID)
	if err != nil {
		return fmt.Errorf("getting coordinator client: %w", err)
	}

	return storeClient.CommitTransaction(ctx, coordinatorID, txnID[:])
}

// abortTransaction tells coordinator to abort
func (ms *MetadataStore) abortTransaction(
	ctx context.Context,
	coordinatorID types.ID,
	txnID uuid.UUID,
) error {
	// Get shard leader client
	storeClient, err := ms.leaderClientForShard(ctx, coordinatorID)
	if err != nil {
		return fmt.Errorf("getting coordinator client: %w", err)
	}

	return storeClient.AbortTransaction(ctx, coordinatorID, txnID[:])
}
