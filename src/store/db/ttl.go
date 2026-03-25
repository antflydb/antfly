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

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/antflydb/antfly/lib/clock"
	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"go.uber.org/zap"
)

const (
	// TTLCleanupInterval defines how often the TTL cleanup job runs
	TTLCleanupInterval = 30 * time.Second
	// TTLBatchSize is the number of documents to process in each cleanup batch
	TTLBatchSize = 1000
	// TTLGracePeriod is added to expiration time to prevent premature deletion
	// This ensures writes are fully replicated before expiration processing begins
	TTLGracePeriod = 5 * time.Second
)

// TTLCleaner handles background cleanup of expired documents
type TTLCleaner struct {
	db                  *DBImpl
	logger              *zap.Logger
	documentsExpired    int64 // Total documents expired since start
	lastCleanupDuration time.Duration
	clock               clock.Clock
}

// NewTTLCleaner creates a new TTL cleanup worker
func NewTTLCleaner(db *DBImpl) *TTLCleaner {
	return NewTTLCleanerWithClock(db, clock.RealClock{})
}

// NewTTLCleanerWithClock creates a new TTL cleanup worker with a custom clock.
func NewTTLCleanerWithClock(db *DBImpl, clk clock.Clock) *TTLCleaner {
	if clk == nil {
		clk = clock.RealClock{}
	}
	return &TTLCleaner{
		db:     db,
		logger: db.logger.Named("ttl-cleaner"),
		clock:  clk,
	}
}

// Start begins the TTL cleanup job (runs only on Raft leader)
// This follows the LeaderFactory pattern and runs until ctx is cancelled
func (tc *TTLCleaner) Start(ctx context.Context, persistFunc PersistFunc) error {
	// Check if TTL is configured
	if tc.db.schema.TtlDuration == "" {
		tc.logger.Debug("TTL not configured, cleanup job will not run")
		return nil
	}

	ttlDuration, err := time.ParseDuration(tc.db.schema.TtlDuration)
	if err != nil {
		return fmt.Errorf("parsing TTL duration %q: %w", tc.db.schema.TtlDuration, err)
	}

	tc.logger.Info("Starting TTL cleanup job",
		zap.Duration("ttl_duration", ttlDuration),
		zap.Duration("cleanup_interval", TTLCleanupInterval),
		zap.Duration("grace_period", TTLGracePeriod))

	ticker := tc.clock.NewTicker(TTLCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			tc.logger.Info("TTL cleanup job stopped",
				zap.Int64("total_documents_expired", tc.documentsExpired))
			return ctx.Err()

		case <-ticker.C():
			startTime := tc.clock.Now()
			expired, err := tc.cleanupExpiredDocuments(ctx, ttlDuration, persistFunc)
			tc.lastCleanupDuration = tc.clock.Now().Sub(startTime)

			if err != nil && !errors.Is(err, context.Canceled) {
				tc.logger.Error("Failed to cleanup expired documents",
					zap.Error(err),
					zap.Duration("duration", tc.lastCleanupDuration))
				continue
			}

			if expired > 0 {
				tc.documentsExpired += int64(expired)
				tc.logger.Info("Cleaned up expired documents",
					zap.Int("count", expired),
					zap.Duration("duration", tc.lastCleanupDuration),
					zap.Int64("total_expired", tc.documentsExpired))
			} else if tc.lastCleanupDuration > time.Second {
				tc.logger.Debug("TTL cleanup scan completed",
					zap.Int("expired_count", 0),
					zap.Duration("duration", tc.lastCleanupDuration))
			}
		}
	}
}

// cleanupExpiredDocuments scans for and deletes expired documents
// Uses the HLC transaction timestamp keys (:t suffix) written by the distributed transaction system
// Returns the number of documents deleted
func (tc *TTLCleaner) cleanupExpiredDocuments(
	ctx context.Context,
	ttlDuration time.Duration,
	persistFunc PersistFunc,
) (_ int, err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)

	// Check if context is already cancelled before proceeding
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	// Create iterator to scan all keys in the shard's range
	byteRange := tc.db.getByteRange()
	iterOpts := &pebble.IterOptions{
		LowerBound: byteRange[0],
		UpperBound: byteRange[1],
	}

	pdb := tc.db.getPDB()
	if pdb == nil {
		tc.logger.Debug("Skipping TTL cleanup, database closed")
		return 0, nil
	}

	iter, err := pdb.NewIterWithContext(ctx, iterOpts)
	if err != nil {
		// Check if error is due to closed database
		if errors.Is(err, pebble.ErrClosed) {
			tc.logger.Debug("Skipping TTL cleanup, database closed")
			return 0, nil
		}
		return 0, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	now := tc.clock.Now().UTC()
	// Calculate expiration threshold with grace period
	// Documents expire when: write_timestamp + ttl_duration + grace_period < now
	expirationThreshold := uint64(now.Add(-ttlDuration - TTLGracePeriod).UnixNano())
	var expiredKeys [][]byte
	totalExpired := 0
	scannedTimestampKeys := 0

	// Scan through all keys looking for transaction timestamp keys (:t suffix)
	for iter.First(); iter.Valid(); iter.Next() {
		select {
		case <-ctx.Done():
			return totalExpired + len(expiredKeys), ctx.Err()
		default:
		}

		key := iter.Key()

		// Skip metadata keys
		if bytes.HasPrefix(key, MetadataPrefix) {
			continue
		}

		// Only process transaction timestamp keys (those ending with :t)
		if !bytes.HasSuffix(key, storeutils.TransactionSuffix) {
			continue
		}

		scannedTimestampKeys++

		// Extract the base key (remove :t suffix)
		baseKey := bytes.TrimSuffix(key, storeutils.TransactionSuffix)

		// Read the timestamp value (uint64 encoded with EncodeUint64Ascending)
		timestampBytes := iter.Value()
		if len(timestampBytes) < 8 {
			tc.logger.Warn("Invalid timestamp metadata",
				zap.String("key", types.FormatKey(baseKey)),
				zap.Int("bytesLength", len(timestampBytes)))
			continue
		}

		// Decode the HLC timestamp (nanoseconds since Unix epoch)
		_, timestamp, err := encoding.DecodeUint64Ascending(timestampBytes)
		if err != nil {
			tc.logger.Warn("Failed to decode timestamp",
				zap.String("key", types.FormatKey(baseKey)),
				zap.Error(err))
			continue
		}

		// Check if document is expired
		if timestamp < expirationThreshold {
			expiredKeys = append(expiredKeys, bytes.Clone(baseKey))

			// Process batch if we've hit the limit
			if len(expiredKeys) >= TTLBatchSize {
				if err := tc.deleteExpiredBatch(ctx, expiredKeys, persistFunc); err != nil {
					return totalExpired + len(expiredKeys), fmt.Errorf("deleting expired batch: %w", err)
				}
				totalExpired += len(expiredKeys)
				expiredKeys = expiredKeys[:0] // Reset slice
			}
		}
	}

	if err := iter.Error(); err != nil {
		return totalExpired + len(expiredKeys), fmt.Errorf("iterator error: %w", err)
	}

	tc.logger.Debug("TTL scan completed",
		zap.Int("scanned_timestamp_keys", scannedTimestampKeys),
		zap.Int("expired_documents", totalExpired+len(expiredKeys)))

	// Delete remaining expired documents
	if len(expiredKeys) > 0 {
		if err := tc.deleteExpiredBatch(ctx, expiredKeys, persistFunc); err != nil {
			return totalExpired + len(expiredKeys), fmt.Errorf("deleting final expired batch: %w", err)
		}
		totalExpired += len(expiredKeys)
	}

	return totalExpired, nil
}

// deleteExpiredBatch deletes a batch of expired documents through Raft consensus
func (tc *TTLCleaner) deleteExpiredBatch(
	ctx context.Context,
	keys [][]byte,
	persistFunc PersistFunc,
) error {
	return deleteExpiredKeysBatch(ctx, keys, persistFunc, func(keys [][]byte) error {
		// Direct deletion fallback for documents: delete entire key range
		pdb := tc.db.getPDB()
		if pdb == nil {
			return fmt.Errorf("database closed during TTL cleanup")
		}
		batch := pdb.NewBatch()
		defer func() { _ = batch.Close() }()

		for _, key := range keys {
			deleteStart := storeutils.KeyRangeStart(key)
			deleteEnd := storeutils.KeyRangeEnd(key)
			if err := batch.DeleteRange(deleteStart, deleteEnd, nil); err != nil {
				return fmt.Errorf("deleting key range: %w", err)
			}
		}

		if err := batch.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("committing delete batch: %w", err)
		}

		if err := tc.db.BatchIndex(ctx, nil, keys, Op_SyncLevelFullText); err != nil {
			tc.logger.Error("Failed to delete from indexes", zap.Error(err))
		}
		return nil
	}, tc.logger)
}

// deleteExpiredKeysBatch is the shared implementation for deleting expired keys
// through Raft consensus, used by both TTLCleaner and EdgeTTLCleaner.
// directDeleteFn handles type-specific direct deletion when persistFunc is nil.
func deleteExpiredKeysBatch(
	ctx context.Context,
	keys [][]byte,
	persistFunc PersistFunc,
	directDeleteFn func(keys [][]byte) error,
	logger *zap.Logger,
) error {
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case error:
				if errors.Is(e, pebble.ErrClosed) {
					logger.Debug("Skipping TTL delete batch, database closed")
					return
				}
			}
			panic(r)
		}
	}()

	if len(keys) == 0 {
		return nil
	}

	// Convert deletes to [][2][]byte format expected by persistFunc
	writes := make([][2][]byte, len(keys))
	for i, key := range keys {
		writes[i] = [2][]byte{key, nil}
	}

	if persistFunc != nil {
		if err := persistFunc(ctx, writes); err != nil {
			return fmt.Errorf("persisting TTL delete batch: %w", err)
		}
		return nil
	}

	// Fallback to direct deletion (should not happen in leader-only context)
	logger.Warn("No persist function provided, deleting directly (may cause inconsistency)")
	return directDeleteFn(keys)
}

// Stats returns statistics about TTL cleanup
func (tc *TTLCleaner) Stats() map[string]any {
	return map[string]any{
		"total_documents_expired":  tc.documentsExpired,
		"last_cleanup_duration_ms": tc.lastCleanupDuration.Milliseconds(),
	}
}

// isDocumentExpiredByTimestamp checks if a document is expired by reading its HLC timestamp.
// This is used for query filtering. Returns true if expired, false otherwise.
func (db *DBImpl) isDocumentExpiredByTimestamp(ctx context.Context, key []byte) (bool, error) {
	// Check if TTL is configured (cachedTTLDuration is pre-parsed from schema)
	ttlDuration := db.cachedTTLDuration
	if ttlDuration == 0 {
		return false, nil
	}

	// Take read lock to prevent access during close/reopen cycle (e.g., during splits)
	pdb := db.getPDB()

	// Guard against accessing a nil or closed database (can happen during splits)
	if pdb == nil {
		return false, fmt.Errorf("database is closed or not initialized")
	}

	// Build timestamp key: <baseKey>:t
	timestampKey := append(bytes.Clone(key), storeutils.TransactionSuffix...)

	// Read the timestamp value
	timestampBytes, closer, err := pdb.Get(timestampKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// No timestamp key means document was inserted before timestamps were enabled
			// Treat as not expired
			return false, nil
		}
		if errors.Is(err, pebble.ErrClosed) {
			// Database is closed (can happen during splits) - treat as transient error
			return false, fmt.Errorf("database is closed: %w", err)
		}
		return false, fmt.Errorf("reading timestamp key: %w", err)
	}
	defer func() { _ = closer.Close() }()

	// Decode the HLC timestamp
	if len(timestampBytes) < 8 {
		return false, fmt.Errorf("invalid timestamp metadata, length %d", len(timestampBytes))
	}

	_, timestamp, err := encoding.DecodeUint64Ascending(timestampBytes)
	if err != nil {
		return false, fmt.Errorf("decoding timestamp: %w", err)
	}

	// Check if expired (without grace period for query filtering - immediate filtering)
	// Convert timestamp (nanoseconds) to time.Time
	writeTime := time.Unix(0, int64(timestamp)) //nolint:gosec // G115: bounded value, cannot overflow in practice
	expirationTime := writeTime.Add(ttlDuration)
	now := time.Now().UTC()

	return now.After(expirationTime), nil
}
