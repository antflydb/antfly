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

	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"

	json "github.com/antflydb/antfly/pkg/libaf/json"
	"go.uber.org/zap"
)

// EdgeTTLCleaner handles background cleanup of expired edges
type EdgeTTLCleaner struct {
	db                  *DBImpl
	logger              *zap.Logger
	indexConfigs        map[string]time.Duration // index name → TTL duration
	edgesExpired        int64                    // Total edges expired since start
	lastCleanupDuration time.Duration
}

// NewEdgeTTLCleaner creates a new edge TTL cleanup worker
func NewEdgeTTLCleaner(db *DBImpl) *EdgeTTLCleaner {
	return &EdgeTTLCleaner{
		db:           db,
		logger:       db.logger.Named("edge-ttl-cleaner"),
		indexConfigs: db.getEdgeTTLConfigs(),
	}
}

// Start begins the edge TTL cleanup job (runs only on Raft leader)
// This follows the LeaderFactory pattern and runs until ctx is cancelled
func (etc *EdgeTTLCleaner) Start(ctx context.Context, persistFunc PersistFunc) error {
	if len(etc.indexConfigs) == 0 {
		etc.logger.Debug("No edge TTL configured, cleanup job will not run")
		return nil
	}

	etc.logger.Info("Starting edge TTL cleanup job",
		zap.Int("indexes_with_ttl", len(etc.indexConfigs)),
		zap.Duration("cleanup_interval", TTLCleanupInterval),
		zap.Duration("grace_period", TTLGracePeriod))

	ticker := time.NewTicker(TTLCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			etc.logger.Info("Edge TTL cleanup job stopped",
				zap.Int64("total_edges_expired", etc.edgesExpired))
			return ctx.Err()

		case <-ticker.C:
			startTime := time.Now()
			expired, err := etc.cleanupExpiredEdges(ctx, persistFunc)
			etc.lastCleanupDuration = time.Since(startTime)

			if err != nil && !errors.Is(err, context.Canceled) {
				etc.logger.Error("Failed to cleanup expired edges",
					zap.Error(err),
					zap.Duration("duration", etc.lastCleanupDuration))
				continue
			}

			if expired > 0 {
				etc.edgesExpired += int64(expired)
				etc.logger.Info("Cleaned up expired edges",
					zap.Int("count", expired),
					zap.Duration("duration", etc.lastCleanupDuration),
					zap.Int64("total_expired", etc.edgesExpired))
			} else if etc.lastCleanupDuration > time.Second {
				etc.logger.Debug("Edge TTL cleanup scan completed",
					zap.Int("expired_count", 0),
					zap.Duration("duration", etc.lastCleanupDuration))
			}
		}
	}
}

// cleanupExpiredEdges scans for and deletes expired edges
// Uses the HLC transaction timestamp keys (:t suffix) to determine expiration
// Returns the number of edges deleted
func (etc *EdgeTTLCleaner) cleanupExpiredEdges(
	ctx context.Context,
	persistFunc PersistFunc,
) (int, error) {
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case error:
				if errors.Is(e, pebble.ErrClosed) {
					etc.logger.Debug("Skipping edge TTL cleanup, database closed")
					return
				}
			}
			panic(r)
		}
	}()

	// Check if context is already cancelled before proceeding
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	now := time.Now().UTC()
	var expiredKeys [][]byte
	totalExpired := 0

	// Create iterator to scan all keys in the shard's range
	iterOpts := &pebble.IterOptions{}

	// Only set bounds if they're not empty (test DBs may have empty ranges)
	if len(etc.db.byteRange[0]) > 0 {
		iterOpts.LowerBound = etc.db.byteRange[0]
	}
	if len(etc.db.byteRange[1]) > 0 {
		iterOpts.UpperBound = etc.db.byteRange[1]
	}

	pdb := etc.db.getPDB()
	if pdb == nil {
		etc.logger.Debug("Skipping edge TTL cleanup, database closed")
		return 0, nil
	}

	iter, err := pdb.NewIterWithContext(ctx, iterOpts)
	if err != nil {
		// Check if error is due to closed database
		if errors.Is(err, pebble.ErrClosed) {
			etc.logger.Debug("Skipping edge TTL cleanup, database closed")
			return 0, nil
		}
		return 0, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() {
		_ = iter.Close()
	}()

	scannedTimestampKeys := 0

	// Scan through all keys looking for edge timestamp keys
	for iter.First(); iter.Valid(); iter.Next() {
		select {
		case <-ctx.Done():
			return totalExpired + len(expiredKeys), ctx.Err()
		default:
		}

		key := iter.Key()

		// Only process transaction timestamp keys (those ending with :t)
		if !bytes.HasSuffix(key, storeutils.TransactionSuffix) {
			continue
		}

		// Extract the base edge key (remove :t suffix)
		baseEdgeKey := bytes.TrimSuffix(key, storeutils.TransactionSuffix)

		// Only process edge keys (skip document keys)
		// Edge keys have pattern: <docKey>:o:i:<indexName>:out:<edgeType>:<target>:o
		// or: <docKey>:o:i:<indexName>:in:<edgeType>:<source>:i
		if !storeutils.IsEdgeKey(baseEdgeKey) {
			continue
		}

		// Extract index name from edge key to check if it has TTL configured
		// Pattern: <docKey>:o:i:<indexName>:out|in:...
		indexName := extractIndexName(baseEdgeKey)
		if indexName == "" {
			continue
		}

		// Check if this index has TTL configured
		ttlDuration, hasTTL := etc.indexConfigs[indexName]
		if !hasTTL {
			continue
		}

		scannedTimestampKeys++

		// Calculate expiration threshold for this index
		expirationThreshold := uint64(now.Add(-ttlDuration - TTLGracePeriod).UnixNano())

		// Read the timestamp value (same encoding as document timestamps)
		timestampBytes := iter.Value()
		if len(timestampBytes) < 8 {
			etc.logger.Warn("Invalid timestamp metadata",
				zap.ByteString("edgeKey", baseEdgeKey),
				zap.Int("bytesLength", len(timestampBytes)))
			continue
		}

		// Decode the HLC timestamp (nanoseconds since Unix epoch)
		_, timestamp, err := encoding.DecodeUint64Ascending(timestampBytes)
		if err != nil {
			etc.logger.Warn("Failed to decode timestamp",
				zap.ByteString("edgeKey", baseEdgeKey),
				zap.Error(err))
			continue
		}

		// Check if edge is expired
		if timestamp < expirationThreshold {
			expiredKeys = append(expiredKeys, bytes.Clone(baseEdgeKey))

			// Process batch if we've hit the limit
			if len(expiredKeys) >= TTLBatchSize {
				if err := etc.deleteExpiredBatch(ctx, expiredKeys, persistFunc); err != nil {
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

	etc.logger.Debug("Edge TTL scan completed",
		zap.Int("scanned_timestamp_keys", scannedTimestampKeys),
		zap.Int("expired_edges", totalExpired+len(expiredKeys)))

	// Delete remaining expired edges
	if len(expiredKeys) > 0 {
		if err := etc.deleteExpiredBatch(ctx, expiredKeys, persistFunc); err != nil {
			return totalExpired + len(expiredKeys), fmt.Errorf("deleting final expired batch: %w", err)
		}
		totalExpired += len(expiredKeys)
	}

	return totalExpired, nil
}

// deleteExpiredBatch deletes a batch of expired edges through Raft consensus
func (etc *EdgeTTLCleaner) deleteExpiredBatch(
	ctx context.Context,
	keys [][]byte,
	persistFunc PersistFunc,
) error {
	return deleteExpiredKeysBatch(ctx, keys, persistFunc, func(keys [][]byte) error {
		// Direct deletion fallback for edges: delete individual keys + timestamp keys
		pdb := etc.db.getPDB()
		if pdb == nil {
			return fmt.Errorf("database closed during edge TTL cleanup")
		}
		batch := pdb.NewBatch()
		defer func() { _ = batch.Close() }()

		for _, key := range keys {
			if err := batch.Delete(key, nil); err != nil {
				return fmt.Errorf("deleting edge key: %w", err)
			}
			timestampKey := append(bytes.Clone(key), storeutils.TransactionSuffix...)
			if err := batch.Delete(timestampKey, nil); err != nil {
				return fmt.Errorf("deleting edge timestamp key: %w", err)
			}
		}

		if err := batch.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("committing delete batch: %w", err)
		}

		if err := etc.db.indexManager.Batch(ctx, nil, keys, Op_SyncLevelFullText); err != nil {
			etc.logger.Error("Failed to update graph indexes after edge deletion", zap.Error(err))
		}
		return nil
	}, etc.logger)
}

// Stats returns statistics about edge TTL cleanup
func (etc *EdgeTTLCleaner) Stats() map[string]any {
	return map[string]any{
		"total_edges_expired":      etc.edgesExpired,
		"last_cleanup_duration_ms": etc.lastCleanupDuration.Milliseconds(),
		"indexes_with_ttl":         len(etc.indexConfigs),
	}
}

// extractIndexName extracts the index name from an edge key
// Edge keys have pattern: <docKey>:o:i:<indexName>:out|in:...
func extractIndexName(edgeKey []byte) string {
	// Find :i: marker
	iMarker := []byte(":i:")
	iPos := bytes.Index(edgeKey, iMarker)
	if iPos == -1 {
		return ""
	}

	// Start after :i:
	start := iPos + len(iMarker)

	// Find next : which ends the index name
	remaining := edgeKey[start:]
	before, _, ok := bytes.Cut(remaining, []byte{':'})
	if !ok {
		return ""
	}

	return string(before)
}

// getEdgeTTLConfigs extracts edge TTL configurations from graph indexes
func (db *DBImpl) getEdgeTTLConfigs() map[string]time.Duration {
	db.indexesMu.RLock()
	defer db.indexesMu.RUnlock()

	configs := make(map[string]time.Duration)

	for name, indexConfig := range db.indexes {
		if !indexes.IsGraphType(indexConfig.Type) {
			continue
		}

		// Marshal the IndexConfig back to JSON to access the union field
		jsonBytes, err := json.Marshal(indexConfig)
		if err != nil {
			db.logger.Warn("Failed to marshal graph index config",
				zap.String("index", name),
				zap.Error(err))
			continue
		}

		// Unmarshal to a map to extract config fields
		var configMap map[string]any
		if err := json.Unmarshal(jsonBytes, &configMap); err != nil {
			db.logger.Warn("Failed to unmarshal graph index config",
				zap.String("index", name),
				zap.Error(err))
			continue
		}

		// Extract TTL duration from config
		if ttlDuration, ok := configMap["ttl_duration"].(string); ok && ttlDuration != "" {
			duration, err := time.ParseDuration(ttlDuration)
			if err != nil {
				db.logger.Warn("Invalid edge TTL duration for graph index",
					zap.String("index", name),
					zap.String("duration", ttlDuration),
					zap.Error(err))
				continue
			}

			configs[name] = duration
			db.logger.Debug("Configured edge TTL for graph index",
				zap.String("index", name),
				zap.Duration("ttl", duration))
		}
	}

	return configs
}
