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
	"net/http"
	"slices"
	"sync"
	"time"

	json "github.com/antflydb/antfly/pkg/libaf/json"

	"github.com/antflydb/antfly/lib/workerpool"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/usermgr"
	"go.uber.org/zap"
)

func (t *TableApi) LinearMerge(w http.ResponseWriter, r *http.Request, tableName string) {
	startTime := time.Now()
	t.logger.Debug("LinearMerge handler called",
		zap.String("tableName", tableName),
		zap.String("method", r.Method),
		zap.String("url", r.URL.String()),
		zap.String("path", r.URL.Path))

	// Auth check
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeWrite) {
		t.logger.Debug("LinearMerge auth check failed", zap.String("tableName", tableName))
		return
	}
	defer func() { _ = r.Body.Close() }()

	// Decode request
	var req LinearMergeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		t.logger.Debug("LinearMerge failed to decode request", zap.String("tableName", tableName), zap.Error(err))
		errorResponse(w, fmt.Sprintf("decoding request: %v", err), http.StatusBadRequest)
		return
	}
	t.logger.Debug("LinearMerge request decoded successfully",
		zap.String("tableName", tableName),
		zap.Int("numRecords", len(req.Records)))

	// Parse sync level from request (default to propose)
	syncLevel, err := parseSyncLevel(req.SyncLevel)
	if err != nil {
		t.logger.Debug("LinearMerge failed to parse sync level", zap.String("tableName", tableName), zap.Error(err))
		errorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get table
	t.logger.Debug("LinearMerge getting table", zap.String("tableName", tableName))
	table, err := t.tm.GetTable(tableName)
	if err != nil {
		t.logger.Debug("LinearMerge table not found", zap.String("tableName", tableName), zap.Error(err))
		errorResponse(w, fmt.Sprintf("getting table %s: %v", tableName, err), http.StatusNotFound)
		return
	}
	t.logger.Debug("LinearMerge table found successfully", zap.String("tableName", tableName))

	// Constants
	const (
		MaxRecordsPerRequest = 10000
		MaxKeysScanned       = 100000
	)

	// Validate batch size
	if len(req.Records) > MaxRecordsPerRequest {
		errorResponse(
			w,
			fmt.Sprintf("Batch size exceeds maximum of %d records", MaxRecordsPerRequest),
			http.StatusBadRequest,
		)
		return
	}

	// Extract and sort keys
	keys := make([]string, 0, len(req.Records))
	for id := range req.Records {
		keys = append(keys, id)
	}
	slices.Sort(keys)

	// Handle empty records with last_merged_id (delete-only operation)
	var maxID string
	var shardRange [2][]byte

	if len(keys) > 0 {
		// Determine which shard owns the first record
		startKey := keys[0]
		shardID, err := table.FindShardForKey(startKey)
		if err != nil {
			errorResponse(
				w,
				fmt.Sprintf("Failed to find shard for key: %v", startKey),
				http.StatusInternalServerError,
			)
			return
		}

		shardStatus, err := t.tm.GetShardStatus(shardID)
		if err != nil {
			errorResponse(
				w,
				fmt.Sprintf("Failed to get shard status: %v", err),
				http.StatusInternalServerError,
			)
			return
		}
		shardRange = shardStatus.ByteRange

		// Find max ID we can process in this shard.
		// Empty shardRange[1] means unbounded (last shard), so all keys fit.
		for _, id := range keys {
			if len(shardRange[1]) > 0 && id > string(shardRange[1]) {
				break
			}
			maxID = id
		}
		// Validate we're not going backwards
		if req.LastMergedId != "" && req.LastMergedId >= keys[len(keys)-1] {
			result := LinearMergeResult{
				Status:     LinearMergePageStatusError,
				Message:    "last_merged_id must be less than max record ID (empty range)",
				NextCursor: req.LastMergedId,
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(result)
			return
		}

		// Check if first record is beyond current shard boundary
		if len(shardRange[1]) > 0 && keys[0] > string(shardRange[1]) {
			result := LinearMergeResult{
				Status: LinearMergePageStatusError,
				Message: fmt.Sprintf(
					"First record %s is beyond current shard boundary %s",
					keys[0],
					string(shardRange[1]),
				),
				NextCursor: req.LastMergedId,
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(result)
			return
		}
	} else if req.LastMergedId != "" {
		// Empty records map with last_merged_id - use shard boundary as upper limit
		shardID, err := table.FindShardForKey(req.LastMergedId)
		if err != nil {
			errorResponse(w, fmt.Sprintf("Failed to find shard for last_merged_id: %v", err), http.StatusInternalServerError)
			return
		}
		shardStatus, err := t.tm.GetShardStatus(shardID)
		if err != nil {
			errorResponse(w, fmt.Sprintf("Failed to get shard status: %v", err), http.StatusInternalServerError)
			return
		}
		shardRange = shardStatus.ByteRange
		maxID = string(shardRange[1])
	} else {
		// Empty records and no last_merged_id - nothing to do
		result := LinearMergeResult{
			Status:     LinearMergePageStatusSuccess,
			Upserted:   0,
			Deleted:    0,
			Skipped:    0,
			NextCursor: "",
			Message:    "No records to process",
			Took:       time.Since(startTime),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
		return
	}

	// Query Antfly keys and hashes in range (fromKey, toKey]
	existingHashes, err := t.ln.forwardRangeScanToShards(
		r.Context(),
		tableName,
		req.LastMergedId,
		maxID,
	)
	if err != nil {
		errorResponse(
			w,
			fmt.Sprintf("Failed to scan existing keys: %v", err),
			http.StatusInternalServerError,
		)
		return
	}

	// Check scan limit
	if len(existingHashes) > MaxKeysScanned {
		result := LinearMergeResult{
			Status: LinearMergePageStatusError,
			Message: fmt.Sprintf(
				"Range scan exceeded maximum of %d keys. Reduce batch size.",
				MaxKeysScanned,
			),
			NextCursor: req.LastMergedId,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
		return
	}

	// Identify deletes: Antfly keys NOT in input
	deletes := []string{}
	for existingID := range existingHashes {
		if existingID <= req.LastMergedId {
			continue // Skip already processed range
		}
		if _, exists := req.Records[existingID]; !exists {
			deletes = append(deletes, existingID)
		}
	}

	// Compute hashes for input records and identify upserts (only if hash differs or new)
	upsertMap := make(map[string][2][]byte, len(keys))
	skipped := 0
	for _, id := range keys {
		if id <= req.LastMergedId {
			continue // Skip already processed
		}
		if id > maxID {
			break // Stop at shard boundary
		}

		rawDoc := req.Records[id]
		doc, ok := rawDoc.(map[string]any)
		if !ok {
			errorResponse(
				w,
				fmt.Sprintf("document %s is not a valid object", id),
				http.StatusBadRequest,
			)
			return
		}

		// Compute hash of input document
		newHash, err := db.ComputeDocumentHash(doc)
		if err != nil {
			t.logger.Warn("Failed to compute hash, will upsert",
				zap.String("id", id), zap.Error(err))
		} else if existingHash, exists := existingHashes[id]; exists && existingHash == newHash {
			// Document unchanged - skip upsert
			skipped++
			continue
		}

		// Add timestamp if not present
		if _, exists := doc["_timestamp"]; !exists {
			doc["_timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
		}

		// Validate document
		if _, err := table.ValidateDoc(doc); err != nil {
			errorResponse(
				w,
				fmt.Sprintf("validation error for key %s: %v", id, err),
				http.StatusBadRequest,
			)
			return
		}

		docBytes, err := json.Marshal(doc)
		if err != nil {
			errorResponse(
				w,
				fmt.Sprintf("marshalling document %s: %v", id, err),
				http.StatusInternalServerError,
			)
			return
		}
		upsertMap[id] = [2][]byte{[]byte(id), docBytes}
	}

	// Determine if we crossed a boundary
	crossesBoundary := len(keys) > 0 && keys[len(keys)-1] > maxID

	// Dry run handling
	if req.DryRun {
		status := LinearMergePageStatusSuccess
		if crossesBoundary {
			status = LinearMergePageStatusPartial
		}
		result := LinearMergeResult{
			Status:      status,
			Upserted:    0,
			Deleted:     len(deletes),
			Skipped:     skipped,
			DeletedIds:  deletes,
			NextCursor:  maxID,
			KeyRange:    KeyRange{From: req.LastMergedId, To: maxID},
			KeysScanned: len(existingHashes),
			Message:     "dry run - no changes made",
			Took:        time.Since(startTime),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
		return
	}

	// Partition upsert keys by shard
	upsertKeys := make([]string, 0, len(upsertMap))
	for k := range upsertMap {
		upsertKeys = append(upsertKeys, k)
	}
	upsertPartitions, unfoundKeys, err := partitionWriteKeysByShard(t.tm, table, upsertKeys)
	if err != nil {
		errorResponse(
			w,
			fmt.Sprintf("Failed to partition upsert keys for writes: %v", err),
			http.StatusInternalServerError,
		)
		return
	}
	if len(unfoundKeys) > 0 {
		errorResponse(
			w,
			fmt.Sprintf("Failed to find partitions for upsert keys: %v", unfoundKeys),
			http.StatusInternalServerError,
		)
		return
	}

	deletePartitions, unfoundKeys, err := partitionWriteKeysByShard(t.tm, table, deletes)
	if err != nil {
		errorResponse(
			w,
			fmt.Sprintf("Failed to partition delete keys for writes: %v", err),
			http.StatusInternalServerError,
		)
		return
	}
	if len(unfoundKeys) > 0 {
		errorResponse(
			w,
			fmt.Sprintf("Failed to find partitions for delete keys: %v", unfoundKeys),
			http.StatusInternalServerError,
		)
		return
	}

	// Execute batch operations per shard
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	g, _ := workerpool.NewGroup(ctx, t.pool)
	var failed []FailedOperation
	var failedMu sync.Mutex

	// Process upserts
	for shardID, partition := range upsertPartitions {
		g.Go(func(ctx context.Context) error {
			writes := make([][2][]byte, len(partition))
			for i, key := range partition {
				writes[i] = upsertMap[key]
			}

			if err := t.ln.forwardBatchToShard(ctx, shardID, writes, nil, nil, syncLevel); err != nil {
				failedMu.Lock()
				for _, kv := range writes {
					failed = append(failed, FailedOperation{
						Id:        string(kv[0]),
						Operation: "upsert",
						Error:     err.Error(),
					})
				}
				failedMu.Unlock()
				if !errors.Is(err, context.Canceled) {
					t.logger.Error("Error forwarding upserts", zap.Error(err))
				}
				return nil // Continue with other shards
			}
			return nil
		})
	}

	// Process deletes
	for shardID, partition := range deletePartitions {
		g.Go(func(ctx context.Context) error {
			delKeys := make([][]byte, len(partition))
			for i, key := range partition {
				delKeys[i] = []byte(key)
			}

			if err := t.ln.forwardBatchToShard(ctx, shardID, nil, delKeys, nil, syncLevel); err != nil {
				failedMu.Lock()
				for _, key := range delKeys {
					failed = append(failed, FailedOperation{
						Id:        string(key),
						Operation: "delete",
						Error:     err.Error(),
					})
				}
				failedMu.Unlock()
				if !errors.Is(err, context.Canceled) {
					t.logger.Error("Error forwarding deletes", zap.Error(err))
				}
				return nil // Continue with other shards
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		errorResponse(
			w,
			fmt.Sprintf("Error executing merge: %v", err),
			http.StatusInternalServerError,
		)
		return
	}

	// Determine final status
	status := LinearMergePageStatusSuccess
	message := ""
	if crossesBoundary {
		status = LinearMergePageStatusPartial
		message = "stopped at shard boundary"
	}

	// Count successful operations
	successfulUpserts := len(upsertMap)
	successfulDeletes := len(deletes)
	for _, f := range failed {
		switch f.Operation {
		case "upsert":
			successfulUpserts--
		case "delete":
			successfulDeletes--
		}
	}

	result := LinearMergeResult{
		Status:      status,
		Upserted:    successfulUpserts,
		Skipped:     skipped,
		Deleted:     successfulDeletes,
		Failed:      failed,
		NextCursor:  maxID,
		KeyRange:    KeyRange{From: req.LastMergedId, To: maxID},
		KeysScanned: len(existingHashes),
		Message:     message,
		Took:        time.Since(startTime),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		t.logger.Error("Error encoding response", zap.Error(err))
	}
}
