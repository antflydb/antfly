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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/usermgr"
	"golang.org/x/sync/errgroup"
)

// ErrVersionConflict is returned when an OCC version check fails.
type ErrVersionConflict struct {
	Table    string
	Key      string
	Expected uint64
	Actual   uint64
}

func (e *ErrVersionConflict) Error() string {
	return fmt.Sprintf("version conflict on %s/%s: expected %d, got %d", e.Table, e.Key, e.Expected, e.Actual)
}

// CommitTransaction handles stateless OCC transaction commits.
func (t *TableApi) CommitTransaction(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	var req TransactionCommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if len(req.ReadSet) == 0 && len(req.Tables) == 0 {
		errorResponse(w, "Empty transaction: no reads or writes", http.StatusBadRequest)
		return
	}

	// Auth: check read permission for read-set tables, write permission for write-set tables.
	// Write permission implies read, so tables in both sets only need write checked.
	tablePerms := make(map[string]usermgr.PermissionType)
	for _, item := range req.ReadSet {
		if _, hasWrite := tablePerms[item.Table]; !hasWrite {
			tablePerms[item.Table] = usermgr.PermissionTypeRead
		}
	}
	for tableName := range req.Tables {
		tablePerms[tableName] = usermgr.PermissionTypeWrite
	}
	if !t.ln.ensureMultiTableAuth(w, r, tablePerms) {
		return
	}

	// Use top-level sync level, or derive from per-table sync levels (max wins)
	syncLevel, err := resolveSyncLevel(req.SyncLevel, req.Tables)
	if err != nil {
		errorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Validate read set: early-exit optimization to detect conflicts before paying
	// the cost of 2PC. This is NOT the atomicity boundary — the atomic predicate
	// check inside WriteIntent (during 2PC phase 1) is what prevents lost updates.
	// A conflict detected here avoids the 2PC round-trip; a pass here does not
	// guarantee commit (the predicate check may still fail).
	if err := t.validateReadSet(r.Context(), req.ReadSet); err != nil {
		var conflict *ErrVersionConflict
		if errors.As(err, &conflict) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(TransactionCommitResponse{
				Status: TransactionCommitResponseStatusAborted,
				Conflict: struct {
					Key     string `json:"key,omitempty,omitzero"`
					Message string `json:"message,omitempty,omitzero"`
					Table   string `json:"table,omitempty,omitzero"`
				}{
					Table:   conflict.Table,
					Key:     conflict.Key,
					Message: conflict.Error(),
				},
			})
			return
		}
		errorResponse(w, fmt.Sprintf("Failed to validate read set: %v", err), http.StatusInternalServerError)
		return
	}

	// Partition write-set operations by shard
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	pr, err := partitionBatchRequestsByTable(t.tm, req.Tables, timestamp)
	if writeBatchPartitionErr(w, err) {
		return
	}

	// Check for splitting shards (once, after all tables are partitioned)
	if writeBatchPartitionErr(w, checkShardsNotSplitting(r.Context(), t.tm, pr.shards)) {
		return
	}

	// Build version predicates from read set, grouped by the current write owner.
	// During an active split, writes to the split-off range must remain on the parent
	// until the child shard is fully writable.
	var allPredicates map[types.ID][]*db.VersionPredicate
	if len(req.ReadSet) > 0 {
		allPredicates = make(map[types.ID][]*db.VersionPredicate)
		for _, item := range req.ReadSet {
			table, err := t.tm.GetTable(item.Table)
			if err != nil {
				errorResponse(w, fmt.Sprintf("Table %s not found: %v", item.Table, err), http.StatusNotFound)
				return
			}
			shardID, err := findWriteShardForKey(t.tm, table, item.Key)
			if err != nil {
				errorResponse(w, fmt.Sprintf("Failed to find shard for key %s: %v", item.Key, err), http.StatusInternalServerError)
				return
			}
			expectedVersion, _ := strconv.ParseUint(item.Version, 10, 64) // already validated above
			allPredicates[shardID] = append(allPredicates[shardID], db.VersionPredicate_builder{
				Key:             []byte(item.Key),
				ExpectedVersion: expectedVersion,
			}.Build())
		}
	}

	// Execute writes
	if len(pr.shards) > 0 {
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		// OCC transactions always use distributed transaction path for predicate enforcement,
		// even with a single shard, to ensure predicates are checked atomically with writes.
		if len(pr.shards) == 1 && allPredicates == nil {
			var shardID types.ID
			for sid := range pr.shards {
				shardID = sid
			}
			if err := t.ln.forwardBatchToShard(ctx, shardID, pr.writes[shardID], flattenDeletes(pr.deletes[shardID]), pr.transforms[shardID], syncLevel); err != nil {
				errorResponse(w, fmt.Sprintf("Failed to execute transaction writes: %v", err), http.StatusInternalServerError)
				return
			}
		} else {
			if err := t.ln.ExecuteTransaction(ctx, pr.writes, pr.deletes, pr.transforms, allPredicates, syncLevel); err != nil {
				// Check if it's a version conflict from the predicate check inside 2PC
				if isVersionConflict(err) {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusConflict)
					_ = json.NewEncoder(w).Encode(TransactionCommitResponse{
						Status: TransactionCommitResponseStatusAborted,
						Conflict: struct {
							Key     string `json:"key,omitempty,omitzero"`
							Message string `json:"message,omitempty,omitzero"`
							Table   string `json:"table,omitempty,omitzero"`
						}{
							Message: err.Error(),
						},
					})
					return
				}
				errorResponse(w, fmt.Sprintf("Failed to execute transaction: %v", err), http.StatusInternalServerError)
				return
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(TransactionCommitResponse{
		Status: TransactionCommitResponseStatusCommitted,
		Tables: pr.tables,
	})
}

// validateReadSet checks that all keys in the read set still have the expected versions.
func (t *TableApi) validateReadSet(ctx context.Context, readSet []TransactionReadItem) error {
	eg, egCtx := errgroup.WithContext(ctx)

	for _, item := range readSet {
		eg.Go(func() error {
			table, err := t.tm.GetTable(item.Table)
			if err != nil {
				return fmt.Errorf("table %s not found: %w", item.Table, err)
			}

			shardID, err := table.FindShardForKey(item.Key)
			if err != nil {
				return fmt.Errorf("failed to find shard for key %s in table %s: %w", item.Key, item.Table, err)
			}

			_, currentVersion, err := t.ln.forwardStrictLookupToShardWithVersion(egCtx, shardID, item.Key)
			if err != nil {
				return fmt.Errorf("failed to lookup key %s in table %s: %w", item.Key, item.Table, err)
			}

			expectedVersion, err := strconv.ParseUint(item.Version, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid version %q for key %s in table %s: %w", item.Version, item.Key, item.Table, err)
			}

			if currentVersion != expectedVersion {
				return &ErrVersionConflict{
					Table:    item.Table,
					Key:      item.Key,
					Expected: expectedVersion,
					Actual:   currentVersion,
				}
			}

			return nil
		})
	}

	return eg.Wait()
}

// isVersionConflict checks if an error (possibly wrapped) indicates a version predicate failure.
// Uses errors.Is to match against typed client errors (ResponseError classifies by HTTP
// status code and body content). Covers "version predicate check failed", "version conflict",
// and "intent conflict" from store/db/db.go.
func isVersionConflict(err error) bool {
	return errors.Is(err, client.ErrVersionConflict) ||
		errors.Is(err, client.ErrIntentConflict)
}
