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
	"fmt"
	"net/http"
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/usermgr"
	"go.uber.org/zap"
)

// MultiBatchWrite handles cross-table batch operations atomically via 2PC.
func (t *TableApi) MultiBatchWrite(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	var req MultiBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		errorResponse(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if len(req.Tables) == 0 {
		errorResponse(w, "No tables specified", http.StatusBadRequest)
		return
	}

	// Auth: check write permission for each table in the request body
	tablePerms := make(map[string]usermgr.PermissionType, len(req.Tables))
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

	// Partition all operations by shard
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	pr, err := partitionBatchRequestsByTable(t.tm, req.Tables, timestamp)
	if writeBatchPartitionErr(w, err) {
		return
	}

	if len(pr.shards) == 0 {
		errorResponse(w, "No operations to execute", http.StatusBadRequest)
		return
	}

	// Check for splitting shards (once, after all tables are partitioned)
	if writeBatchPartitionErr(w, checkShardsNotSplitting(r.Context(), t.tm, pr.shards)) {
		return
	}

	// Execute: single shard fast path or distributed transaction
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	if len(pr.shards) == 1 {
		var shardID types.ID
		for sid := range pr.shards {
			shardID = sid
		}
		if err := t.ln.forwardBatchToShard(ctx, shardID, pr.writes[shardID], flattenDeletes(pr.deletes[shardID]), pr.transforms[shardID], syncLevel); err != nil {
			errorResponse(w, fmt.Sprintf("Failed to execute batch: %v", err), http.StatusInternalServerError)
			return
		}
	} else {
		if err := t.ln.ExecuteTransaction(ctx, pr.writes, pr.deletes, pr.transforms, nil, syncLevel); err != nil {
			errorResponse(w, fmt.Sprintf("Failed to execute cross-table transaction: %v", err), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(MultiBatchResponse{
		Tables: pr.tables,
	}); err != nil {
		t.logger.Warn("Failed to encode multi-batch response", zap.Error(err))
	}
}
