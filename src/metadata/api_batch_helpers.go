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
	"time"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/tablemgr"
	"golang.org/x/sync/errgroup"
)

// partitionResult holds the shard-partitioned operations accumulated across
// multiple tables, ready for execution via forwardBatchToShard or
// ExecuteTransaction.
type partitionResult struct {
	writes     map[types.ID][][2][]byte
	deletes    map[types.ID][][]byte
	transforms map[types.ID][]*db.Transform
	shards     map[types.ID]struct{}
	tables     map[string]BatchResponse
}

// batchPartitionError is returned by partitionBatchRequestsByTable when
// validation or partitioning fails. It carries an HTTP status code so the
// caller can translate it directly into an HTTP response.
type batchPartitionError struct {
	Message    string
	StatusCode int
}

func (e *batchPartitionError) Error() string { return e.Message }

// writeBatchPartitionErr writes an HTTP response for an error returned by
// partitionBatchRequestsByTable or checkShardsNotSplitting. Returns true if
// the error was handled (caller should return), false if err is nil.
func writeBatchPartitionErr(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	var bpe *batchPartitionError
	if errors.As(err, &bpe) {
		errorResponse(w, bpe.Message, bpe.StatusCode)
		return true
	}
	// context.Canceled / context.DeadlineExceeded — no response body needed.
	return true
}

// partitionBatchRequestsByTable validates documents, injects timestamps,
// and partitions inserts/deletes/transforms by shard across all tables.
func partitionBatchRequestsByTable(
	tm *tablemgr.TableManager,
	tables map[string]BatchRequest,
	timestamp string,
) (*partitionResult, error) {
	result := &partitionResult{
		writes:     make(map[types.ID][][2][]byte),
		deletes:    make(map[types.ID][][]byte),
		transforms: make(map[types.ID][]*db.Transform),
		shards:     make(map[types.ID]struct{}),
		tables:     make(map[string]BatchResponse),
	}

	for tableName, batchReq := range tables {
		if len(batchReq.Deletes) == 0 && len(batchReq.Inserts) == 0 && len(batchReq.Transforms) == 0 {
			continue
		}

		table, err := tm.GetTable(tableName)
		if err != nil {
			return nil, &batchPartitionError{
				Message:    fmt.Sprintf("Table %s not found: %v", tableName, err),
				StatusCode: http.StatusNotFound,
			}
		}

		// Inject _timestamp if not present
		for _, doc := range batchReq.Inserts {
			if _, exists := doc["_timestamp"]; !exists {
				doc["_timestamp"] = timestamp
			}
		}

		// Validate docs and collect insert keys
		insertKeys := make([]string, 0, len(batchReq.Inserts))
		for key, doc := range batchReq.Inserts {
			if err := validateDocumentInsertKey(table, key); err != nil {
				return nil, &batchPartitionError{
					Message:    fmt.Sprintf("Table %s: invalid document id %q: %v", tableName, key, err),
					StatusCode: http.StatusBadRequest,
				}
			}
			if _, err := table.ValidateDoc(doc); err != nil {
				return nil, &batchPartitionError{
					Message:    fmt.Sprintf("Table %s: validation error for key %s: %v", tableName, key, err),
					StatusCode: http.StatusBadRequest,
				}
			}
			insertKeys = append(insertKeys, key)
		}
		for _, key := range batchReq.Deletes {
			if err := validateDocumentMutationKey(key); err != nil {
				return nil, &batchPartitionError{
					Message:    fmt.Sprintf("Table %s: invalid document id %q: %v", tableName, key, err),
					StatusCode: http.StatusBadRequest,
				}
			}
		}

		// Partition insert keys by shard
		if len(insertKeys) > 0 {
			partitions, unfoundKeys, err := partitionWriteKeysByShard(tm, table, insertKeys)
			if err != nil {
				return nil, &batchPartitionError{
					Message:    fmt.Sprintf("Table %s: failed to partition keys for writes: %v", tableName, err),
					StatusCode: http.StatusInternalServerError,
				}
			}
			if len(unfoundKeys) > 0 {
				return nil, &batchPartitionError{
					Message:    fmt.Sprintf("Table %s: failed to find partitions for keys: %v", tableName, unfoundKeys),
					StatusCode: http.StatusInternalServerError,
				}
			}
			for shardID, keys := range partitions {
				shardWrites := make([][2][]byte, 0, len(keys))
				for _, key := range keys {
					jsonBytes, err := json.Marshal(batchReq.Inserts[key])
					if err != nil {
						return nil, &batchPartitionError{
							Message:    fmt.Sprintf("Table %s: marshalling value for key %s: %v", tableName, key, err),
							StatusCode: http.StatusInternalServerError,
						}
					}
					shardWrites = append(shardWrites, [2][]byte{[]byte(key), jsonBytes})
				}
				result.writes[shardID] = append(result.writes[shardID], shardWrites...)
				result.shards[shardID] = struct{}{}
			}
		}

		// Partition delete keys by shard
		if len(batchReq.Deletes) > 0 {
			partitions, unfoundKeys, err := partitionWriteKeysByShard(tm, table, batchReq.Deletes)
			if err != nil {
				return nil, &batchPartitionError{
					Message:    fmt.Sprintf("Table %s: failed to partition delete keys for writes: %v", tableName, err),
					StatusCode: http.StatusInternalServerError,
				}
			}
			if len(unfoundKeys) > 0 {
				return nil, &batchPartitionError{
					Message:    fmt.Sprintf("Table %s: failed to find partitions for delete keys: %v", tableName, unfoundKeys),
					StatusCode: http.StatusInternalServerError,
				}
			}
			for shardID, keys := range partitions {
				shardDeletes := make([][]byte, len(keys))
				for i, key := range keys {
					shardDeletes[i] = []byte(key)
				}
				result.deletes[shardID] = append(result.deletes[shardID], shardDeletes...)
				result.shards[shardID] = struct{}{}
			}
		}

		// Convert and partition transforms by shard
		if len(batchReq.Transforms) > 0 {
			transformMap := make(map[string]*db.Transform, len(batchReq.Transforms))
			transformKeys := make([]string, 0, len(batchReq.Transforms))

			for _, transformReq := range batchReq.Transforms {
				if err := validateDocumentTransformKey(table, transformReq.Key, transformReq.Upsert); err != nil {
					return nil, &batchPartitionError{
						Message:    fmt.Sprintf("Table %s: invalid document id %q: %v", tableName, transformReq.Key, err),
						StatusCode: http.StatusBadRequest,
					}
				}
				transform, err := TransformFromAPI(transformReq)
				if err != nil {
					return nil, &batchPartitionError{
						Message:    fmt.Sprintf("Table %s: %v", tableName, err),
						StatusCode: http.StatusBadRequest,
					}
				}
				transformMap[transformReq.Key] = transform
				transformKeys = append(transformKeys, transformReq.Key)
			}

			keyPartitions, unfoundKeys, err := partitionWriteKeysByShard(tm, table, transformKeys)
			if err != nil {
				return nil, &batchPartitionError{
					Message:    fmt.Sprintf("Table %s: failed to partition transform keys for writes: %v", tableName, err),
					StatusCode: http.StatusInternalServerError,
				}
			}
			if len(unfoundKeys) > 0 {
				return nil, &batchPartitionError{
					Message:    fmt.Sprintf("Table %s: failed to find partitions for transform keys: %v", tableName, unfoundKeys),
					StatusCode: http.StatusInternalServerError,
				}
			}

			for shardID, keys := range keyPartitions {
				for _, key := range keys {
					if transform, ok := transformMap[key]; ok {
						result.transforms[shardID] = append(result.transforms[shardID], transform)
					}
				}
				result.shards[shardID] = struct{}{}
			}
		}

		result.tables[tableName] = BatchResponse{
			Inserted:    len(batchReq.Inserts),
			Deleted:     len(batchReq.Deletes),
			Transformed: len(batchReq.Transforms),
		}
	}

	return result, nil
}

// checkShardsNotSplitting waits up to 10 seconds for any splitting shards to
// finish. Shards are checked concurrently so worst-case latency is 10 seconds
// regardless of how many shards are splitting.
func checkShardsNotSplitting(ctx context.Context, tm *tablemgr.TableManager, shards map[types.ID]struct{}) error {
	eg, egCtx := errgroup.WithContext(ctx)
	for shardID := range shards {
		eg.Go(func() error {
			status, err := tm.GetShardStatus(shardID)
			if err != nil {
				return &batchPartitionError{
					Message:    fmt.Sprintf("Shard %s not found: %v", shardID, err),
					StatusCode: http.StatusInternalServerError,
				}
			}
			if status.State != store.ShardState_SplittingOff {
				return nil
			}
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			case <-time.After(10 * time.Second):
			}
			status, err = tm.GetShardStatus(shardID)
			if err != nil {
				return &batchPartitionError{
					Message:    fmt.Sprintf("Shard %s not found: %v", shardID, err),
					StatusCode: http.StatusInternalServerError,
				}
			}
			if status.State == store.ShardState_SplittingOff {
				return &batchPartitionError{
					Message:    fmt.Sprintf("Shard %s is currently splitting, please try again later", shardID),
					StatusCode: http.StatusInternalServerError,
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

// resolveSyncLevel parses the top-level sync level and, when it is empty,
// derives the effective level from the per-table sync levels (max wins).
func resolveSyncLevel(topLevel SyncLevel, tables map[string]BatchRequest) (db.Op_SyncLevel, error) {
	syncLevel, err := parseSyncLevel(topLevel)
	if err != nil {
		return 0, err
	}
	if topLevel == "" {
		for _, batchReq := range tables {
			if batchReq.SyncLevel != "" {
				tableSL, err := parseSyncLevel(batchReq.SyncLevel)
				if err == nil && tableSL > syncLevel {
					syncLevel = tableSL
				}
			}
		}
	}
	return syncLevel, nil
}

// flattenDeletes converts [][]byte to nil if empty (for forwardBatchToShard compatibility).
func flattenDeletes(deletes [][]byte) [][]byte {
	if len(deletes) == 0 {
		return nil
	}
	return deletes
}
