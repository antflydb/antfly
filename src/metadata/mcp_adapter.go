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
	"time"

	"github.com/blevesearch/bleve/v2/search/query"
	"golang.org/x/sync/errgroup"

	"github.com/antflydb/antfly/lib/ai"
	"github.com/antflydb/antfly/lib/embeddings"
	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/src/common"
	antflymcp "github.com/antflydb/antfly/src/mcp"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/tablemgr"
)

// mcpAdapter implements antflymcp.AntflyHandler by delegating to the internal
// TableApi methods, following the same pattern as a2aAdapter.
type mcpAdapter struct {
	t *TableApi
}

// newMCPAdapter creates an adapter that bridges the MCP handler interface
// to the concrete TableApi methods.
func newMCPAdapter(t *TableApi) *mcpAdapter {
	return &mcpAdapter{t: t}
}

// CreateTable implements antflymcp.AntflyHandler.
func (a *mcpAdapter) CreateTable(ctx context.Context, name string, numShards int, schemaJSON string) error {
	fullTextIndex := "full_text_index_v0"
	tc := tablemgr.TableConfig{
		NumShards: uint(numShards), //nolint:gosec // G115: bounded value, cannot overflow in practice
		Indexes: map[string]indexes.IndexConfig{
			fullTextIndex: *indexes.NewFullTextIndexConfig(fullTextIndex, false),
		},
	}
	if schemaJSON != "" {
		var ts schema.TableSchema
		if err := json.Unmarshal([]byte(schemaJSON), &ts); err != nil {
			return fmt.Errorf("invalid schema JSON: %w", err)
		}
		tc.Schema = &ts
	}
	if _, err := a.t.tm.CreateTable(name, tc); err != nil {
		return err
	}
	a.t.ln.TriggerReconciliation()
	return nil
}

// DropTable implements antflymcp.AntflyHandler.
func (a *mcpAdapter) DropTable(ctx context.Context, name string) error {
	if err := a.t.tm.RemoveTable(name); err != nil {
		return err
	}
	a.t.ln.TriggerReconciliation()
	return nil
}

// ListTables implements antflymcp.AntflyHandler.
func (a *mcpAdapter) ListTables(ctx context.Context) ([]antflymcp.TableInfo, error) {
	tables, err := a.t.tm.Tables(nil, nil)
	if err != nil {
		return nil, err
	}

	result := make([]antflymcp.TableInfo, len(tables))
	for i, table := range tables {
		// Build status map with shard and storage info
		status := map[string]any{
			"indexes": table.Indexes,
		}
		if table.Schema != nil {
			status["schema"] = table.Schema
		}

		diskSize, empty := tableStorageStatus(table, a.t.tm)
		status["storage"] = map[string]any{
			"disk_usage": diskSize,
			"empty":      empty,
		}

		result[i] = antflymcp.TableInfo{
			Name:   table.Name,
			Status: status,
		}
	}
	return result, nil
}

// CreateIndex implements antflymcp.AntflyHandler.
func (a *mcpAdapter) CreateIndex(
	ctx context.Context,
	tableName, indexName, field, template string,
	dimension int,
	embedderJSON, summarizerJSON string,
) error {
	embConfig := indexes.EmbeddingsIndexConfig{
		Dimension: dimension,
		Field:     field,
		Template:  template,
	}

	// Parse embedder configuration
	if embedderJSON != "" {
		var ec embeddings.EmbedderConfig
		if err := json.Unmarshal([]byte(embedderJSON), &ec); err != nil {
			return fmt.Errorf("invalid embedder JSON: %w", err)
		}
		if err := ec.Validate(); err != nil {
			return fmt.Errorf("invalid embedder configuration: %w", err)
		}

		// Test the embedder
		embedder, err := embeddings.NewEmbedder(ec)
		if err != nil {
			return fmt.Errorf("failed to create embedding plugin: %w", err)
		}
		testCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		testEmbeddings, err := embeddings.EmbedText(testCtx, embedder, []string{"test"})
		if err != nil {
			return fmt.Errorf("failed to validate embedding configuration: %w", err)
		}
		if embConfig.Dimension <= 0 && len(testEmbeddings) > 0 {
			embConfig.Dimension = len(testEmbeddings[0])
		}
		if len(testEmbeddings) > 0 && len(testEmbeddings[0]) != embConfig.Dimension {
			return fmt.Errorf("embedding dimension mismatch: expected %d, got %d",
				embConfig.Dimension, len(testEmbeddings[0]))
		}
		embConfig.Embedder = &ec
	}

	// Parse summarizer configuration
	if summarizerJSON != "" {
		var gc ai.GeneratorConfig
		if err := json.Unmarshal([]byte(summarizerJSON), &gc); err != nil {
			return fmt.Errorf("invalid summarizer JSON: %w", err)
		}
		embConfig.Summarizer = &gc
	}

	if !embConfig.Sparse && embConfig.Dimension <= 0 {
		return fmt.Errorf("embedding dimension must be set and greater than 0 for dense indexes")
	}

	config := indexes.NewEmbeddingsConfig(indexName, embConfig)
	return a.t.ln.addIndexToTable(ctx, tableName, indexName, *config)
}

// DropIndex implements antflymcp.AntflyHandler.
func (a *mcpAdapter) DropIndex(ctx context.Context, tableName, indexName string) error {
	if err := a.t.ln.dropIndexFromTable(ctx, tableName, indexName); err != nil {
		return err
	}
	a.t.ln.TriggerReconciliation()
	return nil
}

// ListIndexes implements antflymcp.AntflyHandler.
func (a *mcpAdapter) ListIndexes(ctx context.Context, tableName string) ([]antflymcp.IndexInfo, error) {
	idxs, err := a.t.tm.Indexes(tableName)
	if err != nil {
		return nil, err
	}

	result := make([]antflymcp.IndexInfo, 0, len(idxs))
	for _, idx := range idxs {
		// Convert shard status keys to strings
		shardStatus := make(map[string]any, len(idx.ShardStatus))
		for k, v := range idx.ShardStatus {
			shardStatus[k.String()] = v
		}

		status := map[string]any{
			"config":       idx.IndexConfig,
			"status":       idx.Status,
			"shard_status": shardStatus,
		}
		result = append(result, antflymcp.IndexInfo{
			Name:   idx.Name,
			Status: status,
		})
	}
	return result, nil
}

// Query implements antflymcp.AntflyHandler.
func (a *mcpAdapter) Query(ctx context.Context, req antflymcp.QueryRequest) (*antflymcp.QueryResult, error) {
	internalReq := QueryRequest{
		Table:          req.TableName,
		SemanticSearch: req.SemanticSearch,
		Fields:         req.Fields,
		Limit:          req.Limit,
		OrderBy:        req.OrderBy,
		Indexes:        req.Indexes,
	}

	if req.FilterPrefix != "" {
		internalReq.FilterPrefix = []byte(req.FilterPrefix)
	}

	// Convert bleve query string syntax to json.RawMessage
	if req.FullTextSearch != "" {
		q := query.NewQueryStringQuery(req.FullTextSearch)
		ftsJSON, err := json.Marshal(q)
		if err != nil {
			return nil, fmt.Errorf("invalid full text search query: %w", err)
		}
		internalReq.FullTextSearch = ftsJSON
	}

	qr := a.t.runQuery(ctx, &internalReq)
	if qr.Error != "" {
		return nil, fmt.Errorf("query error: %s", qr.Error)
	}

	// Marshal the full result to a generic map for structured output
	raw, err := json.Marshal(qr)
	if err != nil {
		return nil, fmt.Errorf("marshaling query result: %w", err)
	}
	var structured map[string]any
	if err := json.Unmarshal(raw, &structured); err != nil {
		return nil, fmt.Errorf("unmarshaling query result: %w", err)
	}

	hitCount := 0
	if qr.Hits.Hits != nil {
		hitCount = len(qr.Hits.Hits)
	}

	return &antflymcp.QueryResult{
		HitCount:   hitCount,
		Structured: structured,
	}, nil
}

// Batch implements antflymcp.AntflyHandler.
func (a *mcpAdapter) Batch(ctx context.Context, tableName string, inserts map[string]any, deletes []string) (*antflymcp.BatchResult, error) {
	table, err := a.t.tm.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("getting table %s: %w", tableName, err)
	}

	syncLevel := db.Op_SyncLevelPropose

	// Prepare insert documents
	insertDocs := make(map[string]map[string]any, len(inserts))
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	for k, v := range inserts {
		doc, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid document format for key %s: expected object", k)
		}
		if err := validateDocumentInsertKey(table, k); err != nil {
			return nil, fmt.Errorf("invalid document id %q: %w", k, err)
		}
		if _, exists := doc["_timestamp"]; !exists {
			doc["_timestamp"] = timestamp
		}
		if _, err := table.ValidateDoc(doc); err != nil {
			return nil, fmt.Errorf("validation error for key %s: %w", k, err)
		}
		insertDocs[k] = doc
	}

	// Single-insert fast path
	if len(insertDocs) == 1 && len(deletes) == 0 {
		for k, v := range insertDocs {
			if err := a.t.ln.forwardInsertToShard(ctx, tableName, k, v, syncLevel); err != nil {
				return nil, fmt.Errorf("failed to insert data: %w", err)
			}
		}
		return &antflymcp.BatchResult{Inserted: 1, Deleted: 0}, nil
	}

	eg, egCtx := errgroup.WithContext(ctx)

	// Partition and forward inserts
	if len(insertDocs) > 0 {
		keys := make([]string, 0, len(insertDocs))
		for k := range insertDocs {
			keys = append(keys, k)
		}
		partitions, unfound, err := partitionWriteKeysByShard(a.t.tm, table, keys)
		if err != nil {
			return nil, fmt.Errorf("partitioning insert keys: %w", err)
		}
		if len(unfound) > 0 {
			return nil, fmt.Errorf("failed to find partitions for keys: %v", unfound)
		}
		for shardID, shardKeys := range partitions {
			writes := make([][2][]byte, 0, len(shardKeys))
			for _, key := range shardKeys {
				val, err := json.Marshal(insertDocs[key])
				if err != nil {
					return nil, fmt.Errorf("marshal doc %s: %w", key, err)
				}
				writes = append(writes, [2][]byte{[]byte(key), val})
			}
			eg.Go(func() error {
				return a.t.ln.forwardBatchToShard(egCtx, shardID, writes, nil, nil, syncLevel)
			})
		}
	}

	// Partition and forward deletes
	if len(deletes) > 0 {
		for _, key := range deletes {
			if err := validateDocumentMutationKey(key); err != nil {
				return nil, fmt.Errorf("invalid document id %q: %w", key, err)
			}
		}
		deletePartitions, unfound, err := partitionWriteKeysByShard(a.t.tm, table, deletes)
		if err != nil {
			return nil, fmt.Errorf("partitioning delete keys: %w", err)
		}
		if len(unfound) > 0 {
			return nil, fmt.Errorf("failed to find partitions for delete keys: %v", unfound)
		}
		for shardID, shardKeys := range deletePartitions {
			deleteBytes := make([][]byte, len(shardKeys))
			for i, key := range shardKeys {
				deleteBytes[i] = []byte(key)
			}
			eg.Go(func() error {
				return a.t.ln.forwardBatchToShard(egCtx, shardID, nil, deleteBytes, nil, syncLevel)
			})
		}
	}

	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("batch operation failed: %w", err)
	}

	return &antflymcp.BatchResult{
		Inserted: len(insertDocs),
		Deleted:  len(deletes),
	}, nil
}

// Backup implements antflymcp.AntflyHandler.
func (a *mcpAdapter) Backup(ctx context.Context, tableName, backupID, location string) error {
	table, err := a.t.tm.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("getting table %s: %w", tableName, err)
	}

	eg, egCtx := errgroup.WithContext(ctx)
	for shardID := range table.Shards {
		eg.Go(func() error {
			if err := a.t.ln.forwardBackupToShard(egCtx, shardID, location, backupID); err != nil {
				return fmt.Errorf("backing up shard %s: %w", shardID, err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("backup failed: %w", err)
	}

	// Write backup metadata
	if err := newBackupStore(location, &a.t.ln.config.Storage.S3).WriteMetadata(ctx, backupID, table); err != nil {
		return fmt.Errorf("writing backup metadata: %w", err)
	}

	return nil
}

// Restore implements antflymcp.AntflyHandler.
func (a *mcpAdapter) Restore(ctx context.Context, tableName, backupID, location string) error {
	tableMetadata, err := newBackupStore(location, &a.t.ln.config.Storage.S3).ReadMetadata(ctx, backupID)
	if err != nil {
		return fmt.Errorf("reading backup metadata: %w", err)
	}

	if tableMetadata.Name != tableName {
		return fmt.Errorf("table name mismatch: expected %s, but backup metadata is for %s",
			tableName, tableMetadata.Name)
	}

	if err := a.t.tm.RestoreTable(tableMetadata, &common.BackupConfig{
		Location: location,
		BackupID: backupID,
	}); err != nil {
		return fmt.Errorf("restoring table: %w", err)
	}

	a.t.ln.TriggerReconciliation()
	return nil
}

// Compile-time check that mcpAdapter implements AntflyHandler.
var _ antflymcp.AntflyHandler = (*mcpAdapter)(nil)
