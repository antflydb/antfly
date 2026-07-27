// Copyright 2026 Antfly, Inc.
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
	"time"

	"github.com/blevesearch/bleve/v2/search/query"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/ai"
	"github.com/antflydb/antfly/go/pkg/antfly/lib/embeddings"
	"github.com/antflydb/antfly/go/pkg/antfly/lib/schema"
	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	antflymcp "github.com/antflydb/antfly/go/pkg/antfly/src/mcp"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store/db"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/go/pkg/antfly/src/tablemgr"
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
	if req.RawQueryRequest != nil {
		if _, ok := req.RawQueryRequest["table"]; ok {
			return nil, fmt.Errorf("queryRequest.table is not allowed; pass the table through tableName")
		}
		raw, err := json.Marshal(req.RawQueryRequest)
		if err != nil {
			return nil, fmt.Errorf("marshaling queryRequest: %w", err)
		}
		var internalReq QueryRequest
		if err := json.Unmarshal(raw, &internalReq); err != nil {
			return nil, fmt.Errorf("unmarshaling queryRequest: %w", err)
		}
		internalReq.Table = req.TableName
		return a.runMCPQuery(ctx, &internalReq)
	}

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

	// Convert MCP full-text args to the same structured query JSON accepted by
	// the REST query API.
	if req.FullTextSearch != nil {
		ftsJSON, err := mcpFullTextSearchJSON(req.FullTextSearch, req.FullTextSearchField)
		if err != nil {
			return nil, fmt.Errorf("invalid full text search query: %w", err)
		}
		internalReq.FullTextSearch = ftsJSON
	}

	return a.runMCPQuery(ctx, &internalReq)
}

func (a *mcpAdapter) runMCPQuery(ctx context.Context, req *QueryRequest) (*antflymcp.QueryResult, error) {
	qr := a.t.runQuery(ctx, req)
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

func mcpFullTextSearchJSON(fullTextSearch any, field string) (json.RawMessage, error) {
	switch value := fullTextSearch.(type) {
	case string:
		if value == "" {
			return nil, nil
		}
		if field != "" {
			ftsJSON, err := json.Marshal(map[string]any{
				"match": value,
				"field": field,
			})
			if err != nil {
				return nil, err
			}
			return ftsJSON, nil
		}

		q := query.NewQueryStringQuery(value)
		ftsJSON, err := json.Marshal(q)
		if err != nil {
			return nil, err
		}
		return ftsJSON, nil
	case map[string]any:
		return json.Marshal(value)
	case json.RawMessage:
		return value, nil
	default:
		return nil, fmt.Errorf("expected string or object")
	}
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
func (a *mcpAdapter) Backup(
	ctx context.Context,
	tableName, backupID, connection, location string,
) error {
	if err := common.ValidateBackupID(backupID); err != nil {
		return err
	}
	table, err := a.t.tm.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("getting table %s: %w", tableName, err)
	}

	backup := common.BackupConfig{
		BackupID:   backupID,
		Connection: connection,
		Location:   location,
		Format:     common.DefaultBackupFormat,
	}
	metadataStore, err := newBackupStore(
		a.t.ln.config,
		connection,
		"backup.write",
		location,
	)
	if err != nil {
		return err
	}
	reservationOwner, err := newClusterBackupAttemptID()
	if err != nil {
		return fmt.Errorf("initializing backup attempt: %w", err)
	}
	if err := metadataStore.ReserveBackupID(ctx, backupID, reservationOwner); err != nil {
		return err
	}
	committed := false
	cleanupSafe := true
	createdArtifacts := backupArtifactNamesForFormat(backupID, table, backup.Format)
	defer func() {
		if committed {
			return
		}
		if !cleanupSafe {
			a.t.logger.Error(
				"MCP backup publication outcome is ambiguous; retaining fenced attempt",
				zap.String("backup_id", backupID),
			)
			return
		}
		if err := cleanupBackupAttempt(
			metadataStore,
			backupID,
			reservationOwner,
			nil,
			createdArtifacts,
		); err != nil {
			a.t.logger.Error("Failed to clean abandoned MCP backup", zap.String("backup_id", backupID), zap.Error(err))
		}
	}()
	backup.ResolvedLocation = metadataStore.ResolvedLocation()
	artifactIntegrities, err := a.t.backupShardsWithIntegrity(ctx, table, backup)
	if err != nil {
		return fmt.Errorf("backup failed: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Write backup metadata
	cleanupSafe = false
	if err := metadataStore.WriteMetadata(
		ctx,
		backupID,
		table,
		backup.Format,
		artifactIntegrities,
	); err != nil {
		cleanupSafe = errors.Is(err, ErrBackupAlreadyExists) ||
			errors.Is(err, ErrBackupMetadataTooLarge)
		return fmt.Errorf("writing backup metadata: %w", err)
	}
	committed = true

	return nil
}

// Restore implements antflymcp.AntflyHandler.
func (a *mcpAdapter) Restore(
	ctx context.Context,
	tableName, backupID, connection, location string,
) error {
	if err := common.ValidateBackupID(backupID); err != nil {
		return err
	}
	metadataStore, err := newBackupStore(
		a.t.ln.config,
		connection,
		"restore.read",
		location,
	)
	if err != nil {
		return err
	}
	metadata, err := metadataStore.ReadMetadata(ctx, backupID)
	if err != nil {
		return fmt.Errorf("reading backup metadata: %w", err)
	}
	tableMetadata := metadata.Table

	if tableMetadata.Name != tableName {
		return fmt.Errorf("table name mismatch: expected %s, but backup metadata is for %s",
			tableName, tableMetadata.Name)
	}
	if err := validateBackupMetadataArtifactIdentities(
		ctx,
		metadataStore,
		backupID,
		metadata,
	); err != nil {
		return fmt.Errorf("validating backup artifacts: %w", err)
	}

	if err := a.t.tm.RestoreTable(tableMetadata, &common.BackupConfig{
		Location:         location,
		ResolvedLocation: metadataStore.ResolvedLocation(),
		Connection:       connection,
		BackupID:         backupID,
		Format:           metadata.Format,
	}, metadata.Artifacts); err != nil {
		return fmt.Errorf("restoring table: %w", err)
	}

	a.t.ln.TriggerReconciliation()
	return nil
}

// Compile-time check that mcpAdapter implements AntflyHandler.
var _ antflymcp.AntflyHandler = (*mcpAdapter)(nil)
