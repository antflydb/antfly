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

//go:build go1.24

//go:generate go tool oapi-codegen --config=cfg.yaml ./api.yaml
package metadata

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/antflydb/antfly/lib/evaluator"
	"github.com/antflydb/antfly/lib/schema"
	_ "github.com/antflydb/antfly/lib/template" // Import for side effects: registers remoteMedia, remotePDF, remoteText helpers
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/workerpool"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/metadata/foreign"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/client"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/antflydb/antfly/src/tablemgr"
	"github.com/antflydb/antfly/src/usermgr"
	"go.uber.org/zap"
)

// batchRequestPool is a sync.Pool for reusing BatchRequest objects
var batchRequestPool = sync.Pool{
	New: func() any {
		return &BatchRequest{
			Deletes: []string{},
			Inserts: map[string]map[string]any{},
		}
	},
}

// writesPool is a sync.Pool for reusing [][2][]byte slices
var writesPool = sync.Pool{
	New: func() any {
		return &[][2][]byte{}
	},
}

// deletesPool is a sync.Pool for reusing [][]byte slices
var deletesPool = sync.Pool{
	New: func() any {
		return &[][]byte{}
	},
}

type TableApi struct {
	ln          *MetadataStore
	tm          *tablemgr.TableManager
	logger      *zap.Logger
	pool        *workerpool.Pool
	joinOnce    sync.Once
	joinService *JoinService
	joinInitErr error
	foreignPool *foreign.PoolManager
	prunerCache     sync.Map // documentRenderer string → *ai.Pruner
	baseIndexCache  sync.Map // shardTopologyKey (uint64) → indexes.ShardIndexes
}
type ClusterApi struct {
	ln     *MetadataStore
	logger *zap.Logger
}

func tableSchemaSnapshot(tableSchema *schema.TableSchema) schema.TableSchema {
	if tableSchema == nil {
		return schema.TableSchema{}
	}
	return *tableSchema
}

func tableMigration(table *store.Table) *TableMigration {
	if table.ReadSchema == nil {
		return nil
	}
	return &TableMigration{
		State:      TableMigrationStateRebuilding,
		ReadSchema: *table.ReadSchema,
	}
}

func tableShardsMap(table *store.Table) map[string]ShardConfig {
	shards := make(map[string]ShardConfig, len(table.Shards))
	for id, shard := range table.Shards {
		shards[id.String()] = ShardConfig{
			ByteRange: ByteRange{shard.ByteRange[0], shard.ByteRange[1]},
		}
	}
	return shards
}

func tableResponse(table *store.Table) Table {
	return Table{
		Name:               table.Name,
		Description:        table.Description,
		Shards:             tableShardsMap(table),
		Indexes:            table.Indexes,
		Schema:             tableSchemaSnapshot(table.Schema),
		Migration:          tableMigration(table),
		ReplicationSources: replicationSourcesToAPI(table.ReplicationSources),
	}
}

func validateEmbeddingsIndexAPIConfig(
	ctx context.Context,
	embeddingsConfig *indexes.EmbeddingsIndexConfig,
	indexLabel string,
) error {
	if embeddingsConfig.IsExternal() {
		if !embeddingsConfig.Sparse && embeddingsConfig.Dimension <= 0 {
			return fmt.Errorf("embedding dimension must be set and greater than 0 for external dense %s", indexLabel)
		}
		return nil
	}

	if embeddingsConfig.Embedder == nil {
		return fmt.Errorf("managed %s must specify an embedder; set external=true for client-supplied embeddings", indexLabel)
	}

	if err := validateEmbedderConfig(ctx, embeddingsConfig, indexLabel); err != nil {
		return err
	}

	return nil
}

func tableStatusResponse(
	table *store.Table,
	tm *tablemgr.TableManager,
) TableStatus {
	diskSize, empty := tableStorageStatus(table, tm)
	return TableStatus{
		Name:               table.Name,
		Description:        table.Description,
		Shards:             tableShardsMap(table),
		Indexes:            normalizeTableIndexTypes(table.Indexes),
		Schema:             tableSchemaSnapshot(table.Schema),
		Migration:          tableMigration(table),
		ReplicationSources: replicationSourcesToAPI(table.ReplicationSources),
		StorageStatus: StorageStatus{
			DiskUsage: diskSize,
			Empty:     empty,
		},
	}
}

// handleGetStatus returns the current status of all stores and shards
func (ca *ClusterApi) GetStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	stores, shards := ca.ln.tm.Status()
	health := ClusterHealthHealthy
	message := ""
	for id, status := range stores.Statuses {
		if status == nil {
			continue
		}
		if status.State != store.StoreState_Healthy {
			health = ClusterHealthUnhealthy
			message = fmt.Sprintf("store %s is not healthy", id)
		}
	}
	replicationFactor := ca.ln.config.ReplicationFactor
	for id, status := range shards.Statuses {
		if status == nil {
			continue
		}
		if status.State.Transitioning() {
			health = ClusterHealthDegraded
			message = fmt.Sprintf("shard %s is in a transition state", id)
		}
		if !ca.ln.config.SwarmMode {

			if status.RaftStatus == nil {
				health = ClusterHealthDegraded
				message = fmt.Sprintf("shard %s has no raft status", id)
				continue
			}
			if status.RaftStatus.Lead == 0 {
				health = ClusterHealthDegraded
				message = fmt.Sprintf("shard %s has no leader elected", id)
				continue
			}
			if len(status.RaftStatus.Voters) < int(replicationFactor) { //nolint:gosec // G115: bounded value, cannot overflow in practice
				health = ClusterHealthDegraded
				message = fmt.Sprintf(
					"shard %s is under-replicated in raft: current %d, expected %d",
					id,
					len(status.RaftStatus.Voters),
					replicationFactor,
				)
			}
		}
		if len(status.Peers) < int(replicationFactor) { //nolint:gosec // G115: bounded value, cannot overflow in practice
			health = ClusterHealthDegraded
			message = fmt.Sprintf(
				"shard %s is under-replicated: current %d, expected %d",
				id,
				len(status.Peers),
				replicationFactor,
			)
		}
		// if maps.Equal(status.RaftStatus.Voters, status.Peers) {
		// 	health = clusterHealth_Degraded
		// 	message = fmt.Sprintf("shard %s is moving peers: current %s, expected %s", id, status.RaftStatus.Voters, status.Peers)
		// }
	}
	authEnabled := ca.ln.config.EnableAuth
	status := ClusterStatus{
		Health:      health,
		Message:     message,
		AuthEnabled: authEnabled,
		SwarmMode:   ca.ln.config.SwarmMode,
		AdditionalProperties: map[string]any{
			"shards":        shards,
			"stores":        stores,
			"metadata_info": ca.ln.metadataStore.Info(),
		},
	}
	if err := json.NewEncoder(w).Encode(status); err != nil {
		ca.ln.logger.Warn("Failed to marshal status", zap.Error(err))
	}
}

type wrapper struct {
	*TableApi
	*ClusterApi
	*SecretsApi
}

// NewTableApi creates a new TableApi instance. This is used by the A2A facade
// package to call agent methods directly.
func NewTableApi(logger *zap.Logger, ln *MetadataStore, tm *tablemgr.TableManager) *TableApi {
	return &TableApi{
		logger:      logger,
		tm:          tm,
		ln:          ln,
		pool:        ln.pool,
		foreignPool: foreign.NewPoolManager(),
	}
}

func NewPublicApi(logger *zap.Logger, ln *MetadataStore, tm *tablemgr.TableManager) http.Handler {
	api := NewTableApi(logger, ln, tm)
	logger.Debug("Creating public API handler with StdHTTPServerOptions",
		zap.String("baseURL", ""), // StdHTTPServerOptions has empty BaseURL by default
		zap.Bool("hasAuthMiddleware", true))
	return HandlerWithOptions(wrapper{
		TableApi:   api,
		ClusterApi: &ClusterApi{ln: ln, logger: logger},
		SecretsApi: &SecretsApi{ms: ln, logger: logger},
	}, StdHTTPServerOptions{
		BaseRouter:  http.NewServeMux(),
		Middlewares: []MiddlewareFunc{ln.authnMiddleware},
	})
}

// tableStorageStatus computes aggregate disk usage and emptiness across all
// shards in a table.
func tableStorageStatus(table *store.Table, tm *tablemgr.TableManager) (diskSize uint64, empty bool) {
	empty = true
	for id := range table.Shards {
		shardStatus, err := tm.GetShardStatus(id)
		if err != nil {
			continue
		}
		if shardStatus.ShardStats != nil {
			diskSize += shardStatus.ShardStats.Storage.DiskSize
			empty = empty && shardStatus.ShardStats.Storage.Empty
		}
	}
	return diskSize, empty
}

// normalizeTableIndexTypes returns a copy of the indexes map with legacy type names
// (aknn_v0, full_text_v0, graph_v0) translated to their canonical forms
// (embeddings, full_text, graph) for user-facing API responses.
func normalizeTableIndexTypes(idxs map[string]indexes.IndexConfig) map[string]indexes.IndexConfig {
	if len(idxs) == 0 {
		return idxs
	}
	normalized := make(map[string]indexes.IndexConfig, len(idxs))
	for name, cfg := range idxs {
		cfg.Type = indexes.NormalizeIndexType(cfg.Type)
		normalized[name] = cfg
	}
	return normalized
}

func (t *TableApi) GlobalQuery(w http.ResponseWriter, r *http.Request) {
	// TODO (ajr) Get the table name from the request context before ensuring auth
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, "*", usermgr.PermissionTypeRead) {
		return
	}
	t.handleQuery(w, r, "")
}

func (t *TableApi) ListTables(w http.ResponseWriter, r *http.Request, params ListTablesParams) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, "*", usermgr.PermissionTypeRead) {
		return
	}
	// Get tables from TableManager with optional filtering
	tables, err := t.tm.Tables(&params.Prefix, &params.Pattern)
	if err != nil {
		errorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	tableStatus := make([]TableStatus, len(tables))
	for i, table := range tables {
		tableStatus[i] = tableStatusResponse(table, t.tm)
	}

	// Return the enhanced response as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(tableStatus); err != nil {
		t.logger.Warn("Failed to encode table response", zap.Error(err))
	}
}

func (t *TableApi) DropTable(w http.ResponseWriter, r *http.Request, tableName string) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeAdmin) {
		return
	}
	// MVP (ajr) Contains side-effects for raft log
	if err := t.tm.RemoveTable(tableName); err != nil {
		if errors.Is(err, tablemgr.ErrNotFound) {
			errorResponse(w, err.Error(), http.StatusBadRequest)
			return
		}
		errorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}
	t.ln.TriggerReconciliation()
	// FIXME (ajr) ensure that the table was dropped from all shards
	// 1. Add tests that the table is dropped from all shards
	t.logger.Info("Table dropped successfully", zap.String("table", tableName))
	w.WriteHeader(http.StatusNoContent) // 204 No Content is appropriate for successful DELETE
}

func (t *TableApi) GetTable(w http.ResponseWriter, r *http.Request, tableName string) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeRead) {
		return
	}
	// Get the table from the TableManager
	table, err := t.tm.GetTable(tableName)
	if err != nil {
		errorResponse(w, fmt.Sprintf("Table not found: %v", err), http.StatusNotFound)
		return
	}

	tableStatus := tableStatusResponse(table, t.tm)

	// Return the enhanced response as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(tableStatus); err != nil {
		t.logger.Warn("Failed to encode table response", zap.Error(err))
	}
}

func (t *TableApi) CreateTable(w http.ResponseWriter, r *http.Request, tableName string) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeAdmin) {
		return
	}
	t.logger.Debug("Creating table", zap.String("tableName", tableName))
	var create CreateTableRequest
	if err := json.NewDecoder(r.Body).Decode(&create); err != nil {
		errorResponse(w, "Failed to parse create table request", http.StatusBadRequest)
		return
	}
	numShards := create.NumShards
	if numShards == 0 {
		numShards = uint(t.ln.config.DefaultShardsPerTable)
	}
	fullTextIndex := "full_text_index_v0"
	tc := &tablemgr.TableConfig{
		NumShards:   numShards,
		Description: create.Description,
		Schema:      &create.Schema,
		Indexes: map[string]indexes.IndexConfig{
			fullTextIndex: *indexes.NewFullTextIndexConfig(fullTextIndex, false),
		},
	}
	for index, config := range create.Indexes {
		if index == "" {
			errorResponse(w, "Index name cannot be empty", http.StatusBadRequest)
			return
		}
		if strings.HasPrefix(index, "full_text_index") {
			errorResponse(w, "Index name `full_text_index` is reserved", http.StatusBadRequest)
			return
		}

		// Validate the index configuration
		if err := config.Validate(); err != nil {
			errorResponse(w, fmt.Sprintf("Invalid index configuration for %q: %v", index, err), http.StatusBadRequest)
			return
		}

		// Normalize legacy type names (e.g., aknn_v0 → embeddings)
		config.Type = indexes.NormalizeIndexType(config.Type)

		if indexes.IsFullTextType(config.Type) {
			continue
		}
		if indexes.IsGraphType(config.Type) {
			tc.Indexes[index] = config
			continue
		}

		embeddingsConfig, err := config.AsEmbeddingsIndexConfig()
		if err != nil {
			errorResponse(
				w,
				fmt.Sprintf("Invalid index configuration for %s: %v", index, err),
				http.StatusBadRequest,
			)
			return
		}
		if err := validateEmbeddingsIndexAPIConfig(r.Context(), &embeddingsConfig, fmt.Sprintf("index %q", index)); err != nil {
			errorResponse(w, err.Error(), http.StatusBadRequest)
			return
		}

		tc.Indexes[index] = *indexes.NewEmbeddingsConfig(config.Name, embeddingsConfig)
	}
	// FIXME (ajr) We need Schema for this
	// for vectorField, dimension := range needsVectorIndex {
	// 	tc.Indexes[vectorField] = &common.IndexConfig{
	// 		Name: vectorField,
	// 		Type: "vector/v2",
	// 		Config: map[string]any{
	// 			"mem_only":        !onDiskIndex,
	// 			"dimension":      dimension,
	// 			"embeddingField": vectorField,
	// 		},
	// 	}
	// }

	// Map replication sources from API type to store type
	if len(create.ReplicationSources) > 0 {
		tc.ReplicationSources = make([]store.ReplicationSourceConfig, 0, len(create.ReplicationSources))
		for _, src := range create.ReplicationSources {
			keyTemplate := src.KeyTemplate
			if keyTemplate == "" {
				keyTemplate = "id"
			}
			cfg := store.ReplicationSourceConfig{
				Type:              string(src.Type),
				DSN:               src.Dsn,
				PostgresTable:     src.PostgresTable,
				KeyTemplate:       keyTemplate,
				SlotName:          src.SlotName,
				PublicationName:   src.PublicationName,
				PublicationFilter: src.PublicationFilter,
				OnUpdate:          apiOpsToStore(src.OnUpdate),
				OnDelete:          apiOpsToStore(src.OnDelete),
			}
			for _, route := range src.Routes {
				if route.TargetTable == "" {
					errorResponse(w, "replication route missing target_table", http.StatusBadRequest)
					return
				}
				// Validate route filter parses correctly
				if len(route.Where) > 0 {
					if _, err := evaluator.ParseFilter(route.Where); err != nil {
						errorResponse(w, fmt.Sprintf("invalid route where filter for target %q: %v", route.TargetTable, err), http.StatusBadRequest)
						return
					}
				}
				cfg.Routes = append(cfg.Routes, store.ReplicationRouteConfig{
					TargetTable: route.TargetTable,
					Where:       route.Where,
					KeyTemplate: route.KeyTemplate,
					OnUpdate:    apiOpsToStore(route.OnUpdate),
					OnDelete:    apiOpsToStore(route.OnDelete),
				})
			}
			// Validate publication_filter translates to SQL
			if len(cfg.PublicationFilter) > 0 {
				if _, err := foreign.FilterToLiteralSQL(cfg.PublicationFilter, nil); err != nil {
					errorResponse(w, fmt.Sprintf("invalid publication_filter: %v", err), http.StatusBadRequest)
					return
				}
			}
			tc.ReplicationSources = append(tc.ReplicationSources, cfg)
		}
	}

	// Create the table
	// MVP (ajr) Contains side-effects for raft log
	t.logger.Debug("Creating table inside table manager", zap.String("tableName", tableName))
	table, err := t.tm.CreateTable(tableName, *tc)
	if err != nil {
		errorResponse(w, fmt.Sprintf("Failed to create table: %v", err), http.StatusBadRequest)
		return
	}
	t.logger.Debug("Triggering shard reconciliation", zap.String("tableName", tableName))
	t.ln.TriggerReconciliation()
	resp := tableResponse(table)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		t.logger.Warn("Error encoding response", zap.Error(err))
	}
}

func (t *TableApi) ListIndexes(w http.ResponseWriter, r *http.Request, tableName string) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeAdmin) {
		return
	}
	// Get the indexes from the TableManager
	idxs, err := t.tm.Indexes(tableName)
	if err != nil {
		errorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := make([]IndexStatus, 0, len(idxs))

	for _, idx := range idxs {
		shardStatus := make(map[string]indexes.IndexStats, len(idx.ShardStatus))
		for k, v := range idx.ShardStatus {
			shardStatus[k.String()] = v
		}
		cfg := idx.IndexConfig
		cfg.Type = indexes.NormalizeIndexType(cfg.Type)
		resp = append(resp, IndexStatus{
			Config:      cfg,
			Status:      idx.Status,
			ShardStatus: shardStatus,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		t.logger.Warn("Failed to encode table response", zap.Error(err))
	}
}

func (t *TableApi) DropIndex(
	w http.ResponseWriter,
	r *http.Request,
	tableName string,
	indexName string,
) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeAdmin) {
		return
	}
	if strings.HasPrefix(indexName, "full_text_index") {
		errorResponse(w, "Index name `full_text_index` is reserved", http.StatusBadRequest)
		return
	}
	if err := t.ln.dropIndexFromTable(r.Context(), tableName, indexName); err != nil {
		if !errors.Is(err, context.Canceled) {
			errorResponse(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	// FIXME (ajr) index was dropped from all shards
	t.ln.TriggerReconciliation()
	w.WriteHeader(http.StatusCreated)
}

func (t *TableApi) GetIndex(
	w http.ResponseWriter,
	r *http.Request,
	tableName string,
	indexName string,
) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeRead) {
		return
	}
	index, err := t.tm.GetIndex(tableName, indexName)
	if err != nil {
		errorResponse(w, fmt.Sprintf("Table not found: %v", err), http.StatusNotFound)
		return
	}
	shardStatus := make(map[string]indexes.IndexStats, len(index.ShardStatus))
	for k, v := range index.ShardStatus {
		shardStatus[k.String()] = v
	}
	cfg := index.IndexConfig
	cfg.Type = indexes.NormalizeIndexType(cfg.Type)
	resp := IndexStatus{
		Config:      cfg,
		Status:      index.Status,
		ShardStatus: shardStatus,
	}

	// Return the enhanced response as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		t.logger.Warn("Failed to encode table response", zap.Error(err))
	}
}

func errorResponse(w http.ResponseWriter, err string, code int) {
	h := w.Header()
	// Delete the Content-Length header, which might be for some other content.
	// Assuming the error string fits in the writer's buffer, we'll figure
	// out the correct Content-Length for it later.
	//
	// We don't delete Content-Encoding, because some middleware sets
	// Content-Encoding: gzip and wraps the ResponseWriter to compress on-the-fly.
	// See https://go.dev/issue/66343.
	h.Del("Content-Length")

	// There might be content type already set, but we reset it to
	// text/plain for the error message.
	h.Set("Content-Type", "application/json")
	h.Set("X-Content-Type-Options", "nosniff")

	w.WriteHeader(code)
	_, _ = fmt.Fprintf(w, "{ \"error\" : %q }\n", err) //nolint:gosec // G705: JSON/SSE API response, not HTML
}

func (t *TableApi) CreateIndex(
	w http.ResponseWriter,
	r *http.Request,
	tableName string,
	indexName string,
) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeAdmin) {
		return
	}
	var config indexes.IndexConfig
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		errorResponse(w, "Failed to parse registration request", http.StatusBadRequest)
		return
	}

	// Validate the index configuration
	if err := config.Validate(); err != nil {
		errorResponse(w, fmt.Sprintf("Invalid index configuration: %v", err), http.StatusBadRequest)
		return
	}

	// Normalize legacy type names (e.g., aknn_v0 → embeddings)
	config.Type = indexes.NormalizeIndexType(config.Type)

	if indexes.IsFullTextType(config.Type) {
		errorResponse(
			w,
			"Creating alternative full text indexes is not implemented",
			http.StatusBadRequest,
		)
		return
	}

	embeddingsConfig, err := config.AsEmbeddingsIndexConfig()
	if err != nil {
		errorResponse(w, fmt.Sprintf("Invalid index configuration: %v", err), http.StatusBadRequest)
		return
	}
	if err := validateEmbeddingsIndexAPIConfig(r.Context(), &embeddingsConfig, fmt.Sprintf("index %q", indexName)); err != nil {
		errorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Normalize chunker config: infer provider if missing (for backwards compatibility)
	if embeddingsConfig.Chunker != nil && embeddingsConfig.Chunker.Provider == "" {
		// Try to unmarshal as TermiteChunkerConfig to check if it's a termite config
		if _, err := embeddingsConfig.Chunker.AsTermiteChunkerConfig(); err == nil {
			embeddingsConfig.Chunker.Provider = "termite"
		}
	}

	config = *indexes.NewEmbeddingsConfig(config.Name, embeddingsConfig)
	// MVP (ajr) Contains side-effects for raft log
	if err := t.ln.addIndexToTable(r.Context(), tableName, indexName, config); err != nil {
		if !errors.Is(err, context.Canceled) {
			errorResponse(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusCreated)
}

func (t *TableApi) LookupKey(w http.ResponseWriter, r *http.Request, tableName string, key string, params LookupKeyParams) {
	defer func() { _ = r.Body.Close() }()
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeRead) {
		return
	}
	table, err := t.tm.GetTable(tableName)
	if err != nil {
		err := fmt.Errorf("getting table %s: %w", tableName, err)
		errorResponse(w, err.Error(), http.StatusNotFound)
		return
	}
	shardID, err := table.FindShardForKey(key)
	if err != nil {
		errorResponse(
			w,
			fmt.Sprintf("Failed to find shard for key: %v", key),
			http.StatusInternalServerError,
		)
		return
	}
	lookupResp, version, err := t.ln.forwardLookupToShardWithVersion(r.Context(), shardID, key)
	if err != nil {
		if errors.Is(err, client.ErrKeyOutOfRange) {
			shardStatus, statusErr := t.tm.GetShardStatus(shardID)
			if statusErr == nil {
				errorResponse(
					w,
					fmt.Sprintf("Failed to lookup key: out of range for shard (range: %s)", shardStatus.ByteRange),
					http.StatusInternalServerError,
				)
			} else {
				errorResponse(
					w,
					fmt.Sprintf("Failed to lookup key: out of range (shard status unavailable: %v)", statusErr),
					http.StatusInternalServerError,
				)
			}
			return
		}
		errorResponse(
			w,
			fmt.Sprintf("Failed to lookup key: %v", err),
			http.StatusInternalServerError,
		)
		return
	}

	// Set version header for OCC transaction support
	w.Header().Set("X-Antfly-Version", strconv.FormatUint(version, 10))

	// Decode the response for field filtering
	var doc map[string]any
	if err := json.Unmarshal(lookupResp, &doc); err != nil {
		t.logger.Error("Failed to unmarshal document", zap.Error(err))
		errorResponse(w, "Failed to process document", http.StatusInternalServerError)
		return
	}

	// Apply field filtering using the same logic as ParseFieldSelection
	// Default behavior matches query: include summaries, exclude embeddings and chunks
	if params.Fields != "" {
		// User specified fields - parse and apply projection
		fields := strings.Split(params.Fields, ",")
		for i := range fields {
			fields[i] = strings.TrimSpace(fields[i])
		}

		// Use ParseFieldSelection to handle special fields (_embeddings, _summaries, _chunks)
		regularPaths, queryOpts := db.ParseFieldSelection(fields)

		// Build the result document
		result := make(map[string]any)

		// Project regular fields if specified
		if len(regularPaths) > 0 {
			projected := db.ProjectFields(doc, regularPaths)
			maps.Copy(result, projected)
		} else if !hasSpecialFieldsOnly(fields) {
			// No regular fields and no special-only request - include all regular fields
			for k, v := range doc {
				if !strings.HasPrefix(k, "_") {
					result[k] = v
				}
			}
		}

		// Add special fields based on queryOpts
		if queryOpts.AllEmbeddings {
			if emb, ok := doc["_embeddings"]; ok {
				result["_embeddings"] = emb
			}
		}
		if queryOpts.AllSummaries {
			if sum, ok := doc["_summaries"]; ok {
				result["_summaries"] = sum
			}
		}
		if queryOpts.AllChunks {
			if chunks, ok := doc["_chunks"]; ok {
				result["_chunks"] = chunks
			}
		}

		doc = result
	} else {
		// No fields specified - apply query defaults:
		// Include summaries, exclude embeddings and chunks
		delete(doc, "_embeddings")
		delete(doc, "_chunks")
		// Keep _summaries (matches ParseFieldSelection default)
	}

	lookupResp, err = json.Marshal(doc)
	if err != nil {
		t.logger.Error("Failed to marshal document", zap.Error(err))
		errorResponse(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(lookupResp)))
	if _, err := w.Write(lookupResp); err != nil {
		t.logger.Error("Error writing response", zap.Error(err))
	}
}

// hasSpecialFieldsOnly returns true if all fields are special fields (_embeddings, _summaries, _chunks)
func hasSpecialFieldsOnly(fields []string) bool {
	for _, f := range fields {
		if !strings.HasPrefix(f, "_") {
			return false
		}
	}
	return len(fields) > 0
}

func (t *TableApi) QueryTable(w http.ResponseWriter, r *http.Request, tableName string) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeRead) {
		return
	}
	t.handleQuery(w, r, tableName)
}

// ScanKeys scans keys in a table within an optional key range and returns them as
// newline-delimited JSON (NDJSON). Each line contains a JSON object with a "key" field
// and optionally projected document fields.
func (t *TableApi) ScanKeys(w http.ResponseWriter, r *http.Request, tableName string) {
	defer func() { _ = r.Body.Close() }()
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeRead) {
		return
	}

	table, err := t.tm.GetTable(tableName)
	if err != nil {
		errorResponse(w, fmt.Sprintf("table not found: %s", tableName), http.StatusNotFound)
		return
	}

	// Parse request body (optional)
	var req ScanKeysRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			errorResponse(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
			return
		}
	}

	// Determine if we need to include documents (when fields are requested OR filter_query is used)
	hasFilterQuery := len(req.FilterQuery) > 0 && !bytes.Equal(req.FilterQuery, []byte("null"))
	includeDocuments := len(req.Fields) > 0 || hasFilterQuery

	// Get shards sorted by byte range for ordered output
	type shardWithRange struct {
		ID    types.ID
		Range types.Range
	}
	shards := make([]shardWithRange, 0, len(table.Shards))
	for shardID, shardConfig := range table.Shards {
		shards = append(shards, shardWithRange{ID: shardID, Range: shardConfig.ByteRange})
	}
	slices.SortFunc(shards, func(a, b shardWithRange) int {
		return bytes.Compare(a.Range[0], b.Range[0])
	})

	// Set response headers for NDJSON streaming
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	flusher, ok := w.(http.Flusher)
	if !ok {
		t.logger.Warn("ResponseWriter does not support Flusher interface")
	}

	// Scan each shard and stream results
	enc := json.NewEncoder(w)
	resultCount := 0
	for _, shard := range shards {
		// Check if we've reached the limit
		if req.Limit > 0 && resultCount >= req.Limit {
			break
		}
		// Determine the scan range for this shard
		fromKey := []byte(req.From)
		toKey := []byte(req.To)

		// If no 'from' specified, use the shard's start boundary
		if req.From == "" {
			fromKey = shard.Range[0]
		}
		// If no 'to' specified, use the shard's end boundary
		if req.To == "" {
			toKey = shard.Range[1]
		}

		// Skip shards that don't overlap with the requested range
		if req.To != "" && bytes.Compare(shard.Range[0], []byte(req.To)) >= 0 {
			continue // Shard starts after the requested range ends
		}
		if req.From != "" && bytes.Compare(shard.Range[1], []byte(req.From)) <= 0 {
			continue // Shard ends before the requested range starts
		}

		// Clamp the scan range to the shard boundaries
		if bytes.Compare(fromKey, shard.Range[0]) < 0 {
			fromKey = shard.Range[0]
		}
		if bytes.Compare(toKey, shard.Range[1]) > 0 {
			toKey = shard.Range[1]
		}

		// Calculate remaining limit for this shard
		shardLimit := 0
		if req.Limit > 0 {
			shardLimit = req.Limit - resultCount
			if shardLimit <= 0 {
				break // Already reached total limit
			}
		}

		// Call scan on this shard with filter and limit pushed down
		scanResult, err := t.ln.forwardScanToShard(
			r.Context(),
			shard.ID,
			fromKey,
			toKey,
			req.InclusiveFrom,
			req.ExclusiveTo,
			includeDocuments,
			req.FilterQuery,
			shardLimit,
		)
		if err != nil {
			t.logger.Error("Failed to scan shard",
				zap.Stringer("shardID", shard.ID),
				zap.Error(err))
			// Continue with other shards rather than failing entirely
			continue
		}

		// Stream results as NDJSON
		// Sort keys for deterministic output
		keys := make([]string, 0, len(scanResult.Hashes))
		for key := range scanResult.Hashes {
			keys = append(keys, key)
		}
		slices.Sort(keys)

		for _, key := range keys {
			// Check if we've reached the limit (safety check, store should already limit)
			if req.Limit > 0 && resultCount >= req.Limit {
				break
			}

			result := map[string]any{"key": key}

			// Add projected fields if requested
			if len(req.Fields) > 0 {
				if doc, ok := scanResult.Documents[key]; ok {
					projected := db.ProjectFields(doc, req.Fields)
					maps.Copy(result, projected)
				}
			}

			if err := enc.Encode(result); err != nil {
				t.logger.Error("Failed to encode result", zap.String("key", key), zap.Error(err))
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			resultCount++
		}
	}
}

func (t *TableApi) BatchWrite(w http.ResponseWriter, r *http.Request, tableName string) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeWrite) {
		return
	}
	defer func() { _ = r.Body.Close() }()

	// Get a BatchRequest from the pool
	kvs := batchRequestPool.Get().(*BatchRequest)
	defer func() {
		// Clear the BatchRequest before returning it to the pool
		kvs.Deletes = kvs.Deletes[:0]
		clear(kvs.Inserts)
		batchRequestPool.Put(kvs)
	}()

	if err := json.NewDecoder(r.Body).Decode(kvs); err != nil {
		errorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(kvs.Deletes) == 0 && len(kvs.Inserts) == 0 && len(kvs.Transforms) == 0 {
		errorResponse(w, "No data", http.StatusBadRequest)
		return
	}

	// Parse sync level from request (default to propose)
	syncLevel, err := parseSyncLevel(kvs.SyncLevel)
	if err != nil {
		errorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}

	table, err := t.tm.GetTable(tableName)
	if err != nil {
		err := fmt.Errorf("getting table %s: %w", tableName, err)
		errorResponse(w, err.Error(), http.StatusNotFound)
		return
	}

	// Add timestamp to all documents if not already present
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	for _, doc := range kvs.Inserts {
		if _, exists := doc["_timestamp"]; !exists {
			doc["_timestamp"] = timestamp
		}
	}

	// Validate all documents and collect keys
	keys := make([]string, 0, len(kvs.Inserts))
	for providedKey, doc := range kvs.Inserts {
		if err := validateDocumentInsertKey(table, providedKey); err != nil {
			errorResponse(w, fmt.Sprintf("invalid document id %q: %v", providedKey, err), http.StatusBadRequest)
			return
		}

		// Validate the document using Table.ValidateDoc
		_, err := table.ValidateDoc(doc)
		if err != nil {
			errorResponse(
				w,
				fmt.Sprintf("validation error for key %s: %v", providedKey, err),
				http.StatusBadRequest,
			)
			return
		}

		keys = append(keys, providedKey)
	}
	for _, key := range kvs.Deletes {
		if err := validateDocumentMutationKey(key); err != nil {
			errorResponse(w, fmt.Sprintf("invalid document id %q: %v", key, err), http.StatusBadRequest)
			return
		}
	}
	for _, transformReq := range kvs.Transforms {
		if err := validateDocumentTransformKey(table, transformReq.Key, transformReq.Upsert); err != nil {
			errorResponse(w, fmt.Sprintf("invalid document id %q: %v", transformReq.Key, err), http.StatusBadRequest)
			return
		}
	}
	if len(kvs.Inserts) == 1 {
		// forwardInsertToShard handles routing and re-routes on each retry to pick up
		// routing table changes during splits
		if err := t.ln.forwardInsertToShard(r.Context(), tableName, keys[0], kvs.Inserts[keys[0]], syncLevel); err != nil {
			errorResponse(
				w,
				fmt.Sprintf("Failed to insert data: %v", err),
				http.StatusInternalServerError,
			)
			return
		}
		w.WriteHeader(http.StatusCreated)
		return
	}
	partitions, unfoundKeys, err := partitionWriteKeysByShard(t.tm, table, keys)
	if err != nil {
		errorResponse(
			w,
			fmt.Sprintf("Failed to partition keys for writes: %v", err),
			http.StatusInternalServerError,
		)
		return
	}
	if len(unfoundKeys) > 0 {
		errorResponse(
			w,
			fmt.Sprintf("Failed to find partitions for keys: %v", unfoundKeys),
			http.StatusInternalServerError,
		)
		return
	}
	deletePartitions, unfoundKeys, err := partitionWriteKeysByShard(t.tm, table, kvs.Deletes)
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
			fmt.Sprintf("Failed to find partitions for keys: %v", unfoundKeys),
			http.StatusInternalServerError,
		)
		return
	}

	// Convert API transforms to protobuf and partition by shard
	var transformPartitions map[types.ID][]*db.Transform
	if len(kvs.Transforms) > 0 {
		// Convert API transforms to protobuf
		batchTransforms := make([]*db.Transform, 0, len(kvs.Transforms))
		transformKeys := make([]string, 0, len(kvs.Transforms))

		for _, transformReq := range kvs.Transforms {
			transform, err := TransformFromAPI(transformReq)
			if err != nil {
				errorResponse(w, err.Error(), http.StatusBadRequest)
				return
			}
			batchTransforms = append(batchTransforms, transform)
			transformKeys = append(transformKeys, transformReq.Key)
		}

		// Partition transforms by shard
		keyPartitions, unfoundKeys, err := partitionWriteKeysByShard(t.tm, table, transformKeys)
		if err != nil {
			errorResponse(
				w,
				fmt.Sprintf("Failed to partition transform keys for writes: %v", err),
				http.StatusInternalServerError,
			)
			return
		}
		if len(unfoundKeys) > 0 {
			errorResponse(
				w,
				fmt.Sprintf("Failed to find partitions for transform keys: %v", unfoundKeys),
				http.StatusInternalServerError,
			)
			return
		}

		// Group transforms by shard
		transformPartitions = make(map[types.ID][]*db.Transform)
		transformMap := make(map[string]*db.Transform)
		for i, t := range batchTransforms {
			transformMap[transformKeys[i]] = t
		}

		for shardID, keys := range keyPartitions {
			shardTransforms := make([]*db.Transform, 0, len(keys))
			for _, key := range keys {
				if transform, ok := transformMap[key]; ok {
					shardTransforms = append(shardTransforms, transform)
				}
			}
			transformPartitions[shardID] = shardTransforms
		}
	}

	// Collect all shards across writes, deletes, and transforms.
	allBatchShards := make(map[types.ID]struct{}, len(partitions)+len(deletePartitions)+len(transformPartitions))
	for sid := range partitions {
		allBatchShards[sid] = struct{}{}
	}
	for sid := range deletePartitions {
		allBatchShards[sid] = struct{}{}
	}
	for sid := range transformPartitions {
		allBatchShards[sid] = struct{}{}
	}
	if writeBatchPartitionErr(w, checkShardsNotSplitting(r.Context(), t.tm, allBatchShards)) {
		return
	}
	// Single-shard fast path for transform-only batches
	if len(kvs.Inserts) == 0 && len(kvs.Deletes) == 0 && len(kvs.Transforms) == 1 {
		// Get the first (and only) transform from transformPartitions
		var transform *db.Transform
		var shardID types.ID
		for sid, transforms := range transformPartitions {
			shardID = sid
			transform = transforms[0]
			break
		}
		if err := t.ln.forwardBatchToShard(r.Context(), shardID, nil, nil, []*db.Transform{transform}, syncLevel); err != nil {
			errorResponse(
				w,
				fmt.Sprintf("Failed to apply transform: %v", err),
				http.StatusInternalServerError,
			)
			return
		}
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(BatchResponse{
			Inserted:    0,
			Deleted:     0,
			Transformed: 1,
		}); err != nil {
			t.logger.Warn("Failed to encode batch response", zap.Error(err))
		}
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	if len(partitions) == 0 && len(transformPartitions) == 0 {
		g, _ := workerpool.NewGroup(ctx, t.pool)
		for shardID, partition := range deletePartitions {
			g.Go(func(ctx context.Context) error {
				deletes := make([][]byte, len(partition))
				for i, key := range partition {
					deletes[i] = []byte(key)
				}
				if err := t.ln.forwardBatchToShard(ctx, shardID, nil, deletes, nil, syncLevel); err != nil {
					if errors.Is(err, client.ErrNotFound) {
						status, _ := t.tm.GetShardStatus(shardID)
						t.logger.Error("Shard not found for batch forwarding",
							zap.Any("status", status),
							zap.Stringer("shardID", shardID),
							zap.Error(err))
						return fmt.Errorf("shard %s not found on store: %v", shardID, err)
					}
					if !errors.Is(err, context.Canceled) {
						t.logger.Error("Error forwarding batch", zap.Error(err))
					}
					return fmt.Errorf("batching data to shard %s: %v", shardID, err)
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			if errors.Is(err, context.Canceled) {
				t.logger.Warn("Batch operation cancelled", zap.Error(err))
			} else {
				errorResponse(w, fmt.Sprintf("Failed to forward batches: %v", err), http.StatusInternalServerError)
				return
			}
		}
	} else {
		// Check if we should use distributed transactions
		// Use transactions when multiple shards are involved for atomicity
		// (allBatchShards already computed above for split checking)
		useTransaction := len(allBatchShards) > 1

		if useTransaction {
			// Use distributed transactions for multi-shard operations
			if err := t.ln.executeBatchWithTransaction(ctx, partitions, deletePartitions, transformPartitions, kvs.Inserts, syncLevel); err != nil {
				if errors.Is(err, context.Canceled) {
					t.logger.Warn("Transactional batch operation cancelled", zap.Error(err))
				} else {
					errorResponse(w, fmt.Sprintf("Failed to execute transactional batch: %v", err), http.StatusInternalServerError)
					return
				}
			}
		} else {
			// Use non-transactional batch for single-shard operations
			if err := t.ln.executeBatchNonTransactional(ctx, partitions, deletePartitions, transformPartitions, kvs.Inserts, syncLevel); err != nil {
				if errors.Is(err, context.Canceled) {
					t.logger.Warn("Batch operation cancelled", zap.Error(err))
				} else {
					errorResponse(w, fmt.Sprintf("Failed to forward batches: %v", err), http.StatusInternalServerError)
					return
				}
			}
		}
	}
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(BatchResponse{
		Inserted:    len(kvs.Inserts),
		Deleted:     len(kvs.Deletes),
		Transformed: len(kvs.Transforms),
	}); err != nil {
		t.logger.Warn("Failed to encode batch response", zap.Error(err))
	}
}

// executeBatchWithTransaction uses distributed transactions for multi-shard batch operations
func (ms *MetadataStore) executeBatchWithTransaction(
	ctx context.Context,
	writePartitions map[types.ID][]string,
	deletePartitions map[types.ID][]string,
	transformPartitions map[types.ID][]*db.Transform,
	inserts map[string]map[string]any,
	syncLevel db.Op_SyncLevel,
) error {
	// Prepare writes, deletes, and transforms by shard
	writes := make(map[types.ID][][2][]byte)
	deletes := make(map[types.ID][][]byte)
	transforms := make(map[types.ID][]*db.Transform)

	// Encode writes
	for shardID, partition := range writePartitions {
		shardWrites := make([][2][]byte, 0, len(partition))
		for _, key := range partition {
			jsonBytes, err := json.Marshal(inserts[key])
			if err != nil {
				return fmt.Errorf("marshalling value for key %s: %w", key, err)
			}
			shardWrites = append(shardWrites, [2][]byte{[]byte(key), jsonBytes})
		}
		writes[shardID] = shardWrites
	}

	// Encode deletes
	for shardID, partition := range deletePartitions {
		shardDeletes := make([][]byte, len(partition))
		for i, key := range partition {
			shardDeletes[i] = []byte(key)
		}
		deletes[shardID] = shardDeletes
	}

	// Copy transforms (already in protobuf format)
	maps.Copy(transforms, transformPartitions)

	// Execute as distributed transaction
	return ms.ExecuteTransaction(ctx, writes, deletes, transforms, nil, syncLevel)
}

// executeBatchNonTransactional uses the original parallel batch forwarding (no atomicity guarantees)
func (ms *MetadataStore) executeBatchNonTransactional(
	ctx context.Context,
	writePartitions map[types.ID][]string,
	deletePartitions map[types.ID][]string,
	transformPartitions map[types.ID][]*db.Transform,
	inserts map[string]map[string]any,
	syncLevel db.Op_SyncLevel,
) error {
	// Allocate HLC timestamp for this batch (ensures consistent timestamps across all shards)
	timestamp := ms.hlc.Now()

	// Add timestamp to context so storage layer can write :t keys
	ctx = storeutils.WithTimestamp(ctx, timestamp)

	g, _ := workerpool.NewGroup(ctx, ms.pool)

	// Track which shards we've processed to handle delete-only shards later
	processedShards := make(map[types.ID]bool)

	// Process shards with writes (and their associated deletes)
	for shardID, partition := range writePartitions {
		processedShards[shardID] = true
		shardID, partition := shardID, partition

		g.Go(func(ctx context.Context) error {
			// Get writes slice from pool
			writesPtr := writesPool.Get().(*[][2][]byte)
			writes := (*writesPtr)[:0]
			defer func() {
				// Clear and return to pool
				*writesPtr = writes[:0]
				writesPool.Put(writesPtr)
			}()

			// Resize writes slice if needed
			if cap(writes) < len(partition) {
				writes = make([][2][]byte, len(partition))
			} else {
				writes = writes[:len(partition)]
			}

			for i, k := range partition {
				jsonBytes, err := json.Marshal(inserts[k])
				if err != nil {
					return fmt.Errorf("marshalling value: %v", err)
				}
				writes[i] = [2][]byte{[]byte(k), jsonBytes}
			}

			// Get deletes slice from pool
			deletesPtr := deletesPool.Get().(*[][]byte)
			deletes := (*deletesPtr)[:0]
			defer func() {
				// Clear and return to pool
				*deletesPtr = deletes[:0]
				deletesPool.Put(deletesPtr)
			}()

			// Resize deletes slice if needed
			deletePartition := deletePartitions[shardID]
			if cap(deletes) < len(deletePartition) {
				deletes = make([][]byte, len(deletePartition))
			} else {
				deletes = deletes[:len(deletePartition)]
			}

			for i, k := range deletePartition {
				deletes[i] = []byte(k)
			}

			// Get transforms for this shard
			transforms := transformPartitions[shardID]

			// Forward the batch to the appropriate shard.
			// forwardBatchToShard already retries transient errors internally.
			if err := ms.forwardBatchToShard(ctx, shardID, writes, deletes, transforms, syncLevel); err != nil {
				if !errors.Is(err, context.Canceled) {
					ms.logger.Error("Error forwarding batch",
						zap.Stringer("shardID", shardID),
						zap.Error(err))
				}
				return fmt.Errorf("batching data to shard %s: %v", shardID, err)
			}
			return nil
		})
	}

	// Process delete-only shards (shards that have deletes but no writes)
	for shardID, partition := range deletePartitions {
		if processedShards[shardID] {
			continue // Already handled in write loop
		}
		processedShards[shardID] = true
		shardID, partition := shardID, partition

		g.Go(func(ctx context.Context) error {
			deletes := make([][]byte, len(partition))
			for i, key := range partition {
				deletes[i] = []byte(key)
			}
			// Get transforms for this shard
			transforms := transformPartitions[shardID]
			if err := ms.forwardBatchToShard(ctx, shardID, nil, deletes, transforms, syncLevel); err != nil {
				if errors.Is(err, client.ErrNotFound) {
					status, _ := ms.tm.GetShardStatus(shardID)
					ms.logger.Error("Shard not found for batch forwarding",
						zap.Any("status", status),
						zap.Stringer("shardID", shardID),
						zap.Error(err))
					return fmt.Errorf("shard %s not found on store: %v", shardID, err)
				}
				if !errors.Is(err, context.Canceled) {
					ms.logger.Error("Error forwarding batch", zap.Error(err))
				}
				return fmt.Errorf("batching data to shard %s: %v", shardID, err)
			}
			return nil
		})
	}

	// Process transform-only shards (shards that have transforms but no writes or deletes)
	for shardID, transforms := range transformPartitions {
		if processedShards[shardID] {
			continue // Already handled in write or delete loop
		}
		shardID, transforms := shardID, transforms
		g.Go(func(ctx context.Context) error {
			if err := ms.forwardBatchToShard(ctx, shardID, nil, nil, transforms, syncLevel); err != nil {
				if errors.Is(err, client.ErrNotFound) {
					status, _ := ms.tm.GetShardStatus(shardID)
					ms.logger.Error("Shard not found for batch forwarding",
						zap.Any("status", status),
						zap.Stringer("shardID", shardID),
						zap.Error(err))
					return fmt.Errorf("shard %s not found on store: %v", shardID, err)
				}
				if !errors.Is(err, context.Canceled) {
					ms.logger.Error("Error forwarding batch", zap.Error(err))
				}
				return fmt.Errorf("batching data to shard %s: %v", shardID, err)
			}
			return nil
		})
	}

	return g.Wait()
}

func (t *TableApi) UpdateSchema(w http.ResponseWriter, r *http.Request, tableName string) {
	if !t.ln.ensureAuth(w, r, usermgr.ResourceTypeTable, tableName, usermgr.PermissionTypeAdmin) {
		return
	}
	t.logger.Debug("Updating schema for table", zap.String("tableName", tableName))
	var tableSchema schema.TableSchema
	if err := json.NewDecoder(r.Body).Decode(&tableSchema); err != nil {
		errorResponse(w, "Failed to parse schema update request", http.StatusBadRequest)
		return
	}

	table, err := t.tm.UpdateSchema(tableName, &tableSchema)
	if err != nil {
		if errors.Is(err, tablemgr.ErrNotFound) {
			errorResponse(w, err.Error(), http.StatusNotFound)
			return
		}
		errorResponse(
			w,
			fmt.Sprintf("Failed to update schema: %v", err),
			http.StatusInternalServerError,
		)
		return
	}
	t.ln.TriggerReconciliation()
	resp := tableResponse(table)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		t.logger.Warn("Error encoding response", zap.Error(err))
		errorResponse(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

// setupRoutes configures the HTTP routes for the leader API
func (ms *MetadataStore) publicApiRoutes() *http.ServeMux {
	mux := http.NewServeMux()
	userAPI := usermgr.NewUserApi(ms.logger, ms.um)
	mux.Handle(
		"/users",
		ms.authMiddleware(
			usermgr.ResourceTypeUser,
			"*",
			usermgr.PermissionTypeAdmin,
			userAPI.ServeHTTP,
		),
	)
	mux.Handle(
		"/users/",
		ms.authMiddleware(
			usermgr.ResourceTypeUser,
			"*",
			usermgr.PermissionTypeAdmin,
			userAPI.ServeHTTP,
		),
	)

	ms.logger.Debug("Creating table API handler")
	tableAPI := NewPublicApi(ms.logger, ms, ms.tm)
	ms.logger.Debug("Mounting table API routes",
		zap.Strings("patterns", []string{"/query", "/agents/", "/status", "/backup", "/restore", "/backups", "/tables", "/tables/", "/batch", "/transactions/", "/eval", "/secrets", "/secrets/"}))
	mux.Handle("/query", tableAPI)
	mux.Handle("/agents/", tableAPI)
	mux.Handle("/status", tableAPI)
	mux.Handle("/backup", tableAPI)
	mux.Handle("/restore", tableAPI)
	mux.Handle("/backups", tableAPI)
	mux.Handle("/tables", tableAPI)
	mux.Handle("/tables/", tableAPI)
	mux.Handle("/batch", tableAPI)
	mux.Handle("/transactions/", tableAPI)
	mux.Handle("/eval", tableAPI)
	mux.Handle("/secrets", tableAPI)
	mux.Handle("/secrets/", tableAPI)
	ms.logger.Debug("Public API routes setup complete")
	return mux
}

// Helper functions

// replicationSourcesToAPI converts internal replication source configs to the API response type.
func replicationSourcesToAPI(sources []store.ReplicationSourceConfig) []ReplicationSource {
	if len(sources) == 0 {
		return nil
	}
	result := make([]ReplicationSource, 0, len(sources))
	for _, src := range sources {
		apiSrc := ReplicationSource{
			Type:              ReplicationSourceType(src.Type),
			Dsn:               src.DSN,
			PostgresTable:     src.PostgresTable,
			KeyTemplate:       src.KeyTemplate,
			SlotName:          src.SlotName,
			PublicationName:   src.PublicationName,
			PublicationFilter: src.PublicationFilter,
			OnUpdate:          storeOpsToAPI(src.OnUpdate),
			OnDelete:          storeOpsToAPI(src.OnDelete),
		}
		for _, route := range src.Routes {
			apiSrc.Routes = append(apiSrc.Routes, ReplicationRoute{
				TargetTable: route.TargetTable,
				Where:       route.Where,
				KeyTemplate: route.KeyTemplate,
				OnUpdate:    storeOpsToAPI(route.OnUpdate),
				OnDelete:    storeOpsToAPI(route.OnDelete),
			})
		}
		result = append(result, apiSrc)
	}
	return result
}

// parseTransformOpType converts API transform operator to protobuf enum
func parseTransformOpType(op TransformOpType) db.TransformOp_OpType {
	switch op {
	case "$set":
		return db.TransformOp_SET
	case "$unset":
		return db.TransformOp_UNSET
	case "$inc":
		return db.TransformOp_INC
	case "$push":
		return db.TransformOp_PUSH
	case "$pull":
		return db.TransformOp_PULL
	case "$addToSet":
		return db.TransformOp_ADD_TO_SET
	case "$pop":
		return db.TransformOp_POP
	case "$mul":
		return db.TransformOp_MUL
	case "$min":
		return db.TransformOp_MIN
	case "$max":
		return db.TransformOp_MAX
	case "$currentDate":
		return db.TransformOp_CURRENT_DATE
	case "$rename":
		return db.TransformOp_RENAME
	default:
		return db.TransformOp_SET // default to SET
	}
}

func apiOpsToStore(ops []ReplicationTransformOp) []store.ReplicationTransformOp {
	if len(ops) == 0 {
		return nil
	}
	out := make([]store.ReplicationTransformOp, len(ops))
	for i, op := range ops {
		out[i] = store.ReplicationTransformOp{Op: op.Op, Path: op.Path, Value: op.Value}
	}
	return out
}

func storeOpsToAPI(ops []store.ReplicationTransformOp) []ReplicationTransformOp {
	if len(ops) == 0 {
		return nil
	}
	out := make([]ReplicationTransformOp, len(ops))
	for i, op := range ops {
		out[i] = ReplicationTransformOp{Op: op.Op, Path: op.Path, Value: op.Value}
	}
	return out
}

// TransformFromAPI converts an API Transform to a db.Transform protobuf.
// Returns an error if the transform is invalid (e.g., empty key, invalid operation value).
func TransformFromAPI(apiTransform Transform) (*db.Transform, error) {
	// Validate key
	if apiTransform.Key == "" {
		return nil, fmt.Errorf("nonempty key required for transform")
	}

	// Convert operations
	ops := make([]*db.TransformOp, 0, len(apiTransform.Operations))
	for _, opReq := range apiTransform.Operations {
		opBuilder := db.TransformOp_builder{
			Path: opReq.Path,
			Op:   parseTransformOpType(opReq.Op),
		}

		// Marshal value if present
		if opReq.Value != nil {
			valueBytes, err := json.Marshal(opReq.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid transform value: %w", err)
			}
			opBuilder.Value = valueBytes
		}

		ops = append(ops, opBuilder.Build())
	}

	// Build transform
	transformBuilder := db.Transform_builder{
		Key:        []byte(apiTransform.Key),
		Operations: ops,
	}
	if apiTransform.Upsert {
		upsert := apiTransform.Upsert
		transformBuilder.Upsert = &upsert
	}

	return transformBuilder.Build(), nil
}

// parseSyncLevel converts an API SyncLevel to a db.Op_SyncLevel protobuf enum.
// Returns an error if the sync level is invalid.
// If the input is empty, defaults to "propose" (Raft proposal acceptance).
func parseSyncLevel(level SyncLevel) (db.Op_SyncLevel, error) {
	// Handle empty string (use default)
	if level == "" {
		return db.Op_SyncLevelPropose, nil
	}

	switch level {
	case "propose":
		return db.Op_SyncLevelPropose, nil
	case "write":
		return db.Op_SyncLevelWrite, nil
	case "full_text":
		return db.Op_SyncLevelFullText, nil
	case "embeddings", "aknn":
		return db.Op_SyncLevelEmbeddings, nil
	default:
		return db.Op_SyncLevelPropose, fmt.Errorf("invalid sync_level: %s", level)
	}
}
