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

const std = @import("std");
const Allocator = std.mem.Allocator;
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const backups_api = @import("../../api/backups.zig");
const distributed_join = @import("../../api/distributed_join.zig");
const indexes_api = @import("../../api/indexes.zig");
const foreign_sources_api = @import("../../api/foreign_sources.zig");
const join_model = @import("../../api/join_model.zig");
const query_api = @import("../../api/query.zig");
const public_graph_query = @import("../../api/public_graph_query.zig");
const public_search_request = @import("../../api/public_search_request.zig");
const public_text_query = @import("../../api/public_text_query.zig");
const public_table_http = @import("../../api/public_table_http.zig");
const table_contract = @import("../../api/table_contract.zig");
const tables_api = @import("../../api/tables.zig");
const table_writes = @import("../../api/table_writes.zig");
const analysis_mod = @import("../../search/analysis.zig");
const shared_vector = @import("antfly_vector").vector;
const db_mod = @import("../../storage/db/mod.zig");
const db_transform = @import("../../storage/db/transform.zig");
const db_types = @import("../../storage/db/types.zig");
const db_query_graph = @import("../../storage/db/query/graph_exec.zig");
const distributed_stats_mod = @import("../../search/distributed_stats.zig");
const graph_mod = @import("../../graph/graph.zig");
const graph_pattern_mod = @import("../../graph/pattern.zig");
const graph_paths = @import("../../graph/paths.zig");
const graph_query_mod = @import("../../graph/query.zig");
const http_routes = @import("http_routes.zig");
const http_types = @import("http_types.zig");
const api_service = @import("service.zig");
const api_types = @import("types.zig");
const query_types = @import("query_types.zig");
const artifacts_mod = @import("../artifacts/mod.zig");
const build_mod = @import("../build/mod.zig");
const catalog_types = @import("../catalog/types.zig");
const catalog_mod = @import("../catalog/mod.zig");
const manifest_mod = @import("../manifest/mod.zig");
const query_mod = @import("../query/mod.zig");
const query_materializer = @import("../query/materializer.zig");
const runtime_bootstrap = @import("../runtime/bootstrap.zig");
const runtime_manager = @import("../runtime/manager.zig");
const document_segment_mod = @import("../document_segment/mod.zig");
const segment_mod = @import("../segment/mod.zig");
const wal_mod = @import("../wal/mod.zig");
const search_sources = @import("../search_sources.zig");
const managed_embedder = @import("../../inference/managed_embedder.zig");
const platform_time = @import("../../platform/time.zig");
const graph_segment_mod = @import("../graph_segment/mod.zig");
const foreign_mod = @import("../../foreign/mod.zig");
const query_execution = @import("query_execution.zig");
const json_helpers = @import("../../api/json_helpers.zig");
const ParsedJsonPathValue = json_helpers.ParsedJsonPathValue;
const parseJsonValueAlloc = json_helpers.parseJsonValueAlloc;
const parseJsonObjectAlloc = json_helpers.parseJsonObjectAlloc;
const parseJsonPathValueAlloc = json_helpers.parseJsonPathValueAlloc;
const parseOwnedJsonValueAlloc = json_helpers.parseOwnedJsonValueAlloc;

pub const HttpRequest = http_types.HttpRequest;
pub const HttpResponse = http_types.HttpResponse;

const SearchExecution = struct {
    plan: query_mod.SearchPlan,
    status: catalog_types.BuildStatus,
    session: ?query_mod.QuerySession,
    hits: []query_mod.QuerySearchHit,
    execution_stats: query_mod.QuerySearchExecutionStats,
    requested_offset: usize,
    requested_limit: usize,
    profile_requested: bool,

    fn deinit(self: *SearchExecution, alloc: Allocator) void {
        query_mod.freeSearchHits(alloc, self.hits);
        if (self.session) |*session| session.deinit();
        self.status.deinit(alloc);
        self.plan.deinit(alloc);
        self.* = undefined;
    }

    fn takeSession(self: *SearchExecution) query_mod.QuerySession {
        const session = self.session.?;
        self.session = null;
        return session;
    }
};

const ServerlessAggregationComputation = struct {
    total_hits: u32,
    requests: []const db_mod.aggregations.SearchAggregationRequest = &.{},
    results: []db_mod.aggregations.SearchAggregationResult,

    fn deinit(self: *@This(), alloc: Allocator) void {
        query_api.freeAggregationRequests(alloc, self.requests);
        db_mod.aggregations.deinitResults(alloc, self.results);
        self.* = undefined;
    }
};

const ServerlessAggregationContextOwned = struct {
    ctx: db_mod.aggregations.Context = .{},

    fn deinit(self: *@This(), alloc: Allocator) void {
        distributed_stats_mod.deinitTextFieldStats(alloc, self.ctx.distributed_text_stats);
        db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, self.ctx.distributed_background_text_stats);
        self.* = undefined;
    }
};

const SignificantTermFieldSet = struct {
    field: []u8,
    terms: std.ArrayListUnmanaged([]u8) = .empty,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.field);
        for (self.terms.items) |term| alloc.free(term);
        self.terms.deinit(alloc);
        self.* = undefined;
    }
};

const JsonValueMap = std.json.ArrayHashMap(std.json.Value);

const GraphResultSet = struct {
    name: []const u8,
    hits: []const db_types.SearchHit,
    total_hits: u32,
};

pub const SupportedJoinRequest = query_execution.SupportedJoinRequest;
const SupportedJoinFilters = query_execution.SupportedJoinFilters;
const JoinedQueryStats = query_execution.JoinedQueryStats;
const JoinTableStats = query_execution.JoinTableStats;
const PlannedJoinExecution = query_execution.PlannedJoinExecution;
const RightJoinQueryResult = query_execution.RightJoinQueryResult;
const ParsedSupportedJoinRequest = query_execution.ParsedSupportedJoinRequest;
const freeSupportedJoinRequest = query_execution.freeSupportedJoinRequest;
const joinUsesForeignSource = query_execution.joinUsesForeignSource;
const parseSupportedJoinRequest = query_execution.parseSupportedJoinRequest;
const parseSupportedJoinClauseValue = query_execution.parseSupportedJoinClauseValue;

pub const HttpHandler = struct {
    alloc: Allocator,
    api: *api_service.Service,
    catalog: *catalog_mod.CatalogService,
    manifests: *manifest_mod.ManifestStore,
    progress: *catalog_mod.ProgressStore,
    query: *query_mod.QueryRuntime,
    query_cache: ?*query_mod.QueryCache = null,
    managed_query_embedder: ?*managed_embedder.ManagedEmbedder = null,
    foreign_registry: ?*const foreign_mod.Registry = null,
    published_search_sources: search_sources.PublishedSearchSources = .{},
    runtime_status: *const api_types.RuntimeStatusResult,
    runtime_metrics: ?*runtime_manager.ManagedRuntime = null,

    pub fn init(
        alloc: Allocator,
        api: *api_service.Service,
        catalog: *catalog_mod.CatalogService,
        manifests: *manifest_mod.ManifestStore,
        progress: *catalog_mod.ProgressStore,
        query: *query_mod.QueryRuntime,
        runtime_status: *const api_types.RuntimeStatusResult,
    ) HttpHandler {
        return .{
            .alloc = alloc,
            .api = api,
            .catalog = catalog,
            .manifests = manifests,
            .progress = progress,
            .query = query,
            .runtime_status = runtime_status,
        };
    }

    pub fn handle(self: *HttpHandler, req: HttpRequest) !HttpResponse {
        const route = http_routes.match(req.method, req.path) orelse return try textResponse(self.alloc, 404, "not found");

        return switch (route) {
            .health => try self.handleHealth(),
            .healthz => try self.handleHealthz(),
            .readyz => try self.handleReadyz(),
            .metrics => try self.handleMetrics(),
            .status => try self.handleStatus(),
            .list_namespaces => try self.handleListNamespaces(),
            .list_tables => try self.handleListTables(),
            .ensure_namespace => |value| try self.handleEnsureNamespace(value.namespace, req.body),
            .ensure_table => |value| try self.handleEnsureTable(value.table_name, req.body),
            .table_indexes => |value| try self.handlePublicTableListIndexes(value.table_name),
            .table_index => |value| switch (req.method) {
                .get => try self.handlePublicTableGetIndex(value.table_name, value.index_name),
                .post => try self.handlePublicTableCreateIndex(value.table_name, value.index_name, req.body),
                .delete => try self.handlePublicTableDeleteIndex(value.table_name, value.index_name),
                else => unreachable,
            },
            .ingest_batch => |value| try self.handleIngestBatch(value.namespace, req.body),
            .ingest_table_batch => |value| try self.handleIngestTableBatch(value.table_name, req.body),
            .table_batch => |value| try self.handleTableBatch(value.table_name, req.body),
            .build_namespace => |value| try self.handleBuildNamespace(value.namespace),
            .internal_table_build => |value| try self.handleBuildTable(value.table_name),
            .build_status => |value| try self.handleBuildStatus(value.namespace),
            .internal_table_build_status => |value| try self.handleTableBuildStatus(value.table_name),
            .policy => |value| switch (req.method) {
                .get => try self.handleGetPolicy(value.namespace),
                .put => try self.handleSetPolicy(value.namespace, req.body),
                else => unreachable,
            },
            .internal_table_policy => |value| switch (req.method) {
                .get => try self.handleGetTablePolicy(value.table_name),
                .put => try self.handleSetTablePolicy(value.table_name, req.body),
                else => unreachable,
            },
            .head => |value| try self.handleHead(value.namespace),
            .publish_head => |value| try self.handlePublishHead(value.namespace, req.body),
            .query => |value| try self.handleQuery(value.namespace),
            .table_query => |value| try self.handleTableQuery(value.table_name),
            .table_query_request => |value| try self.handleTableQueryRequest(value.table_name, req.body),
            .table_query_published => |value| try self.handleTableQueryPublished(value.table_name),
            .table_query_latest => |value| try self.handleTableQueryLatest(value.table_name),
            .query_search => |value| try self.handleQuerySearch(value.namespace, req.body),
            .table_query_search => |value| try self.handleTableQuerySearch(value.table_name, req.body),
            .table_query_graph_neighbors => |value| try self.handleTableQueryGraphNeighbors(value.table_name, req.body),
            .table_query_graph_traverse => |value| try self.handleTableQueryGraphTraverse(value.table_name, req.body),
            .table_query_graph_shortest_path => |value| try self.handleTableQueryGraphShortestPath(value.table_name, req.body),
            .query_graph_neighbors => |value| try self.handleQueryGraphNeighbors(value.namespace, req.body),
            .query_graph_traverse => |value| try self.handleQueryGraphTraverse(value.namespace, req.body),
            .query_graph_shortest_path => |value| try self.handleQueryGraphShortestPath(value.namespace, req.body),
            .query_head => |value| try self.handleQueryHead(value.namespace),
            .query_latest => |value| try self.handleQueryLatest(value.namespace),
            .query_version => |value| try self.handleQueryVersion(value.namespace, value.version),
            .query_version_graph_neighbors => |value| try self.handleQueryVersionGraphNeighbors(value.namespace, value.version, req.body),
            .query_version_graph_traverse => |value| try self.handleQueryVersionGraphTraverse(value.namespace, value.version, req.body),
            .query_version_graph_shortest_path => |value| try self.handleQueryVersionGraphShortestPath(value.namespace, value.version, req.body),
            .query_head_artifact => |value| try self.handleQueryHeadArtifact(value.namespace, value.artifact_index),
            .query_version_artifact => |value| try self.handleQueryVersionArtifact(value.namespace, value.version.?, value.artifact_index),
        };
    }

    pub fn setRuntimeMetrics(self: *HttpHandler, runtime: *runtime_manager.ManagedRuntime) void {
        self.runtime_metrics = runtime;
    }

    pub fn setQueryCache(self: *HttpHandler, query_cache: *query_mod.QueryCache) void {
        self.query_cache = query_cache;
    }

    pub fn setManagedDenseQueryEmbedder(
        self: *HttpHandler,
        embedder: *managed_embedder.ManagedEmbedder,
        index_name: []const u8,
    ) void {
        self.managed_query_embedder = embedder;
        self.published_search_sources = search_sources.withDenseQueryIndexName(self.published_search_sources, index_name);
    }

    pub fn setPublishedSearchSources(self: *HttpHandler, sources: search_sources.PublishedSearchSources) void {
        self.published_search_sources = sources;
    }

    pub fn setForeignRegistry(self: *HttpHandler, registry: *const foreign_mod.Registry) void {
        self.foreign_registry = registry;
    }

    fn requireMutableRoute(self: *HttpHandler) !?HttpResponse {
        if (self.runtime_status.role == .query_only) {
            return try textResponse(self.alloc, 503, "maintenance routes unavailable on query-only runtime");
        }
        return null;
    }

    fn requirePublishRoute(self: *HttpHandler) !?HttpResponse {
        if (try self.requireMutableRoute()) |resp| return resp;
        if (!self.runtime_status.publish_enabled) {
            return try textResponse(self.alloc, 503, "publish routes disabled");
        }
        return null;
    }

    fn handleHealth(self: *HttpHandler) !HttpResponse {
        const namespaces = try self.catalog.listNamespacesAlloc(self.alloc);
        defer self.catalog.freeNamespaces(self.alloc, namespaces);
        return try jsonResponse(self.alloc, 200, api_types.HealthResult{
            .live = true,
            .ready = self.runtime_status.validated,
            .validated = self.runtime_status.validated,
            .namespace_count = namespaces.len,
        });
    }

    fn handleHealthz(self: *HttpHandler) !HttpResponse {
        return try jsonResponse(self.alloc, 200, .{ .status = "ok" });
    }

    fn handleReadyz(self: *HttpHandler) !HttpResponse {
        const ready = self.runtime_status.validated;
        return try jsonResponse(self.alloc, if (ready) 200 else 503, .{
            .status = if (ready) "ready" else "not_ready",
            .validated = ready,
        });
    }

    fn handleStatus(self: *HttpHandler) !HttpResponse {
        return try jsonResponse(self.alloc, 200, self.runtime_status.*);
    }

    fn handleMetrics(self: *HttpHandler) !HttpResponse {
        const namespaces = try self.catalog.listNamespacesAlloc(self.alloc);
        defer self.catalog.freeNamespaces(self.alloc, namespaces);

        var namespace_metrics = try self.alloc.alloc(api_types.MetricsNamespace, namespaces.len);
        errdefer self.alloc.free(namespace_metrics);

        var initialized: usize = 0;
        errdefer {
            for (namespace_metrics[0..initialized]) |*ns| ns.deinit(self.alloc);
            self.alloc.free(namespace_metrics);
        }

        var total_pending_records: u64 = 0;
        var total_retained_versions: usize = 0;
        var total_retained_artifacts: usize = 0;
        var publish_recommended_namespaces: usize = 0;
        var namespaces_with_head_republish_pending: usize = 0;
        var namespaces_with_wal_artifact_publish_pending: usize = 0;
        var namespaces_with_wal_enrichment_publish_pending: usize = 0;
        var namespaces_with_enrichment_in_progress: usize = 0;
        var namespaces_with_enrichment_complete: usize = 0;
        var namespaces_with_enrichment_executing: usize = 0;
        var namespaces_with_enrichment_awaiting_execution: usize = 0;
        var namespaces_with_enrichment_deferred_for_publish_threshold: usize = 0;
        var namespaces_with_enrichment_ready_for_publish: usize = 0;
        var namespaces_with_full_text_materialization_pending: usize = 0;
        var namespaces_with_dense_vector_materialization_pending: usize = 0;
        var namespaces_with_sparse_vector_materialization_pending: usize = 0;
        var namespaces_with_chunk_preview_materialization_pending: usize = 0;
        var namespaces_with_chunk_embeddings_materialization_pending: usize = 0;
        var namespaces_with_rerank_terms_materialization_pending: usize = 0;
        var total_enrichment_pending_documents: u64 = 0;
        var lexical_sparse_incomplete_namespaces: usize = 0;
        var chunk_preview_incomplete_namespaces: usize = 0;
        var chunk_embeddings_incomplete_namespaces: usize = 0;
        var rerank_terms_incomplete_namespaces: usize = 0;
        const namespace_query_metrics = try self.query.namespaceMetricsAlloc(self.alloc);
        defer {
            for (namespace_query_metrics) |*metric| metric.deinit(self.alloc);
            self.alloc.free(namespace_query_metrics);
        }

        for (namespaces, 0..) |namespace, idx| {
            var status = try self.catalog.buildStatus(namespace.name);
            defer status.deinit(self.alloc);
            const ns_query_metrics = findNamespaceQueryMetrics(namespace_query_metrics, namespace.name);
            const ns_query_count_f32: f32 = if (ns_query_metrics) |metric|
                (if (metric.metrics.total_queries == 0) 0 else @floatFromInt(metric.metrics.total_queries))
            else
                0;

            namespace_metrics[idx] = .{
                .namespace = try self.alloc.dupe(u8, namespace.name),
                .head_version = status.head_version,
                .latest_wal_lsn = status.latest_wal_lsn,
                .freshness_lag_records = status.freshness_lag_records,
                .pending_records = status.pending_records,
                .retained_versions = status.retained_versions,
                .retained_artifacts = status.retained_artifacts,
                .publish_admitted = status.publish_admitted,
                .publish_recommended = status.publish_recommended,
                .next_publish_reason = status.next_publish_reason,
                .mutation_tail_resolution = status.mutation_tail_resolution,
                .head_document_publish_mode = status.head_document_publish_mode,
                .next_document_publish_mode = status.next_document_publish_mode,
                .document_base_version = status.document_base_version,
                .document_lineage_versions = status.document_lineage_versions,
                .pending_materialization_families = status.pending_materialization_families,
                .derived_output_resolutions = status.derived_output_resolutions,
                .compaction_recommended = status.compaction_recommended,
                .mutation_tail_compaction_recommended = status.mutation_tail_compaction_recommended,
                .vector_compaction_recommended = status.vector_compaction_recommended,
                .vector_compaction_driver_index_name = if (status.vector_compaction_driver_index_name) |value| try self.alloc.dupe(u8, value) else null,
                .vector_compaction_distance_metric = status.vector_compaction_distance_metric,
                .vector_cluster_count = status.vector_cluster_count,
                .vector_target_cluster_count = status.vector_target_cluster_count,
                .vector_base_probe_count = status.vector_base_probe_count,
                .vector_target_base_probe_count = status.vector_target_base_probe_count,
                .vector_shortlist_multiplier = status.vector_shortlist_multiplier,
                .vector_target_shortlist_multiplier = status.vector_target_shortlist_multiplier,
                .vector_cluster_imbalance = status.vector_cluster_imbalance,
                .vector_cluster_distance_span_max = status.vector_cluster_distance_span_max,
                .enrichment_enabled = status.enrichment_enabled,
                .lexical_sparse_model_preference = status.lexical_sparse_model_preference,
                .lexical_sparse_complete = status.lexical_sparse_complete,
                .chunk_preview_enabled = status.chunk_preview_enabled,
                .chunk_preview_complete = status.chunk_preview_complete,
                .chunk_embeddings_enabled = status.chunk_embeddings_enabled,
                .chunk_embeddings_model_preference = status.chunk_embeddings_model_preference,
                .chunk_embeddings_complete = status.chunk_embeddings_complete,
                .rerank_terms_enabled = status.rerank_terms_enabled,
                .rerank_terms_complete = status.rerank_terms_complete,
                .enrichment_failure_policy = status.enrichment_failure_policy,
                .enrichment_active_stage = status.enrichment_active_stage,
                .enrichment_stage_source = status.enrichment_stage_source,
                .enrichment_stage_state = status.enrichment_stage_state,
                .enrichment_in_progress = status.enrichment_in_progress,
                .enrichment_complete = status.enrichment_complete,
                .enrichment_pending_documents = status.enrichment_pending_document_count,
                .ann_total_queries = if (ns_query_metrics) |metric| metric.metrics.total_queries else 0,
                .ann_vector_queries = if (ns_query_metrics) |metric| metric.metrics.vector_queries else 0,
                .ann_hybrid_queries = if (ns_query_metrics) |metric| metric.metrics.hybrid_queries else 0,
                .ann_sparse_queries = if (ns_query_metrics) |metric| metric.metrics.sparse_queries else 0,
                .ann_avg_actual_probes = if (ns_query_count_f32 == 0) 0 else @as(f32, @floatFromInt(ns_query_metrics.?.metrics.total_actual_probes)) / ns_query_count_f32,
                .ann_avg_shortlist_candidates = if (ns_query_count_f32 == 0) 0 else @as(f32, @floatFromInt(ns_query_metrics.?.metrics.total_shortlist_candidates)) / ns_query_count_f32,
                .ann_avg_exact_reranks = if (ns_query_count_f32 == 0) 0 else @as(f32, @floatFromInt(ns_query_metrics.?.metrics.total_exact_reranks)) / ns_query_count_f32,
                .ann_avg_cluster_prunes = if (ns_query_count_f32 == 0) 0 else @as(f32, @floatFromInt(ns_query_metrics.?.metrics.total_cluster_prunes)) / ns_query_count_f32,
            };
            initialized += 1;
            total_pending_records += status.pending_records;
            total_retained_versions += status.retained_versions;
            total_retained_artifacts += status.retained_artifacts;
            if (status.publish_recommended) publish_recommended_namespaces += 1;
            if (status.next_publish_reason) |reason| switch (reason) {
                .head_republish => namespaces_with_head_republish_pending += 1,
                .wal_artifact_update => namespaces_with_wal_artifact_publish_pending += 1,
                .wal_enrichment => namespaces_with_wal_enrichment_publish_pending += 1,
            };
            if (status.pending_materialization_families.full_text) namespaces_with_full_text_materialization_pending += 1;
            if (status.pending_materialization_families.dense_vector) namespaces_with_dense_vector_materialization_pending += 1;
            if (status.pending_materialization_families.sparse_vector) namespaces_with_sparse_vector_materialization_pending += 1;
            if (status.pending_materialization_families.chunk_preview) namespaces_with_chunk_preview_materialization_pending += 1;
            if (status.pending_materialization_families.chunk_embeddings) namespaces_with_chunk_embeddings_materialization_pending += 1;
            if (status.pending_materialization_families.rerank_terms) namespaces_with_rerank_terms_materialization_pending += 1;
            if (status.enrichment_in_progress) namespaces_with_enrichment_in_progress += 1;
            if (status.enrichment_complete) namespaces_with_enrichment_complete += 1;
            if (status.enrichment_stage_state) |stage_state| switch (stage_state) {
                .executing => namespaces_with_enrichment_executing += 1,
                .awaiting_execution => namespaces_with_enrichment_awaiting_execution += 1,
                .deferred_for_publish_threshold => namespaces_with_enrichment_deferred_for_publish_threshold += 1,
                .ready_for_publish => namespaces_with_enrichment_ready_for_publish += 1,
            };
            if (!status.lexical_sparse_complete) lexical_sparse_incomplete_namespaces += 1;
            if (status.chunk_preview_enabled and !status.chunk_preview_complete) chunk_preview_incomplete_namespaces += 1;
            if (status.chunk_embeddings_enabled and !status.chunk_embeddings_complete) chunk_embeddings_incomplete_namespaces += 1;
            if (status.rerank_terms_enabled and !status.rerank_terms_complete) rerank_terms_incomplete_namespaces += 1;
            total_enrichment_pending_documents += status.enrichment_pending_document_count;
        }

        const runtime_stats = if (self.runtime_metrics) |runtime|
            runtime.metricsSnapshot()
        else
            runtime_manager.RuntimeRunStats{};
        const cache_stats = if (self.query_cache) |query_cache|
            query_cache.statsSnapshot()
        else
            query_mod.QueryCacheStats{};
        const query_metrics = self.query.metricsSnapshot();
        const query_count_f32: f32 = if (query_metrics.total_queries == 0) 0 else @floatFromInt(query_metrics.total_queries);

        var result = api_types.MetricsResult{
            .live = true,
            .ready = self.runtime_status.validated,
            .validated = self.runtime_status.validated,
            .namespace_count = namespaces.len,
            .total_pending_records = total_pending_records,
            .total_retained_versions = total_retained_versions,
            .total_retained_artifacts = total_retained_artifacts,
            .publish_recommended_namespaces = publish_recommended_namespaces,
            .namespaces_with_head_republish_pending = namespaces_with_head_republish_pending,
            .namespaces_with_wal_artifact_publish_pending = namespaces_with_wal_artifact_publish_pending,
            .namespaces_with_wal_enrichment_publish_pending = namespaces_with_wal_enrichment_publish_pending,
            .namespaces_with_enrichment_in_progress = namespaces_with_enrichment_in_progress,
            .namespaces_with_enrichment_complete = namespaces_with_enrichment_complete,
            .namespaces_with_enrichment_executing = namespaces_with_enrichment_executing,
            .namespaces_with_enrichment_awaiting_execution = namespaces_with_enrichment_awaiting_execution,
            .namespaces_with_enrichment_deferred_for_publish_threshold = namespaces_with_enrichment_deferred_for_publish_threshold,
            .namespaces_with_enrichment_ready_for_publish = namespaces_with_enrichment_ready_for_publish,
            .namespaces_with_full_text_materialization_pending = namespaces_with_full_text_materialization_pending,
            .namespaces_with_dense_vector_materialization_pending = namespaces_with_dense_vector_materialization_pending,
            .namespaces_with_sparse_vector_materialization_pending = namespaces_with_sparse_vector_materialization_pending,
            .namespaces_with_chunk_preview_materialization_pending = namespaces_with_chunk_preview_materialization_pending,
            .namespaces_with_chunk_embeddings_materialization_pending = namespaces_with_chunk_embeddings_materialization_pending,
            .namespaces_with_rerank_terms_materialization_pending = namespaces_with_rerank_terms_materialization_pending,
            .total_enrichment_pending_documents = total_enrichment_pending_documents,
            .lexical_sparse_incomplete_namespaces = lexical_sparse_incomplete_namespaces,
            .chunk_preview_incomplete_namespaces = chunk_preview_incomplete_namespaces,
            .chunk_embeddings_incomplete_namespaces = chunk_embeddings_incomplete_namespaces,
            .rerank_terms_incomplete_namespaces = rerank_terms_incomplete_namespaces,
            .published_namespaces = runtime_stats.published_namespaces,
            .publish_head_conflicts = runtime_stats.publish_head_conflicts,
            .compacted_namespaces = runtime_stats.compacted_namespaces,
            .compact_head_conflicts = runtime_stats.compact_head_conflicts,
            .pruned_namespaces = runtime_stats.pruned_namespaces,
            .prune_gc_conflicts = runtime_stats.prune_gc_conflicts,
            .deleted_versions = runtime_stats.deleted_versions,
            .deleted_artifacts = runtime_stats.deleted_artifacts,
            .wal_records_removed = runtime_stats.wal_records_removed,
            .enriched_namespaces = runtime_stats.enriched_namespaces,
            .enriched_documents = runtime_stats.enriched_documents,
            .enrichment_wal_appends = runtime_stats.enrichment_wal_appends,
            .enrichment_model_documents = runtime_stats.enrichment_model_documents,
            .enrichment_fallback_documents = runtime_stats.enrichment_fallback_documents,
            .enrichment_failed_documents = runtime_stats.enrichment_failed_documents,
            .enrichment_stage_failures = runtime_stats.enrichment_stage_failures,
            .cache_hits = cache_stats.hits,
            .cache_misses = cache_stats.misses,
            .cache_writes = cache_stats.writes,
            .cache_full_hits = cache_stats.full_hits,
            .cache_full_misses = cache_stats.full_misses,
            .cache_full_writes = cache_stats.full_writes,
            .cache_range_hits = cache_stats.range_hits,
            .cache_range_misses = cache_stats.range_misses,
            .cache_range_writes = cache_stats.range_writes,
            .cache_block_hits = cache_stats.block_hits,
            .cache_block_misses = cache_stats.block_misses,
            .cache_block_writes = cache_stats.block_writes,
            .cache_routing_block_hits = cache_stats.routing_block_hits,
            .cache_routing_block_misses = cache_stats.routing_block_misses,
            .cache_routing_block_writes = cache_stats.routing_block_writes,
            .cache_payload_block_hits = cache_stats.payload_block_hits,
            .cache_payload_block_misses = cache_stats.payload_block_misses,
            .cache_payload_block_writes = cache_stats.payload_block_writes,
            .cache_approx_payload_block_hits = cache_stats.approx_payload_block_hits,
            .cache_approx_payload_block_misses = cache_stats.approx_payload_block_misses,
            .cache_approx_payload_block_writes = cache_stats.approx_payload_block_writes,
            .cache_exact_payload_block_hits = cache_stats.exact_payload_block_hits,
            .cache_exact_payload_block_misses = cache_stats.exact_payload_block_misses,
            .cache_exact_payload_block_writes = cache_stats.exact_payload_block_writes,
            .cache_evictions = cache_stats.evictions,
            .cache_current_bytes = cache_stats.current_bytes,
            .cache_pinned_bytes = cache_stats.pinned_bytes,
            .cache_payload_bytes = cache_stats.payload_bytes,
            .cache_pinned_block_count = cache_stats.pinned_block_count,
            .cache_payload_block_count = cache_stats.payload_block_count,
            .cache_max_bytes = cache_stats.max_bytes,
            .cache_max_payload_bytes = cache_stats.max_payload_bytes,
            .ann_total_queries = query_metrics.total_queries,
            .ann_vector_queries = query_metrics.vector_queries,
            .ann_hybrid_queries = query_metrics.hybrid_queries,
            .ann_sparse_queries = query_metrics.sparse_queries,
            .ann_total_actual_probes = query_metrics.total_actual_probes,
            .ann_total_shortlist_candidates = query_metrics.total_shortlist_candidates,
            .ann_total_quantized_candidates = query_metrics.total_quantized_candidates,
            .ann_total_exact_reranks = query_metrics.total_exact_reranks,
            .ann_total_cluster_prunes = query_metrics.total_cluster_prunes,
            .ann_avg_actual_probes = if (query_count_f32 == 0) 0 else @as(f32, @floatFromInt(query_metrics.total_actual_probes)) / query_count_f32,
            .ann_avg_shortlist_candidates = if (query_count_f32 == 0) 0 else @as(f32, @floatFromInt(query_metrics.total_shortlist_candidates)) / query_count_f32,
            .ann_avg_quantized_candidates = if (query_count_f32 == 0) 0 else @as(f32, @floatFromInt(query_metrics.total_quantized_candidates)) / query_count_f32,
            .ann_avg_exact_reranks = if (query_count_f32 == 0) 0 else @as(f32, @floatFromInt(query_metrics.total_exact_reranks)) / query_count_f32,
            .ann_avg_cluster_prunes = if (query_count_f32 == 0) 0 else @as(f32, @floatFromInt(query_metrics.total_cluster_prunes)) / query_count_f32,
            .namespaces = namespace_metrics,
        };
        defer result.deinit(self.alloc);
        return try jsonResponse(self.alloc, 200, result);
    }

    fn handleListNamespaces(self: *HttpHandler) !HttpResponse {
        const namespaces = try self.catalog.listNamespacesAlloc(self.alloc);
        defer self.catalog.freeNamespaces(self.alloc, namespaces);
        return try jsonResponse(self.alloc, 200, namespaces);
    }

    fn handleListTables(self: *HttpHandler) !HttpResponse {
        const tables = try self.catalog.listTablesAlloc(self.alloc);
        defer self.catalog.freeTables(self.alloc, tables);

        const result = try self.alloc.alloc(api_types.TableRecord, tables.len);
        var initialized: usize = 0;
        defer {
            for (result[0..initialized]) |*record| record.deinit(self.alloc);
            self.alloc.free(result);
        }

        for (tables, 0..) |table, idx| {
            result[idx] = .{
                .table_name = try self.alloc.dupe(u8, table.table_name),
                .created_at_ns = table.created_at_ns,
                .policy = table.policy,
                .schema_json = try self.alloc.dupe(u8, table.schema_json),
                .read_schema_json = try self.alloc.dupe(u8, table.read_schema_json),
                .indexes_json = try self.alloc.dupe(u8, table.indexes_json),
            };
            initialized += 1;
        }

        return try jsonResponse(self.alloc, 200, result);
    }

    fn handleEnsureNamespace(self: *HttpHandler, namespace: []const u8, body: []const u8) !HttpResponse {
        if (try self.requireMutableRoute()) |resp| return resp;
        const req = parseEnsureNamespaceRequest(self.alloc, body) catch return try textResponse(self.alloc, 400, "invalid namespace request");
        const policy = req.policy orelse catalog_mod.NamespacePolicy{};
        const created = try self.catalog.ensureNamespaceWithPolicy(namespace, req.created_at_ns, policy);

        var result = api_types.EnsureNamespaceResult{
            .namespace = try self.alloc.dupe(u8, namespace),
            .created = created,
            .created_at_ns = req.created_at_ns,
            .policy = if (created) policy else try self.catalog.getPolicy(namespace),
        };
        defer result.deinit(self.alloc);
        return try jsonResponse(self.alloc, if (created) 201 else 200, result);
    }

    fn handleEnsureTable(self: *HttpHandler, table_name: []const u8, body: []const u8) !HttpResponse {
        if (try self.requireMutableRoute()) |resp| return resp;
        var req = parseEnsureTableRequest(self.alloc, body) catch return try textResponse(self.alloc, 400, "invalid table request");
        defer req.deinit(self.alloc);
        const policy = req.policy orelse catalog_mod.NamespacePolicy{};
        const indexes_json = req.indexes_json orelse tables_api.default_indexes_json;
        tables_api.validatePublicAlgebraicIndexesJson(self.alloc, indexes_json) catch |err| switch (err) {
            error.InvalidCreateTableRequest => return try textResponse(self.alloc, 400, "unsupported table index configuration"),
            else => return err,
        };
        validateServerlessIndexCatalog(self.alloc, indexes_json) catch |err| switch (err) {
            error.UnsupportedCreateTableRequest, error.InvalidTableIndexMetadata => return try textResponse(self.alloc, 400, "unsupported table index configuration"),
            else => return err,
        };
        const created = try self.catalog.ensureTableWithDefinition(
            table_name,
            req.created_at_ns,
            policy,
            req.schema_json orelse "",
            req.read_schema_json orelse "",
            indexes_json,
        );

        if (!created) {
            const has_definition_update = req.schema_json != null or req.read_schema_json != null or req.indexes_json != null;
            if (has_definition_update) {
                var table = (try self.catalog.getTableAlloc(self.alloc, table_name)) orelse return try textResponse(self.alloc, 404, "not found");
                defer table.deinit(self.alloc);
                _ = try self.catalog.setTableDefinition(
                    table_name,
                    req.schema_json orelse table.schema_json,
                    req.read_schema_json orelse table.read_schema_json,
                    indexes_json,
                );
            }
            return try jsonResponse(self.alloc, 200, struct {}{});
        }

        var result = api_types.EnsureTableResult{
            .table_name = try self.alloc.dupe(u8, table_name),
            .created = true,
            .created_at_ns = req.created_at_ns,
            .policy = policy,
        };
        defer result.deinit(self.alloc);
        return try jsonResponse(self.alloc, 201, result);
    }

    fn handleListTableIndexes(self: *HttpHandler, table_name: []const u8) !HttpResponse {
        var table = (try self.catalog.getTableAlloc(self.alloc, table_name)) orelse return try textResponse(self.alloc, 404, "not found");
        defer table.deinit(self.alloc);
        var status = self.catalog.tableBuildStatus(table_name) catch |err| {
            std.log.err("table index list status failed table={s} err={}", .{ table_name, err });
            return error.InternalFailure;
        };
        defer status.deinit(self.alloc);
        const body = encodeServerlessIndexListAlloc(self.alloc, table.indexes_json, status) catch |err| {
            std.log.err("table index list encode failed table={s} err={}", .{ table_name, err });
            return error.InternalFailure;
        };
        defer self.alloc.free(body);
        return try jsonSliceResponse(self.alloc, 200, body);
    }

    fn handleGetTableIndex(self: *HttpHandler, table_name: []const u8, index_name: []const u8) !HttpResponse {
        var table = (try self.catalog.getTableAlloc(self.alloc, table_name)) orelse return try textResponse(self.alloc, 404, "not found");
        defer table.deinit(self.alloc);
        var status = self.catalog.tableBuildStatus(table_name) catch |err| {
            std.log.err("table index status failed table={s} index={s} err={}", .{ table_name, index_name, err });
            return error.InternalFailure;
        };
        defer status.deinit(self.alloc);
        const body = (encodeServerlessSingleIndexAlloc(self.alloc, table.indexes_json, index_name, status) catch |err| {
            std.log.err("table index encode failed table={s} index={s} err={}", .{ table_name, index_name, err });
            return error.InternalFailure;
        }) orelse {
            return try textResponse(self.alloc, 404, "not found");
        };
        defer self.alloc.free(body);
        return try jsonSliceResponse(self.alloc, 200, body);
    }

    fn handleCreateTableIndex(self: *HttpHandler, table_name: []const u8, index_name: []const u8, body: []const u8) !HttpResponse {
        if (try self.requireMutableRoute()) |resp| return resp;
        var table = (try self.catalog.getTableAlloc(self.alloc, table_name)) orelse return try textResponse(self.alloc, 404, "not found");
        defer table.deinit(self.alloc);

        const index_json = table_contract.parseCreateIndexRequest(self.alloc, index_name, body) catch {
            return try textResponse(self.alloc, 400, "invalid create index request");
        };
        defer self.alloc.free(index_json);
        tables_api.validatePublicAlgebraicIndexJson(self.alloc, index_json) catch |err| switch (err) {
            error.InvalidCreateTableRequest => return try textResponse(self.alloc, 400, "unsupported index configuration"),
            else => return err,
        };
        const expanded_index_json = tables_api.expandSchemaDerivedAlgebraicIndexAlloc(self.alloc, table_name, index_json, table.schema_json) catch |err| switch (err) {
            error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest => return try textResponse(self.alloc, 400, "unsupported index configuration"),
            else => return err,
        };
        defer self.alloc.free(expanded_index_json);

        table_writes.validateIndexConfig(self.alloc, index_name, expanded_index_json) catch |err| switch (err) {
            error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest => return try textResponse(self.alloc, 400, "unsupported index configuration"),
            else => return err,
        };

        const next_indexes_json = try indexes_api.addIndexToTableIndexesJson(self.alloc, table.indexes_json, index_name, expanded_index_json);
        defer self.alloc.free(next_indexes_json);
        validateServerlessIndexCatalog(self.alloc, next_indexes_json) catch |err| switch (err) {
            error.UnsupportedCreateTableRequest => return try textResponse(self.alloc, 400, "unsupported index configuration"),
            error.InvalidTableIndexMetadata => return try textResponse(self.alloc, 400, "invalid index configuration"),
            else => return err,
        };

        const updated = try self.catalog.setTableDefinition(
            table_name,
            table.schema_json,
            table.read_schema_json,
            next_indexes_json,
        );
        if (!updated) return try textResponse(self.alloc, 404, "not found");
        return try jsonResponse(self.alloc, 201, struct {}{});
    }

    fn handleDeleteTableIndex(self: *HttpHandler, table_name: []const u8, index_name: []const u8) !HttpResponse {
        if (try self.requireMutableRoute()) |resp| return resp;
        var table = (try self.catalog.getTableAlloc(self.alloc, table_name)) orelse return try textResponse(self.alloc, 404, "not found");
        defer table.deinit(self.alloc);

        const next_indexes_json = (try indexes_api.removeIndexFromTableIndexesJson(self.alloc, table.indexes_json, index_name)) orelse {
            return try textResponse(self.alloc, 404, "not found");
        };
        defer self.alloc.free(next_indexes_json);
        validateServerlessIndexCatalog(self.alloc, next_indexes_json) catch |err| switch (err) {
            error.UnsupportedCreateTableRequest => return try textResponse(self.alloc, 400, "unsupported index configuration"),
            error.InvalidTableIndexMetadata => return try textResponse(self.alloc, 400, "invalid index configuration"),
            else => return err,
        };

        const updated = try self.catalog.setTableDefinition(
            table_name,
            table.schema_json,
            table.read_schema_json,
            next_indexes_json,
        );
        if (!updated) return try textResponse(self.alloc, 404, "not found");
        return try jsonResponse(self.alloc, 201, struct {}{});
    }

    fn handleIngestBatch(self: *HttpHandler, namespace: []const u8, body: []const u8) !HttpResponse {
        if (try self.requireMutableRoute()) |resp| return resp;
        var status = self.catalog.buildStatus(namespace) catch return try textResponse(self.alloc, 500, "status failed");
        defer status.deinit(self.alloc);
        if (!status.publish_admitted) {
            return try textResponse(self.alloc, 429, "namespace backpressured");
        }

        const req = parseIngestBatchRequest(self.alloc, namespace, body) catch return try textResponse(self.alloc, 400, "invalid ingest request");
        defer freeDocumentMutations(self.alloc, req.mutations);

        var result = self.api.ingestBatch(req) catch return try textResponse(self.alloc, 500, "ingest failed");
        defer result.deinit(self.alloc);
        return try jsonResponse(self.alloc, 202, result);
    }

    fn handleIngestTableBatch(self: *HttpHandler, table_name: []const u8, body: []const u8) !HttpResponse {
        if (try self.requireMutableRoute()) |resp| return resp;
        var status = self.catalog.tableBuildStatus(table_name) catch return try textResponse(self.alloc, 500, "status failed");
        defer status.deinit(self.alloc);
        if (!status.publish_admitted) {
            return try textResponse(self.alloc, 429, "table backpressured");
        }

        const req = parseTableIngestBatchRequest(self.alloc, table_name, body) catch return try textResponse(self.alloc, 400, "invalid ingest request");
        defer freeDocumentMutations(self.alloc, req.mutations);
        const namespace = self.catalog.resolveTableNamespaceAlloc(table_name) catch return try textResponse(self.alloc, 404, "not found");
        defer self.alloc.free(namespace);

        var result = self.api.ingestBatch(.{
            .namespace = namespace,
            .timestamp_ns = req.timestamp_ns,
            .mutations = req.mutations,
        }) catch return try textResponse(self.alloc, 500, "ingest failed");
        defer result.deinit(self.alloc);

        var table_result = api_types.TableIngestBatchResult{
            .table_name = try self.alloc.dupe(u8, table_name),
            .mutation_count = result.mutation_count,
            .start_lsn = result.start_lsn,
            .end_lsn = result.end_lsn,
        };
        defer table_result.deinit(self.alloc);
        return try jsonResponse(self.alloc, 202, table_result);
    }

    fn handleTableBatch(self: *HttpHandler, table_name: []const u8, body: []const u8) !HttpResponse {
        var resp = try public_table_http.handleTableBatch(self.alloc, table_name, body, self.tableApi());
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            201 => blk: {
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const parsed = try parseJsonResponseBody(metadata_openapi.BatchResponse, arena_impl.allocator(), resp.body);
                break :blk try jsonResponse(self.alloc, 201, parsed);
            },
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    fn handlePublicTableListIndexes(self: *HttpHandler, table_name: []const u8) !HttpResponse {
        var resp = try public_table_http.handleTableListIndexes(self.alloc, table_name, self.tableApi());
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            200 => try jsonSliceResponse(self.alloc, 200, resp.body),
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    fn handlePublicTableGetIndex(self: *HttpHandler, table_name: []const u8, index_name: []const u8) !HttpResponse {
        var resp = try public_table_http.handleTableGetIndex(self.alloc, table_name, index_name, self.tableApi());
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            200 => try jsonSliceResponse(self.alloc, 200, resp.body),
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    fn handlePublicTableCreateIndex(self: *HttpHandler, table_name: []const u8, index_name: []const u8, body: []const u8) !HttpResponse {
        var resp = try public_table_http.handleTableCreateIndex(self.alloc, table_name, index_name, body, self.tableApi());
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            201 => try typedJsonResponse(struct {}, self.alloc, 201, resp.body),
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    fn handlePublicTableDeleteIndex(self: *HttpHandler, table_name: []const u8, index_name: []const u8) !HttpResponse {
        var resp = try public_table_http.handleTableDeleteIndex(self.alloc, table_name, index_name, self.tableApi());
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            201 => try typedJsonResponse(struct {}, self.alloc, 201, resp.body),
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    fn handleBuildNamespace(self: *HttpHandler, namespace: []const u8) !HttpResponse {
        if (try self.requirePublishRoute()) |resp| return resp;
        var result = self.catalog.buildNamespace(namespace) catch |err| switch (err) {
            error.HeadChanged => return try textResponse(self.alloc, 409, "head changed"),
            else => return try textResponse(self.alloc, 500, "build failed"),
        };
        defer result.deinit(self.alloc);
        return try jsonResponse(self.alloc, 202, result);
    }

    fn handleBuildTable(self: *HttpHandler, table_name: []const u8) !HttpResponse {
        if (try self.requirePublishRoute()) |resp| return resp;
        var result = self.catalog.buildTable(table_name) catch |err| switch (err) {
            error.NamespaceNotFound => return try textResponse(self.alloc, 404, "not found"),
            error.HeadChanged => return try textResponse(self.alloc, 409, "head changed"),
            else => return try textResponse(self.alloc, 500, "build failed"),
        };
        defer result.deinit(self.alloc);
        var table_result = api_types.TableBuildResult{
            .table_name = try self.alloc.dupe(u8, table_name),
            .published = result.published,
            .version = result.version,
            .wal_start_lsn = result.wal_start_lsn,
            .wal_end_lsn = result.wal_end_lsn,
            .artifact_count = result.artifact_count,
        };
        defer table_result.deinit(self.alloc);
        return try jsonResponse(self.alloc, 202, table_result);
    }

    fn handleBuildStatus(self: *HttpHandler, namespace: []const u8) !HttpResponse {
        var status = self.catalog.buildStatus(namespace) catch return try textResponse(self.alloc, 500, "status failed");
        defer status.deinit(self.alloc);
        return try jsonResponse(self.alloc, 200, status);
    }

    fn handleTableBuildStatus(self: *HttpHandler, table_name: []const u8) !HttpResponse {
        var status = self.catalog.tableBuildStatus(table_name) catch |err| switch (err) {
            error.NamespaceNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "status failed"),
        };
        defer status.deinit(self.alloc);
        var table_status = api_types.TableBuildStatus.fromNamespaceBuildStatus(self.alloc, table_name, status) catch {
            return try textResponse(self.alloc, 500, "status failed");
        };
        defer table_status.deinit(self.alloc);
        return try jsonResponse(self.alloc, 200, table_status);
    }

    fn handleGetPolicy(self: *HttpHandler, namespace: []const u8) !HttpResponse {
        const policy = self.catalog.getPolicy(namespace) catch return try textResponse(self.alloc, 404, "not found");
        var result = api_types.NamespacePolicyResult{
            .namespace = try self.alloc.dupe(u8, namespace),
            .policy = policy,
        };
        defer result.deinit(self.alloc);
        return try jsonResponse(self.alloc, 200, result);
    }

    fn handleGetTablePolicy(self: *HttpHandler, table_name: []const u8) !HttpResponse {
        const policy = self.catalog.getTablePolicy(table_name) catch |err| switch (err) {
            error.NamespaceNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "policy failed"),
        };
        var result = api_types.TablePolicyResult{
            .table_name = try self.alloc.dupe(u8, table_name),
            .policy = policy,
        };
        defer result.deinit(self.alloc);
        return try jsonResponse(self.alloc, 200, result);
    }

    fn handleSetPolicy(self: *HttpHandler, namespace: []const u8, body: []const u8) !HttpResponse {
        if (try self.requireMutableRoute()) |resp| return resp;
        const req = parseNamespacePolicyRequest(self.alloc, body) catch return try textResponse(self.alloc, 400, "invalid policy request");
        const policy = self.catalog.setPolicy(namespace, req) catch return try textResponse(self.alloc, 404, "not found");
        var result = api_types.NamespacePolicyResult{
            .namespace = try self.alloc.dupe(u8, namespace),
            .policy = policy,
        };
        defer result.deinit(self.alloc);
        return try jsonResponse(self.alloc, 200, result);
    }

    fn handleSetTablePolicy(self: *HttpHandler, table_name: []const u8, body: []const u8) !HttpResponse {
        if (try self.requireMutableRoute()) |resp| return resp;
        const req = parseNamespacePolicyRequest(self.alloc, body) catch return try textResponse(self.alloc, 400, "invalid policy request");
        const policy = self.catalog.setTablePolicy(table_name, req) catch |err| switch (err) {
            error.NamespaceNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "policy failed"),
        };
        var result = api_types.TablePolicyResult{
            .table_name = try self.alloc.dupe(u8, table_name),
            .policy = policy,
        };
        defer result.deinit(self.alloc);
        return try jsonResponse(self.alloc, 200, result);
    }

    fn handleHead(self: *HttpHandler, namespace: []const u8) !HttpResponse {
        const version = self.progress.getHead(namespace) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "head failed"),
        };

        var manifest = self.manifests.getAlloc(namespace, version) catch return try textResponse(self.alloc, 500, "head failed");
        defer manifest.deinit(self.alloc);
        return try jsonResponse(self.alloc, 200, manifest);
    }

    fn handlePublishHead(self: *HttpHandler, namespace: []const u8, body: []const u8) !HttpResponse {
        if (try self.requirePublishRoute()) |resp| return resp;
        const req = parseHeadPublishRequest(self.alloc, body) catch return try textResponse(self.alloc, 400, "invalid head publish request");
        var manifest = self.manifests.getAlloc(namespace, req.version) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "manifest version not found"),
            else => return try textResponse(self.alloc, 500, "head publish failed"),
        };
        defer manifest.deinit(self.alloc);
        const published = self.progress.compareAndSwapHead(namespace, req.expected_head, req.version) catch return try textResponse(self.alloc, 500, "head publish failed");
        const current_head = self.progress.getHead(namespace) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return try textResponse(self.alloc, 500, "head publish failed"),
        };

        var result = api_types.HeadPublishResult{
            .namespace = try self.alloc.dupe(u8, namespace),
            .requested_version = req.version,
            .expected_head = req.expected_head,
            .current_head = current_head,
            .published = published,
        };
        defer result.deinit(self.alloc);
        return try jsonResponse(self.alloc, if (published) 200 else 409, result);
    }

    fn handleQuery(self: *HttpHandler, namespace: []const u8) !HttpResponse {
        const policy = self.catalog.getPolicy(namespace) catch return try textResponse(self.alloc, 404, "not found");
        return switch (policy.default_query_view) {
            .published => try self.handleQueryHead(namespace),
            .latest => try self.handleQueryLatest(namespace),
        };
    }

    fn handleTableQuery(self: *HttpHandler, table_name: []const u8) !HttpResponse {
        return try self.handlePublicTableQueryView(table_name, .default_view);
    }

    fn handleTableQueryPublished(self: *HttpHandler, table_name: []const u8) !HttpResponse {
        return try self.handlePublicTableQueryView(table_name, .published);
    }

    fn handleTableQueryLatest(self: *HttpHandler, table_name: []const u8) !HttpResponse {
        return try self.handlePublicTableQueryView(table_name, .latest);
    }

    fn handlePublicTableQueryView(
        self: *HttpHandler,
        table_name: []const u8,
        view: public_table_http.TableApi.TableQueryView,
    ) !HttpResponse {
        var resp = try public_table_http.handleTableQueryView(
            self.alloc,
            table_name,
            view,
            self.tableApi(),
        );
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            200 => try typedJsonResponse(query_types.TableQueryResult, self.alloc, 200, resp.body),
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    fn executePublishedSearch(self: *HttpHandler, namespace: []const u8, table_name: ?[]const u8, body: []const u8) !SearchExecution {
        var status = try self.catalog.buildStatus(namespace);
        errdefer status.deinit(self.alloc);
        var plan = try query_mod.parseSearchPlanAlloc(self.alloc, body, status.published_search_sources);
        errdefer plan.deinit(self.alloc);
        const requested_offset = plan.request.offset;
        const requested_limit = plan.request.limit;
        if (plan.request.count_only) {
            plan.request.offset = 0;
            plan.request.limit = std.math.maxInt(usize);
        }
        try self.resolveSemanticQueryRequest(table_name, &plan);

        var profile_requested = false;
        var public_request = std.json.parseFromSlice(metadata_openapi.QueryRequest, self.alloc, body, .{
            .ignore_unknown_fields = true,
            .allocate = .alloc_always,
        }) catch |err| switch (err) {
            error.UnexpectedToken,
            error.UnknownField,
            error.InvalidEnumTag,
            => return error.InvalidQueryRequest,
            else => return err,
        };
        defer public_request.deinit();
        profile_requested = public_request.value.profile orelse false;
        var session = try self.query.openHeadSession(namespace);
        errdefer session.deinit();
        try query_mod.warmIndexedSearchPlanPath(&session, plan);

        var execution_stats = query_mod.QuerySearchExecutionStats{};
        const hits = try query_mod.searchIndexedPlanWithStatsAlloc(self.alloc, &session, plan, &execution_stats);
        errdefer query_mod.freeSearchHits(self.alloc, hits);
        try self.query.recordSearchStats(session.namespace(), plan.request.mode, execution_stats);

        return .{
            .plan = plan,
            .status = status,
            .session = session,
            .hits = hits,
            .execution_stats = execution_stats,
            .requested_offset = requested_offset,
            .requested_limit = requested_limit,
            .profile_requested = profile_requested,
        };
    }

    fn executePublicTableQueryJsonAlloc(self: *HttpHandler, table_name: []const u8, body: []const u8) anyerror![]u8 {
        if (self.executeForeignPublicTableQueryJsonAlloc(table_name, body) catch |err| switch (err) {
            error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidQueryRequest,
            else => return err,
        }) |json| {
            return json;
        }

        const join_req = parseSupportedJoinRequest(self.alloc, body) catch |err| switch (err) {
            error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidQueryRequest,
            else => return err,
        };
        if (join_req) |parsed_join| {
            defer {
                var owned = parsed_join;
                owned.deinit(self.alloc);
            }
            return try self.executeSupportedJoinedPublicTableQueryRequest(table_name, body, parsed_join.join, parsed_join.foreign_sources);
        }

        return try self.executePlainPublicTableQueryJsonAlloc(table_name, body);
    }

    fn executeForeignPublicTableQueryJsonAlloc(
        self: *HttpHandler,
        table_name: []const u8,
        body: []const u8,
    ) anyerror!?[]u8 {
        var parsed_request = std.json.parseFromSlice(metadata_openapi.QueryRequest, self.alloc, body, .{
            .allocate = .alloc_always,
        }) catch return error.InvalidQueryRequest;
        defer parsed_request.deinit();
        const request = parsed_request.value;

        var foreign_sources = foreign_sources_api.postgresSourceMapFromMetadataOpenApiResolved(self.alloc, request.foreign_sources) catch |err| switch (err) {
            error.UnsupportedSourceKind => return error.UnsupportedQueryRequest,
            else => return err,
        };
        defer foreign_sources.deinit(self.alloc);

        const foreign_source = foreign_sources.get(table_name) orelse return null;
        try validateSupportedForeignPublicQueryRequest(request);

        if (request.join != null) {
            const parsed_join = (try parseSupportedJoinRequest(self.alloc, body)) orelse return error.InvalidQueryRequest;
            defer {
                var owned = parsed_join;
                owned.deinit(self.alloc);
            }
            return try self.executeSupportedJoinedForeignPublicTableQueryJsonAlloc(
                table_name,
                body,
                foreign_source,
                parsed_join.join,
                parsed_join.foreign_sources,
            );
        }

        return try self.encodeForeignPublicTableQueryResponseJsonAlloc(table_name, request, foreign_source);
    }

    fn encodeForeignPublicTableQueryResponseJsonAlloc(
        self: *HttpHandler,
        table_name: []const u8,
        request: metadata_openapi.QueryRequest,
        foreign_source: foreign_mod.PostgresConfig,
    ) anyerror![]u8 {
        const registry = self.foreign_registry orelse return error.UnsupportedQueryRequest;
        const started_ns = platform_time.monotonicNs();
        const limit = try foreignQueryLimit(request.limit);
        const offset = try foreignQueryOffset(request.offset);
        const aggregations_json = if (request.aggregations) |aggregations|
            try std.json.Stringify.valueAlloc(self.alloc, aggregations, .{})
        else
            null;
        defer if (aggregations_json) |json| self.alloc.free(json);
        const aggregation_requests = if (aggregations_json) |json|
            try query_api.parseAggregationRequestsJson(self.alloc, json)
        else
            &.{};
        defer query_api.freeAggregationRequests(self.alloc, aggregation_requests);

        const raw_filter_query_json = if (request.filter_query) |query|
            try stringifyJsonValueAlloc(self.alloc, query)
        else
            null;
        defer if (raw_filter_query_json) |query| self.alloc.free(query);
        const filter_query_json = try foreign_sources_api.buildEffectiveFilterQueryJsonAlloc(
            self.alloc,
            foreign_source,
            raw_filter_query_json,
            request.filter_prefix,
        );
        defer if (filter_query_json) |query| self.alloc.free(query);

        const foreign_order_by = try cloneForeignSortFieldsAlloc(self.alloc, request.order_by);
        defer freeForeignSortFields(self.alloc, foreign_order_by);

        var params = try foreign_source.toQueryParams(self.alloc, .{
            .fields = if (request.fields) |fields| fields else &.{},
            .filter_query_json = filter_query_json,
            .limit = limit,
            .offset = offset,
            .order_by = foreign_order_by,
        });
        defer params.deinit(self.alloc);

        const source_config = try foreign_source.toSourceConfig(self.alloc);
        var foreign_query_source = try registry.create(self.alloc, source_config);
        defer foreign_query_source.deinit(self.alloc);

        var query_result = try foreign_query_source.query(self.alloc, params);
        defer query_result.deinit(self.alloc);
        const aggregation_results: []db_mod.aggregations.SearchAggregationResult = if (aggregation_requests.len > 0) blk: {
            var aggregate_params = try foreign_sources_api.buildPostgresAggregateParamsAlloc(
                self.alloc,
                foreign_source,
                aggregation_requests,
                filter_query_json,
            );
            defer aggregate_params.deinit(self.alloc);
            var aggregate_result = foreign_query_source.aggregate(self.alloc, aggregate_params) catch |err| switch (err) {
                error.UnsupportedAggregate => return error.UnsupportedQueryRequest,
                else => return err,
            };
            defer aggregate_result.deinit(self.alloc);
            break :blk try foreign_sources_api.foreignAggregateResultsToSearchResultsAlloc(self.alloc, aggregation_requests, aggregate_result);
        } else @constCast(@as([]const db_mod.aggregations.SearchAggregationResult, &.{}));

        const result_hits = if (request.count == true)
            try self.alloc.alloc(db_types.SearchHit, 0)
        else
            try buildForeignSearchHitsAlloc(self.alloc, foreign_source, query_result.rows);
        var result: db_types.SearchResult = .{
            .alloc = self.alloc,
            .hits = result_hits,
            .total_hits = @intCast(@min(query_result.total, std.math.maxInt(u32))),
        };
        defer result.deinit();

        var response_meta: query_api.QueryResponseMeta = .{
            .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - started_ns, std.time.ns_per_ms)),
            .aggregation_results = aggregation_results,
        };
        defer response_meta.deinit(self.alloc);

        var response = try query_api.encodeQueryResponses(self.alloc, table_name, .{
            .count_only = request.count == true,
            .profile = request.profile == true,
            .limit = @intCast(limit orelse 10),
            .offset = @intCast(offset),
            .aggregations_json = aggregations_json orelse &.{},
        }, response_meta, result);
        defer response.deinit(self.alloc);
        return try self.alloc.dupe(u8, response.json);
    }

    fn executeSupportedJoinedForeignPublicTableQueryJsonAlloc(
        self: *HttpHandler,
        table_name: []const u8,
        body: []const u8,
        foreign_source: foreign_mod.PostgresConfig,
        join: SupportedJoinRequest,
        foreign_sources: foreign_mod.PostgresSourceMap,
    ) anyerror![]u8 {
        var contract_request = std.json.parseFromSlice(metadata_openapi.QueryRequest, self.alloc, body, .{
            .ignore_unknown_fields = true,
            .allocate = .alloc_always,
        }) catch return error.InvalidQueryRequest;
        defer contract_request.deinit();
        if (contract_request.value.count == true) return error.InvalidQueryRequest;

        const rewrite = distributed_join.rewriteJoinedBaseQueryBodyAlloc(self.alloc, contract_request.value, join.left_field) catch return error.InternalQueryFailure;
        const appended_left_field = rewrite.appended_left_field;
        const primary_body = rewrite.body;
        defer self.alloc.free(primary_body);

        var primary_request = std.json.parseFromSlice(metadata_openapi.QueryRequest, self.alloc, primary_body, .{
            .allocate = .alloc_always,
        }) catch return error.InvalidQueryRequest;
        defer primary_request.deinit();
        const primary_json = try self.encodeForeignPublicTableQueryResponseJsonAlloc(table_name, primary_request.value, foreign_source);
        errdefer self.alloc.free(primary_json);

        var owned_response = parseOwnedJsonValueAlloc(self.alloc, primary_json) catch return error.InternalQueryFailure;
        defer deinitJsonValue(self.alloc, &owned_response);
        const hits_ptr = queryHitsArrayPtr(&owned_response) catch return error.InvalidQueryRequest;
        if (hits_ptr.items.len == 0) return primary_json;

        const plan = planSupportedJoinExecution(self, self.alloc, join, hits_ptr.items, foreign_sources);
        var right_result = try self.executeSupportedRightJoinQuery(join, hits_ptr.items, plan, foreign_sources);
        defer right_result.deinit(self.alloc);

        var stats: JoinedQueryStats = .{
            .left_rows_scanned = @intCast(hits_ptr.items.len),
            .right_rows_scanned = @intCast(right_result.hits.len),
        };
        var matched_right_ids = std.StringHashMapUnmanaged(void){};
        defer matched_right_ids.deinit(self.alloc);

        var joined_hits = std.json.Array.init(self.alloc);
        defer {
            for (joined_hits.items) |*item| deinitJsonValue(self.alloc, item);
            joined_hits.deinit();
        }

        for (hits_ptr.items) |hit_value| {
            var joined_hit = cloneJsonValue(self.alloc, hit_value) catch return error.InternalQueryFailure;
            errdefer deinitJsonValue(self.alloc, &joined_hit);
            const source_value = joined_hit.object.getPtr("_source") orelse return error.InvalidQueryRequest;
            if (source_value.* != .object) return error.InvalidQueryRequest;
            if (appended_left_field) removeFieldFromSourceObject(self.alloc, source_value, join.left_field);

            const left_value = extractJoinValueFromHit(hit_value, join.left_field) orelse {
                stats.rows_unmatched_left += 1;
                if (join.join_type == .left) {
                    try joined_hits.append(joined_hit);
                } else {
                    deinitJsonValue(self.alloc, &joined_hit);
                }
                continue;
            };
            const matched_right = findFirstMatchingRightHit(join, left_value, right_result.hits) orelse {
                stats.rows_unmatched_left += 1;
                if (join.join_type == .left) {
                    try joined_hits.append(joined_hit);
                } else {
                    deinitJsonValue(self.alloc, &joined_hit);
                }
                continue;
            };

            try mergeRightHitIntoSource(self.alloc, source_value, join, matched_right);
            if (matched_right.object.get("_id")) |matched_id| {
                if (matched_id == .string) try matched_right_ids.put(self.alloc, matched_id.string, {});
            }
            try joined_hits.append(joined_hit);
            stats.rows_matched += 1;
        }

        if (join.join_type == .right) {
            const requested_left_fields = try allocRequestedFieldsFromRequest(self.alloc, contract_request.value);
            defer freeJsonStringArray(self.alloc, requested_left_fields);
            for (right_result.hits) |right_hit| {
                const right_id = switch (right_hit) {
                    .object => |obj| switch (obj.get("_id") orelse continue) {
                        .string => |text| text,
                        else => continue,
                    },
                    else => continue,
                };
                if (matched_right_ids.contains(right_id)) continue;
                try joined_hits.append(try buildUnmatchedRightJoinHit(self.alloc, right_hit, join, requested_left_fields, appended_left_field));
                stats.rows_unmatched_right += 1;
            }
        }

        var existing_hits_value: std.json.Value = .{ .array = hits_ptr.* };
        deinitJsonValue(self.alloc, &existing_hits_value);
        hits_ptr.* = std.json.Array.init(self.alloc);
        for (joined_hits.items) |item| {
            try hits_ptr.append(try cloneJsonValue(self.alloc, item));
        }
        try updateJoinedResponseMetadata(self.alloc, &owned_response, joined_hits.items.len, hits_ptr.items);
        try maybeAttachJoinProfile(self.alloc, &owned_response, stats, plan, right_result.strategy_used);

        self.alloc.free(primary_json);
        return stringifyJsonValueAlloc(self.alloc, owned_response) catch error.InternalQueryFailure;
    }

    fn validateSupportedForeignPublicQueryRequest(request: metadata_openapi.QueryRequest) !void {
        if (request.full_text_search != null) return error.UnsupportedQueryRequest;
        if (request.semantic_search != null) return error.UnsupportedQueryRequest;
        if (request.embedding_template != null) return error.UnsupportedQueryRequest;
        if (request.indexes != null) return error.UnsupportedQueryRequest;
        if (request.exclusion_query != null) return error.UnsupportedQueryRequest;
        if (request.embeddings != null) return error.UnsupportedQueryRequest;
        if (request.distance_under != null) return error.UnsupportedQueryRequest;
        if (request.distance_over != null) return error.UnsupportedQueryRequest;
        if (request.merge_config != null) return error.UnsupportedQueryRequest;
        if (request.reranker != null) return error.UnsupportedQueryRequest;
        if (request.analyses != null) return error.UnsupportedQueryRequest;
        if (request.graph_searches != null) return error.UnsupportedQueryRequest;
        if (request.expand_strategy != null) return error.UnsupportedQueryRequest;
        if (request.document_renderer != null) return error.UnsupportedQueryRequest;
        if (request.pruner != null) return error.UnsupportedQueryRequest;
        if (request.search_after != null) return error.UnsupportedQueryRequest;
        if (request.search_before != null) return error.UnsupportedQueryRequest;
    }

    fn foreignQueryLimit(limit: ?i64) !?usize {
        const raw = limit orelse 10;
        if (raw <= 0) return 10;
        return std.math.cast(usize, raw) orelse error.InvalidQueryRequest;
    }

    fn foreignQueryOffset(offset: ?i64) !usize {
        const raw = offset orelse 0;
        if (raw < 0) return error.InvalidQueryRequest;
        return std.math.cast(usize, raw) orelse error.InvalidQueryRequest;
    }

    fn cloneForeignSortFieldsAlloc(
        alloc: Allocator,
        order_by: ?[]const metadata_openapi.SortField,
    ) ![]foreign_mod.SortField {
        const fields = order_by orelse return &.{};
        if (fields.len == 0) return &.{};

        const out = try alloc.alloc(foreign_mod.SortField, fields.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |*item| item.deinit(alloc);
            alloc.free(out);
        }
        for (fields, 0..) |field, idx| {
            out[idx] = .{
                .field = try alloc.dupe(u8, field.field),
                .desc = field.desc orelse false,
            };
            initialized += 1;
        }
        return out;
    }

    fn freeForeignSortFields(alloc: Allocator, fields: []foreign_mod.SortField) void {
        for (fields) |*field| field.deinit(alloc);
        if (fields.len > 0) alloc.free(fields);
    }

    fn buildForeignSearchHitsAlloc(
        alloc: Allocator,
        foreign_source: foreign_mod.PostgresConfig,
        rows: []const std.json.Value,
    ) ![]db_types.SearchHit {
        if (rows.len == 0) return &.{};

        const hits = try alloc.alloc(db_types.SearchHit, rows.len);
        var initialized: usize = 0;
        errdefer {
            for (hits[0..initialized]) |*hit| hit.deinit(alloc);
            alloc.free(hits);
        }

        for (rows, 0..) |row, idx| {
            if (row != .object) return error.InvalidQueryRequest;
            const id = if (try foreign_sources_api.deriveSearchIdAlloc(alloc, foreign_source, row)) |value|
                value
            else
                try std.fmt.allocPrint(alloc, "{d}", .{idx});
            errdefer alloc.free(id);

            const stored_data = try stringifyJsonValueAlloc(alloc, row);
            errdefer alloc.free(stored_data);

            hits[idx] = .{
                .id = id,
                .score = 1,
                .stored_data = stored_data,
            };
            initialized += 1;
        }
        return hits;
    }

    fn executePlainPublicTableQueryJsonAlloc(self: *HttpHandler, table_name: []const u8, body: []const u8) anyerror![]u8 {
        const aggregations_json = parsePublicAggregationsJsonAlloc(self.alloc, body) catch |err| switch (err) {
            error.InvalidQueryRequest => return error.InvalidQueryRequest,
            else => return error.InternalQueryFailure,
        };
        defer if (aggregations_json) |json| self.alloc.free(json);

        const namespace = self.catalog.resolveTableNamespaceAlloc(table_name) catch return error.FileNotFound;
        defer self.alloc.free(namespace);

        if (try self.handleTablePublicGraphQueryRequest(table_name, namespace, body)) |owned_response| {
            var response = owned_response;
            defer response.deinit(self.alloc);
            if (response.status == 200) return try self.alloc.dupe(u8, response.body);
            return switch (response.status) {
                400 => error.InvalidQueryRequest,
                404 => error.FileNotFound,
                else => error.InternalQueryFailure,
            };
        }

        var execution = self.executePublishedSearch(namespace, table_name, body) catch |err| switch (err) {
            error.InvalidQueryRequest,
            error.UnsupportedQueryRequest,
            error.EmbeddingIndexNotFound,
            error.InvalidEmbeddingDimensions,
            error.PermanentPromptFailure,
            error.TransientPromptFailure,
            error.VectorQueryRequired,
            error.VectorDimsMismatch,
            error.SparseQueryRequired,
            => return error.InvalidQueryRequest,
            error.FileNotFound,
            error.VectorSegmentNotFound,
            error.SparseSegmentNotFound,
            => return error.FileNotFound,
            else => return error.InternalQueryFailure,
        };
        defer execution.deinit(self.alloc);

        const db_hits = try allocDbSearchHitsAlloc(self.alloc, execution.hits);
        defer freeDbSearchHits(self.alloc, db_hits);

        const req: db_types.SearchRequest = .{
            .count_only = execution.plan.request.count_only,
            .profile = execution.profile_requested,
            .aggregations_json = aggregations_json orelse "",
        };
        var meta: query_api.QueryResponseMeta = .{};
        defer meta.deinit(self.alloc);
        var total_hits_u32: u32 = @intCast(@min(execution.hits.len, std.math.maxInt(u32)));

        var result = db_types.SearchResult{
            .alloc = self.alloc,
            .hits = db_hits,
            .total_hits = total_hits_u32,
        };
        var computed_aggregations: ?ServerlessAggregationComputation = null;
        defer if (computed_aggregations) |*computed| computed.deinit(self.alloc);

        if (aggregations_json) |json| {
            computed_aggregations = computeServerlessAggregationResultsAlloc(self, &execution, json) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest, error.UnsupportedAggregation, error.InvalidAggregation => return error.InvalidQueryRequest,
                else => return error.InternalQueryFailure,
            };
            const computed = &computed_aggregations.?;
            total_hits_u32 = computed.total_hits;
            result.total_hits = total_hits_u32;
            meta.aggregation_results = computed.results;
            computed.results = &.{};
        }

        var response = try query_api.encodeQueryResponses(
            self.alloc,
            table_name,
            req,
            meta,
            result,
        );
        defer response.deinit(self.alloc);
        return try self.alloc.dupe(u8, response.json);
    }

    fn parsePublicAggregationsJsonAlloc(alloc: Allocator, body: []const u8) !?[]u8 {
        var public_request = std.json.parseFromSlice(metadata_openapi.QueryRequest, alloc, body, .{
            .ignore_unknown_fields = true,
            .allocate = .alloc_always,
        }) catch return error.InvalidQueryRequest;
        defer public_request.deinit();
        const aggregations = public_request.value.aggregations orelse return null;
        return try std.json.Stringify.valueAlloc(alloc, aggregations, .{});
    }

    fn computeServerlessAggregationResultsAlloc(
        self: *HttpHandler,
        execution: *SearchExecution,
        aggregations_json: []const u8,
    ) !ServerlessAggregationComputation {
        const requests = try query_api.parseAggregationRequestsJson(self.alloc, aggregations_json);
        errdefer query_api.freeAggregationRequests(self.alloc, requests);
        if (requests.len == 0) return .{
            .total_hits = @intCast(@min(execution.hits.len, std.math.maxInt(u32))),
            .requests = requests,
            .results = &.{},
        };

        const current_complete = !execution.plan.request.count_only and
            execution.requested_offset == 0 and
            execution.hits.len < execution.requested_limit;
        const source_hits = if (current_complete)
            execution.hits
        else blk: {
            const saved_offset = execution.plan.request.offset;
            const saved_limit = execution.plan.request.limit;
            const saved_count_only = execution.plan.request.count_only;
            defer {
                execution.plan.request.offset = saved_offset;
                execution.plan.request.limit = saved_limit;
                execution.plan.request.count_only = saved_count_only;
            }

            execution.plan.request.offset = 0;
            execution.plan.request.limit = std.math.maxInt(usize);
            execution.plan.request.count_only = false;

            var aggregation_stats = query_mod.QuerySearchExecutionStats{};
            break :blk try query_mod.searchIndexedPlanWithStatsAlloc(
                self.alloc,
                &execution.session.?,
                execution.plan,
                &aggregation_stats,
            );
        };
        defer if (source_hits.ptr != execution.hits.ptr) query_mod.freeSearchHits(self.alloc, source_hits);

        var ctx_owned = try @This().collectServerlessAggregationContextAlloc(
            self.alloc,
            &execution.session.?,
            requests,
            source_hits,
        );
        defer ctx_owned.deinit(self.alloc);

        const db_hits = try allocDbAggregationHitsAlloc(self.alloc, &execution.session.?, source_hits);
        defer freeDbSearchHits(self.alloc, db_hits);

        return .{
            .total_hits = @intCast(@min(source_hits.len, std.math.maxInt(u32))),
            .requests = requests,
            .results = try db_mod.aggregations.computeSearchAggregations(
                self.alloc,
                requests,
                .{
                    .alloc = self.alloc,
                    .hits = db_hits,
                    .total_hits = @intCast(@min(source_hits.len, std.math.maxInt(u32))),
                },
                ctx_owned.ctx,
            ),
        };
    }

    fn allocDbAggregationHitsAlloc(
        alloc: Allocator,
        session: *query_mod.QuerySession,
        hits: []const query_mod.QuerySearchHit,
    ) ![]db_types.SearchHit {
        const docs = try @This().loadPublishedDocumentsAlloc(alloc, session);
        defer query_materializer.freeDocuments(alloc, docs);

        var bodies = std.StringHashMap([]const u8).init(alloc);
        defer bodies.deinit();
        try bodies.ensureTotalCapacity(@intCast(docs.len));
        for (docs) |doc| {
            try bodies.put(doc.doc_id, doc.body);
        }

        const out_hits = try alloc.alloc(db_types.SearchHit, hits.len);
        errdefer alloc.free(out_hits);
        var initialized_hits: usize = 0;
        errdefer {
            for (out_hits[0..initialized_hits]) |*hit| hit.deinit(alloc);
        }

        for (hits, 0..) |hit, idx| {
            const body = bodies.get(hit.doc_id) orelse hit.body;
            out_hits[idx] = .{
                .id = try alloc.dupe(u8, hit.doc_id),
                .score = @as(f32, @floatFromInt(hit.score)) / 1000.0,
                .stored_data = try normalizeServerlessAggregationStoredDataAlloc(alloc, body),
            };
            initialized_hits += 1;
        }
        return out_hits;
    }

    fn normalizeServerlessAggregationStoredDataAlloc(
        alloc: Allocator,
        body: []const u8,
    ) ![]u8 {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch {
            return try std.fmt.allocPrint(alloc, "{{\"body\":{f}}}", .{std.json.fmt(body, .{})});
        };
        defer parsed.deinit();
        if (parsed.value == .object) return try alloc.dupe(u8, body);
        return try std.fmt.allocPrint(alloc, "{{\"body\":{f}}}", .{std.json.fmt(parsed.value, .{})});
    }

    fn encodeServerlessAggregationsValueAlloc(
        self: *HttpHandler,
        table_name: []const u8,
        execution: *SearchExecution,
        aggregations_json: []const u8,
    ) !std.json.Value {
        var computed = try computeServerlessAggregationResultsAlloc(self, execution, aggregations_json);
        defer computed.deinit(self.alloc);

        const db_hits = try allocDbSearchHitsAlloc(self.alloc, execution.hits);
        defer freeDbSearchHits(self.alloc, db_hits);

        var meta: query_api.QueryResponseMeta = .{
            .aggregation_results = computed.results,
        };
        defer meta.deinit(self.alloc);
        computed.results = &.{};

        var response = try query_api.encodeQueryResponses(
            self.alloc,
            table_name,
            .{
                .count_only = execution.plan.request.count_only,
                .profile = execution.profile_requested,
                .aggregations_json = aggregations_json,
            },
            meta,
            .{
                .alloc = self.alloc,
                .hits = db_hits,
                .total_hits = computed.total_hits,
            },
        );
        defer response.deinit(self.alloc);

        var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, self.alloc, response.json, .{});
        defer parsed.deinit();

        const responses = parsed.value.responses orelse return error.InternalQueryFailure;
        if (responses.len == 0) return error.InternalQueryFailure;
        const aggregations = responses[0].aggregations orelse return .null;
        const encoded_aggregations = try std.json.Stringify.valueAlloc(self.alloc, aggregations, .{});
        defer self.alloc.free(encoded_aggregations);
        var parsed_aggregations = try parseOwnedJsonValueAlloc(self.alloc, encoded_aggregations);
        errdefer deinitJsonValue(self.alloc, &parsed_aggregations);
        return parsed_aggregations;
    }

    fn collectServerlessAggregationContextAlloc(
        alloc: Allocator,
        session: *query_mod.QuerySession,
        requests: []const db_mod.aggregations.SearchAggregationRequest,
        hits: []const query_mod.QuerySearchHit,
    ) !ServerlessAggregationContextOwned {
        const field_sets = try @This().collectServerlessSignificantTermFieldSetsAlloc(alloc, requests, hits);
        defer {
            for (field_sets) |*item| item.deinit(alloc);
            if (field_sets.len > 0) alloc.free(field_sets);
        }
        if (field_sets.len == 0) return .{};

        const docs = try @This().loadPublishedDocumentsAlloc(alloc, session);
        defer query_materializer.freeDocuments(alloc, docs);

        const stats = try alloc.alloc(distributed_stats_mod.TextFieldStats, field_sets.len);
        var initialized: usize = 0;
        errdefer {
            distributed_stats_mod.deinitTextFieldStats(alloc, stats[0..initialized]);
            if (stats.len > 0) alloc.free(stats);
        }
        for (field_sets, 0..) |field_set, idx| {
            stats[idx] = try @This().computeServerlessFieldTextStatsAlloc(alloc, docs, field_set.field, field_set.terms.items);
            initialized = idx + 1;
        }

        const background_count = blk: {
            var count: usize = 0;
            for (requests) |request| {
                if (std.mem.eql(u8, request.type, "significant_terms") and request.background_query != null) count += 1;
            }
            break :blk count;
        };
        const background_stats = if (background_count == 0)
            &.{}
        else blk: {
            const out = try alloc.alloc(db_mod.aggregations.DistributedBackgroundTextStats, background_count);
            var bg_initialized: usize = 0;
            errdefer {
                db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, out[0..bg_initialized]);
                if (out.len > 0) alloc.free(out);
            }
            for (requests) |request| {
                if (!std.mem.eql(u8, request.type, "significant_terms")) continue;
                const background_query = request.background_query orelse continue;
                const terms = for (field_sets) |field_set| {
                    if (std.mem.eql(u8, field_set.field, request.field)) break field_set.terms.items;
                } else &.{};
                out[bg_initialized] = try @This().computeServerlessBackgroundTextStatsAlloc(
                    alloc,
                    docs,
                    request.name,
                    request.field,
                    terms,
                    background_query,
                );
                bg_initialized += 1;
            }
            break :blk out;
        };
        return .{
            .ctx = .{
                .distributed_text_stats = stats,
                .distributed_background_text_stats = background_stats,
            },
        };
    }

    fn collectServerlessSignificantTermFieldSetsAlloc(
        alloc: Allocator,
        requests: []const db_mod.aggregations.SearchAggregationRequest,
        hits: []const query_mod.QuerySearchHit,
    ) ![]SignificantTermFieldSet {
        var sets = std.ArrayListUnmanaged(SignificantTermFieldSet).empty;
        errdefer {
            for (sets.items) |*item| item.deinit(alloc);
            sets.deinit(alloc);
        }

        for (requests) |request| {
            if (!std.mem.eql(u8, request.type, "significant_terms")) continue;
            const field_index = blk: {
                for (sets.items, 0..) |item, idx| {
                    if (std.mem.eql(u8, item.field, request.field)) break :blk idx;
                }
                try sets.append(alloc, .{
                    .field = try alloc.dupe(u8, request.field),
                });
                break :blk sets.items.len - 1;
            };

            for (hits) |hit| try @This().appendUniqueSignificantTermsFromBodyAlloc(
                alloc,
                hit.body,
                request.field,
                &sets.items[field_index].terms,
            );
        }

        return try sets.toOwnedSlice(alloc);
    }

    fn computeServerlessFieldTextStatsAlloc(
        alloc: Allocator,
        docs: []const query_materializer.Document,
        field: []const u8,
        terms: []const []u8,
    ) !distributed_stats_mod.TextFieldStats {
        const counts = try alloc.alloc(u32, terms.len);
        defer alloc.free(counts);
        @memset(counts, 0);

        for (docs) |doc| try @This().countMatchingSignificantTermsInBodyAlloc(
            alloc,
            doc.body,
            field,
            terms,
            counts,
        );

        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, terms.len);
        var initialized: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        for (terms, 0..) |term, idx| {
            term_doc_freqs[idx] = .{
                .term = try alloc.dupe(u8, term),
                .doc_freq = counts[idx],
            };
            initialized = idx + 1;
        }

        return .{
            .field = try alloc.dupe(u8, field),
            .global_doc_count = @intCast(@min(docs.len, std.math.maxInt(u32))),
            .global_total_field_len = 0,
            .term_doc_freqs = term_doc_freqs,
        };
    }

    fn computeServerlessBackgroundTextStatsAlloc(
        alloc: Allocator,
        docs: []const query_materializer.Document,
        aggregation_name: []const u8,
        field: []const u8,
        terms: []const []u8,
        background_query: db_mod.aggregations.BackgroundQuery,
    ) !db_mod.aggregations.DistributedBackgroundTextStats {
        const counts = try alloc.alloc(u32, terms.len);
        defer alloc.free(counts);
        @memset(counts, 0);

        var background_doc_count: u32 = 0;
        for (docs) |doc| try @This().countBackgroundSignificantTermsInBodyAlloc(
            alloc,
            doc.body,
            field,
            terms,
            background_query,
            &background_doc_count,
            counts,
        );

        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, terms.len);
        var initialized: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        for (terms, 0..) |term, idx| {
            term_doc_freqs[idx] = .{
                .term = try alloc.dupe(u8, term),
                .doc_freq = counts[idx],
            };
            initialized = idx + 1;
        }

        return .{
            .aggregation_name = try alloc.dupe(u8, aggregation_name),
            .field = try alloc.dupe(u8, field),
            .background_doc_count = background_doc_count,
            .term_doc_freqs = term_doc_freqs,
        };
    }

    fn appendUniqueSignificantTermsFromValueAlloc(
        alloc: Allocator,
        value: std.json.Value,
        out: *std.ArrayListUnmanaged([]u8),
    ) !void {
        var seen = std.StringHashMap(void).init(alloc);
        defer {
            var it = seen.keyIterator();
            while (it.next()) |key| alloc.free(key.*);
            seen.deinit();
        }
        try @This().collectSignificantTermsFromValueAlloc(alloc, value, &seen);
        var it = seen.keyIterator();
        while (it.next()) |term| {
            if (@This().containsOwnedString(out.items, term.*)) continue;
            try out.append(alloc, try alloc.dupe(u8, term.*));
        }
    }

    fn appendUniqueSignificantTermsFromBodyAlloc(
        alloc: Allocator,
        body: []const u8,
        field: []const u8,
        out: *std.ArrayListUnmanaged([]u8),
    ) !void {
        var parsed = (try parseJsonPathValueAlloc(alloc, body, field)) orelse return;
        defer parsed.deinit();
        try @This().appendUniqueSignificantTermsFromValueAlloc(alloc, parsed.value, out);
    }

    fn countMatchingSignificantTermsInBodyAlloc(
        alloc: Allocator,
        body: []const u8,
        field: []const u8,
        terms: []const []u8,
        counts: []u32,
    ) !void {
        var parsed = (try parseJsonPathValueAlloc(alloc, body, field)) orelse return;
        defer parsed.deinit();

        var seen = std.StringHashMap(void).init(alloc);
        defer {
            var it = seen.keyIterator();
            while (it.next()) |key| alloc.free(key.*);
            seen.deinit();
        }
        try @This().collectSignificantTermsFromValueAlloc(alloc, parsed.value, &seen);

        for (terms, 0..) |term, idx| {
            if (seen.contains(term)) counts[idx] += 1;
        }
    }

    fn countBackgroundSignificantTermsInBodyAlloc(
        alloc: Allocator,
        body: []const u8,
        field: []const u8,
        terms: []const []u8,
        background_query: db_mod.aggregations.BackgroundQuery,
        background_doc_count: *u32,
        counts: []u32,
    ) !void {
        var parsed = (try parseBackgroundMatchedJsonPathValueAlloc(alloc, body, field, background_query)) orelse return;
        defer parsed.deinit();
        background_doc_count.* += 1;

        var seen = std.StringHashMap(void).init(alloc);
        defer {
            var it = seen.keyIterator();
            while (it.next()) |key| alloc.free(key.*);
            seen.deinit();
        }
        try @This().collectSignificantTermsFromValueAlloc(alloc, parsed.value, &seen);

        for (terms, 0..) |term, idx| {
            if (seen.contains(term)) counts[idx] += 1;
        }
    }

    fn collectSignificantTermsFromValueAlloc(
        alloc: Allocator,
        value: std.json.Value,
        seen_terms: *std.StringHashMap(void),
    ) !void {
        switch (value) {
            .array => |arr| for (arr.items) |item| try @This().collectSignificantTermsFromValueAlloc(alloc, item, seen_terms),
            .string => {
                const tokens = try analysis_mod.default_analyzer.analyze(alloc, value.string);
                defer analysis_mod.Analyzer.freeTokens(alloc, tokens);
                for (tokens) |tok| {
                    const entry = try seen_terms.getOrPut(tok.term);
                    if (entry.found_existing) continue;
                    entry.key_ptr.* = try alloc.dupe(u8, tok.term);
                    entry.value_ptr.* = {};
                }
            },
            else => {},
        }
    }

    fn documentMatchesBackgroundQueryAlloc(
        alloc: Allocator,
        root: std.json.Value,
        query: db_mod.aggregations.BackgroundQuery,
    ) !bool {
        return switch (query) {
            .match_all => true,
            .term => |term_query| blk: {
                const value = extractJsonPathValue(root, term_query.field) orelse break :blk false;
                break :blk jsonValueContainsExactTerm(value, term_query.term);
            },
            .match => |match_query| blk: {
                const value = extractJsonPathValue(root, match_query.field) orelse break :blk false;
                var doc_terms = std.StringHashMap(void).init(alloc);
                defer {
                    var it = doc_terms.keyIterator();
                    while (it.next()) |key| alloc.free(key.*);
                    doc_terms.deinit();
                }
                try @This().collectSignificantTermsFromValueAlloc(alloc, value, &doc_terms);

                const query_tokens = try analysis_mod.default_analyzer.analyze(alloc, match_query.text);
                defer analysis_mod.Analyzer.freeTokens(alloc, query_tokens);
                for (query_tokens) |token| {
                    if (!doc_terms.contains(token.term)) break :blk false;
                }
                break :blk query_tokens.len > 0;
            },
        };
    }

    fn jsonValueContainsExactTerm(value: std.json.Value, term: []const u8) bool {
        return switch (value) {
            .string => std.mem.eql(u8, value.string, term),
            .array => |arr| blk: {
                for (arr.items) |item| {
                    if (@This().jsonValueContainsExactTerm(item, term)) break :blk true;
                }
                break :blk false;
            },
            else => false,
        };
    }

    fn containsOwnedString(items: []const []u8, needle: []const u8) bool {
        for (items) |item| {
            if (std.mem.eql(u8, item, needle)) return true;
        }
        return false;
    }

    fn loadPublishedDocumentsAlloc(
        alloc: Allocator,
        session: *query_mod.QuerySession,
    ) ![]query_materializer.Document {
        const doc_index = session.findArtifactIndex(.document_segment) orelse return error.DocumentSegmentNotFound;
        const doc_payload = try session.fetchArtifactAlloc(doc_index);
        defer alloc.free(doc_payload);
        const doc_entries = try document_segment_mod.decodeAlloc(alloc, doc_payload);
        defer document_segment_mod.freeEntries(alloc, doc_entries);

        const base_docs = try allocMaterializedDocumentsFromDocumentEntries(alloc, doc_entries);
        errdefer query_materializer.freeDocuments(alloc, base_docs);

        const mutation_index = session.findArtifactIndex(.mutation_segment) orelse return base_docs;
        const mutation_payload = try session.fetchArtifactAlloc(mutation_index);
        defer alloc.free(mutation_payload);
        const mutation_entries = try segment_mod.decodeAlloc(alloc, mutation_payload);
        defer segment_mod.freeEntries(alloc, mutation_entries);
        const overlay = try allocMaterializerMutationsFromEntries(alloc, mutation_entries);
        defer freeMaterializerMutations(alloc, overlay);
        const docs = try query_materializer.materializeOverBaseAlloc(alloc, base_docs, overlay);
        query_materializer.freeDocuments(alloc, base_docs);
        return docs;
    }

    fn executeSupportedJoinedPublicTableQueryRequest(
        self: *HttpHandler,
        table_name: []const u8,
        body: []const u8,
        join: SupportedJoinRequest,
        foreign_sources: foreign_mod.PostgresSourceMap,
    ) anyerror![]u8 {
        var contract_request = std.json.parseFromSlice(metadata_openapi.QueryRequest, self.alloc, body, .{
            .ignore_unknown_fields = true,
            .allocate = .alloc_always,
        }) catch return error.InvalidQueryRequest;
        defer contract_request.deinit();
        if (contract_request.value.count == true) return error.InvalidQueryRequest;

        const rewrite = distributed_join.rewriteJoinedBaseQueryBodyAlloc(self.alloc, contract_request.value, join.left_field) catch return error.InternalQueryFailure;
        const appended_left_field = rewrite.appended_left_field;
        const primary_body = rewrite.body;
        defer self.alloc.free(primary_body);

        const primary_json = try self.executePlainPublicTableQueryJsonAlloc(table_name, primary_body);
        errdefer self.alloc.free(primary_json);

        var owned_response = parseOwnedJsonValueAlloc(self.alloc, primary_json) catch return error.InternalQueryFailure;
        defer deinitJsonValue(self.alloc, &owned_response);
        const hits_ptr = queryHitsArrayPtr(&owned_response) catch return error.InvalidQueryRequest;
        if (hits_ptr.items.len == 0) return primary_json;

        const plan = planSupportedJoinExecution(self, self.alloc, join, hits_ptr.items, foreign_sources);
        var right_result = try self.executeSupportedRightJoinQuery(join, hits_ptr.items, plan, foreign_sources);
        defer right_result.deinit(self.alloc);

        var stats: JoinedQueryStats = .{
            .left_rows_scanned = @intCast(hits_ptr.items.len),
            .right_rows_scanned = @intCast(right_result.hits.len),
        };
        var matched_right_ids = std.StringHashMapUnmanaged(void){};
        defer matched_right_ids.deinit(self.alloc);

        var joined_hits = std.json.Array.init(self.alloc);
        defer {
            for (joined_hits.items) |*item| deinitJsonValue(self.alloc, item);
            joined_hits.deinit();
        }

        for (hits_ptr.items) |hit_value| {
            var joined_hit = cloneJsonValue(self.alloc, hit_value) catch return error.InternalQueryFailure;
            errdefer deinitJsonValue(self.alloc, &joined_hit);
            const source_value = joined_hit.object.getPtr("_source") orelse return error.InvalidQueryRequest;
            if (source_value.* != .object) return error.InvalidQueryRequest;
            if (appended_left_field) removeFieldFromSourceObject(self.alloc, source_value, join.left_field);

            const left_value = extractJoinValueFromHit(hit_value, join.left_field) orelse {
                stats.rows_unmatched_left += 1;
                if (join.join_type == .left) {
                    try joined_hits.append(joined_hit);
                } else {
                    deinitJsonValue(self.alloc, &joined_hit);
                }
                continue;
            };
            const matched_right = findFirstMatchingRightHit(join, left_value, right_result.hits) orelse {
                stats.rows_unmatched_left += 1;
                if (join.join_type == .left) {
                    try joined_hits.append(joined_hit);
                } else {
                    deinitJsonValue(self.alloc, &joined_hit);
                }
                continue;
            };

            try mergeRightHitIntoSource(self.alloc, source_value, join, matched_right);
            if (matched_right.object.get("_id")) |matched_id| {
                if (matched_id == .string) try matched_right_ids.put(self.alloc, matched_id.string, {});
            }
            try joined_hits.append(joined_hit);
            stats.rows_matched += 1;
        }

        if (join.join_type == .right) {
            const requested_left_fields = try allocRequestedFieldsFromRequest(self.alloc, contract_request.value);
            defer freeJsonStringArray(self.alloc, requested_left_fields);
            for (right_result.hits) |right_hit| {
                const right_id = switch (right_hit) {
                    .object => |obj| switch (obj.get("_id") orelse continue) {
                        .string => |text| text,
                        else => continue,
                    },
                    else => continue,
                };
                if (matched_right_ids.contains(right_id)) continue;
                try joined_hits.append(try buildUnmatchedRightJoinHit(self.alloc, right_hit, join, requested_left_fields, appended_left_field));
                stats.rows_unmatched_right += 1;
            }
        }

        var existing_hits_value: std.json.Value = .{ .array = hits_ptr.* };
        deinitJsonValue(self.alloc, &existing_hits_value);
        hits_ptr.* = std.json.Array.init(self.alloc);
        for (joined_hits.items) |item| {
            try hits_ptr.append(try cloneJsonValue(self.alloc, item));
        }
        try updateJoinedResponseMetadata(self.alloc, &owned_response, joined_hits.items.len, hits_ptr.items);
        try maybeAttachJoinProfile(self.alloc, &owned_response, stats, plan, right_result.strategy_used);

        self.alloc.free(primary_json);
        return stringifyJsonValueAlloc(self.alloc, owned_response) catch error.InternalQueryFailure;
    }

    fn executeSupportedRightJoinQuery(
        self: *HttpHandler,
        join: SupportedJoinRequest,
        left_hits: []const std.json.Value,
        plan: PlannedJoinExecution,
        foreign_sources: foreign_mod.PostgresSourceMap,
    ) anyerror!RightJoinQueryResult {
        if (foreign_sources.get(join.right_table)) |foreign_source| {
            return try self.executeForeignRightJoinQuery(foreign_source, join, left_hits, foreign_sources);
        }
        const namespace = self.catalog.resolveTableNamespaceAlloc(join.right_table) catch return error.FileNotFound;
        defer self.alloc.free(namespace);

        var session = self.query.openHeadSession(namespace) catch |err| switch (err) {
            error.FileNotFound => return error.FileNotFound,
            else => return err,
        };
        defer session.deinit();

        const docs = try self.allocPublishedDocumentsAlloc(&session);
        defer query_materializer.freeDocuments(self.alloc, docs);

        var filter_text: ?public_text_query.PublicTextSpec = null;
        defer if (filter_text) |*spec| spec.deinit(self.alloc);
        if (join.right_filters) |filters| {
            if (filters.filter_query) |filter_query| {
                filter_text = public_text_query.parseTextSpecAlloc(self.alloc, filter_query) catch |err| switch (err) {
                    error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidQueryRequest,
                    else => return err,
                };
            }
        }

        const limit = if (join.right_filters) |filters| filters.limit else null;
        var hits = std.ArrayListUnmanaged(std.json.Value).empty;
        errdefer {
            for (hits.items) |*item| deinitJsonValue(self.alloc, item);
            hits.deinit(self.alloc);
        }

        for (docs) |doc| {
            if (join.right_filters) |filters| {
                if (filters.filter_prefix) |prefix| {
                    if (!std.mem.startsWith(u8, doc.doc_id, prefix)) continue;
                }
            }

            var parsed_body = parseJsonObjectAlloc(self.alloc, doc.body) catch return error.InvalidQueryRequest;
            defer parsed_body.deinit();

            if (filter_text) |spec| {
                if (!try documentMatchesPublicTextSpec(self.alloc, doc.doc_id, parsed_body.value, spec)) continue;
            }

            if (join.join_type != .right) {
                const right_value = extractJoinValueFromDocument(doc.doc_id, parsed_body.value, join.right_field) orelse continue;
                var matched = false;
                for (left_hits) |left_hit| {
                    const left_value = extractJoinValueFromHit(left_hit, join.left_field) orelse continue;
                    if (jsonValuesEqual(left_value, right_value)) {
                        matched = true;
                        break;
                    }
                }
                if (!matched) continue;
            }

            try hits.append(self.alloc, try buildRightJoinHitFromDocument(self.alloc, doc.doc_id, parsed_body.value));
            if (limit) |max_hits| {
                if (hits.items.len >= max_hits) break;
            }
        }

        if (join.nested_join) |nested_join| {
            const nested_hits = try self.executeSupportedJoinedHitsAlloc(nested_join.*, hits.items, foreign_sources);
            for (hits.items) |*item| deinitJsonValue(self.alloc, item);
            hits.deinit(self.alloc);
            return .{
                .hits = nested_hits,
                .strategy_used = plan.strategy,
            };
        }

        return .{
            .hits = try hits.toOwnedSlice(self.alloc),
            .strategy_used = plan.strategy,
        };
    }

    fn executeForeignRightJoinQuery(
        self: *HttpHandler,
        foreign_source: foreign_mod.PostgresConfig,
        join: SupportedJoinRequest,
        left_hits: []const std.json.Value,
        foreign_sources: foreign_mod.PostgresSourceMap,
    ) anyerror!RightJoinQueryResult {
        const registry = self.foreign_registry orelse return error.UnsupportedQueryRequest;

        const effective_fields = try buildForeignJoinFieldListAlloc(self.alloc, join);
        defer freeOwnedStringSlice(self.alloc, effective_fields);
        const raw_filter_query_json = if (join.right_filters) |filters|
            if (filters.filter_query) |query|
                try stringifyJsonValueAlloc(self.alloc, query)
            else
                null
        else
            null;
        defer if (raw_filter_query_json) |query| self.alloc.free(query);
        const filter_query_json = try foreign_sources_api.buildEffectiveFilterQueryJsonAlloc(
            self.alloc,
            foreign_source,
            raw_filter_query_json,
            if (join.right_filters) |filters| filters.filter_prefix else null,
        );
        defer if (filter_query_json) |query| self.alloc.free(query);

        var params = try foreign_source.toQueryParams(self.alloc, .{
            .fields = effective_fields,
            .filter_query_json = filter_query_json,
            .limit = if (join.right_filters) |filters| filters.limit else null,
        });
        defer params.deinit(self.alloc);

        const source_config = try foreign_source.toSourceConfig(self.alloc);
        var foreign_query_source = try registry.create(self.alloc, source_config);
        defer foreign_query_source.deinit(self.alloc);

        var result = try foreign_query_source.query(self.alloc, params);
        defer result.deinit(self.alloc);

        var hits = std.ArrayListUnmanaged(std.json.Value).empty;
        errdefer {
            for (hits.items) |*item| deinitJsonValue(self.alloc, item);
            hits.deinit(self.alloc);
        }

        for (result.rows) |row| {
            if (row != .object) return error.UnsupportedQueryRequest;
            const match_value = extractJsonPathValue(row, join.right_field) orelse continue;
            if (join.join_type != .right) {
                var matched = false;
                for (left_hits) |left_hit| {
                    const left_value = extractJoinValueFromHit(left_hit, join.left_field) orelse continue;
                    if (jsonValuesEqual(left_value, match_value)) {
                        matched = true;
                        break;
                    }
                }
                if (!matched) continue;
            }
            try hits.append(self.alloc, try buildForeignRightJoinHit(self.alloc, foreign_source, row, match_value));
        }

        const owned_hits = try hits.toOwnedSlice(self.alloc);
        if (join.nested_join) |nested_join| {
            try self.applyNestedJoinToRightHitsAlloc(join.right_table, owned_hits, nested_join, foreign_sources);
        }

        return .{
            .hits = owned_hits,
            .strategy_used = .broadcast,
        };
    }

    fn executeSupportedJoinedHitsAlloc(
        self: *HttpHandler,
        join: SupportedJoinRequest,
        left_hits: []const std.json.Value,
        foreign_sources: foreign_mod.PostgresSourceMap,
    ) anyerror![]std.json.Value {
        if (left_hits.len == 0 and join.join_type != .right) return try self.alloc.alloc(std.json.Value, 0);

        const plan = planSupportedJoinExecution(self, self.alloc, join, left_hits, foreign_sources);
        var right_result = try self.executeSupportedRightJoinQuery(join, left_hits, plan, foreign_sources);
        defer right_result.deinit(self.alloc);

        const requested_left_fields = if (join.join_type == .right)
            try allocRequestedFieldsFromHits(self.alloc, left_hits)
        else
            try self.alloc.alloc(std.json.Value, 0);
        defer freeJsonStringArray(self.alloc, requested_left_fields);

        var matched_right_ids = std.StringHashMapUnmanaged(void){};
        defer matched_right_ids.deinit(self.alloc);

        var joined_hits = std.ArrayListUnmanaged(std.json.Value).empty;
        errdefer {
            for (joined_hits.items) |*item| deinitJsonValue(self.alloc, item);
            joined_hits.deinit(self.alloc);
        }

        for (left_hits) |hit_value| {
            var joined_hit = cloneJsonValue(self.alloc, hit_value) catch return error.InternalQueryFailure;
            errdefer deinitJsonValue(self.alloc, &joined_hit);
            const source_value = joined_hit.object.getPtr("_source") orelse return error.InvalidQueryRequest;
            if (source_value.* != .object) return error.InvalidQueryRequest;

            const left_value = extractJoinValueFromHit(hit_value, join.left_field) orelse {
                if (join.join_type == .left) {
                    try joined_hits.append(self.alloc, joined_hit);
                } else {
                    deinitJsonValue(self.alloc, &joined_hit);
                }
                continue;
            };
            const matched_right = findFirstMatchingRightHit(join, left_value, right_result.hits) orelse {
                if (join.join_type == .left) {
                    try joined_hits.append(self.alloc, joined_hit);
                } else {
                    deinitJsonValue(self.alloc, &joined_hit);
                }
                continue;
            };

            try mergeRightHitIntoSource(self.alloc, source_value, join, matched_right);
            if (matched_right.object.get("_id")) |matched_id| {
                if (matched_id == .string) try matched_right_ids.put(self.alloc, matched_id.string, {});
            }
            try joined_hits.append(self.alloc, joined_hit);
        }

        if (join.join_type == .right) {
            for (right_result.hits) |right_hit| {
                const right_id = switch (right_hit) {
                    .object => |obj| switch (obj.get("_id") orelse continue) {
                        .string => |text| text,
                        else => continue,
                    },
                    else => continue,
                };
                if (matched_right_ids.contains(right_id)) continue;
                try joined_hits.append(self.alloc, try buildUnmatchedRightJoinHit(self.alloc, right_hit, join, requested_left_fields, false));
            }
        }

        return try joined_hits.toOwnedSlice(self.alloc);
    }

    fn applyNestedJoinToRightHitsAlloc(
        self: *HttpHandler,
        parent_table_name: []const u8,
        right_hits: []std.json.Value,
        nested_join: *SupportedJoinRequest,
        foreign_sources: foreign_mod.PostgresSourceMap,
    ) anyerror!void {
        _ = parent_table_name;
        if (right_hits.len == 0) return;

        const plan = planSupportedJoinExecution(self, self.alloc, nested_join.*, right_hits, foreign_sources);
        var nested_result = try self.executeSupportedRightJoinQuery(nested_join.*, right_hits, plan, foreign_sources);
        defer nested_result.deinit(self.alloc);

        for (right_hits) |*hit| {
            const left_value = extractJoinValueFromHit(hit.*, nested_join.left_field) orelse continue;
            const matched_right = findFirstMatchingRightHit(nested_join.*, left_value, nested_result.hits) orelse continue;
            const source_value = hit.object.getPtr("_source") orelse return error.InvalidQueryRequest;
            if (source_value.* != .object) return error.InvalidQueryRequest;
            try mergeRightHitIntoSource(self.alloc, source_value, nested_join.*, matched_right);
        }
    }

    fn executePublicTableQueryViewJsonAlloc(
        self: *HttpHandler,
        table_name: []const u8,
        requested_view: public_table_http.TableApi.TableQueryView,
    ) ![]u8 {
        const namespace = self.catalog.resolveTableNamespaceAlloc(table_name) catch return error.FileNotFound;
        defer self.alloc.free(namespace);

        const resolved_view: query_types.QueryView = switch (requested_view) {
            .published => .published,
            .latest => .latest,
            .default_view => blk: {
                const policy = self.catalog.getPolicy(namespace) catch return error.FileNotFound;
                break :blk switch (policy.default_query_view) {
                    .published => .published,
                    .latest => .latest,
                };
            },
        };

        var session = self.query.openHeadSession(namespace) catch |err| switch (err) {
            error.FileNotFound => return error.FileNotFound,
            else => return err,
        };
        defer session.deinit();

        if (resolved_view == .latest) {
            const records = try self.api.wal.readFromAlloc(namespace, session.manifest.wal_end_lsn + 1);
            defer wal_mod.freeRecords(self.alloc, records);

            const tail = try allocTailMutations(self.alloc, records);
            defer freeQueryMutations(self.alloc, tail);
            return try self.encodeTableQuerySessionResponseJsonAlloc(table_name, &session, .latest, tail);
        }

        return try self.encodeTableQuerySessionResponseJsonAlloc(table_name, &session, .published, &.{});
    }

    fn handleTableQueryRequest(self: *HttpHandler, table_name: []const u8, body: []const u8) !HttpResponse {
        var resp = try public_table_http.handleTableQueryRequest(
            self.alloc,
            table_name,
            body,
            null,
            self.tableApi(),
        );
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            200 => try typedJsonResponse(metadata_openapi.QueryResponses, self.alloc, 200, resp.body),
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    fn handleTablePublicGraphQueryRequest(
        self: *HttpHandler,
        table_name: []const u8,
        namespace: []const u8,
        body: []const u8,
    ) !?HttpResponse {
        var parsed_request = std.json.parseFromSlice(metadata_openapi.QueryRequest, self.alloc, body, .{
            .ignore_unknown_fields = true,
            .allocate = .alloc_always,
        }) catch return null;
        defer parsed_request.deinit();
        const request = parsed_request.value;
        if (request.graph_searches == null) return null;

        if (request.aggregations != null or
            request.analyses != null or
            request.order_by != null or
            request.search_after != null or
            request.search_before != null or
            request.document_renderer != null or
            request.join != null or
            request.foreign_sources != null or
            request.merge_config != null or
            request.pruner != null or
            request.reranker != null or
            request.expand_strategy != null or
            request.distance_over != null or
            request.distance_under != null)
        {
            return error.UnsupportedQueryRequest;
        }

        const started_ns = platform_time.monotonicNs();
        const graph_queries = try public_graph_query.parseSupportedGraphQueriesAlloc(self.alloc, request);
        defer public_graph_query.freeNamedGraphQueries(self.alloc, graph_queries);

        var req: db_types.SearchRequest = .{
            .count_only = request.count == true,
            .profile = request.profile == true,
            .graph_queries = graph_queries,
            .limit = if (request.limit) |limit| std.math.cast(u32, limit) orelse 10 else 10,
            .offset = if (request.offset) |offset| std.math.cast(u32, offset) orelse 0 else 0,
        };
        var search_hits: []db_types.SearchHit = &.{};
        defer if (search_hits.len > 0) freeDbSearchHits(self.alloc, search_hits);
        var search_total_hits: u32 = 0;
        var initial_sets = std.ArrayListUnmanaged(GraphResultSet).empty;
        defer initial_sets.deinit(self.alloc);

        var session: query_mod.QuerySession = undefined;
        var session_initialized = false;
        defer if (session_initialized) session.deinit();

        if (requestHasSearchInputs(request)) {
            var search_request = request;
            search_request.graph_searches = null;
            const search_body = try std.json.Stringify.valueAlloc(self.alloc, search_request, .{});
            defer self.alloc.free(search_body);

            var execution = self.executePublishedSearch(namespace, table_name, search_body) catch |err| switch (err) {
                error.InvalidQueryRequest,
                error.UnsupportedQueryRequest,
                error.EmbeddingIndexNotFound,
                error.InvalidEmbeddingDimensions,
                error.PermanentPromptFailure,
                error.TransientPromptFailure,
                error.VectorQueryRequired,
                error.VectorDimsMismatch,
                error.SparseQueryRequired,
                => return error.InvalidQueryRequest,
                else => return err,
            };
            defer execution.deinit(self.alloc);

            req.count_only = execution.plan.request.count_only;
            req.profile = execution.profile_requested;
            search_hits = try allocDbSearchHitsAlloc(self.alloc, execution.hits);
            search_total_hits = @intCast(@min(execution.hits.len, std.math.maxInt(u32)));
            try initial_sets.append(self.alloc, .{
                .name = "$fused_results",
                .hits = search_hits,
                .total_hits = search_total_hits,
            });
            if (request.full_text_search != null) {
                try initial_sets.append(self.alloc, .{
                    .name = "$full_text_results",
                    .hits = search_hits,
                    .total_hits = search_total_hits,
                });
            }
            if (execution.plan.usesVectorLane() and !execution.plan.usesTextLane() and !execution.plan.usesSparseLane()) {
                try initial_sets.append(self.alloc, .{
                    .name = "$embeddings_results",
                    .hits = search_hits,
                    .total_hits = search_total_hits,
                });
            }
            if (execution.plan.usesSparseLane() and !execution.plan.usesTextLane() and !execution.plan.usesVectorLane()) {
                try initial_sets.append(self.alloc, .{
                    .name = "$embeddings_results",
                    .hits = search_hits,
                    .total_hits = search_total_hits,
                });
            }

            session = execution.takeSession();
            session_initialized = true;
        } else {
            session = self.query.openHeadSession(namespace) catch |err| switch (err) {
                error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
                else => return err,
            };
            session_initialized = true;
        }

        const results = try self.executePublicGraphQueriesAlloc(&session, graph_queries, initial_sets.items);
        defer {
            for (results) |*result| result.deinit(self.alloc);
            if (results.len > 0) self.alloc.free(results);
        }

        const result: db_types.SearchResult = .{
            .alloc = self.alloc,
            .hits = search_hits,
            .total_hits = search_total_hits,
            .graph_results = results,
        };

        var response = try query_api.encodeQueryResponses(
            self.alloc,
            table_name,
            req,
            .{ .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - started_ns, std.time.ns_per_ms)) },
            result,
        );
        defer response.deinit(self.alloc);
        return try typedJsonResponse(metadata_openapi.QueryResponses, self.alloc, 200, response.json);
    }

    fn handleQuerySearch(self: *HttpHandler, namespace: []const u8, body: []const u8) !HttpResponse {
        const aggregations_json = parsePublicAggregationsJsonAlloc(self.alloc, body) catch |err| switch (err) {
            error.InvalidQueryRequest => return try textResponse(self.alloc, 400, "invalid query request"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer if (aggregations_json) |json| self.alloc.free(json);

        var execution = self.executePublishedSearch(namespace, null, body) catch |err| {
            switch (err) {
                error.InvalidQueryRequest,
                error.UnsupportedQueryRequest,
                error.EmbeddingIndexNotFound,
                error.InvalidEmbeddingDimensions,
                error.PermanentPromptFailure,
                error.TransientPromptFailure,
                error.VectorQueryRequired,
                error.VectorDimsMismatch,
                error.SparseQueryRequired,
                => {
                    std.log.err("serverless query invalid namespace={s} err={}", .{ namespace, err });
                    return try textResponse(self.alloc, 400, "invalid query request");
                },
                error.FileNotFound => {
                    std.log.err("serverless query missing namespace={s} err={}", .{ namespace, err });
                    return try textResponse(self.alloc, 404, "not found");
                },
                error.VectorSegmentNotFound => {
                    std.log.err("serverless query missing vector segment namespace={s} err={}", .{ namespace, err });
                    return try textResponse(self.alloc, 404, "vector segment not found");
                },
                error.SparseSegmentNotFound => {
                    std.log.err("serverless query missing sparse segment namespace={s} err={}", .{ namespace, err });
                    return try textResponse(self.alloc, 404, "sparse segment not found");
                },
                else => {
                    std.log.err("serverless query failed namespace={s} err={}", .{ namespace, err });
                    return try textResponse(self.alloc, 500, "query failed");
                },
            }
        };
        defer execution.deinit(self.alloc);

        const out_hits = try allocQueryHitsAlloc(self.alloc, execution.hits, execution.plan.request.fields, execution.plan.request.count_only);
        defer freeQueryHits(self.alloc, out_hits);

        var aggregations: ?std.json.Value = null;
        defer if (aggregations) |*value| deinitJsonValue(self.alloc, value);
        if (aggregations_json) |json| {
            aggregations = encodeServerlessAggregationsValueAlloc(self, execution.session.?.namespace(), &execution, json) catch |err| switch (err) {
                error.InvalidQueryRequest,
                error.UnsupportedQueryRequest,
                error.UnsupportedAggregation,
                error.InvalidAggregation,
                => return try textResponse(self.alloc, 400, "invalid query request"),
                else => {
                    std.log.err("namespace aggregations failed namespace={s} err={}", .{ namespace, err });
                    return try textResponse(self.alloc, 500, "query failed");
                },
            };
        }

        return try jsonResponse(self.alloc, 200, query_types.QuerySearchResult{
            .namespace = execution.session.?.namespace(),
            .version = execution.session.?.version(),
            .view = .published,
            .published_wal_end_lsn = execution.session.?.manifest.wal_end_lsn,
            .latest_wal_lsn = execution.status.latest_wal_lsn,
            .freshness_lag_records = execution.status.latest_wal_lsn -| execution.session.?.manifest.wal_end_lsn,
            .query_text = execution.plan.request.text,
            .mode = execution.plan.request.mode,
            .operator = execution.plan.request.operator,
            .fusion_strategy = execution.plan.request.fusion_strategy,
            .vector_dims = if (execution.plan.request.vector) |vector| vector.len else 0,
            .sparse_term_count = if (execution.plan.request.sparse) |sparse| sparse.len else 0,
            .num_probes = execution.plan.request.num_probes,
            .actual_probe_count = execution.execution_stats.actual_probe_count,
            .actual_shortlist_count = execution.execution_stats.actual_shortlist_count,
            .quantized_candidate_count = execution.execution_stats.quantized_candidate_count,
            .exact_rerank_count = execution.execution_stats.exact_rerank_count,
            .cluster_prune_count = execution.execution_stats.cluster_prune_count,
            .count_only = execution.plan.request.count_only,
            .offset = execution.requested_offset,
            .limit = execution.requested_limit,
            .min_score = execution.plan.request.min_score,
            .hit_count = execution.hits.len,
            .hits = out_hits,
            .aggregations = aggregations,
            .enrichment = queryEnrichmentStatus(execution.status, execution.session.?.manifest.stats.document_count),
        });
    }

    fn handleTableQuerySearch(self: *HttpHandler, table_name: []const u8, body: []const u8) !HttpResponse {
        const aggregations_json = parsePublicAggregationsJsonAlloc(self.alloc, body) catch |err| switch (err) {
            error.InvalidQueryRequest => return try textResponse(self.alloc, 400, "invalid query request"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer if (aggregations_json) |json| self.alloc.free(json);

        const namespace = self.catalog.resolveTableNamespaceAlloc(table_name) catch return try textResponse(self.alloc, 404, "not found");
        defer self.alloc.free(namespace);
        var execution = self.executePublishedSearch(namespace, table_name, body) catch |err| switch (err) {
            error.InvalidQueryRequest,
            error.UnsupportedQueryRequest,
            error.EmbeddingIndexNotFound,
            error.InvalidEmbeddingDimensions,
            error.PermanentPromptFailure,
            error.TransientPromptFailure,
            error.VectorQueryRequired,
            error.VectorDimsMismatch,
            error.SparseQueryRequired,
            => return try textResponse(self.alloc, 400, "invalid query request"),
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            error.VectorSegmentNotFound => return try textResponse(self.alloc, 404, "vector segment not found"),
            error.SparseSegmentNotFound => return try textResponse(self.alloc, 404, "sparse segment not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer execution.deinit(self.alloc);

        const out_hits = try allocQueryHitsAlloc(self.alloc, execution.hits, execution.plan.request.fields, execution.plan.request.count_only);
        defer freeQueryHits(self.alloc, out_hits);

        var aggregations: ?std.json.Value = null;
        defer if (aggregations) |*value| deinitJsonValue(self.alloc, value);
        if (aggregations_json) |json| {
            aggregations = encodeServerlessAggregationsValueAlloc(self, table_name, &execution, json) catch |err| switch (err) {
                error.InvalidQueryRequest,
                error.UnsupportedQueryRequest,
                error.UnsupportedAggregation,
                error.InvalidAggregation,
                => return try textResponse(self.alloc, 400, "invalid query request"),
                else => {
                    std.log.err("table aggregations failed table={s} err={}", .{ table_name, err });
                    return try textResponse(self.alloc, 500, "query failed");
                },
            };
        }

        return try jsonResponse(self.alloc, 200, query_types.TableQuerySearchResult{
            .table_name = table_name,
            .version = execution.session.?.version(),
            .view = .published,
            .published_wal_end_lsn = execution.session.?.manifest.wal_end_lsn,
            .latest_wal_lsn = execution.status.latest_wal_lsn,
            .freshness_lag_records = execution.status.latest_wal_lsn -| execution.session.?.manifest.wal_end_lsn,
            .query_text = execution.plan.request.text,
            .mode = execution.plan.request.mode,
            .operator = execution.plan.request.operator,
            .fusion_strategy = execution.plan.request.fusion_strategy,
            .vector_dims = if (execution.plan.request.vector) |vector| vector.len else 0,
            .sparse_term_count = if (execution.plan.request.sparse) |sparse| sparse.len else 0,
            .num_probes = execution.plan.request.num_probes,
            .actual_probe_count = execution.execution_stats.actual_probe_count,
            .actual_shortlist_count = execution.execution_stats.actual_shortlist_count,
            .quantized_candidate_count = execution.execution_stats.quantized_candidate_count,
            .exact_rerank_count = execution.execution_stats.exact_rerank_count,
            .cluster_prune_count = execution.execution_stats.cluster_prune_count,
            .count_only = execution.plan.request.count_only,
            .offset = execution.requested_offset,
            .limit = execution.requested_limit,
            .min_score = execution.plan.request.min_score,
            .hit_count = execution.hits.len,
            .hits = out_hits,
            .aggregations = aggregations,
            .enrichment = queryEnrichmentStatus(execution.status, execution.session.?.manifest.stats.document_count),
        });
    }

    fn handleTableQueryGraphNeighbors(self: *HttpHandler, table_name: []const u8, body: []const u8) !HttpResponse {
        var req = query_mod.parseGraphNeighborsPlanAlloc(self.alloc, body) catch {
            return try textResponse(self.alloc, 400, "invalid graph query request");
        };
        defer req.deinit(self.alloc);

        const namespace = self.catalog.resolveTableNamespaceAlloc(table_name) catch return try textResponse(self.alloc, 404, "not found");
        defer self.alloc.free(namespace);

        var session = self.query.openHeadSession(namespace) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer session.deinit();
        return try self.tableQueryGraphNeighborsResponse(&session, table_name, namespace, req);
    }

    fn handleTableQueryGraphTraverse(self: *HttpHandler, table_name: []const u8, body: []const u8) !HttpResponse {
        var req = query_mod.parseGraphTraversePlanAlloc(self.alloc, body) catch {
            return try textResponse(self.alloc, 400, "invalid graph traverse request");
        };
        defer req.deinit(self.alloc);

        const namespace = self.catalog.resolveTableNamespaceAlloc(table_name) catch return try textResponse(self.alloc, 404, "not found");
        defer self.alloc.free(namespace);

        var session = self.query.openHeadSession(namespace) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer session.deinit();
        return try self.tableQueryGraphTraverseResponse(&session, table_name, namespace, req);
    }

    fn handleTableQueryGraphShortestPath(self: *HttpHandler, table_name: []const u8, body: []const u8) !HttpResponse {
        var req = query_mod.parseGraphShortestPathPlanAlloc(self.alloc, body) catch {
            return try textResponse(self.alloc, 400, "invalid graph shortest path request");
        };
        defer req.deinit(self.alloc);

        const namespace = self.catalog.resolveTableNamespaceAlloc(table_name) catch return try textResponse(self.alloc, 404, "not found");
        defer self.alloc.free(namespace);

        var session = self.query.openHeadSession(namespace) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer session.deinit();
        return try self.tableQueryGraphShortestPathResponse(&session, table_name, namespace, req);
    }

    fn resolveSemanticQueryRequest(self: *HttpHandler, table_name: ?[]const u8, plan: *query_mod.SearchPlan) !void {
        const semantic_search = plan.request.semantic_search orelse return;
        if (plan.request.vector != null) return error.InvalidQueryRequest;

        const index_name = (plan.vectorSource() orelse return error.InvalidQueryRequest).index_name;
        if (table_name) |name| {
            var table = (try self.catalog.getTableAlloc(self.alloc, name)) orelse return error.InvalidQueryRequest;
            defer table.deinit(self.alloc);

            var runtime = try managed_embedder.ManagedEmbedder.initFromIndexesJson(self.alloc, table.indexes_json);
            defer runtime.deinit();
            if (runtime.hasDenseEntries()) {
                plan.request.vector = if (plan.request.embedding_template) |value|
                    try runtime.embedQueryWithTemplate(self.alloc, index_name, semantic_search, value)
                else
                    try runtime.embedQuery(self.alloc, index_name, semantic_search);
                return;
            }
        }

        const runtime = self.managed_query_embedder orelse return error.InvalidQueryRequest;
        plan.request.vector = if (plan.request.embedding_template) |value|
            try runtime.embedQueryWithTemplate(self.alloc, index_name, semantic_search, value)
        else
            try runtime.embedQuery(self.alloc, index_name, semantic_search);
    }

    fn handleQueryGraphNeighbors(self: *HttpHandler, namespace: []const u8, body: []const u8) !HttpResponse {
        var req = query_mod.parseGraphNeighborsPlanAlloc(self.alloc, body) catch {
            return try textResponse(self.alloc, 400, "invalid graph query request");
        };
        defer req.deinit(self.alloc);

        var session = self.query.openHeadSession(namespace) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer session.deinit();
        return try self.queryGraphNeighborsResponse(&session, namespace, req);
    }

    fn handleQueryVersionGraphNeighbors(self: *HttpHandler, namespace: []const u8, version: u64, body: []const u8) !HttpResponse {
        var req = query_mod.parseGraphNeighborsPlanAlloc(self.alloc, body) catch {
            return try textResponse(self.alloc, 400, "invalid graph query request");
        };
        defer req.deinit(self.alloc);

        var session = self.query.openVersionSession(namespace, version) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer session.deinit();
        return try self.queryGraphNeighborsResponse(&session, namespace, req);
    }

    fn executePublicGraphQueriesAlloc(
        self: *HttpHandler,
        session: *query_mod.QuerySession,
        graph_queries: []const db_types.NamedGraphQuery,
        initial_sets: []const GraphResultSet,
    ) ![]db_types.GraphSearchResult {
        const sorted_query_indexes = try public_graph_query.sortQueriesByDependencies(self.alloc, graph_queries);
        defer self.alloc.free(sorted_query_indexes);

        var available_sets = std.ArrayListUnmanaged(GraphResultSet).empty;
        defer available_sets.deinit(self.alloc);
        try available_sets.appendSlice(self.alloc, initial_sets);

        const results = try self.alloc.alloc(db_types.GraphSearchResult, graph_queries.len);
        var initialized: usize = 0;
        errdefer {
            for (results[0..initialized]) |*result| result.deinit(self.alloc);
            if (graph_queries.len > 0) self.alloc.free(results);
        }
        for (sorted_query_indexes, 0..) |query_index, idx| {
            results[idx] = try self.executePublicGraphQueryAlloc(session, graph_queries[query_index], available_sets.items);
            try available_sets.append(self.alloc, .{
                .name = results[idx].name,
                .hits = results[idx].hits,
                .total_hits = results[idx].total_hits,
            });
            initialized += 1;
        }
        return results;
    }

    fn executePublicGraphQueryAlloc(
        self: *HttpHandler,
        session: *query_mod.QuerySession,
        named_query: db_types.NamedGraphQuery,
        available_sets: []const GraphResultSet,
    ) !db_types.GraphSearchResult {
        return switch (named_query.query.query_type) {
            .neighbors => try self.executePublicNeighborsQueryAlloc(session, named_query, available_sets),
            .traverse => try self.executePublicTraverseQueryAlloc(session, named_query, available_sets),
            .shortest_path => try self.executePublicShortestPathQueryAlloc(session, named_query, available_sets),
            .pattern => try self.executePublicPatternQueryAlloc(session, named_query, available_sets),
            else => error.UnsupportedQueryRequest,
        };
    }

    fn executePublicNeighborsQueryAlloc(
        self: *HttpHandler,
        session: *query_mod.QuerySession,
        named_query: db_types.NamedGraphQuery,
        available_sets: []const GraphResultSet,
    ) !db_types.GraphSearchResult {
        const start_keys = try public_graph_query.resolveGraphSelectorAlloc(self.alloc, named_query.query.start_nodes, available_sets);
        defer freeOwnedKeySlice(self.alloc, start_keys);

        var nodes = std.ArrayListUnmanaged(graph_query_mod.GraphResultNode).empty;
        errdefer {
            for (nodes.items) |*node| node.deinit(self.alloc);
            nodes.deinit(self.alloc);
        }
        var hits = std.ArrayListUnmanaged(db_types.SearchHit).empty;
        errdefer {
            for (hits.items) |*hit| hit.deinit(self.alloc);
            hits.deinit(self.alloc);
        }

        var seen = std.StringHashMapUnmanaged(void).empty;
        defer {
            var it = seen.keyIterator();
            while (it.next()) |key| self.alloc.free(key.*);
            seen.deinit(self.alloc);
        }

        for (start_keys) |start_key| {
            var req = query_mod.GraphNeighborsRequest{
                .index_name = try self.alloc.dupe(u8, named_query.query.index_name),
                .doc_id = try self.alloc.dupe(u8, start_key),
                .direction = toServerlessGraphDirection(named_query.query.params.direction),
                .edge_types = try dupFieldsAlloc(self.alloc, named_query.query.params.edge_types),
                .limit = named_query.query.params.max_results,
            };
            defer req.deinit(self.alloc);

            const neighbors = try query_mod.graphNeighborsAlloc(self.alloc, session, req);
            defer query_mod.freeGraphNeighbors(self.alloc, neighbors);

            for (neighbors) |neighbor| {
                if (named_query.query.params.deduplicate) {
                    const seen_key = try self.alloc.dupe(u8, neighbor.doc_id);
                    const gop = try seen.getOrPut(self.alloc, seen_key);
                    if (gop.found_existing) {
                        self.alloc.free(seen_key);
                        continue;
                    }
                }

                try nodes.append(self.alloc, .{
                    .key = try self.alloc.dupe(u8, neighbor.doc_id),
                    .depth = 1,
                    .distance = neighbor.weight,
                    .path = if (named_query.query.params.include_paths)
                        try allocPathNodes(self.alloc, &.{ start_key, neighbor.doc_id })
                    else
                        null,
                    .path_edges = if (named_query.query.params.include_paths)
                        try allocSinglePathEdgeInfo(self.alloc, start_key, neighbor.doc_id, neighbor.edge_type, neighbor.weight)
                    else
                        null,
                });
                try hits.append(self.alloc, .{
                    .id = try self.alloc.dupe(u8, neighbor.doc_id),
                    .score = neighbor.weight,
                    .stored_data = null,
                });
            }
        }

        const total_hits: u32 = @intCast(nodes.items.len);
        return .{
            .name = try self.alloc.dupe(u8, named_query.name),
            .nodes = try nodes.toOwnedSlice(self.alloc),
            .hits = try hits.toOwnedSlice(self.alloc),
            .total_hits = total_hits,
        };
    }

    fn executePublicTraverseQueryAlloc(
        self: *HttpHandler,
        session: *query_mod.QuerySession,
        named_query: db_types.NamedGraphQuery,
        available_sets: []const GraphResultSet,
    ) !db_types.GraphSearchResult {
        const start_keys = try public_graph_query.resolveGraphSelectorAlloc(self.alloc, named_query.query.start_nodes, available_sets);
        defer freeOwnedKeySlice(self.alloc, start_keys);

        var nodes = std.ArrayListUnmanaged(graph_query_mod.GraphResultNode).empty;
        errdefer {
            for (nodes.items) |*node| node.deinit(self.alloc);
            nodes.deinit(self.alloc);
        }
        var hits = std.ArrayListUnmanaged(db_types.SearchHit).empty;
        errdefer {
            for (hits.items) |*hit| hit.deinit(self.alloc);
            hits.deinit(self.alloc);
        }

        var seen = std.StringHashMapUnmanaged(void).empty;
        defer {
            var it = seen.keyIterator();
            while (it.next()) |key| self.alloc.free(key.*);
            seen.deinit(self.alloc);
        }

        for (start_keys) |start_key| {
            var req = query_mod.GraphTraverseRequest{
                .index_name = try self.alloc.dupe(u8, named_query.query.index_name),
                .start_doc_id = try self.alloc.dupe(u8, start_key),
                .direction = toServerlessGraphDirection(named_query.query.params.direction),
                .edge_types = try dupFieldsAlloc(self.alloc, named_query.query.params.edge_types),
                .max_depth = named_query.query.params.max_depth,
                .limit = named_query.query.params.max_results,
                .include_start = false,
            };
            defer req.deinit(self.alloc);

            const traversal_nodes = try query_mod.graphTraverseAlloc(self.alloc, session, req);
            defer query_mod.freeGraphTraversalNodes(self.alloc, traversal_nodes);

            for (traversal_nodes) |node| {
                if (named_query.query.params.deduplicate) {
                    const seen_key = try self.alloc.dupe(u8, node.doc_id);
                    const gop = try seen.getOrPut(self.alloc, seen_key);
                    if (gop.found_existing) {
                        self.alloc.free(seen_key);
                        continue;
                    }
                }

                try nodes.append(self.alloc, .{
                    .key = try self.alloc.dupe(u8, node.doc_id),
                    .depth = node.depth,
                    .distance = traversalDistance(node),
                    .path = if (named_query.query.params.include_paths and node.path != null)
                        try normalizeTraversalPathAlloc(self.alloc, start_key, node.path.?)
                    else
                        null,
                    .path_edges = if (named_query.query.params.include_paths and node.edge_path != null)
                        try allocPathEdgeInfos(self.alloc, node.edge_path.?)
                    else
                        null,
                });
                try hits.append(self.alloc, .{
                    .id = try self.alloc.dupe(u8, node.doc_id),
                    .score = @floatCast(traversalDistance(node)),
                    .stored_data = null,
                });
            }
        }

        const total_hits: u32 = @intCast(nodes.items.len);
        return .{
            .name = try self.alloc.dupe(u8, named_query.name),
            .nodes = try nodes.toOwnedSlice(self.alloc),
            .hits = try hits.toOwnedSlice(self.alloc),
            .total_hits = total_hits,
        };
    }

    fn executePublicShortestPathQueryAlloc(
        self: *HttpHandler,
        session: *query_mod.QuerySession,
        named_query: db_types.NamedGraphQuery,
        available_sets: []const GraphResultSet,
    ) !db_types.GraphSearchResult {
        const start_keys = try public_graph_query.resolveGraphSelectorAlloc(self.alloc, named_query.query.start_nodes, available_sets);
        defer freeOwnedKeySlice(self.alloc, start_keys);
        const target_selector = named_query.query.target_nodes orelse return error.UnsupportedQueryRequest;
        const target_keys = try public_graph_query.resolveGraphSelectorAlloc(self.alloc, target_selector, available_sets);
        defer freeOwnedKeySlice(self.alloc, target_keys);

        var paths = std.ArrayListUnmanaged(db_types.GraphPath).empty;
        errdefer {
            for (paths.items) |path| graph_paths.freePath(self.alloc, path);
            paths.deinit(self.alloc);
        }

        for (start_keys) |start_key| {
            for (target_keys) |target_key| {
                var req = query_mod.GraphShortestPathRequest{
                    .index_name = try self.alloc.dupe(u8, named_query.query.index_name),
                    .start_doc_id = try self.alloc.dupe(u8, start_key),
                    .end_doc_id = try self.alloc.dupe(u8, target_key),
                    .direction = toServerlessGraphDirection(named_query.query.params.direction),
                    .edge_types = try dupFieldsAlloc(self.alloc, named_query.query.params.edge_types),
                    .max_depth = named_query.query.params.max_depth,
                };
                defer req.deinit(self.alloc);

                const maybe_path = try query_mod.graphShortestPathAlloc(self.alloc, session, req);
                if (maybe_path) |owned_path| {
                    var path = owned_path;
                    defer query_mod.freeGraphShortestPath(self.alloc, &path);
                    try paths.append(self.alloc, try toDbGraphPath(self.alloc, path));
                }
            }
        }

        const total_hits: u32 = @intCast(paths.items.len);
        return .{
            .name = try self.alloc.dupe(u8, named_query.name),
            .paths = try paths.toOwnedSlice(self.alloc),
            .hits = &.{},
            .total_hits = total_hits,
        };
    }

    fn executePublicPatternQueryAlloc(
        self: *HttpHandler,
        session: *query_mod.QuerySession,
        named_query: db_types.NamedGraphQuery,
        available_sets: []const GraphResultSet,
    ) !db_types.GraphSearchResult {
        const start_keys = try public_graph_query.resolveGraphSelectorAlloc(self.alloc, named_query.query.start_nodes, available_sets);
        defer freeOwnedKeySlice(self.alloc, start_keys);
        const start_key_refs = try castOwnedKeysToConst(self.alloc, start_keys);
        defer self.alloc.free(start_key_refs);

        const need_docs = named_query.query.include_documents or patternRequiresDocumentFilter(named_query.query.pattern);
        const docs: []const query_materializer.Document = if (need_docs) try self.allocPublishedDocumentsAlloc(session) else &.{};
        defer if (need_docs) query_materializer.freeDocuments(self.alloc, @constCast(docs));

        const graph_index = query_mod.graph_reader.findGraphArtifactIndex(session, named_query.query.index_name) orelse return error.GraphSegmentNotFound;
        try session.warmArtifact(graph_index);
        const payload = try session.fetchArtifactAlloc(graph_index);
        defer self.alloc.free(payload);
        var segment = try graph_segment_mod.decodeAlloc(self.alloc, payload);
        defer graph_segment_mod.freeSegment(self.alloc, &segment);

        const ServerlessPatternEdgeReader = struct {
            segment: *const graph_segment_mod.Segment,

            pub fn getEdges(reader: @This(), alloc: Allocator, key: []const u8, direction: graph_mod.EdgeDirection) ![]graph_mod.Edge {
                const adjacency = findGraphSegmentAdjacency(reader.segment.adjacencies, key) orelse return try alloc.alloc(graph_mod.Edge, 0);
                const edge_count: usize = switch (direction) {
                    .out => adjacency.out_edges.len,
                    .in => adjacency.in_edges.len,
                    .both => adjacency.out_edges.len + adjacency.in_edges.len,
                };
                const edges = try alloc.alloc(graph_mod.Edge, edge_count);
                var initialized: usize = 0;
                errdefer {
                    for (edges[0..initialized]) |edge| freeOwnedGraphEdge(alloc, edge);
                    if (edges.len > 0) alloc.free(edges);
                }

                if (direction == .out or direction == .both) {
                    for (adjacency.out_edges) |edge| {
                        edges[initialized] = .{
                            .source = try alloc.dupe(u8, adjacency.node_id),
                            .target = try alloc.dupe(u8, edge.neighbor_id),
                            .edge_type = try alloc.dupe(u8, edge.edge_type),
                            .weight = edge.weight,
                            .created_at = 0,
                            .updated_at = 0,
                            .metadata = &.{},
                        };
                        initialized += 1;
                    }
                }
                if (direction == .in or direction == .both) {
                    for (adjacency.in_edges) |edge| {
                        edges[initialized] = .{
                            .source = try alloc.dupe(u8, edge.neighbor_id),
                            .target = try alloc.dupe(u8, adjacency.node_id),
                            .edge_type = try alloc.dupe(u8, edge.edge_type),
                            .weight = edge.weight,
                            .created_at = 0,
                            .updated_at = 0,
                            .metadata = &.{},
                        };
                        initialized += 1;
                    }
                }
                return edges;
            }

            pub fn freeEdges(_: @This(), alloc: Allocator, edges: []graph_mod.Edge) void {
                for (edges) |edge| freeOwnedGraphEdge(alloc, edge);
                if (edges.len > 0) alloc.free(edges);
            }
        };

        var filter_ctx = PatternDocumentFilterContext{
            .alloc = self.alloc,
            .docs = docs,
        };

        const raw_matches = try graph_pattern_mod.matchPatternWithEdgeReader(
            self.alloc,
            ServerlessPatternEdgeReader{ .segment = &segment },
            start_key_refs,
            named_query.query.pattern,
            .{
                .max_results = named_query.query.params.max_results,
                .return_aliases = named_query.query.return_aliases,
                .evaluator = if (need_docs) .{
                    .ctx = @ptrCast(&filter_ctx),
                    .func = publishedPatternNodeFilterEvaluator,
                } else null,
            },
        );
        defer graph_pattern_mod.freeMatches(self.alloc, raw_matches);

        const matches = try self.convertPatternMatchesToGraphMatchesAlloc(raw_matches);
        errdefer {
            for (matches) |*match| match.deinit(self.alloc);
            if (matches.len > 0) self.alloc.free(matches);
        }

        const hits = try self.buildPatternDocumentHitsAlloc(named_query.query, matches, if (need_docs) docs else null);
        errdefer {
            for (hits) |*hit| hit.deinit(self.alloc);
            if (hits.len > 0) self.alloc.free(hits);
        }

        return .{
            .name = try self.alloc.dupe(u8, named_query.name),
            .nodes = &.{},
            .paths = &.{},
            .matches = matches,
            .hits = hits,
            .total_hits = @intCast(raw_matches.len),
        };
    }

    fn convertPatternMatchesToGraphMatchesAlloc(
        self: *HttpHandler,
        raw_matches: []const graph_pattern_mod.PatternMatch,
    ) ![]db_types.GraphPatternMatch {
        const matches = try self.alloc.alloc(db_types.GraphPatternMatch, raw_matches.len);
        var initialized: usize = 0;
        errdefer {
            for (matches[0..initialized]) |*match| match.deinit(self.alloc);
            if (matches.len > 0) self.alloc.free(matches);
        }

        for (raw_matches, 0..) |raw_match, i| {
            const bindings = try self.alloc.alloc(db_types.GraphPatternBinding, raw_match.bindings.len);
            var initialized_bindings: usize = 0;
            errdefer {
                for (bindings[0..initialized_bindings]) |*binding| binding.deinit(self.alloc);
                if (bindings.len > 0) self.alloc.free(bindings);
            }
            for (raw_match.bindings, 0..) |binding, binding_index| {
                bindings[binding_index] = .{
                    .alias = try self.alloc.dupe(u8, binding.alias),
                    .node = .{
                        .key = try self.alloc.dupe(u8, binding.key),
                        .depth = binding.depth,
                        .distance = @floatFromInt(binding.depth),
                        .path = null,
                        .path_edges = null,
                    },
                };
                initialized_bindings += 1;
            }

            const path = try self.alloc.alloc(graph_query_mod.PathEdgeInfo, raw_match.path.len);
            var initialized_path: usize = 0;
            errdefer {
                for (path[0..initialized_path]) |edge| {
                    self.alloc.free(edge.source);
                    self.alloc.free(edge.target);
                    self.alloc.free(edge.edge_type);
                }
                if (path.len > 0) self.alloc.free(path);
            }
            for (raw_match.path, 0..) |edge, edge_index| {
                path[edge_index] = .{
                    .source = try self.alloc.dupe(u8, edge.source),
                    .target = try self.alloc.dupe(u8, edge.target),
                    .edge_type = try self.alloc.dupe(u8, edge.edge_type),
                    .weight = edge.weight,
                };
                initialized_path += 1;
            }

            matches[i] = .{
                .bindings = bindings,
                .path = path,
            };
            initialized += 1;
        }

        return matches;
    }

    fn buildPatternDocumentHitsAlloc(
        self: *HttpHandler,
        query: graph_query_mod.GraphQuery,
        matches: []const db_types.GraphPatternMatch,
        docs_override: ?[]const query_materializer.Document,
    ) ![]db_types.SearchHit {
        const docs = if (query.include_documents) docs_override orelse return error.InvalidArgument else &.{};

        var hits = std.ArrayListUnmanaged(db_types.SearchHit).empty;
        errdefer {
            for (hits.items) |*hit| hit.deinit(self.alloc);
            hits.deinit(self.alloc);
        }

        var seen = std.StringHashMapUnmanaged(void).empty;
        defer {
            var it = seen.keyIterator();
            while (it.next()) |key| self.alloc.free(key.*);
            seen.deinit(self.alloc);
        }

        for (matches) |match| {
            for (match.bindings) |binding| {
                if (seen.contains(binding.node.key)) continue;
                try seen.put(self.alloc, try self.alloc.dupe(u8, binding.node.key), {});
                const body = if (query.include_documents)
                    if (findMaterializedDocumentBody(docs, binding.node.key)) |stored|
                        if (query.include_all_fields)
                            try self.alloc.dupe(u8, stored)
                        else blk: {
                            const projected_fields = try dupFieldsAlloc(self.alloc, query.fields);
                            defer freeFields(self.alloc, projected_fields);
                            break :blk try projectBodyFieldsAlloc(self.alloc, stored, projected_fields.?);
                        }
                    else
                        null
                else
                    null;
                try hits.append(self.alloc, .{
                    .id = try self.alloc.dupe(u8, binding.node.key),
                    .score = @floatCast(binding.node.distance),
                    .stored_data = body,
                });
            }
        }

        return try hits.toOwnedSlice(self.alloc);
    }

    fn queryGraphNeighborsResponse(self: *HttpHandler, session: *query_mod.QuerySession, namespace: []const u8, req: query_mod.GraphNeighborsRequest) !HttpResponse {
        return self.graphNeighborsResponseImpl(session, namespace, null, req);
    }

    fn tableQueryGraphNeighborsResponse(self: *HttpHandler, session: *query_mod.QuerySession, table_name: []const u8, namespace: []const u8, req: query_mod.GraphNeighborsRequest) !HttpResponse {
        return self.graphNeighborsResponseImpl(session, namespace, table_name, req);
    }

    fn graphNeighborsResponseImpl(self: *HttpHandler, session: *query_mod.QuerySession, namespace: []const u8, table_name: ?[]const u8, req: query_mod.GraphNeighborsRequest) !HttpResponse {
        var status = self.catalog.buildStatus(namespace) catch return try textResponse(self.alloc, 500, "query failed");
        defer status.deinit(self.alloc);
        const neighbors = query_mod.graphNeighborsAlloc(self.alloc, session, req) catch |err| switch (err) {
            error.GraphSegmentNotFound => return try textResponse(self.alloc, 404, "graph segment not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer query_mod.freeGraphNeighbors(self.alloc, neighbors);

        const out_neighbors = try self.alloc.alloc(query_types.GraphNeighbor, neighbors.len);
        errdefer self.alloc.free(out_neighbors);
        for (neighbors, 0..) |neighbor, idx| {
            out_neighbors[idx] = .{
                .doc_id = try self.alloc.dupe(u8, neighbor.doc_id),
                .edge_type = try self.alloc.dupe(u8, neighbor.edge_type),
                .weight = neighbor.weight,
                .direction = neighbor.direction,
            };
        }
        defer freeGraphNeighbors(self.alloc, out_neighbors);

        if (table_name) |tn| {
            return try jsonResponse(self.alloc, 200, query_types.TableGraphNeighborsResult{
                .table_name = tn,
                .version = session.version(),
                .published_wal_end_lsn = session.manifest.wal_end_lsn,
                .latest_wal_lsn = status.latest_wal_lsn,
                .freshness_lag_records = status.latest_wal_lsn -| session.manifest.wal_end_lsn,
                .node_id = req.doc_id,
                .direction = req.direction,
                .edge_type = firstEdgeType(req.edge_types),
                .limit = req.limit,
                .neighbor_count = out_neighbors.len,
                .neighbors = out_neighbors,
            });
        }
        return try jsonResponse(self.alloc, 200, query_types.GraphNeighborsResult{
            .namespace = session.namespace(),
            .version = session.version(),
            .published_wal_end_lsn = session.manifest.wal_end_lsn,
            .latest_wal_lsn = status.latest_wal_lsn,
            .freshness_lag_records = status.latest_wal_lsn -| session.manifest.wal_end_lsn,
            .node_id = req.doc_id,
            .direction = req.direction,
            .edge_type = firstEdgeType(req.edge_types),
            .limit = req.limit,
            .neighbor_count = out_neighbors.len,
            .neighbors = out_neighbors,
        });
    }

    fn handleQueryGraphTraverse(self: *HttpHandler, namespace: []const u8, body: []const u8) !HttpResponse {
        var req = query_mod.parseGraphTraversePlanAlloc(self.alloc, body) catch {
            return try textResponse(self.alloc, 400, "invalid graph traverse request");
        };
        defer req.deinit(self.alloc);

        var session = self.query.openHeadSession(namespace) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer session.deinit();
        return try self.queryGraphTraverseResponse(&session, namespace, req);
    }

    fn handleQueryVersionGraphTraverse(self: *HttpHandler, namespace: []const u8, version: u64, body: []const u8) !HttpResponse {
        var req = query_mod.parseGraphTraversePlanAlloc(self.alloc, body) catch {
            return try textResponse(self.alloc, 400, "invalid graph traverse request");
        };
        defer req.deinit(self.alloc);

        var session = self.query.openVersionSession(namespace, version) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer session.deinit();
        return try self.queryGraphTraverseResponse(&session, namespace, req);
    }

    fn handleQueryGraphShortestPath(self: *HttpHandler, namespace: []const u8, body: []const u8) !HttpResponse {
        var req = query_mod.parseGraphShortestPathPlanAlloc(self.alloc, body) catch {
            return try textResponse(self.alloc, 400, "invalid graph shortest path request");
        };
        defer req.deinit(self.alloc);

        var session = self.query.openHeadSession(namespace) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer session.deinit();
        return try self.queryGraphShortestPathResponse(&session, namespace, req);
    }

    fn handleQueryVersionGraphShortestPath(self: *HttpHandler, namespace: []const u8, version: u64, body: []const u8) !HttpResponse {
        var req = query_mod.parseGraphShortestPathPlanAlloc(self.alloc, body) catch {
            return try textResponse(self.alloc, 400, "invalid graph shortest path request");
        };
        defer req.deinit(self.alloc);

        var session = self.query.openVersionSession(namespace, version) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer session.deinit();
        return try self.queryGraphShortestPathResponse(&session, namespace, req);
    }

    fn queryGraphTraverseResponse(self: *HttpHandler, session: *query_mod.QuerySession, namespace: []const u8, req: query_mod.GraphTraverseRequest) !HttpResponse {
        return self.graphTraverseResponseImpl(session, namespace, null, req);
    }

    fn tableQueryGraphTraverseResponse(self: *HttpHandler, session: *query_mod.QuerySession, table_name: []const u8, namespace: []const u8, req: query_mod.GraphTraverseRequest) !HttpResponse {
        return self.graphTraverseResponseImpl(session, namespace, table_name, req);
    }

    fn graphTraverseResponseImpl(self: *HttpHandler, session: *query_mod.QuerySession, namespace: []const u8, table_name: ?[]const u8, req: query_mod.GraphTraverseRequest) !HttpResponse {
        var status = self.catalog.buildStatus(namespace) catch return try textResponse(self.alloc, 500, "query failed");
        defer status.deinit(self.alloc);
        const nodes = query_mod.graphTraverseAlloc(self.alloc, session, req) catch |err| switch (err) {
            error.GraphSegmentNotFound => return try textResponse(self.alloc, 404, "graph segment not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer query_mod.freeGraphTraversalNodes(self.alloc, nodes);

        const out_nodes = try self.alloc.alloc(query_types.GraphTraversalNode, nodes.len);
        errdefer self.alloc.free(out_nodes);
        for (nodes, 0..) |node, idx| {
            out_nodes[idx] = .{
                .doc_id = try self.alloc.dupe(u8, node.doc_id),
                .depth = node.depth,
                .parent_doc_id = if (node.parent_doc_id) |value| try self.alloc.dupe(u8, value) else null,
                .via_edge_type = if (node.via_edge_type) |value| try self.alloc.dupe(u8, value) else null,
                .path = if (node.path) |path| try dupGraphPathAlloc(self.alloc, path) else null,
                .edge_path = if (node.edge_path) |path| try dupGraphEdgePathAlloc(self.alloc, path) else null,
            };
        }
        defer freeGraphTraversalNodes(self.alloc, out_nodes);

        if (table_name) |tn| {
            return try jsonResponse(self.alloc, 200, query_types.TableGraphTraverseResult{
                .table_name = tn,
                .version = session.version(),
                .published_wal_end_lsn = session.manifest.wal_end_lsn,
                .latest_wal_lsn = status.latest_wal_lsn,
                .freshness_lag_records = status.latest_wal_lsn -| session.manifest.wal_end_lsn,
                .start_node_id = req.start_doc_id,
                .direction = req.direction,
                .edge_type = firstEdgeType(req.edge_types),
                .max_depth = req.max_depth,
                .limit = req.limit,
                .node_count = out_nodes.len,
                .nodes = out_nodes,
            });
        }
        return try jsonResponse(self.alloc, 200, query_types.GraphTraverseResult{
            .namespace = session.namespace(),
            .version = session.version(),
            .published_wal_end_lsn = session.manifest.wal_end_lsn,
            .latest_wal_lsn = status.latest_wal_lsn,
            .freshness_lag_records = status.latest_wal_lsn -| session.manifest.wal_end_lsn,
            .start_node_id = req.start_doc_id,
            .direction = req.direction,
            .edge_type = firstEdgeType(req.edge_types),
            .max_depth = req.max_depth,
            .limit = req.limit,
            .node_count = out_nodes.len,
            .nodes = out_nodes,
        });
    }

    fn queryGraphShortestPathResponse(self: *HttpHandler, session: *query_mod.QuerySession, namespace: []const u8, req: query_mod.GraphShortestPathRequest) !HttpResponse {
        return self.graphShortestPathResponseImpl(session, namespace, null, req);
    }

    fn tableQueryGraphShortestPathResponse(self: *HttpHandler, session: *query_mod.QuerySession, table_name: []const u8, namespace: []const u8, req: query_mod.GraphShortestPathRequest) !HttpResponse {
        return self.graphShortestPathResponseImpl(session, namespace, table_name, req);
    }

    fn graphShortestPathResponseImpl(self: *HttpHandler, session: *query_mod.QuerySession, namespace: []const u8, table_name: ?[]const u8, req: query_mod.GraphShortestPathRequest) !HttpResponse {
        var status = self.catalog.buildStatus(namespace) catch return try textResponse(self.alloc, 500, "query failed");
        defer status.deinit(self.alloc);
        const maybe_path = query_mod.graphShortestPathAlloc(self.alloc, session, req) catch |err| switch (err) {
            error.GraphSegmentNotFound => return try textResponse(self.alloc, 404, "graph segment not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };

        if (maybe_path) |owned_path| {
            var path = owned_path;
            defer query_mod.freeGraphShortestPath(self.alloc, &path);

            const out_node_path = try dupGraphPathAlloc(self.alloc, path.path);
            defer freeGraphPath(self.alloc, out_node_path);
            const out_edge_path = try dupGraphEdgePathAlloc(self.alloc, path.edge_path);
            defer freeGraphEdgePath(self.alloc, out_edge_path);

            if (table_name) |tn| {
                return try jsonResponse(self.alloc, 200, query_types.TableGraphShortestPathResult{
                    .table_name = tn,
                    .version = session.version(),
                    .published_wal_end_lsn = session.manifest.wal_end_lsn,
                    .latest_wal_lsn = status.latest_wal_lsn,
                    .freshness_lag_records = status.latest_wal_lsn -| session.manifest.wal_end_lsn,
                    .start_node_id = req.start_doc_id,
                    .end_node_id = req.end_doc_id,
                    .direction = req.direction,
                    .edge_type = firstEdgeType(req.edge_types),
                    .max_depth = req.max_depth,
                    .found = true,
                    .depth = path.depth,
                    .node_path = out_node_path,
                    .edge_path = out_edge_path,
                });
            }
            return try jsonResponse(self.alloc, 200, query_types.GraphShortestPathResult{
                .namespace = session.namespace(),
                .version = session.version(),
                .published_wal_end_lsn = session.manifest.wal_end_lsn,
                .latest_wal_lsn = status.latest_wal_lsn,
                .freshness_lag_records = status.latest_wal_lsn -| session.manifest.wal_end_lsn,
                .start_node_id = req.start_doc_id,
                .end_node_id = req.end_doc_id,
                .direction = req.direction,
                .edge_type = firstEdgeType(req.edge_types),
                .max_depth = req.max_depth,
                .found = true,
                .depth = path.depth,
                .node_path = out_node_path,
                .edge_path = out_edge_path,
            });
        }

        if (table_name) |tn| {
            return try jsonResponse(self.alloc, 200, query_types.TableGraphShortestPathResult{
                .table_name = tn,
                .version = session.version(),
                .published_wal_end_lsn = session.manifest.wal_end_lsn,
                .latest_wal_lsn = status.latest_wal_lsn,
                .freshness_lag_records = status.latest_wal_lsn -| session.manifest.wal_end_lsn,
                .start_node_id = req.start_doc_id,
                .end_node_id = req.end_doc_id,
                .direction = req.direction,
                .edge_type = firstEdgeType(req.edge_types),
                .max_depth = req.max_depth,
                .found = false,
            });
        }
        return try jsonResponse(self.alloc, 200, query_types.GraphShortestPathResult{
            .namespace = session.namespace(),
            .version = session.version(),
            .published_wal_end_lsn = session.manifest.wal_end_lsn,
            .latest_wal_lsn = status.latest_wal_lsn,
            .freshness_lag_records = status.latest_wal_lsn -| session.manifest.wal_end_lsn,
            .start_node_id = req.start_doc_id,
            .end_node_id = req.end_doc_id,
            .direction = req.direction,
            .edge_type = firstEdgeType(req.edge_types),
            .max_depth = req.max_depth,
            .found = false,
        });
    }

    fn handleQueryHead(self: *HttpHandler, namespace: []const u8) !HttpResponse {
        var session = self.query.openHeadSession(namespace) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer session.deinit();
        return try self.querySessionResponse(&session, .published, &.{});
    }

    fn handleQueryLatest(self: *HttpHandler, namespace: []const u8) !HttpResponse {
        var session = self.query.openHeadSession(namespace) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer session.deinit();

        const records = self.api.wal.readFromAlloc(namespace, session.manifest.wal_end_lsn + 1) catch return try textResponse(self.alloc, 500, "query failed");
        defer wal_mod.freeRecords(self.alloc, records);

        const tail = try allocTailMutations(self.alloc, records);
        defer freeQueryMutations(self.alloc, tail);
        return try self.querySessionResponse(&session, .latest, tail);
    }

    fn handleQueryVersion(self: *HttpHandler, namespace: []const u8, version: u64) !HttpResponse {
        var session = self.query.openVersionSession(namespace, version) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer session.deinit();
        return try self.querySessionResponse(&session, .published, &.{});
    }

    fn handleQueryHeadArtifact(self: *HttpHandler, namespace: []const u8, artifact_index: usize) !HttpResponse {
        var session = self.query.openHeadSession(namespace) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer session.deinit();
        return try self.queryArtifactResponse(&session, artifact_index);
    }

    fn handleQueryVersionArtifact(self: *HttpHandler, namespace: []const u8, version: u64, artifact_index: usize) !HttpResponse {
        var session = self.query.openVersionSession(namespace, version) catch |err| switch (err) {
            error.FileNotFound => return try textResponse(self.alloc, 404, "not found"),
            else => return try textResponse(self.alloc, 500, "query failed"),
        };
        defer session.deinit();
        return try self.queryArtifactResponse(&session, artifact_index);
    }

    fn querySessionResponse(self: *HttpHandler, session: *query_mod.QuerySession, view: query_types.QueryView, tail: []const query_types.QueryTailMutation) !HttpResponse {
        var status = self.catalog.buildStatus(session.namespace()) catch return try textResponse(self.alloc, 500, "query failed");
        defer status.deinit(self.alloc);
        const artifacts = try self.alloc.alloc(query_types.QueryArtifactSummary, session.artifactCount());
        defer self.alloc.free(artifacts);

        for (artifacts, 0..) |*artifact, idx| {
            const artifact_ref = session.artifactRef(idx) orelse return try textResponse(self.alloc, 500, "query failed");
            artifact.* = .{
                .index = idx,
                .kind = artifact_ref.kind,
                .artifact_id = artifact_ref.artifact_id,
                .byte_len = artifact_ref.byte_len,
                .checksum = artifact_ref.checksum,
                .search_sources = artifactSearchSources(artifact_ref.kind, session.manifest.stats.published_search_sources),
                .materialized_derived_outputs = artifactDerivedOutputs(artifact_ref.kind, session.manifest.stats.derived_outputs),
            };
        }

        const documents = try self.materializeDocumentsAlloc(session, tail);
        defer freeQueryDocuments(self.alloc, documents);

        return try jsonResponse(self.alloc, 200, query_types.QueryResult{
            .namespace = session.namespace(),
            .version = session.version(),
            .view = view,
            .materialized_search_sources = status.materialized_search_sources,
            .materialized_derived_outputs = status.materialized_derived_outputs,
            .published_wal_end_lsn = session.manifest.wal_end_lsn,
            .visible_wal_end_lsn = if (tail.len == 0) session.manifest.wal_end_lsn else tail[tail.len - 1].lsn,
            .latest_wal_lsn = status.latest_wal_lsn,
            .freshness_lag_records = status.latest_wal_lsn -| (if (tail.len == 0) session.manifest.wal_end_lsn else tail[tail.len - 1].lsn),
            .artifact_count = artifacts.len,
            .artifacts = artifacts,
            .document_count = documents.len,
            .documents = documents,
            .overlay_mutation_count = tail.len,
            .publication = queryPublicationStatus(status),
            .enrichment = queryEnrichmentStatus(status, session.manifest.stats.document_count),
        });
    }

    fn tableQuerySessionResponse(
        self: *HttpHandler,
        table_name: []const u8,
        session: *query_mod.QuerySession,
        view: query_types.QueryView,
        tail: []const query_types.QueryTailMutation,
    ) !HttpResponse {
        var result = try self.buildTableQuerySessionResultAlloc(table_name, session, view, tail);
        defer freeTableQueryResult(self.alloc, &result);
        return try jsonResponse(self.alloc, 200, result);
    }

    fn buildTableQuerySessionResultAlloc(
        self: *HttpHandler,
        table_name: []const u8,
        session: *query_mod.QuerySession,
        view: query_types.QueryView,
        tail: []const query_types.QueryTailMutation,
    ) !query_types.TableQueryResult {
        var status = self.catalog.buildStatus(session.namespace()) catch return error.InternalQueryFailure;
        defer status.deinit(self.alloc);
        var materialized_search_sources = try search_sources.clonePublishedSearchSourcesAlloc(self.alloc, status.materialized_search_sources);
        errdefer search_sources.deinitPublishedSearchSources(self.alloc, &materialized_search_sources);
        var materialized_derived_outputs = try search_sources.cloneMaterializedDerivedOutputsAlloc(self.alloc, status.materialized_derived_outputs);
        errdefer search_sources.deinitMaterializedDerivedOutputs(self.alloc, &materialized_derived_outputs);
        var publication = try cloneQueryPublicationStatusAlloc(self.alloc, status);
        errdefer deinitQueryPublicationStatus(self.alloc, &publication);
        const artifacts = try self.alloc.alloc(query_types.QueryArtifactSummary, session.artifactCount());
        errdefer self.alloc.free(artifacts);

        for (artifacts, 0..) |*artifact, idx| {
            const artifact_ref = session.artifactRef(idx) orelse return error.InternalQueryFailure;
            artifact.* = .{
                .index = idx,
                .kind = artifact_ref.kind,
                .artifact_id = artifact_ref.artifact_id,
                .byte_len = artifact_ref.byte_len,
                .checksum = artifact_ref.checksum,
                .search_sources = artifactSearchSources(artifact_ref.kind, session.manifest.stats.published_search_sources),
                .materialized_derived_outputs = artifactDerivedOutputs(artifact_ref.kind, session.manifest.stats.derived_outputs),
            };
        }

        const documents = try self.materializeDocumentsAlloc(session, tail);
        errdefer freeQueryDocuments(self.alloc, documents);

        return .{
            .table_name = table_name,
            .version = session.version(),
            .view = view,
            .materialized_search_sources = materialized_search_sources,
            .materialized_derived_outputs = materialized_derived_outputs,
            .published_wal_end_lsn = session.manifest.wal_end_lsn,
            .visible_wal_end_lsn = if (tail.len == 0) session.manifest.wal_end_lsn else tail[tail.len - 1].lsn,
            .latest_wal_lsn = status.latest_wal_lsn,
            .freshness_lag_records = status.latest_wal_lsn -| (if (tail.len == 0) session.manifest.wal_end_lsn else tail[tail.len - 1].lsn),
            .artifact_count = artifacts.len,
            .artifacts = artifacts,
            .document_count = documents.len,
            .documents = documents,
            .overlay_mutation_count = tail.len,
            .publication = publication,
            .enrichment = queryEnrichmentStatus(status, session.manifest.stats.document_count),
        };
    }

    fn encodeTableQuerySessionResponseJsonAlloc(
        self: *HttpHandler,
        table_name: []const u8,
        session: *query_mod.QuerySession,
        view: query_types.QueryView,
        tail: []const query_types.QueryTailMutation,
    ) ![]u8 {
        var result = try self.buildTableQuerySessionResultAlloc(table_name, session, view, tail);
        defer freeTableQueryResult(self.alloc, &result);
        const payload = try std.json.Stringify.valueAlloc(self.alloc, result, .{});
        return payload;
    }

    fn queryArtifactResponse(self: *HttpHandler, session: *query_mod.QuerySession, artifact_index: usize) !HttpResponse {
        const artifact_ref = session.artifactRef(artifact_index) orelse return try textResponse(self.alloc, 404, "not found");
        const mutations = self.allocArtifactMutations(session, artifact_index) catch return try textResponse(self.alloc, 500, "query failed");
        defer freeQueryMutations(self.alloc, mutations);
        const documents = self.allocArtifactDocuments(session, artifact_index) catch return try textResponse(self.alloc, 500, "query failed");
        defer freeQueryDocuments(self.alloc, documents);

        return try jsonResponse(self.alloc, 200, query_types.QueryArtifactResult{
            .namespace = session.namespace(),
            .version = session.version(),
            .artifact = .{
                .index = artifact_index,
                .kind = artifact_ref.kind,
                .artifact_id = artifact_ref.artifact_id,
                .byte_len = artifact_ref.byte_len,
                .checksum = artifact_ref.checksum,
                .search_sources = artifactSearchSources(artifact_ref.kind, session.manifest.stats.published_search_sources),
                .materialized_derived_outputs = artifactDerivedOutputs(artifact_ref.kind, session.manifest.stats.derived_outputs),
                .mutations = mutations,
                .documents = documents,
            },
        });
    }

    fn artifactSearchSources(
        artifact_kind: manifest_mod.ArtifactKind,
        published_search_sources: search_sources.PublishedSearchSources,
    ) search_sources.PublishedSearchSources {
        return search_sources.publishedSearchSourcesForArtifactKind(artifact_kind, published_search_sources);
    }

    fn artifactDerivedOutputs(
        artifact_kind: manifest_mod.ArtifactKind,
        outputs: search_sources.MaterializedDerivedOutputs,
    ) search_sources.MaterializedDerivedOutputs {
        return search_sources.materializedDerivedOutputsForArtifactKind(artifact_kind, outputs);
    }

    fn allocSessionEntries(self: *HttpHandler, session: *query_mod.QuerySession) ![]query_types.QueryMutation {
        var out = std.ArrayListUnmanaged(query_types.QueryMutation).empty;
        errdefer freeQueryMutations(self.alloc, out.items);

        for (0..session.artifactCount()) |artifact_index| {
            const artifact_ref = session.artifactRef(artifact_index) orelse continue;
            if (artifact_ref.kind != .mutation_segment) continue;

            const entries = try self.allocArtifactMutations(session, artifact_index);
            defer freeQueryMutations(self.alloc, entries);
            for (entries) |entry| {
                const doc_id = try self.alloc.dupe(u8, entry.doc_id);
                errdefer self.alloc.free(doc_id);
                const body = if (entry.body) |value| try self.alloc.dupe(u8, value) else null;
                errdefer if (body) |value| self.alloc.free(value);
                try out.append(self.alloc, .{
                    .lsn = entry.lsn,
                    .timestamp_ns = entry.timestamp_ns,
                    .kind = entry.kind,
                    .doc_id = doc_id,
                    .body = body,
                });
            }
        }

        return try out.toOwnedSlice(self.alloc);
    }

    fn materializeDocumentsAlloc(self: *HttpHandler, session: *query_mod.QuerySession, tail: []const query_types.QueryTailMutation) ![]query_types.QueryDocument {
        const published = try self.allocPublishedDocumentsAlloc(session);
        defer query_materializer.freeDocuments(self.alloc, published);

        const materialized = if (tail.len == 0)
            try cloneMaterializedDocumentsAlloc(self.alloc, published)
        else blk: {
            const overlay = try allocTailMaterializerMutations(self.alloc, tail);
            defer freeMaterializerMutations(self.alloc, overlay);
            break :blk try query_materializer.materializeOverBaseAlloc(self.alloc, published, overlay);
        };
        defer query_materializer.freeDocuments(self.alloc, materialized);

        const docs = try self.alloc.alloc(query_types.QueryDocument, materialized.len);
        errdefer self.alloc.free(docs);
        var initialized: usize = 0;
        errdefer {
            for (docs[0..initialized]) |doc| {
                self.alloc.free(doc.doc_id);
                self.alloc.free(doc.body);
            }
        }

        for (materialized, 0..) |doc, idx| {
            docs[idx] = .{
                .doc_id = try self.alloc.dupe(u8, doc.doc_id),
                .body = try self.alloc.dupe(u8, doc.body),
            };
            initialized += 1;
        }
        return docs;
    }

    fn allocPublishedDocumentsAlloc(self: *HttpHandler, session: *query_mod.QuerySession) ![]query_materializer.Document {
        try session.warmArtifactKind(.document_segment);
        try session.warmArtifactKind(.mutation_segment);
        for (0..session.artifactCount()) |artifact_index| {
            const artifact_ref = session.artifactRef(artifact_index) orelse continue;
            if (artifact_ref.kind != .document_segment) continue;

            const contents = try session.fetchArtifactAlloc(artifact_index);
            defer self.alloc.free(contents);
            const entries = try document_segment_mod.decodeAlloc(self.alloc, contents);
            defer document_segment_mod.freeEntries(self.alloc, entries);
            const published = try allocMaterializedDocumentsFromDocumentEntries(self.alloc, entries);
            const mutations = try allocSessionMutationOverlayAlloc(self, session);
            defer freeMaterializerMutations(self.alloc, mutations);
            if (mutations.len == 0) return published;
            defer query_materializer.freeDocuments(self.alloc, published);
            return try query_materializer.materializeOverBaseAlloc(self.alloc, published, mutations);
        }

        const published_entries = try self.allocSessionEntries(session);
        defer freeQueryMutations(self.alloc, published_entries);

        const all_mutations = try self.alloc.alloc(query_materializer.Mutation, published_entries.len);
        defer self.alloc.free(all_mutations);
        for (published_entries, 0..) |entry, idx| {
            all_mutations[idx] = .{
                .lsn = entry.lsn,
                .timestamp_ns = entry.timestamp_ns,
                .kind = entry.kind,
                .doc_id = entry.doc_id,
                .body = entry.body,
            };
        }
        return try query_materializer.materializeAlloc(self.alloc, all_mutations);
    }

    fn allocSessionMutationOverlayAlloc(self: *HttpHandler, session: *query_mod.QuerySession) ![]query_materializer.Mutation {
        var total_entries: usize = 0;
        for (0..session.artifactCount()) |artifact_index| {
            const artifact_ref = session.artifactRef(artifact_index) orelse continue;
            if (artifact_ref.kind != .mutation_segment) continue;
            const contents = try session.fetchArtifactAlloc(artifact_index);
            defer self.alloc.free(contents);
            const entries = try segment_mod.decodeAlloc(self.alloc, contents);
            defer segment_mod.freeEntries(self.alloc, entries);
            total_entries += entries.len;
        }
        if (total_entries == 0) return try self.alloc.alloc(query_materializer.Mutation, 0);

        const mutations = try self.alloc.alloc(query_materializer.Mutation, total_entries);
        errdefer self.alloc.free(mutations);
        var initialized: usize = 0;
        errdefer freeMaterializerMutations(self.alloc, mutations[0..initialized]);

        for (0..session.artifactCount()) |artifact_index| {
            const artifact_ref = session.artifactRef(artifact_index) orelse continue;
            if (artifact_ref.kind != .mutation_segment) continue;
            const contents = try session.fetchArtifactAlloc(artifact_index);
            defer self.alloc.free(contents);
            const entries = try segment_mod.decodeAlloc(self.alloc, contents);
            defer segment_mod.freeEntries(self.alloc, entries);
            for (entries) |entry| {
                mutations[initialized] = .{
                    .lsn = entry.lsn,
                    .timestamp_ns = entry.timestamp_ns,
                    .kind = entry.kind,
                    .doc_id = try self.alloc.dupe(u8, entry.doc_id),
                    .body = if (entry.body) |body| try self.alloc.dupe(u8, body) else null,
                };
                initialized += 1;
            }
        }
        return mutations;
    }

    fn allocArtifactMutations(self: *HttpHandler, session: *query_mod.QuerySession, artifact_index: usize) ![]query_types.QueryMutation {
        const artifact_ref = session.artifactRef(artifact_index) orelse return error.ArtifactNotFound;
        if (artifact_ref.kind != .mutation_segment) return try self.alloc.alloc(query_types.QueryMutation, 0);

        const contents = try session.fetchArtifactAlloc(artifact_index);
        defer self.alloc.free(contents);
        return try decodeSegmentEntriesAlloc(self.alloc, contents);
    }

    fn allocArtifactDocuments(self: *HttpHandler, session: *query_mod.QuerySession, artifact_index: usize) ![]query_types.QueryDocument {
        const artifact_ref = session.artifactRef(artifact_index) orelse return error.ArtifactNotFound;
        if (artifact_ref.kind != .document_segment) return try self.alloc.alloc(query_types.QueryDocument, 0);

        const contents = try session.fetchArtifactAlloc(artifact_index);
        defer self.alloc.free(contents);
        const entries = try document_segment_mod.decodeAlloc(self.alloc, contents);
        defer document_segment_mod.freeEntries(self.alloc, entries);

        const docs = try self.alloc.alloc(query_types.QueryDocument, entries.len);
        errdefer self.alloc.free(docs);

        var initialized: usize = 0;
        errdefer {
            for (docs[0..initialized]) |doc| {
                self.alloc.free(doc.doc_id);
                self.alloc.free(doc.body);
            }
        }

        for (entries, 0..) |entry, idx| {
            docs[idx] = .{
                .doc_id = try self.alloc.dupe(u8, entry.doc_id),
                .body = try self.alloc.dupe(u8, entry.body),
            };
            initialized += 1;
        }
        return docs;
    }

    fn resolveTableBatchMutationsAlloc(self: *HttpHandler, namespace: []const u8, req: db_types.BatchRequest) ![]api_types.DocumentMutation {
        const latest_docs = try self.allocLatestDocumentsAlloc(namespace);
        defer query_materializer.freeDocuments(self.alloc, latest_docs);

        var deletes = std.ArrayListUnmanaged(api_types.DocumentMutation).empty;
        defer freeDocumentMutationList(self.alloc, &deletes);
        for (req.deletes) |doc_id| {
            try deletes.append(self.alloc, .{
                .kind = .delete,
                .doc_id = try self.alloc.dupe(u8, doc_id),
                .body = null,
            });
        }

        var upserts = std.ArrayListUnmanaged(api_types.DocumentMutation).empty;
        defer freeDocumentMutationList(self.alloc, &upserts);
        for (req.writes) |write| {
            try upserts.append(self.alloc, .{
                .kind = .upsert,
                .doc_id = try self.alloc.dupe(u8, write.key),
                .body = try self.alloc.dupe(u8, write.value),
            });
        }

        for (req.transforms) |transform| {
            const existing_body = findMaterializedDocumentBody(latest_docs, transform.key);
            const resolved = try db_transform.resolveDocumentTransform(self.alloc, existing_body, transform) orelse continue;
            errdefer self.alloc.free(resolved);

            removeDocumentMutationById(self.alloc, &deletes, transform.key);
            removeDocumentMutationById(self.alloc, &upserts, transform.key);
            try upserts.append(self.alloc, .{
                .kind = .upsert,
                .doc_id = try self.alloc.dupe(u8, transform.key),
                .body = resolved,
            });
        }

        const mutations = try self.alloc.alloc(api_types.DocumentMutation, deletes.items.len + upserts.items.len);
        var initialized: usize = 0;
        errdefer freeDocumentMutations(self.alloc, mutations[0..initialized]);

        for (deletes.items) |mutation| {
            mutations[initialized] = mutation;
            initialized += 1;
        }
        deletes.clearRetainingCapacity();

        for (upserts.items) |mutation| {
            mutations[initialized] = mutation;
            initialized += 1;
        }
        upserts.clearRetainingCapacity();

        return mutations;
    }

    fn tableApi(self: *HttpHandler) public_table_http.TableApi {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute_table_batch = executePublicTableBatch,
                .execute_table_query_request = executePublicTableQueryRequest,
                .execute_table_query_view = executePublicTableQueryView,
                .execute_table_backup = executePublicTableBackup,
                .execute_table_restore = executePublicTableRestore,
                .execute_table_list_indexes = executePublicTableListIndexes,
                .execute_table_get_index = executePublicTableGetIndex,
                .execute_table_create_index = executePublicTableCreateIndex,
                .execute_table_delete_index = executePublicTableDeleteIndex,
            },
        };
    }

    fn executePublicTableBatch(
        ptr: *anyopaque,
        alloc: Allocator,
        table_name: []const u8,
        req: db_types.BatchRequest,
    ) public_table_http.TableApi.ExecuteBatchError!void {
        _ = alloc;
        const self: *HttpHandler = @ptrCast(@alignCast(ptr));
        if (self.runtime_status.role == .query_only) return error.Unavailable;

        var status = self.catalog.tableBuildStatus(table_name) catch |err| switch (err) {
            error.NamespaceNotFound => return error.NotFound,
            else => {
                std.log.err("serverless public table batch status failed table={s} err={}", .{ table_name, err });
                return error.InternalFailure;
            },
        };
        defer status.deinit(self.alloc);
        if (!status.publish_admitted) return error.Backpressured;

        const namespace = self.catalog.resolveTableNamespaceAlloc(table_name) catch |err| switch (err) {
            error.NamespaceNotFound => return error.NotFound,
            else => {
                std.log.err("serverless public table batch resolve failed table={s} err={}", .{ table_name, err });
                return error.InternalFailure;
            },
        };
        defer self.alloc.free(namespace);

        const mutations = self.resolveTableBatchMutationsAlloc(namespace, req) catch |err| switch (err) {
            error.InvalidArgument => return error.InvalidBatchRequest,
            else => {
                std.log.err("serverless public table batch mutation resolution failed table={s} namespace={s} err={}", .{ table_name, namespace, err });
                return error.InternalFailure;
            },
        };
        defer freeDocumentMutations(self.alloc, mutations);

        var result = self.api.ingestBatch(.{
            .namespace = namespace,
            .timestamp_ns = currentTimeNs(),
            .mutations = mutations,
        }) catch |err| {
            std.log.err("serverless public table batch ingest failed table={s} namespace={s} err={}", .{ table_name, namespace, err });
            return error.InternalFailure;
        };
        defer result.deinit(self.alloc);

        self.enforcePublicTableBatchSyncLevel(table_name, req.sync_level, result.end_lsn, status) catch |err| {
            if (err == error.InternalFailure) {
                std.log.err("serverless public table batch sync wait failed table={s} sync_level={} end_lsn={} err={}", .{
                    table_name,
                    req.sync_level,
                    result.end_lsn,
                    err,
                });
            }
            return err;
        };
    }

    fn enforcePublicTableBatchSyncLevel(
        self: *HttpHandler,
        table_name: []const u8,
        sync_level: db_types.SyncLevel,
        end_lsn: u64,
        status_before_write: catalog_types.BuildStatus,
    ) public_table_http.TableApi.ExecuteBatchError!void {
        const requires_background_materialization =
            status_before_write.enrichment_enabled or
            status_before_write.chunk_preview_enabled or
            status_before_write.chunk_embeddings_enabled or
            status_before_write.rerank_terms_enabled;

        switch (sync_level) {
            .propose, .write => return,
            .full_text => {},
            .enrichments, .aknn, .full_index => if (requires_background_materialization and self.runtime_metrics == null) {
                return error.UnsupportedSyncLevel;
            },
        }

        const timeout_ns = 30 * std.time.ns_per_s;
        const start_ns = platform_time.monotonicNs();
        while (true) {
            const build_result = self.catalog.buildTable(table_name) catch |err| switch (err) {
                error.NamespaceNotFound => return error.NotFound,
                error.HeadChanged => null,
                else => {
                    std.log.err("serverless public table batch build failed table={s} sync_level={} err={}", .{ table_name, sync_level, err });
                    return error.InternalFailure;
                },
            };
            if (build_result) |build| {
                var owned_build = build;
                owned_build.deinit(self.alloc);
            }

            if (self.runtime_metrics) |runtime| {
                _ = runtime.runOnce() catch |err| {
                    std.log.err("serverless public table batch maintenance run failed table={s} sync_level={} err={}", .{ table_name, sync_level, err });
                    return error.InternalFailure;
                };
            }

            var status = self.catalog.tableBuildStatus(table_name) catch |err| switch (err) {
                error.NamespaceNotFound => return error.NotFound,
                else => {
                    std.log.err("serverless public table batch post-build status failed table={s} sync_level={} err={}", .{ table_name, sync_level, err });
                    return error.InternalFailure;
                },
            };
            defer status.deinit(self.alloc);

            if (tableSyncLevelSatisfied(sync_level, end_lsn, status)) return;
            if (platform_time.monotonicNs() -| start_ns >= timeout_ns) {
                std.log.err(
                    "serverless public table batch sync timeout table={s} sync_level={} end_lsn={} published={} latest={} pending_rebuild={} enrichment_complete={} chunk_preview_complete={} chunk_embeddings_complete={} rerank_terms_complete={} active_stage={any}",
                    .{
                        table_name,
                        sync_level,
                        end_lsn,
                        status.published_wal_end_lsn,
                        status.latest_wal_lsn,
                        status.pending_materialization_rebuild,
                        status.enrichment_complete,
                        status.chunk_preview_complete,
                        status.chunk_embeddings_complete,
                        status.rerank_terms_complete,
                        status.enrichment_active_stage,
                    },
                );
                return error.UnsupportedSyncLevel;
            }

            sleepNs(10 * std.time.ns_per_ms);
        }
    }

    fn tableSyncLevelSatisfied(
        sync_level: db_types.SyncLevel,
        end_lsn: u64,
        status: catalog_types.BuildStatus,
    ) bool {
        if (status.published_wal_end_lsn < end_lsn) return false;

        return switch (sync_level) {
            .propose, .write => true,
            .full_text => fullTextSyncSatisfied(status),
            .enrichments => status.enrichment_complete,
            .aknn, .full_index => status.enrichment_complete,
        };
    }

    fn fullTextSyncSatisfied(status: catalog_types.BuildStatus) bool {
        if (status.artifact_actions.document_segment == .rebuild) return false;
        if (status.full_text_index_actions.len > 0) {
            for (status.full_text_index_actions) |entry| {
                if (entry.action == .rebuild) return false;
                if (entry.chunked_source_count > 0 and
                    (status.pending_materialization_families.chunk_preview or !status.chunk_preview_complete))
                {
                    return false;
                }
            }
            return true;
        }
        return status.artifact_actions.full_text != .rebuild;
    }

    fn executePublicTableQueryRequest(
        ptr: *anyopaque,
        alloc: Allocator,
        table_name: []const u8,
        body: []const u8,
        row_filter_json: ?[]const u8,
    ) public_table_http.TableApi.ExecuteQueryError![]u8 {
        _ = alloc;
        _ = row_filter_json;
        const self: *HttpHandler = @ptrCast(@alignCast(ptr));
        return self.executePublicTableQueryJsonAlloc(table_name, body) catch |err| switch (err) {
            error.InvalidQueryRequest => return error.InvalidQueryRequest,
            error.FileNotFound => return error.NotFound,
            else => {
                std.log.err("serverless public table query failed table={s} err={}", .{ table_name, err });
                return error.InternalFailure;
            },
        };
    }

    fn executePublicTableQueryView(
        ptr: *anyopaque,
        alloc: Allocator,
        table_name: []const u8,
        view: public_table_http.TableApi.TableQueryView,
    ) public_table_http.TableApi.ExecuteQueryViewError![]u8 {
        _ = alloc;
        const self: *HttpHandler = @ptrCast(@alignCast(ptr));
        return self.executePublicTableQueryViewJsonAlloc(table_name, view) catch |err| switch (err) {
            error.FileNotFound => return error.NotFound,
            else => return error.InternalFailure,
        };
    }

    fn executePublicTableBackup(
        _: *anyopaque,
        _: Allocator,
        _: []const u8,
        _: []const u8,
        _: *backups_api.BackupLocation,
    ) public_table_http.TableApi.ExecuteBackupError!void {
        return error.MethodNotAllowed;
    }

    fn executePublicTableRestore(
        _: *anyopaque,
        _: Allocator,
        _: []const u8,
        _: []const u8,
        _: []const u8,
        _: *backups_api.BackupLocation,
    ) public_table_http.TableApi.ExecuteRestoreError!void {
        return error.MethodNotAllowed;
    }

    fn executePublicTableListIndexes(
        ptr: *anyopaque,
        alloc: Allocator,
        table_name: []const u8,
    ) public_table_http.TableApi.ExecuteListIndexesError![]u8 {
        _ = alloc;
        const self: *HttpHandler = @ptrCast(@alignCast(ptr));
        var table = (self.catalog.getTableAlloc(self.alloc, table_name) catch return error.InternalFailure) orelse return error.NotFound;
        defer table.deinit(self.alloc);
        var status = self.catalog.tableBuildStatus(table_name) catch return error.InternalFailure;
        defer status.deinit(self.alloc);
        return encodeServerlessIndexListAlloc(self.alloc, table.indexes_json, status) catch return error.InternalFailure;
    }

    fn executePublicTableGetIndex(
        ptr: *anyopaque,
        alloc: Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) public_table_http.TableApi.ExecuteGetIndexError![]u8 {
        _ = alloc;
        const self: *HttpHandler = @ptrCast(@alignCast(ptr));
        var table = (self.catalog.getTableAlloc(self.alloc, table_name) catch return error.InternalFailure) orelse return error.NotFound;
        defer table.deinit(self.alloc);
        var status = self.catalog.tableBuildStatus(table_name) catch return error.InternalFailure;
        defer status.deinit(self.alloc);
        return (encodeServerlessSingleIndexAlloc(self.alloc, table.indexes_json, index_name, status) catch return error.InternalFailure) orelse error.NotFound;
    }

    fn executePublicTableCreateIndex(
        ptr: *anyopaque,
        alloc: Allocator,
        table_name: []const u8,
        index_name: []const u8,
        body: []const u8,
    ) public_table_http.TableApi.ExecuteCreateIndexError!void {
        const self: *HttpHandler = @ptrCast(@alignCast(ptr));
        if (self.runtime_status.role == .query_only) return error.MethodNotAllowed;
        var table = (self.catalog.getTableAlloc(self.alloc, table_name) catch return error.InternalFailure) orelse return error.NotFound;
        defer table.deinit(self.alloc);

        const index_json = table_contract.parseCreateIndexRequest(alloc, index_name, body) catch {
            return error.InvalidIndexRequest;
        };
        defer alloc.free(index_json);
        tables_api.validatePublicAlgebraicIndexJson(alloc, index_json) catch {
            return error.InvalidIndexRequest;
        };
        const expanded_index_json = tables_api.expandSchemaDerivedAlgebraicIndexAlloc(alloc, table_name, index_json, table.schema_json) catch |err| switch (err) {
            error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest => return error.InvalidIndexRequest,
            else => return error.InternalFailure,
        };
        defer alloc.free(expanded_index_json);
        table_writes.validateIndexConfig(alloc, index_name, expanded_index_json) catch |err| switch (err) {
            error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest => return error.InvalidIndexRequest,
            else => return error.InternalFailure,
        };
        const next_indexes_json = indexes_api.addIndexToTableIndexesJson(alloc, table.indexes_json, index_name, expanded_index_json) catch |err| switch (err) {
            error.InvalidTableIndexMetadata, error.InvalidCreateIndexRequest => return error.InvalidIndexRequest,
            else => return error.InternalFailure,
        };
        defer alloc.free(next_indexes_json);
        validateServerlessIndexCatalog(alloc, next_indexes_json) catch |err| switch (err) {
            error.UnsupportedCreateTableRequest, error.InvalidTableIndexMetadata => return error.InvalidIndexRequest,
            else => return error.InternalFailure,
        };
        const updated = self.catalog.setTableDefinition(
            table_name,
            table.schema_json,
            table.read_schema_json,
            next_indexes_json,
        ) catch return error.InternalFailure;
        if (!updated) return error.NotFound;
    }

    fn executePublicTableDeleteIndex(
        ptr: *anyopaque,
        alloc: Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) public_table_http.TableApi.ExecuteDeleteIndexError!void {
        const self: *HttpHandler = @ptrCast(@alignCast(ptr));
        if (self.runtime_status.role == .query_only) return error.MethodNotAllowed;
        var table = (self.catalog.getTableAlloc(self.alloc, table_name) catch return error.InternalFailure) orelse return error.NotFound;
        defer table.deinit(self.alloc);

        const next_indexes_json = (indexes_api.removeIndexFromTableIndexesJson(alloc, table.indexes_json, index_name) catch return error.InternalFailure) orelse {
            return error.NotFound;
        };
        defer alloc.free(next_indexes_json);
        validateServerlessIndexCatalog(alloc, next_indexes_json) catch return error.InternalFailure;
        const updated = self.catalog.setTableDefinition(
            table_name,
            table.schema_json,
            table.read_schema_json,
            next_indexes_json,
        ) catch return error.InternalFailure;
        if (!updated) return error.NotFound;
    }

    fn allocLatestDocumentsAlloc(self: *HttpHandler, namespace: []const u8) ![]query_materializer.Document {
        var status = try self.catalog.buildStatus(namespace);
        defer status.deinit(self.alloc);

        if (status.head_version == 0) {
            const records = try self.api.wal.readFromAlloc(namespace, 1);
            defer wal_mod.freeRecords(self.alloc, records);
            const tail = try allocTailMutations(self.alloc, records);
            defer freeQueryMutations(self.alloc, tail);
            if (tail.len == 0) return try self.alloc.alloc(query_materializer.Document, 0);
            const overlay = try allocTailMaterializerMutations(self.alloc, tail);
            defer freeMaterializerMutations(self.alloc, overlay);
            return try query_materializer.materializeAlloc(self.alloc, overlay);
        }

        var session = try self.query.openHeadSession(namespace);
        defer session.deinit();
        const published = try self.allocPublishedDocumentsAlloc(&session);
        errdefer query_materializer.freeDocuments(self.alloc, published);

        const records = try self.api.wal.readFromAlloc(namespace, session.manifest.wal_end_lsn + 1);
        defer wal_mod.freeRecords(self.alloc, records);
        const tail = try allocTailMutations(self.alloc, records);
        defer freeQueryMutations(self.alloc, tail);
        if (tail.len == 0) return published;

        const overlay = try allocTailMaterializerMutations(self.alloc, tail);
        defer freeMaterializerMutations(self.alloc, overlay);
        const materialized = try query_materializer.materializeOverBaseAlloc(self.alloc, published, overlay);
        query_materializer.freeDocuments(self.alloc, published);
        return materialized;
    }
};

fn allocTailMaterializerMutations(alloc: Allocator, tail: []const query_types.QueryTailMutation) ![]query_materializer.Mutation {
    const mutations = try alloc.alloc(query_materializer.Mutation, tail.len);
    errdefer alloc.free(mutations);

    var initialized: usize = 0;
    errdefer freeMaterializerMutations(alloc, mutations[0..initialized]);

    for (tail, 0..) |entry, idx| {
        mutations[idx] = .{
            .lsn = entry.lsn,
            .timestamp_ns = entry.timestamp_ns,
            .kind = entry.kind,
            .doc_id = try alloc.dupe(u8, entry.doc_id),
            .body = if (entry.body) |body| try alloc.dupe(u8, body) else null,
        };
        initialized += 1;
    }
    return mutations;
}

fn currentTimeNs() u64 {
    return platform_time.monotonicNs();
}

fn sleepNs(duration_ns: u64) void {
    var req = std.posix.timespec{
        .sec = @intCast(duration_ns / std.time.ns_per_s),
        .nsec = @intCast(duration_ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

fn findMaterializedDocumentBody(docs: []const query_materializer.Document, doc_id: []const u8) ?[]const u8 {
    for (docs) |doc| {
        if (std.mem.eql(u8, doc.doc_id, doc_id)) return doc.body;
    }
    return null;
}

const PatternDocumentFilterContext = struct {
    alloc: Allocator,
    docs: []const query_materializer.Document,
};

fn patternRequiresDocumentFilter(pattern: []const graph_pattern_mod.PatternStep) bool {
    for (pattern) |step| {
        if (step.node_filter.filter_query_json != null) return true;
    }
    return false;
}

fn publishedPatternNodeFilterEvaluator(ctx: ?*anyopaque, key: []const u8, filter: graph_pattern_mod.NodeFilter) anyerror!bool {
    const active: *PatternDocumentFilterContext = @ptrCast(@alignCast(ctx orelse return error.UnsupportedNodeFilterQuery));
    if (filter.filter_query_json == null) return true;
    const body = findMaterializedDocumentBody(active.docs, key) orelse return false;
    return try db_query_graph.storedDocMatchesPatternFilter(active.alloc, key, body, filter.filter_query_json.?);
}

fn removeDocumentMutationById(
    alloc: Allocator,
    list: *std.ArrayListUnmanaged(api_types.DocumentMutation),
    doc_id: []const u8,
) void {
    var i: usize = 0;
    while (i < list.items.len) {
        if (!std.mem.eql(u8, list.items[i].doc_id, doc_id)) {
            i += 1;
            continue;
        }
        list.items[i].deinit(alloc);
        _ = list.swapRemove(i);
    }
}

fn freeDocumentMutationList(alloc: Allocator, list: *std.ArrayListUnmanaged(api_types.DocumentMutation)) void {
    for (list.items) |*mutation| mutation.deinit(alloc);
    list.deinit(alloc);
}

fn findNamespaceQueryMetrics(
    metrics: []const query_mod.NamespaceQueryExecutionMetrics,
    namespace: []const u8,
) ?query_mod.NamespaceQueryExecutionMetrics {
    for (metrics) |metric| {
        if (std.mem.eql(u8, metric.namespace, namespace)) return metric;
    }
    return null;
}

fn freeGraphNeighbors(alloc: Allocator, neighbors: []query_types.GraphNeighbor) void {
    for (neighbors) |neighbor| {
        alloc.free(neighbor.doc_id);
        alloc.free(neighbor.edge_type);
    }
    alloc.free(neighbors);
}

fn freeGraphTraversalNodes(alloc: Allocator, nodes: []query_types.GraphTraversalNode) void {
    for (nodes) |node| {
        alloc.free(node.doc_id);
        if (node.parent_doc_id) |value| alloc.free(value);
        if (node.via_edge_type) |value| alloc.free(value);
        if (node.path) |path| {
            for (path) |segment| alloc.free(segment);
            alloc.free(path);
        }
        if (node.edge_path) |path| freeGraphEdgePath(alloc, path);
    }
    alloc.free(nodes);
}

fn graphTotalHits(results: []const db_types.GraphSearchResult) u32 {
    var total: u32 = 0;
    for (results) |result| total +|= result.total_hits;
    return total;
}

fn freeOwnedKeySlice(alloc: Allocator, keys: [][]u8) void {
    for (keys) |key| alloc.free(key);
    alloc.free(keys);
}

fn castOwnedKeysToConst(alloc: Allocator, keys: [][]u8) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, keys.len);
    errdefer alloc.free(out);
    for (keys, 0..) |key, idx| out[idx] = key;
    return out;
}

fn freeOwnedGraphEdge(alloc: Allocator, edge: graph_mod.Edge) void {
    alloc.free(edge.source);
    alloc.free(edge.target);
    alloc.free(edge.edge_type);
    if (edge.metadata.len > 0) alloc.free(edge.metadata);
}

fn findGraphSegmentAdjacency(
    adjacencies: []const graph_segment_mod.Adjacency,
    doc_id: []const u8,
) ?graph_segment_mod.Adjacency {
    for (adjacencies) |adjacency| {
        if (std.mem.eql(u8, adjacency.node_id, doc_id)) return adjacency;
    }
    return null;
}

fn requestHasSearchInputs(request: metadata_openapi.QueryRequest) bool {
    return request.full_text_search != null or
        request.embeddings != null or
        request.semantic_search != null or
        request.filter_query != null or
        request.exclusion_query != null;
}

fn firstEdgeType(edge_types: ?[]const []const u8) ?[]const u8 {
    const values = edge_types orelse return null;
    if (values.len != 1) return null;
    return values[0];
}

fn toServerlessGraphDirection(direction: graph_mod.EdgeDirection) query_mod.GraphQueryDirection {
    return switch (direction) {
        .out => .out,
        .in => .in,
        .both => .both,
    };
}

fn dupFieldsAlloc(alloc: Allocator, values: []const []const u8) !?[][]u8 {
    if (values.len == 0) return null;
    const out = try alloc.alloc([]u8, values.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| alloc.free(value);
    }
    for (values, 0..) |value, idx| {
        out[idx] = try alloc.dupe(u8, value);
        initialized += 1;
    }
    return out;
}

fn freeFields(alloc: Allocator, values: ?[][]u8) void {
    const items = values orelse return;
    for (items) |value| alloc.free(value);
    alloc.free(items);
}

fn allocPathNodes(alloc: Allocator, nodes: []const []const u8) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, nodes.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |node| alloc.free(node);
    }
    for (nodes, 0..) |node, idx| {
        out[idx] = try alloc.dupe(u8, node);
        initialized += 1;
    }
    return out;
}

fn allocSinglePathEdgeInfo(
    alloc: Allocator,
    source: []const u8,
    target: []const u8,
    edge_type: []const u8,
    weight: f32,
) ![]const graph_query_mod.PathEdgeInfo {
    const out = try alloc.alloc(graph_query_mod.PathEdgeInfo, 1);
    errdefer alloc.free(out);
    out[0] = .{
        .source = try alloc.dupe(u8, source),
        .target = try alloc.dupe(u8, target),
        .edge_type = try alloc.dupe(u8, edge_type),
        .weight = weight,
    };
    return out;
}

fn allocPathEdgeInfos(
    alloc: Allocator,
    path: []query_mod.GraphPathHop,
) ![]const graph_query_mod.PathEdgeInfo {
    const out = try alloc.alloc(graph_query_mod.PathEdgeInfo, path.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |edge| {
            alloc.free(edge.source);
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
        }
    }
    for (path, 0..) |hop, idx| {
        out[idx] = .{
            .source = try alloc.dupe(u8, hop.from_doc_id),
            .target = try alloc.dupe(u8, hop.to_doc_id),
            .edge_type = try alloc.dupe(u8, hop.edge_type),
            .weight = hop.weight,
        };
        initialized += 1;
    }
    return out;
}

fn dupConstGraphPathAlloc(alloc: Allocator, path: []const []u8) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, path.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |segment| alloc.free(segment);
    }
    for (path, 0..) |segment, idx| {
        out[idx] = try alloc.dupe(u8, segment);
        initialized += 1;
    }
    return out;
}

fn normalizeTraversalPathAlloc(
    alloc: Allocator,
    start_key: []const u8,
    path: []const []u8,
) ![]const []const u8 {
    if (path.len > 0 and std.mem.eql(u8, path[0], start_key)) {
        return try dupConstGraphPathAlloc(alloc, path);
    }

    const out = try alloc.alloc([]const u8, path.len + 1);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |segment| alloc.free(segment);
    }

    out[0] = try alloc.dupe(u8, start_key);
    initialized += 1;
    for (path, 0..) |segment, idx| {
        out[idx + 1] = try alloc.dupe(u8, segment);
        initialized += 1;
    }
    return out;
}

fn traversalDistance(node: query_mod.GraphTraversalNode) f64 {
    const edge_path = node.edge_path orelse return @floatFromInt(node.depth);
    var total: f64 = 0;
    for (edge_path) |hop| total += hop.weight;
    return total;
}

fn toDbGraphPath(alloc: Allocator, path: query_mod.GraphShortestPath) !db_types.GraphPath {
    const nodes = try alloc.alloc([]const u8, path.path.len);
    errdefer alloc.free(nodes);
    var initialized_nodes: usize = 0;
    errdefer {
        for (nodes[0..initialized_nodes]) |node| alloc.free(node);
    }
    for (path.path, 0..) |node, idx| {
        nodes[idx] = try alloc.dupe(u8, node);
        initialized_nodes += 1;
    }

    const edges = try alloc.alloc(graph_paths.PathEdge, path.edge_path.len);
    errdefer alloc.free(edges);
    var initialized_edges: usize = 0;
    errdefer {
        for (edges[0..initialized_edges]) |edge| {
            alloc.free(edge.source);
            alloc.free(edge.target);
            alloc.free(edge.edge_type);
        }
    }

    var total_weight: f64 = 0;
    for (path.edge_path, 0..) |hop, idx| {
        edges[idx] = .{
            .source = try alloc.dupe(u8, hop.from_doc_id),
            .target = try alloc.dupe(u8, hop.to_doc_id),
            .edge_type = try alloc.dupe(u8, hop.edge_type),
            .weight = hop.weight,
        };
        total_weight += hop.weight;
        initialized_edges += 1;
    }

    return .{
        .nodes = nodes,
        .edges = edges,
        .total_weight = total_weight,
        .length = path.depth,
    };
}

fn dupGraphPathAlloc(alloc: Allocator, path: [][]u8) ![][]u8 {
    const out = try alloc.alloc([]u8, path.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |segment| alloc.free(segment);
    }
    for (path, 0..) |segment, idx| {
        out[idx] = try alloc.dupe(u8, segment);
        initialized += 1;
    }
    return out;
}

fn freeGraphPath(alloc: Allocator, path: [][]u8) void {
    for (path) |segment| alloc.free(segment);
    alloc.free(path);
}

fn dupGraphEdgePathAlloc(alloc: Allocator, path: []query_mod.GraphPathHop) ![]query_types.GraphPathHop {
    const out = try alloc.alloc(query_types.GraphPathHop, path.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |hop| {
            alloc.free(hop.from_doc_id);
            alloc.free(hop.to_doc_id);
            alloc.free(hop.edge_type);
        }
    }
    for (path, 0..) |hop, idx| {
        out[idx] = .{
            .from_doc_id = try alloc.dupe(u8, hop.from_doc_id),
            .to_doc_id = try alloc.dupe(u8, hop.to_doc_id),
            .edge_type = try alloc.dupe(u8, hop.edge_type),
            .weight = hop.weight,
            .direction = hop.direction,
        };
        initialized += 1;
    }
    return out;
}

fn freeGraphEdgePath(alloc: Allocator, path: []query_types.GraphPathHop) void {
    for (path) |hop| {
        alloc.free(hop.from_doc_id);
        alloc.free(hop.to_doc_id);
        alloc.free(hop.edge_type);
    }
    alloc.free(path);
}

fn queryEnrichmentStatus(
    status: catalog_types.BuildStatus,
    total_document_count: u64,
) query_types.QueryEnrichmentStatus {
    const processed_document_count = @min(status.enrichment_doc_offset, total_document_count);
    const complete = !status.enrichment_enabled or total_document_count == 0 or
        (status.enrichment_head_version != null and
            status.enrichment_head_version.? == status.head_version and
            processed_document_count >= total_document_count);
    return .{
        .enabled = status.enrichment_enabled,
        .in_progress = status.enrichment_in_progress,
        .complete = complete,
        .stage_source = status.enrichment_stage_source,
        .stage_state = status.enrichment_stage_state,
        .pipeline_version = status.enrichment_pipeline_version,
        .head_version = status.enrichment_head_version,
        .processed_document_count = processed_document_count,
        .total_document_count = total_document_count,
    };
}

fn queryPublicationStatus(status: catalog_types.BuildStatus) query_types.QueryPublicationStatus {
    const has_vector_driver = status.vector_compaction_driver_index_name != null;
    return .{
        .publish_recommended = status.publish_recommended,
        .next_publish_reason = status.next_publish_reason,
        .mutation_tail_resolution = status.mutation_tail_resolution,
        .mutation_tail_compaction_recommended = status.mutation_tail_compaction_recommended,
        .vector_compaction_driver_index_name = status.vector_compaction_driver_index_name,
        .vector_distance_metric = if (has_vector_driver) status.vector_compaction_distance_metric else null,
        .vector_compaction_recommended = has_vector_driver and status.vector_compaction_recommended and status.vector_target_cluster_count != null,
        .vector_cluster_count = if (has_vector_driver) status.vector_cluster_count else null,
        .vector_base_probe_count = if (has_vector_driver) status.vector_base_probe_count else null,
        .vector_shortlist_multiplier = if (has_vector_driver) status.vector_shortlist_multiplier else null,
        .vector_cluster_imbalance = if (has_vector_driver) status.vector_cluster_imbalance else null,
        .vector_cluster_distance_span_max = if (has_vector_driver) status.vector_cluster_distance_span_max else null,
        .vector_target_cluster_count = if (has_vector_driver) status.vector_target_cluster_count else null,
        .vector_target_base_probe_count = if (has_vector_driver) status.vector_target_base_probe_count else null,
        .vector_target_shortlist_multiplier = if (has_vector_driver) status.vector_target_shortlist_multiplier else null,
        .head_document_publish_mode = status.head_document_publish_mode,
        .next_document_publish_mode = status.next_document_publish_mode,
        .document_base_version = status.document_base_version,
        .document_lineage_versions = status.document_lineage_versions,
        .head_republish_recommended = status.head_republish_recommended,
        .pending_materialization_rebuild = status.pending_materialization_rebuild,
        .pending_materialization_families = status.pending_materialization_families,
        .head_artifact_actions = status.head_artifact_actions,
        .head_full_text_index_actions = status.head_full_text_index_actions,
        .head_vector_index_actions = status.head_vector_index_actions,
        .head_sparse_index_actions = status.head_sparse_index_actions,
        .head_graph_index_actions = status.head_graph_index_actions,
        .artifact_actions = status.artifact_actions,
        .full_text_index_actions = status.full_text_index_actions,
        .vector_index_actions = status.vector_index_actions,
        .sparse_index_actions = status.sparse_index_actions,
        .graph_index_actions = status.graph_index_actions,
        .head_derived_output_actions = status.head_derived_output_actions,
        .derived_output_actions = status.derived_output_actions,
        .derived_output_resolutions = status.derived_output_resolutions,
        .pending_records = status.pending_records,
    };
}

fn cloneFullTextIndexPublicationActionsAlloc(
    alloc: Allocator,
    items: []const catalog_types.FullTextIndexPublicationAction,
) ![]catalog_types.FullTextIndexPublicationAction {
    const out = try alloc.alloc(catalog_types.FullTextIndexPublicationAction, items.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*entry| entry.deinit(alloc);
    }
    for (items, 0..) |item, idx| {
        out[idx] = .{
            .name = try alloc.dupe(u8, item.name),
            .action = item.action,
            .source_mode = item.source_mode,
            .chunked_source_count = item.chunked_source_count,
        };
        initialized += 1;
    }
    return out;
}

fn cloneNamedArtifactPublicationActionsAlloc(
    alloc: Allocator,
    items: []const catalog_types.NamedArtifactPublicationAction,
) ![]catalog_types.NamedArtifactPublicationAction {
    const out = try alloc.alloc(catalog_types.NamedArtifactPublicationAction, items.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*entry| entry.deinit(alloc);
    }
    for (items, 0..) |item, idx| {
        out[idx] = .{
            .name = try alloc.dupe(u8, item.name),
            .action = item.action,
        };
        initialized += 1;
    }
    return out;
}

fn cloneQueryPublicationStatusAlloc(
    alloc: Allocator,
    status: catalog_types.BuildStatus,
) !query_types.QueryPublicationStatus {
    var result = queryPublicationStatus(status);
    result.vector_compaction_driver_index_name = null;
    result.head_full_text_index_actions = &.{};
    result.head_vector_index_actions = &.{};
    result.head_sparse_index_actions = &.{};
    result.head_graph_index_actions = &.{};
    result.full_text_index_actions = &.{};
    result.vector_index_actions = &.{};
    result.sparse_index_actions = &.{};
    result.graph_index_actions = &.{};
    errdefer deinitQueryPublicationStatus(alloc, &result);

    if (status.vector_compaction_driver_index_name) |value| {
        result.vector_compaction_driver_index_name = try alloc.dupe(u8, value);
    }
    result.head_full_text_index_actions = try cloneFullTextIndexPublicationActionsAlloc(alloc, status.head_full_text_index_actions);
    result.head_vector_index_actions = try cloneNamedArtifactPublicationActionsAlloc(alloc, status.head_vector_index_actions);
    result.head_sparse_index_actions = try cloneNamedArtifactPublicationActionsAlloc(alloc, status.head_sparse_index_actions);
    result.head_graph_index_actions = try cloneNamedArtifactPublicationActionsAlloc(alloc, status.head_graph_index_actions);
    result.full_text_index_actions = try cloneFullTextIndexPublicationActionsAlloc(alloc, status.full_text_index_actions);
    result.vector_index_actions = try cloneNamedArtifactPublicationActionsAlloc(alloc, status.vector_index_actions);
    result.sparse_index_actions = try cloneNamedArtifactPublicationActionsAlloc(alloc, status.sparse_index_actions);
    result.graph_index_actions = try cloneNamedArtifactPublicationActionsAlloc(alloc, status.graph_index_actions);
    return result;
}

fn deinitQueryPublicationStatus(
    alloc: Allocator,
    status: *query_types.QueryPublicationStatus,
) void {
    if (status.vector_compaction_driver_index_name) |value| alloc.free(@constCast(value));
    for (@constCast(status.head_full_text_index_actions)) |*entry| entry.deinit(alloc);
    if (status.head_full_text_index_actions.len > 0) alloc.free(@constCast(status.head_full_text_index_actions));
    for (@constCast(status.head_vector_index_actions)) |*entry| entry.deinit(alloc);
    if (status.head_vector_index_actions.len > 0) alloc.free(@constCast(status.head_vector_index_actions));
    for (@constCast(status.head_sparse_index_actions)) |*entry| entry.deinit(alloc);
    if (status.head_sparse_index_actions.len > 0) alloc.free(@constCast(status.head_sparse_index_actions));
    for (@constCast(status.head_graph_index_actions)) |*entry| entry.deinit(alloc);
    if (status.head_graph_index_actions.len > 0) alloc.free(@constCast(status.head_graph_index_actions));
    for (@constCast(status.full_text_index_actions)) |*entry| entry.deinit(alloc);
    if (status.full_text_index_actions.len > 0) alloc.free(@constCast(status.full_text_index_actions));
    for (@constCast(status.vector_index_actions)) |*entry| entry.deinit(alloc);
    if (status.vector_index_actions.len > 0) alloc.free(@constCast(status.vector_index_actions));
    for (@constCast(status.sparse_index_actions)) |*entry| entry.deinit(alloc);
    if (status.sparse_index_actions.len > 0) alloc.free(@constCast(status.sparse_index_actions));
    for (@constCast(status.graph_index_actions)) |*entry| entry.deinit(alloc);
    if (status.graph_index_actions.len > 0) alloc.free(@constCast(status.graph_index_actions));
    status.* = undefined;
}

fn freeMaterializerMutations(alloc: Allocator, mutations: []query_materializer.Mutation) void {
    for (mutations) |mutation| {
        alloc.free(@constCast(mutation.doc_id));
        if (mutation.body) |body| alloc.free(@constCast(body));
    }
    alloc.free(mutations);
}

fn allocMaterializedDocumentsFromDocumentEntries(alloc: Allocator, entries: []const document_segment_mod.Entry) ![]query_materializer.Document {
    const docs = try alloc.alloc(query_materializer.Document, entries.len);
    errdefer alloc.free(docs);

    var initialized: usize = 0;
    errdefer {
        for (docs[0..initialized]) |*doc| doc.deinit(alloc);
    }

    for (entries, 0..) |entry, idx| {
        docs[idx] = .{
            .doc_id = try alloc.dupe(u8, entry.doc_id),
            .body = try alloc.dupe(u8, entry.body),
            .last_lsn = entry.last_lsn,
            .last_timestamp_ns = entry.last_timestamp_ns,
        };
        initialized += 1;
    }
    return docs;
}

fn allocMaterializerMutationsFromEntries(alloc: Allocator, entries: []const segment_mod.Entry) ![]query_materializer.Mutation {
    const mutations = try alloc.alloc(query_materializer.Mutation, entries.len);
    errdefer alloc.free(mutations);

    var initialized: usize = 0;
    errdefer freeMaterializerMutations(alloc, mutations[0..initialized]);

    for (entries, 0..) |entry, idx| {
        mutations[idx] = .{
            .lsn = entry.lsn,
            .timestamp_ns = entry.timestamp_ns,
            .kind = entry.kind,
            .doc_id = try alloc.dupe(u8, entry.doc_id),
            .body = if (entry.body) |body| try alloc.dupe(u8, body) else null,
        };
        initialized += 1;
    }
    return mutations;
}

fn cloneMaterializedDocumentsAlloc(alloc: Allocator, docs: []const query_materializer.Document) ![]query_materializer.Document {
    const cloned = try alloc.alloc(query_materializer.Document, docs.len);
    errdefer alloc.free(cloned);

    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*doc| doc.deinit(alloc);
    }

    for (docs, 0..) |doc, idx| {
        cloned[idx] = .{
            .doc_id = try alloc.dupe(u8, doc.doc_id),
            .body = try alloc.dupe(u8, doc.body),
            .last_lsn = doc.last_lsn,
            .last_timestamp_ns = doc.last_timestamp_ns,
        };
        initialized += 1;
    }
    return cloned;
}

fn parseHeadPublishRequest(alloc: Allocator, body: []const u8) !api_types.HeadPublishRequest {
    var parsed = try std.json.parseFromSlice(api_types.HeadPublishRequest, alloc, body, .{});
    defer parsed.deinit();
    return parsed.value;
}

fn parseIngestBatchRequest(alloc: Allocator, namespace: []const u8, body: []const u8) !api_types.IngestBatchRequest {
    const ParsedMutation = struct {
        kind: []const u8,
        doc_id: []const u8,
        body: ?[]const u8 = null,
    };
    const ParsedRequest = struct {
        timestamp_ns: u64,
        mutations: []ParsedMutation,
    };

    var parsed = try std.json.parseFromSlice(ParsedRequest, alloc, body, .{});
    defer parsed.deinit();

    const mutations = try alloc.alloc(api_types.DocumentMutation, parsed.value.mutations.len);
    errdefer alloc.free(mutations);

    var initialized: usize = 0;
    errdefer {
        for (mutations[0..initialized]) |*mutation| mutation.deinit(alloc);
    }

    for (parsed.value.mutations, 0..) |mutation, idx| {
        mutations[idx] = .{
            .kind = try parseMutationKind(mutation.kind),
            .doc_id = try alloc.dupe(u8, mutation.doc_id),
            .body = if (mutation.body) |body_value| try alloc.dupe(u8, body_value) else null,
        };
        initialized += 1;
    }

    return .{
        .namespace = namespace,
        .timestamp_ns = parsed.value.timestamp_ns,
        .mutations = mutations,
    };
}

fn parseTableIngestBatchRequest(alloc: Allocator, table_name: []const u8, body: []const u8) !api_types.TableIngestBatchRequest {
    const req = try parseIngestBatchRequest(alloc, table_name, body);
    errdefer freeDocumentMutations(alloc, req.mutations);
    return .{
        .table_name = req.namespace,
        .timestamp_ns = req.timestamp_ns,
        .mutations = req.mutations,
    };
}

fn parseEnsureNamespaceRequest(alloc: Allocator, body: []const u8) !api_types.EnsureNamespaceRequest {
    if (std.mem.trim(u8, body, &std.ascii.whitespace).len == 0) return .{};
    var parsed = try std.json.parseFromSlice(api_types.EnsureNamespaceRequest, alloc, body, .{});
    defer parsed.deinit();
    return parsed.value;
}

fn allocRequestedFieldsFromRequest(
    alloc: Allocator,
    request: metadata_openapi.QueryRequest,
) ![]std.json.Value {
    const fields = request.fields orelse return try alloc.alloc(std.json.Value, 0);
    const out = try alloc.alloc(std.json.Value, fields.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| deinitJsonValue(alloc, item);
    }
    for (fields, 0..) |field, idx| {
        out[idx] = .{ .string = try alloc.dupe(u8, field) };
        initialized = idx + 1;
    }
    return out;
}

fn parseEnsureTableRequest(alloc: Allocator, body: []const u8) !api_types.EnsureTableRequest {
    const EnsureTableRequestInput = struct {
        created_at_ns: u64 = 0,
        policy: ?api_types.NamespacePolicyRequest = null,
        schema_json: ?[]const u8 = null,
        schema: ?std.json.Value = null,
        read_schema_json: ?[]const u8 = null,
        read_schema: ?std.json.Value = null,
        indexes_json: ?[]const u8 = null,
        indexes: ?std.json.Value = null,
    };

    if (std.mem.trim(u8, body, &std.ascii.whitespace).len == 0) return .{};

    var parsed = try std.json.parseFromSlice(EnsureTableRequestInput, alloc, body, .{});
    defer parsed.deinit();
    var out: api_types.EnsureTableRequest = .{};
    errdefer out.deinit(alloc);
    out.created_at_ns = parsed.value.created_at_ns;
    out.policy = parsed.value.policy;
    out.schema_json = if (parsed.value.schema_json) |value|
        try alloc.dupe(u8, value)
    else if (parsed.value.schema) |value|
        try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})})
    else
        null;
    out.read_schema_json = if (parsed.value.read_schema_json) |value|
        try alloc.dupe(u8, value)
    else if (parsed.value.read_schema) |value|
        try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})})
    else
        null;
    out.indexes_json = if (parsed.value.indexes) |value|
        table_contract.normalizeTableDefinitionIndexesValueAlloc(alloc, value) catch return error.InvalidTableRequest
    else if (parsed.value.indexes_json) |value|
        try alloc.dupe(u8, value)
    else
        null;
    return out;
}

const ServerlessIndexStatus = struct {
    rebuilding: bool,
    backfill_active: bool,
    doc_count: u64,
    total_indexed: u64,
    materialization_blocked: bool = false,
    materialization_blocker: ?[]const u8 = null,
    planned_publication_action: ?catalog_types.ArtifactPublicationAction = null,
    head_publication_action: ?catalog_types.ArtifactPublicationAction = null,
    vector_compaction_driver: bool = false,
    vector_compaction_recommended: bool = false,
    vector_distance_metric: ?shared_vector.DistanceMetric = null,
    vector_cluster_count: ?u32 = null,
    vector_base_probe_count: ?u32 = null,
    vector_shortlist_multiplier: ?u32 = null,
    vector_cluster_imbalance: ?f32 = null,
    vector_cluster_distance_span_max: ?f32 = null,
    vector_target_cluster_count: ?u32 = null,
    vector_target_base_probe_count: ?u32 = null,
    vector_target_shortlist_multiplier: ?u32 = null,
    full_text_source_mode: ?catalog_types.FullTextSourceMode = null,
    chunked_source_count: usize = 0,
};

fn encodeServerlessIndexListAlloc(
    alloc: Allocator,
    indexes_json: []const u8,
    status: catalog_types.BuildStatus,
) ![]u8 {
    const config_map_json = try indexes_api.encodeIndexConfigMap(alloc, indexes_json);
    defer alloc.free(config_map_json);
    var parsed = try std.json.parseFromSlice(JsonValueMap, alloc, config_map_json, .{});
    defer parsed.deinit();

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '[');
    var first = true;
    var it = parsed.value.map.iterator();
    while (it.next()) |entry| {
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendServerlessIndexEntry(alloc, &out, entry.key_ptr.*, entry.value_ptr.*, status);
    }
    try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

fn encodeServerlessSingleIndexAlloc(
    alloc: Allocator,
    indexes_json: []const u8,
    index_name: []const u8,
    status: catalog_types.BuildStatus,
) !?[]u8 {
    const config_json = try indexes_api.encodeSingleIndexConfig(alloc, indexes_json, index_name);
    defer if (config_json) |value| alloc.free(value);
    const encoded = config_json orelse return null;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    defer parsed.deinit();
    const config = parsed.value;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendServerlessIndexEntry(alloc, &out, index_name, config, status);
    return try out.toOwnedSlice(alloc);
}

fn appendServerlessIndexEntry(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_name: []const u8,
    config: std.json.Value,
    status: catalog_types.BuildStatus,
) !void {
    const config_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(config, .{})});
    defer alloc.free(config_json);
    const runtime = try serverlessIndexStatus(index_name, config, status);
    try out.appendSlice(alloc, "{\"config\":");
    try out.appendSlice(alloc, config_json);
    try out.appendSlice(alloc, ",\"status\":");
    try appendServerlessIndexStatusJson(alloc, out, runtime);
    try out.appendSlice(alloc, ",\"shard_status\":{}}");
}

fn appendServerlessIndexStatusJson(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    status: ServerlessIndexStatus,
) !void {
    try out.appendSlice(alloc, "{\"rebuilding\":");
    try out.appendSlice(alloc, if (status.rebuilding) "true" else "false");
    try out.appendSlice(alloc, ",\"backfill_active\":");
    try out.appendSlice(alloc, if (status.backfill_active) "true" else "false");
    const counts = try std.fmt.allocPrint(
        alloc,
        ",\"doc_count\":{},\"total_indexed\":{},\"materialization_blocked\":{s}",
        .{
            status.doc_count,
            status.total_indexed,
            if (status.materialization_blocked) "true" else "false",
        },
    );
    defer alloc.free(counts);
    try out.appendSlice(alloc, counts);
    if (status.materialization_blocker) |blocker| {
        const encoded_blocker = try std.json.Stringify.valueAlloc(alloc, blocker, .{});
        defer alloc.free(encoded_blocker);
        const encoded_field = try std.fmt.allocPrint(
            alloc,
            ",\"materialization_blocker\":{s}",
            .{encoded_blocker},
        );
        defer alloc.free(encoded_field);
        try out.appendSlice(alloc, encoded_field);
    }
    if (status.planned_publication_action) |action| {
        const encoded_action = try std.json.Stringify.valueAlloc(alloc, action, .{});
        defer alloc.free(encoded_action);
        const encoded_field = try std.fmt.allocPrint(
            alloc,
            ",\"planned_publication_action\":{s}",
            .{encoded_action},
        );
        defer alloc.free(encoded_field);
        try out.appendSlice(alloc, encoded_field);
    }
    if (status.head_publication_action) |action| {
        const encoded_action = try std.json.Stringify.valueAlloc(alloc, action, .{});
        defer alloc.free(encoded_action);
        const encoded_field = try std.fmt.allocPrint(
            alloc,
            ",\"head_publication_action\":{s}",
            .{encoded_action},
        );
        defer alloc.free(encoded_field);
        try out.appendSlice(alloc, encoded_field);
    }
    const encoded_vector_compaction = try std.fmt.allocPrint(
        alloc,
        ",\"vector_compaction_driver\":{s},\"vector_compaction_recommended\":{s}",
        .{
            if (status.vector_compaction_driver) "true" else "false",
            if (status.vector_compaction_recommended) "true" else "false",
        },
    );
    defer alloc.free(encoded_vector_compaction);
    try out.appendSlice(alloc, encoded_vector_compaction);
    if (status.vector_compaction_driver) {
        try out.appendSlice(alloc, ",\"vector_distance_metric\":");
        if (status.vector_distance_metric) |value| {
            const encoded_metric = try std.fmt.allocPrint(alloc, "\"{s}\"", .{@tagName(value)});
            defer alloc.free(encoded_metric);
            try out.appendSlice(alloc, encoded_metric);
        } else try out.appendSlice(alloc, "null");
        try out.appendSlice(alloc, ",\"vector_cluster_count\":");
        if (status.vector_cluster_count) |value|
            try out.print(alloc, "{}", .{value})
        else
            try out.appendSlice(alloc, "null");
        try out.appendSlice(alloc, ",\"vector_base_probe_count\":");
        if (status.vector_base_probe_count) |value|
            try out.print(alloc, "{}", .{value})
        else
            try out.appendSlice(alloc, "null");
        try out.appendSlice(alloc, ",\"vector_shortlist_multiplier\":");
        if (status.vector_shortlist_multiplier) |value|
            try out.print(alloc, "{}", .{value})
        else
            try out.appendSlice(alloc, "null");
        try out.appendSlice(alloc, ",\"vector_cluster_imbalance\":");
        if (status.vector_cluster_imbalance) |value|
            try out.print(alloc, "{d}", .{value})
        else
            try out.appendSlice(alloc, "null");
        try out.appendSlice(alloc, ",\"vector_cluster_distance_span_max\":");
        if (status.vector_cluster_distance_span_max) |value|
            try out.print(alloc, "{d}", .{value})
        else
            try out.appendSlice(alloc, "null");
        try out.appendSlice(alloc, ",\"vector_target_cluster_count\":");
        if (status.vector_target_cluster_count) |value|
            try out.print(alloc, "{}", .{value})
        else
            try out.appendSlice(alloc, "null");
        try out.appendSlice(alloc, ",\"vector_target_base_probe_count\":");
        if (status.vector_target_base_probe_count) |value|
            try out.print(alloc, "{}", .{value})
        else
            try out.appendSlice(alloc, "null");
        try out.appendSlice(alloc, ",\"vector_target_shortlist_multiplier\":");
        if (status.vector_target_shortlist_multiplier) |value|
            try out.print(alloc, "{}", .{value})
        else
            try out.appendSlice(alloc, "null");
    }
    if (status.full_text_source_mode) |source_mode| {
        const encoded_mode = try std.json.Stringify.valueAlloc(alloc, source_mode, .{});
        defer alloc.free(encoded_mode);
        const encoded_chunk_count = try std.fmt.allocPrint(
            alloc,
            ",\"full_text_source_mode\":{s},\"chunked_source_count\":{},\"chunked_full_text\":{s}",
            .{
                encoded_mode,
                status.chunked_source_count,
                if (status.chunked_source_count > 0) "true" else "false",
            },
        );
        defer alloc.free(encoded_chunk_count);
        try out.appendSlice(alloc, encoded_chunk_count);
    }
    try out.append(alloc, '}');
}

fn serverlessIndexStatus(
    index_name: []const u8,
    config: std.json.Value,
    status: catalog_types.BuildStatus,
) !ServerlessIndexStatus {
    if (config != .object) return error.InvalidTableIndexMetadata;
    const kind = switch (indexes_api.inferIndexType(index_name, config) orelse return error.InvalidTableIndexMetadata) {
        .full_text => "full_text",
        .embeddings => "embeddings",
        .graph => "graph",
        .algebraic => "algebraic",
    };

    const has_documents = status.enrichment_total_document_count != 0 or status.latest_wal_lsn != 0;
    const full_text_action = findFullTextIndexPublicationAction(status.full_text_index_actions, index_name);
    const head_full_text_action = findFullTextIndexPublicationAction(status.head_full_text_index_actions, index_name);
    const vector_action = findNamedArtifactPublicationAction(status.vector_index_actions, index_name);
    const head_vector_action = findNamedArtifactPublicationAction(status.head_vector_index_actions, index_name);
    const sparse_action = findNamedArtifactPublicationAction(status.sparse_index_actions, index_name);
    const head_sparse_action = findNamedArtifactPublicationAction(status.head_sparse_index_actions, index_name);
    const graph_action = findNamedArtifactPublicationAction(status.graph_index_actions, index_name);
    const head_graph_action = findNamedArtifactPublicationAction(status.head_graph_index_actions, index_name);
    const built = if (std.mem.eql(u8, kind, "full_text")) blk: {
        if (full_text_action) |action| {
            break :blk action.action == .reuse;
        }
        break :blk status.head_version != 0;
    } else if (std.mem.eql(u8, kind, "embeddings")) blk: {
        const sparse = if (config.object.get("sparse")) |value| switch (value) {
            .bool => |flag| flag,
            else => return error.InvalidTableIndexMetadata,
        } else false;
        if (sparse) {
            if (sparse_action) |action| {
                break :blk action.action == .reuse;
            }
            break :blk search_sources.containsSparseIndexName(status.materialized_search_sources, index_name);
        }
        if (vector_action) |action| {
            break :blk action.action == .reuse;
        }
        break :blk search_sources.containsVectorIndexName(status.materialized_search_sources, index_name);
    } else if (std.mem.eql(u8, kind, "graph")) blk: {
        if (graph_action) |action| {
            break :blk action.action == .reuse;
        }
        break :blk status.head_version != 0;
    } else if (std.mem.eql(u8, kind, "algebraic")) blk: {
        break :blk status.head_version != 0;
    } else return error.InvalidTableIndexMetadata;

    const doc_count: u64 = if (built)
        if (status.enrichment_total_document_count != 0)
            status.enrichment_total_document_count
        else if (has_documents)
            1
        else
            0
    else
        0;
    const materialization_blocker: ?[]const u8 = if (std.mem.eql(u8, kind, "full_text")) blk: {
        if (full_text_action) |action| {
            if (action.chunked_source_count > 0 and status.pending_materialization_families.chunk_preview) break :blk "chunk_preview";
        }
        if (status.pending_materialization_families.full_text) break :blk "full_text";
        break :blk null;
    } else if (std.mem.eql(u8, kind, "embeddings")) blk: {
        const sparse = try isSparseEmbeddingsIndex(config);
        if (sparse) {
            if (status.pending_materialization_families.sparse_vector) break :blk "lexical_sparse";
            break :blk null;
        }
        if (indexUsesChunkEmbeddings(config) and status.pending_materialization_families.chunk_embeddings) break :blk "chunk_embeddings";
        if (status.pending_materialization_families.dense_vector) break :blk "dense_vector";
        break :blk null;
    } else null;
    const is_vector_driver = std.mem.eql(u8, kind, "embeddings") and !try isSparseEmbeddingsIndex(config) and status.vector_compaction_driver_index_name != null and std.mem.eql(u8, status.vector_compaction_driver_index_name.?, index_name);
    return .{
        .rebuilding = !built and has_documents,
        .backfill_active = !built and has_documents,
        .doc_count = doc_count,
        .total_indexed = doc_count,
        .materialization_blocked = materialization_blocker != null,
        .materialization_blocker = materialization_blocker,
        .planned_publication_action = if (std.mem.eql(u8, kind, "full_text"))
            if (full_text_action) |action| action.action else null
        else if (std.mem.eql(u8, kind, "embeddings")) blk: {
            const sparse = if (config.object.get("sparse")) |value| switch (value) {
                .bool => |flag| flag,
                else => return error.InvalidTableIndexMetadata,
            } else false;
            break :blk if (sparse)
                if (sparse_action) |action| action.action else null
            else if (vector_action) |action|
                action.action
            else
                null;
        } else if (std.mem.eql(u8, kind, "graph"))
            if (graph_action) |action| action.action else null
        else
            null,
        .head_publication_action = if (std.mem.eql(u8, kind, "full_text"))
            if (head_full_text_action) |action| action.action else null
        else if (std.mem.eql(u8, kind, "embeddings")) blk: {
            const sparse = if (config.object.get("sparse")) |value| switch (value) {
                .bool => |flag| flag,
                else => return error.InvalidTableIndexMetadata,
            } else false;
            break :blk if (sparse)
                if (head_sparse_action) |action| action.action else null
            else if (head_vector_action) |action|
                action.action
            else
                null;
        } else if (std.mem.eql(u8, kind, "graph"))
            if (head_graph_action) |action| action.action else null
        else
            null,
        .vector_compaction_driver = is_vector_driver,
        .vector_compaction_recommended = is_vector_driver and status.compaction_recommended and status.vector_target_cluster_count != null,
        .vector_distance_metric = if (is_vector_driver) status.vector_compaction_distance_metric else null,
        .vector_cluster_count = if (is_vector_driver) status.vector_cluster_count else null,
        .vector_base_probe_count = if (is_vector_driver) status.vector_base_probe_count else null,
        .vector_shortlist_multiplier = if (is_vector_driver) status.vector_shortlist_multiplier else null,
        .vector_cluster_imbalance = if (is_vector_driver) status.vector_cluster_imbalance else null,
        .vector_cluster_distance_span_max = if (is_vector_driver) status.vector_cluster_distance_span_max else null,
        .vector_target_cluster_count = if (is_vector_driver) status.vector_target_cluster_count else null,
        .vector_target_base_probe_count = if (is_vector_driver) status.vector_target_base_probe_count else null,
        .vector_target_shortlist_multiplier = if (is_vector_driver) status.vector_target_shortlist_multiplier else null,
        .full_text_source_mode = if (std.mem.eql(u8, kind, "full_text") and full_text_action != null) full_text_action.?.source_mode else null,
        .chunked_source_count = if (std.mem.eql(u8, kind, "full_text") and full_text_action != null) full_text_action.?.chunked_source_count else 0,
    };
}

fn isSparseEmbeddingsIndex(config: std.json.Value) !bool {
    if (config != .object) return error.InvalidTableIndexMetadata;
    return if (config.object.get("sparse")) |value| switch (value) {
        .bool => |flag| flag,
        else => return error.InvalidTableIndexMetadata,
    } else false;
}

fn indexUsesChunkEmbeddings(config: std.json.Value) bool {
    if (config != .object) return false;
    return config.object.get("chunker") != null;
}

fn findFullTextIndexPublicationAction(
    actions: []const catalog_types.FullTextIndexPublicationAction,
    index_name: []const u8,
) ?catalog_types.FullTextIndexPublicationAction {
    for (actions) |entry| {
        if (std.mem.eql(u8, entry.name, index_name)) return entry;
    }
    return null;
}

fn findNamedArtifactPublicationAction(
    actions: []const catalog_types.NamedArtifactPublicationAction,
    index_name: []const u8,
) ?catalog_types.NamedArtifactPublicationAction {
    for (actions) |entry| {
        if (std.mem.eql(u8, entry.name, index_name)) return entry;
    }
    return null;
}

fn validateServerlessIndexCatalog(alloc: Allocator, indexes_json: []const u8) !void {
    var parsed = try std.json.parseFromSlice(JsonValueMap, alloc, indexes_json, .{});
    defer parsed.deinit();

    var full_text_count: usize = 0;
    var versioned_full_text_count: usize = 0;
    var graph_count: usize = 0;
    var it = parsed.value.map.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) return error.InvalidTableIndexMetadata;
        const kind = if (entry.value_ptr.object.get("type")) |value|
            switch (value) {
                .string => value.string,
                else => return error.InvalidTableIndexMetadata,
            }
        else
            "full_text";

        if (std.mem.eql(u8, kind, "full_text")) {
            full_text_count += 1;
            if (std.mem.startsWith(u8, entry.key_ptr.*, "full_text_index_v")) {
                versioned_full_text_count += 1;
            }
            continue;
        }
        if (std.mem.eql(u8, kind, "embeddings")) {
            const sparse = if (entry.value_ptr.object.get("sparse")) |value| switch (value) {
                .bool => |flag| flag,
                else => return error.InvalidTableIndexMetadata,
            } else false;
            _ = sparse;
            continue;
        }
        if (std.mem.eql(u8, kind, "graph")) {
            graph_count += 1;
            continue;
        }
        if (std.mem.eql(u8, kind, "algebraic")) {
            try validateServerlessAlgebraicIndexConfig(alloc, entry.value_ptr.*);
            continue;
        }
        return error.UnsupportedCreateTableRequest;
    }

    if ((full_text_count > 1 and versioned_full_text_count != full_text_count) or graph_count > 1) {
        return error.UnsupportedCreateTableRequest;
    }
}

fn validateServerlessAlgebraicIndexConfig(alloc: Allocator, value: std.json.Value) !void {
    if (value != .object) return error.InvalidTableIndexMetadata;
    var parsed = std.json.parseFromValue(db_mod.algebraic.index.Config, alloc, value, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch return error.InvalidTableIndexMetadata;
    defer parsed.deinit();

    db_mod.algebraic.index.validateConfig(parsed.value) catch return error.InvalidTableIndexMetadata;
}

fn parseNamespacePolicyRequest(alloc: Allocator, body: []const u8) !api_types.NamespacePolicyRequest {
    var parsed = try std.json.parseFromSlice(api_types.NamespacePolicyRequest, alloc, body, .{});
    defer parsed.deinit();
    return parsed.value;
}

fn parseMutationKind(raw: []const u8) !api_types.MutationKind {
    if (std.mem.eql(u8, raw, "upsert")) return .upsert;
    if (std.mem.eql(u8, raw, "delete")) return .delete;
    return error.InvalidMutationKind;
}

fn freeDocumentMutations(alloc: Allocator, mutations: []const api_types.DocumentMutation) void {
    for (mutations) |mutation| {
        alloc.free(mutation.doc_id);
        if (mutation.body) |body| alloc.free(body);
    }
    alloc.free(mutations);
}

fn allocTailMutations(alloc: Allocator, records: []const wal_mod.Record) ![]query_types.QueryTailMutation {
    const tail = try alloc.alloc(query_types.QueryTailMutation, records.len);
    errdefer alloc.free(tail);

    var initialized: usize = 0;
    errdefer {
        for (tail[0..initialized]) |entry| {
            alloc.free(entry.doc_id);
            if (entry.body) |body| alloc.free(body);
        }
    }

    for (records, 0..) |record, idx| {
        var mutation = try @import("codec.zig").decodeMutationAlloc(alloc, record.payload);
        defer mutation.deinit(alloc);
        tail[idx] = .{
            .lsn = record.lsn,
            .timestamp_ns = record.timestamp_ns,
            .kind = mutation.kind,
            .doc_id = try alloc.dupe(u8, mutation.doc_id),
            .body = if (mutation.body) |body| try alloc.dupe(u8, body) else null,
        };
        initialized += 1;
    }
    return tail;
}

fn decodeSegmentEntriesAlloc(alloc: Allocator, contents: []const u8) ![]query_types.QueryMutation {
    const segment_entries = try segment_mod.decodeAlloc(alloc, contents);
    defer segment_mod.freeEntries(alloc, segment_entries);

    const entries = try alloc.alloc(query_types.QueryMutation, segment_entries.len);
    errdefer alloc.free(entries);

    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |entry| {
            alloc.free(entry.doc_id);
            if (entry.body) |body| alloc.free(body);
        }
    }

    for (segment_entries, 0..) |entry, idx| {
        entries[idx] = .{
            .lsn = entry.lsn,
            .timestamp_ns = entry.timestamp_ns,
            .kind = entry.kind,
            .doc_id = try alloc.dupe(u8, entry.doc_id),
            .body = if (entry.body) |body| try alloc.dupe(u8, body) else null,
        };
        initialized += 1;
    }
    return entries;
}

fn freeQueryMutations(alloc: Allocator, tail: []const query_types.QueryMutation) void {
    for (tail) |entry| {
        alloc.free(entry.doc_id);
        if (entry.body) |body| alloc.free(body);
    }
    alloc.free(tail);
}

fn freeQueryDocuments(alloc: Allocator, docs: []const query_types.QueryDocument) void {
    for (docs) |doc| {
        alloc.free(doc.doc_id);
        alloc.free(doc.body);
    }
    alloc.free(docs);
}

fn freeTableQueryResult(alloc: Allocator, result: *query_types.TableQueryResult) void {
    search_sources.deinitPublishedSearchSources(alloc, &result.materialized_search_sources);
    search_sources.deinitMaterializedDerivedOutputs(alloc, &result.materialized_derived_outputs);
    deinitQueryPublicationStatus(alloc, &result.publication);
    alloc.free(result.artifacts);
    freeQueryDocuments(alloc, result.documents);
    result.* = undefined;
}

fn freeQueryHits(alloc: Allocator, hits: []const query_types.QueryHit) void {
    for (hits) |hit| {
        alloc.free(hit.doc_id);
        alloc.free(hit.body);
    }
    alloc.free(hits);
}

fn allocDbSearchHitsAlloc(
    alloc: Allocator,
    hits: []const query_mod.QuerySearchHit,
) ![]db_types.SearchHit {
    const out_hits = try alloc.alloc(db_types.SearchHit, hits.len);
    errdefer alloc.free(out_hits);
    var initialized_hits: usize = 0;
    errdefer {
        for (out_hits[0..initialized_hits]) |*hit| hit.deinit(alloc);
    }
    for (hits, 0..) |hit, idx| {
        out_hits[idx] = .{
            .id = try alloc.dupe(u8, hit.doc_id),
            .score = @as(f32, @floatFromInt(hit.score)) / 1000.0,
            .stored_data = try alloc.dupe(u8, hit.body),
        };
        initialized_hits += 1;
    }
    return out_hits;
}

fn freeDbSearchHits(alloc: Allocator, hits: []db_types.SearchHit) void {
    for (hits) |*hit| hit.deinit(alloc);
    alloc.free(hits);
}

fn allocQueryHitsAlloc(
    alloc: Allocator,
    hits: []const query_mod.QuerySearchHit,
    fields: ?[][]u8,
    count_only: bool,
) ![]query_types.QueryHit {
    if (count_only) return try alloc.alloc(query_types.QueryHit, 0);

    const out_hits = try alloc.alloc(query_types.QueryHit, hits.len);
    errdefer alloc.free(out_hits);
    var initialized_hits: usize = 0;
    errdefer {
        for (out_hits[0..initialized_hits]) |hit| {
            alloc.free(hit.doc_id);
            alloc.free(hit.body);
        }
    }
    for (hits, 0..) |hit, idx| {
        out_hits[idx] = .{
            .doc_id = try alloc.dupe(u8, hit.doc_id),
            .body = if (fields) |requested_fields|
                try projectBodyFieldsAlloc(alloc, hit.body, requested_fields)
            else
                try alloc.dupe(u8, hit.body),
            .score = hit.score,
        };
        initialized_hits += 1;
    }
    return out_hits;
}

fn projectBodyFieldsAlloc(alloc: Allocator, body: []const u8, fields: [][]u8) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var parsed = parseJsonObjectAlloc(arena, body) catch {
        return try alloc.dupe(u8, body);
    };
    defer parsed.deinit();

    var out_obj = std.json.ObjectMap.empty;
    for (fields) |field| {
        const value = json_helpers.extractJsonPathValue(parsed.value, field) orelse continue;
        try insertProjectedFieldPath(arena, &out_obj, field, value);
    }

    return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = out_obj }, .{});
}

fn insertProjectedFieldPath(
    alloc: Allocator,
    out_obj: *std.json.ObjectMap,
    field: []const u8,
    value: std.json.Value,
) !void {
    var current = out_obj;
    var remaining = field;
    while (true) {
        if (std.mem.indexOfScalar(u8, remaining, '.')) |dot_idx| {
            const segment = remaining[0..dot_idx];
            remaining = remaining[dot_idx + 1 ..];

            if (current.getPtr(segment)) |existing| {
                if (existing.* != .object) {
                    existing.* = .{ .object = std.json.ObjectMap.empty };
                }
                current = &existing.object;
                continue;
            }

            try current.put(alloc, segment, .{ .object = std.json.ObjectMap.empty });
            current = &current.getPtr(segment).?.object;
            continue;
        }

        try current.put(alloc, remaining, value);
        return;
    }
}

fn planSupportedJoinExecution(self: *HttpHandler, alloc: Allocator, join: SupportedJoinRequest, left_hits: []const std.json.Value, foreign_sources: foreign_mod.PostgresSourceMap) PlannedJoinExecution {
    return query_execution.planSupportedJoinExecution(self.foreign_registry, alloc, join, left_hits, foreign_sources);
}

fn estimateForeignJoinTableStats(self: *HttpHandler, foreign_source: foreign_mod.PostgresConfig) ?JoinTableStats {
    return query_execution.estimateForeignJoinTableStats(self.foreign_registry, self.alloc, foreign_source);
}

const estimateJoinPlanCosts = query_execution.estimateJoinPlanCosts;
const buildRightJoinQueryValue = query_execution.buildRightJoinQueryValue;
const buildCombinedRightFilterQueryValue = query_execution.buildCombinedRightFilterQueryValue;
const buildSupportedJoinClauseValue = query_execution.buildSupportedJoinClauseValue;
const buildJoinEqualityQuery = query_execution.buildJoinEqualityQuery;
const ensureQueryFieldsContains = query_execution.ensureQueryFieldsContains;
const queryHitsArrayPtr = query_execution.queryHitsArrayPtr;
const queryTotalHits = query_execution.queryTotalHits;
const queryRequestedFields = query_execution.queryRequestedFields;
const extractJoinValueFromHit = query_execution.extractJoinValueFromHit;
const extractJoinValueFromDocument = query_execution.extractJoinValueFromDocument;
const extractJsonPathValue = query_execution.extractJsonPathValue;
const freeOwnedStringSlice = query_execution.freeOwnedStringSlice;
const buildForeignJoinFieldListAlloc = query_execution.buildForeignJoinFieldListAlloc;
const scalarJsonValueStringAlloc = query_execution.scalarJsonValueStringAlloc;
const buildForeignRightJoinHit = query_execution.buildForeignRightJoinHit;
const buildUnmatchedRightJoinHit = join_model.buildUnmatchedRightJoinHitAlloc;
const buildRightJoinHitFromDocument = query_execution.buildRightJoinHitFromDocument;
const findFirstMatchingRightHit = query_execution.findFirstMatchingRightHit;
const mergeRightHitIntoSource = join_model.mergeRightHitIntoSourceAlloc;
const maybeAttachJoinProfile = query_execution.maybeAttachJoinProfile;
const allocRequestedFieldsFromHits = query_execution.allocRequestedFieldsFromHits;
const freeJsonStringArray = query_execution.freeJsonStringArray;
const documentMatchesPublicTextSpec = query_execution.documentMatchesPublicTextSpec;

const putJsonObjectFieldOwned = query_execution.putJsonObjectFieldOwned;

fn removeOwnedJsonObjectField(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    key: []const u8,
) void {
    if (obj.fetchOrderedRemove(key)) |kv| {
        alloc.free(@constCast(kv.key));
        var value = kv.value;
        deinitJsonValue(alloc, &value);
    }
}

fn stringifyJsonValueAlloc(alloc: Allocator, value: std.json.Value) ![]u8 {
    return try json_helpers.stringifyJsonValueAlloc(alloc, value);
}

const cloneJsonValue = query_execution.cloneJsonValue;
const deinitJsonValue = query_execution.deinitJsonValue;
const removeFieldFromSourceObject = join_model.removeFieldFromSourceObject;
const computeJsonHitMaxScore = query_execution.computeJsonHitMaxScore;
const jsonValuesEqual = query_execution.jsonValuesEqual;

fn updateJoinedResponseMetadata(
    alloc: Allocator,
    root: *std.json.Value,
    total_hits: usize,
    hits: []const std.json.Value,
) !void {
    try join_model.applyJoinResponseMetadata(alloc, try join_model.queryHitsObjectPtr(root), .{
        .total_hits = total_hits,
        .max_score = computeJsonHitMaxScore(hits),
    });
}

fn jsonResponse(alloc: Allocator, status: u16, value: anytype) !HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "application/json"),
        .body = try std.json.Stringify.valueAlloc(alloc, value, .{}),
    };
}

fn jsonSliceResponse(alloc: Allocator, status: u16, body: []const u8) !HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "application/json"),
        .body = try alloc.dupe(u8, body),
    };
}

fn parseJsonResponseBody(comptime T: type, alloc: Allocator, body: []const u8) !T {
    var parsed = try std.json.parseFromSlice(T, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    return parsed.value;
}

fn typedJsonResponse(comptime T: type, alloc: Allocator, status: u16, body: []const u8) !HttpResponse {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const parsed = try parseJsonResponseBody(T, arena_impl.allocator(), body);
    return try jsonResponse(alloc, status, parsed);
}

test "typed index status response rejects extended variant fields but raw json preserves them" {
    const alloc = std.testing.allocator;
    const body =
        \\{
        \\  "config": { "name": "graph_idx", "type": "graph" },
        \\  "status": {
        \\    "rebuilding": false,
        \\    "backfill_active": false,
        \\    "doc_count": 0,
        \\    "total_indexed": 0,
        \\    "chunked_full_text": true
        \\  },
        \\  "shard_status": {}
        \\}
    ;

    try std.testing.expectError(
        error.UnknownField,
        typedJsonResponse(metadata_openapi.IndexStatus, alloc, 200, body),
    );

    var raw = try jsonSliceResponse(alloc, 200, body);
    defer raw.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), raw.status);
    try std.testing.expectEqualStrings("application/json", raw.content_type);
    try std.testing.expectEqualStrings(body, raw.body);
}

fn parseJsonTestBody(comptime T: type, alloc: Allocator, body: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, body, .{
        .ignore_unknown_fields = true,
    });
}

fn parseBackgroundMatchedJsonPathValueAlloc(
    alloc: Allocator,
    body: []const u8,
    path: []const u8,
    background_query: db_mod.aggregations.BackgroundQuery,
) !?ParsedJsonPathValue {
    var parsed = parseJsonValueAlloc(alloc, body) catch return null;
    errdefer parsed.deinit();
    if (!(try HttpHandler.documentMatchesBackgroundQueryAlloc(alloc, parsed.value, background_query))) return null;
    const value = extractJsonPathValue(parsed.value, path) orelse return null;
    return .{
        .parsed = parsed,
        .value = value,
    };
}

const TestQueryHitInput = struct {
    _id: []const u8,
    _source: std.json.Value,
};

const OwnedJsonValueSlice = struct {
    alloc: Allocator,
    values: []std.json.Value,

    fn deinit(self: *@This()) void {
        for (self.values) |*value| deinitJsonValue(self.alloc, value);
        self.alloc.free(self.values);
    }
};

fn parseTestQueryHitsAlloc(alloc: Allocator, body: []const u8) !OwnedJsonValueSlice {
    var parsed = try std.json.parseFromSlice([]const TestQueryHitInput, alloc, body, .{});
    defer parsed.deinit();

    const hits = try alloc.alloc(std.json.Value, parsed.value.len);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |*value| deinitJsonValue(alloc, value);
        alloc.free(hits);
    }

    for (parsed.value, 0..) |input, idx| {
        var hit = std.json.Value{ .object = std.json.ObjectMap.empty };
        errdefer deinitJsonValue(alloc, &hit);
        try hit.object.put(alloc, try alloc.dupe(u8, "_id"), .{ .string = try alloc.dupe(u8, input._id) });
        try hit.object.put(alloc, try alloc.dupe(u8, "_source"), try cloneJsonValue(alloc, input._source));
        hits[idx] = hit;
        initialized += 1;
    }

    return .{
        .alloc = alloc,
        .values = hits,
    };
}

fn testQueryHitSourcePathValue(hit: anytype, path: []const u8) ?std.json.Value {
    const source = hit._source orelse return null;
    return extractJsonPathValue(source, path);
}

fn testOwnedHitSourcePathValue(hit: std.json.Value, path: []const u8) ?std.json.Value {
    const source = hit.object.get("_source") orelse return null;
    return extractJsonPathValue(source, path);
}

fn testJoinProfileFieldValue(response: anytype, field: []const u8) ?std.json.Value {
    const profile = response.profile orelse return null;
    if (profile != .object) return null;
    const join = profile.object.get("join") orelse return null;
    if (join != .object) return null;
    return join.object.get(field);
}

const ServerlessIndexStatusTestResponse = struct {
    const Status = struct {
        rebuilding: ?bool = null,
        backfill_active: ?bool = null,
        doc_count: ?u64 = null,
        total_indexed: ?u64 = null,
        materialization_blocked: ?bool = null,
        materialization_blocker: ?[]const u8 = null,
        planned_publication_action: ?[]const u8 = null,
        head_publication_action: ?[]const u8 = null,
        vector_compaction_driver: ?bool = null,
        vector_compaction_recommended: ?bool = null,
        vector_distance_metric: ?[]const u8 = null,
        vector_cluster_count: ?u32 = null,
        vector_base_probe_count: ?u32 = null,
        vector_shortlist_multiplier: ?u32 = null,
        vector_cluster_imbalance: ?f32 = null,
        vector_cluster_distance_span_max: ?f32 = null,
        vector_target_cluster_count: ?u32 = null,
        vector_target_base_probe_count: ?u32 = null,
        vector_target_shortlist_multiplier: ?u32 = null,
        full_text_source_mode: ?[]const u8 = null,
        chunked_source_count: ?usize = null,
        chunked_full_text: ?bool = null,
    };

    config: struct {
        name: []const u8,
    },
    status: Status,
};

fn parseServerlessIndexStatusTestResponse(
    alloc: Allocator,
    body: []const u8,
    expected_name: []const u8,
) !std.json.Parsed(ServerlessIndexStatusTestResponse) {
    var parsed_name = try parseOwnedJsonValueAlloc(alloc, body);
    defer deinitJsonValue(alloc, &parsed_name);
    try std.testing.expectEqualStrings(
        expected_name,
        parsed_name.object.get("config").?.object.get("name").?.string,
    );

    const parsed = try std.json.parseFromSlice(ServerlessIndexStatusTestResponse, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    try std.testing.expectEqualStrings(expected_name, parsed.value.config.name);
    return parsed;
}

const TestAggregationBucket = struct {
    key: []const u8,
    doc_count: i64,
    score: ?f64 = null,
    bg_count: ?i64 = null,
};

const TestAggregationMetadata = struct {
    bg_doc_count: ?i64 = null,
};

const TestAggregationValue = struct {
    count: ?i64 = null,
    buckets: ?[]const TestAggregationBucket = null,
    metadata: ?TestAggregationMetadata = null,
};

const TestSearchAggregationsResponse = struct {
    aggregations: ?std.json.ArrayHashMap(TestAggregationValue) = null,
};

const TestQueryAggregationsResponse = struct {
    responses: []const struct {
        aggregations: ?std.json.ArrayHashMap(TestAggregationValue) = null,
    },
};

fn textResponse(alloc: Allocator, status: u16, body: []const u8) !HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "text/plain"),
        .body = try alloc.dupe(u8, body),
    };
}

test "http handler serves internal namespace lifecycle and query head" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests");
    const wal_root = tmpPath(&wal_root_buf, "wal");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    var status = try handler.handle(.{
        .method = .get,
        .path = "/status",
    });
    defer status.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), status.status);
    var parsed_status = try parseJsonTestBody(api_types.RuntimeStatusResult, alloc, status.body);
    defer parsed_status.deinit();
    try std.testing.expect(parsed_status.value.validated);
    try std.testing.expectEqual(api_types.RuntimeRole.combined, parsed_status.value.role);
    try std.testing.expectEqual(@as(u64, 1), parsed_status.value.tick_interval_ms);
    try std.testing.expectEqual(@as(usize, 0), parsed_status.value.targets.len);

    var health = try handler.handle(.{
        .method = .get,
        .path = "/health",
    });
    defer health.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), health.status);
    var parsed_health = try parseJsonTestBody(api_types.HealthResult, alloc, health.body);
    defer parsed_health.deinit();
    try std.testing.expect(parsed_health.value.live);
    try std.testing.expect(parsed_health.value.ready);
    try std.testing.expect(parsed_health.value.validated);
    try std.testing.expectEqual(@as(usize, 0), parsed_health.value.namespace_count);

    var healthz = try handler.handle(.{
        .method = .get,
        .path = "/healthz",
    });
    defer healthz.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), healthz.status);
    try std.testing.expectEqualStrings("application/json", healthz.content_type);
    try std.testing.expect(std.mem.indexOf(u8, healthz.body, "\"status\":\"ok\"") != null);

    var readyz = try handler.handle(.{
        .method = .get,
        .path = "/readyz",
    });
    defer readyz.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), readyz.status);
    try std.testing.expectEqualStrings("application/json", readyz.content_type);
    try std.testing.expect(std.mem.indexOf(u8, readyz.body, "\"status\":\"ready\"") != null);

    runtime_status.validated = false;
    var not_readyz = try handler.handle(.{
        .method = .get,
        .path = "/readyz",
    });
    defer not_readyz.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 503), not_readyz.status);
    try std.testing.expect(std.mem.indexOf(u8, not_readyz.body, "\"status\":\"not_ready\"") != null);
    runtime_status.validated = true;

    var metrics = try handler.handle(.{
        .method = .get,
        .path = "/metrics",
    });
    defer metrics.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), metrics.status);
    var parsed_metrics = try parseJsonTestBody(api_types.MetricsResult, alloc, metrics.body);
    defer parsed_metrics.deinit();
    try std.testing.expectEqual(@as(u64, 0), parsed_metrics.value.total_pending_records);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics.value.publish_head_conflicts);
    try std.testing.expectEqual(@as(u64, 0), parsed_metrics.value.cache_hits);
    try std.testing.expectEqual(@as(u64, 0), parsed_metrics.value.cache_block_hits);
    try std.testing.expectEqual(@as(u64, 0), parsed_metrics.value.cache_routing_block_hits);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics.value.enriched_namespaces);
    try std.testing.expectEqual(@as(u64, 0), parsed_metrics.value.cache_pinned_bytes);
    try std.testing.expectEqual(@as(u64, 0), parsed_metrics.value.cache_pinned_block_count);
    try std.testing.expectEqual(@as(u64, 0), parsed_metrics.value.cache_approx_payload_block_hits);
    try std.testing.expectEqual(@as(u64, 0), parsed_metrics.value.cache_max_payload_bytes);

    var create = try handler.handle(.{
        .method = .put,
        .path = "/internal/v1/namespaces/docs",
        .body = "{\"created_at_ns\":123,\"policy\":{\"default_query_view\":\"published\",\"keep_latest_versions\":4,\"max_pending_records\":4,\"compaction_trigger_version_count\":2,\"vector_distance_metric\":\"inner_product\"}}",
    });
    defer create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), create.status);
    var parsed_create = try parseJsonTestBody(api_types.EnsureNamespaceResult, alloc, create.body);
    defer parsed_create.deinit();
    try std.testing.expectEqualStrings("docs", parsed_create.value.namespace);
    try std.testing.expect(parsed_create.value.created);
    try std.testing.expectEqual(@as(u64, 123), parsed_create.value.created_at_ns);
    try std.testing.expectEqual(@as(u64, 4), parsed_create.value.policy.keep_latest_versions);
    try std.testing.expectEqual(shared_vector.DistanceMetric.inner_product, parsed_create.value.policy.vector_distance_metric);

    var list = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/namespaces",
    });
    defer list.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), list.status);
    var parsed_list = try parseJsonTestBody([]catalog_types.NamespaceRecord, alloc, list.body);
    defer parsed_list.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_list.value.len);
    try std.testing.expectEqualStrings("docs", parsed_list.value[0].name);
    try std.testing.expectEqual(@as(u64, 4), parsed_list.value[0].policy.keep_latest_versions);
    try std.testing.expectEqual(shared_vector.DistanceMetric.inner_product, parsed_list.value[0].policy.vector_distance_metric);

    var policy = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/namespaces/docs/policy",
    });
    defer policy.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), policy.status);
    var parsed_policy = try parseJsonTestBody(api_types.NamespacePolicyResult, alloc, policy.body);
    defer parsed_policy.deinit();
    try std.testing.expectEqualStrings("docs", parsed_policy.value.namespace);
    try std.testing.expectEqual(@as(u64, 4), parsed_policy.value.policy.keep_latest_versions);
    try std.testing.expectEqual(shared_vector.DistanceMetric.inner_product, parsed_policy.value.policy.vector_distance_metric);

    var ingest = try handler.handle(.{
        .method = .put,
        .path = "/internal/v1/namespaces/docs/ingest-batch",
        .body =
        \\{"timestamp_ns":456,"mutations":[{"kind":"upsert","doc_id":"doc-a","body":"alpha"},{"kind":"delete","doc_id":"doc-b"}]}
        ,
    });
    defer ingest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), ingest.status);
    var parsed_ingest = try parseJsonTestBody(api_types.IngestBatchResult, alloc, ingest.body);
    defer parsed_ingest.deinit();
    try std.testing.expectEqualStrings("docs", parsed_ingest.value.namespace);
    try std.testing.expectEqual(@as(usize, 2), parsed_ingest.value.mutation_count);
    try std.testing.expectEqual(@as(u64, 1), parsed_ingest.value.start_lsn);
    try std.testing.expectEqual(@as(u64, 2), parsed_ingest.value.end_lsn);

    var before = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/namespaces/docs/build-status",
    });
    defer before.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), before.status);
    var parsed_before = try parseJsonTestBody(catalog_types.BuildStatus, alloc, before.body);
    defer parsed_before.deinit();
    try std.testing.expectEqualStrings("docs", parsed_before.value.namespace);
    try std.testing.expectEqual(@as(u64, 2), parsed_before.value.pending_records);
    try std.testing.expectEqual(@as(u64, 2), parsed_before.value.freshness_lag_records);
    try std.testing.expect(parsed_before.value.publish_admitted);
    try std.testing.expect(parsed_before.value.publish_recommended);
    try std.testing.expectEqual(@as(?catalog_types.NextPublishReason, .wal_artifact_update), parsed_before.value.next_publish_reason);
    try std.testing.expectEqual(catalog_types.MutationTailResolution.none, parsed_before.value.mutation_tail_resolution);
    try std.testing.expectEqual(@as(?shared_vector.DistanceMetric, null), parsed_before.value.vector_compaction_distance_metric);
    try std.testing.expectEqual(@as(?catalog_types.DocumentPublishMode, null), parsed_before.value.head_document_publish_mode);
    try std.testing.expectEqual(@as(?catalog_types.DocumentPublishMode, .append_mutation_tail), parsed_before.value.next_document_publish_mode);
    try std.testing.expectEqual(@as(u64, 0), parsed_before.value.document_lineage_versions);
    try std.testing.expectEqual(@as(bool, false), parsed_before.value.compaction_recommended);
    try std.testing.expect(parsed_before.value.enrichment_complete);

    var build = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/namespaces/docs/build",
    });
    defer build.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), build.status);
    var parsed_build = try parseJsonTestBody(build_mod.BuildResult, alloc, build.body);
    defer parsed_build.deinit();
    try std.testing.expectEqualStrings("docs", parsed_build.value.namespace);
    try std.testing.expect(parsed_build.value.published);
    try std.testing.expectEqual(@as(u64, 1), parsed_build.value.version);
    try std.testing.expectEqual(@as(u64, 1), parsed_build.value.wal_start_lsn);
    try std.testing.expectEqual(@as(u64, 2), parsed_build.value.wal_end_lsn);
    try std.testing.expectEqual(@as(usize, 3), parsed_build.value.artifact_count);

    var head = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/namespaces/docs/head",
    });
    defer head.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), head.status);
    var parsed_head = try parseJsonTestBody(manifest_mod.Manifest, alloc, head.body);
    defer parsed_head.deinit();
    try std.testing.expectEqual(@as(u64, 1), parsed_head.value.version);
    try std.testing.expectEqual(@as(u64, 2), parsed_head.value.wal_end_lsn);

    var publish_head = try handler.handle(.{
        .method = .put,
        .path = "/internal/v1/namespaces/docs/head",
        .body = "{\"version\":1,\"expected_head\":null}",
    });
    defer publish_head.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), publish_head.status);

    var query_head = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/namespaces/docs/query/head",
    });
    defer query_head.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_head.status);
    var parsed_query_head = try parseJsonTestBody(query_types.QueryResult, alloc, query_head.body);
    defer parsed_query_head.deinit();
    try std.testing.expectEqual(query_types.QueryView.published, parsed_query_head.value.view);
    try std.testing.expectEqual(@as(u64, 2), parsed_query_head.value.latest_wal_lsn);
    try std.testing.expectEqual(@as(u64, 0), parsed_query_head.value.freshness_lag_records);
    try std.testing.expectEqual(@as(usize, 3), parsed_query_head.value.artifact_count);
    try std.testing.expectEqual(@as(usize, 3), parsed_query_head.value.artifacts.len);
    try std.testing.expectEqual(manifest_mod.ArtifactKind.mutation_segment, parsed_query_head.value.artifacts[0].kind);
    try std.testing.expectEqual(@as(usize, 1), parsed_query_head.value.document_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_query_head.value.documents.len);
    try std.testing.expectEqualStrings("doc-a", parsed_query_head.value.documents[0].doc_id);
    try std.testing.expectEqual(@as(usize, 0), parsed_query_head.value.overlay_mutation_count);
    try std.testing.expectEqual(@as(bool, false), parsed_query_head.value.publication.publish_recommended);
    try std.testing.expectEqual(@as(?catalog_types.NextPublishReason, null), parsed_query_head.value.publication.next_publish_reason);
    try std.testing.expectEqual(catalog_types.MutationTailResolution.none, parsed_query_head.value.publication.mutation_tail_resolution);
    try std.testing.expectEqual(@as(?catalog_types.DocumentPublishMode, .append_mutation_tail), parsed_query_head.value.publication.head_document_publish_mode);
    try std.testing.expectEqual(@as(?catalog_types.DocumentPublishMode, null), parsed_query_head.value.publication.next_document_publish_mode);
    try std.testing.expectEqual(@as(u64, 1), parsed_query_head.value.publication.document_lineage_versions);
    try std.testing.expectEqual(@as(bool, false), parsed_query_head.value.publication.pending_materialization_families.full_text);
    try std.testing.expectEqual(@as(bool, false), parsed_query_head.value.enrichment.enabled);
    try std.testing.expectEqual(@as(?catalog_types.EnrichmentStageSource, null), parsed_query_head.value.enrichment.stage_source);
    try std.testing.expectEqual(@as(?catalog_types.EnrichmentStageState, null), parsed_query_head.value.enrichment.stage_state);
    try std.testing.expect(parsed_query_head.value.enrichment.complete);

    var query_search = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/namespaces/docs/query/search",
        .body = "{\"text\":\"alpha\",\"limit\":5}",
    });
    defer query_search.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_search.status);
    var parsed_query_search = try parseJsonTestBody(query_types.QuerySearchResult, alloc, query_search.body);
    defer parsed_query_search.deinit();
    try std.testing.expectEqualStrings("alpha", parsed_query_search.value.query_text);
    try std.testing.expectEqual(query_mod.QueryOperator.all_terms, parsed_query_search.value.operator);
    try std.testing.expectEqual(query_mod.QueryFusionStrategy.weighted_rrf, parsed_query_search.value.fusion_strategy);
    try std.testing.expectEqual(@as(u64, 2), parsed_query_search.value.latest_wal_lsn);
    try std.testing.expectEqual(@as(u64, 0), parsed_query_search.value.freshness_lag_records);
    try std.testing.expectEqual(@as(usize, 0), parsed_query_search.value.offset);
    try std.testing.expectEqual(@as(usize, 1), parsed_query_search.value.hit_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_query_search.value.hits.len);
    try std.testing.expectEqual(@as(usize, 0), parsed_query_search.value.actual_probe_count);
    try std.testing.expectEqual(@as(usize, 0), parsed_query_search.value.cluster_prune_count);
    try std.testing.expectEqualStrings("doc-a", parsed_query_search.value.hits[0].doc_id);
    try std.testing.expectEqual(@as(bool, false), parsed_query_search.value.enrichment.enabled);

    var query_search_aggregated = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/namespaces/docs/query/search",
        .body =
        \\{"full_text_search":{"query":"body:alpha"},"limit":5,"aggregations":{"body_terms":{"type":"terms","field":"body","size":5},"body_sig":{"type":"significant_terms","field":"body","size":5}}}
        ,
    });
    defer query_search_aggregated.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_search_aggregated.status);
    var parsed_query_search_aggregated = try parseJsonTestBody(TestSearchAggregationsResponse, alloc, query_search_aggregated.body);
    defer parsed_query_search_aggregated.deinit();
    const search_aggs = parsed_query_search_aggregated.value.aggregations.?;
    try std.testing.expectEqual(@as(usize, 2), search_aggs.map.count());
    try std.testing.expectEqual(@as(i64, 1), search_aggs.map.get("body_terms").?.buckets.?[0].doc_count);
    try std.testing.expect(search_aggs.map.get("body_sig").?.buckets.?[0].score != null);

    var metrics_after_query = try handler.handle(.{
        .method = .get,
        .path = "/metrics",
    });
    defer metrics_after_query.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), metrics_after_query.status);
    var parsed_metrics_after_query = try parseJsonTestBody(api_types.MetricsResult, alloc, metrics_after_query.body);
    defer parsed_metrics_after_query.deinit();
    try std.testing.expectEqual(@as(u64, 2), parsed_metrics_after_query.value.ann_total_queries);
    try std.testing.expectEqual(@as(u64, 0), parsed_metrics_after_query.value.ann_total_actual_probes);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics_after_query.value.publish_recommended_namespaces);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics_after_query.value.namespaces_with_head_republish_pending);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics_after_query.value.namespaces_with_wal_artifact_publish_pending);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics_after_query.value.namespaces_with_wal_enrichment_publish_pending);
    try std.testing.expectEqual(@as(usize, 1), parsed_metrics_after_query.value.namespaces_with_enrichment_complete);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics_after_query.value.namespaces_with_enrichment_executing);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics_after_query.value.namespaces_with_enrichment_deferred_for_publish_threshold);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics_after_query.value.namespaces_with_full_text_materialization_pending);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics_after_query.value.namespaces_with_dense_vector_materialization_pending);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics_after_query.value.namespaces_with_sparse_vector_materialization_pending);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics_after_query.value.namespaces_with_chunk_preview_materialization_pending);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics_after_query.value.namespaces_with_chunk_embeddings_materialization_pending);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics_after_query.value.namespaces_with_rerank_terms_materialization_pending);
    try std.testing.expectEqual(@as(usize, 1), parsed_metrics_after_query.value.namespaces.len);
    try std.testing.expectEqualStrings("docs", parsed_metrics_after_query.value.namespaces[0].namespace);
    try std.testing.expectEqual(@as(?catalog_types.NextPublishReason, null), parsed_metrics_after_query.value.namespaces[0].next_publish_reason);
    try std.testing.expectEqual(@as(bool, false), parsed_metrics_after_query.value.namespaces[0].pending_materialization_families.full_text);
    try std.testing.expectEqual(@as(?catalog_types.EnrichmentStageSource, null), parsed_metrics_after_query.value.namespaces[0].enrichment_stage_source);
    try std.testing.expectEqual(@as(?catalog_types.EnrichmentStageState, null), parsed_metrics_after_query.value.namespaces[0].enrichment_stage_state);
    try std.testing.expectEqual(@as(f32, 0), parsed_metrics_after_query.value.ann_avg_actual_probes);

    var query_version = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/namespaces/docs/query/versions/1",
    });
    defer query_version.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_version.status);
    var parsed_query_version = try parseJsonTestBody(query_types.QueryResult, alloc, query_version.body);
    defer parsed_query_version.deinit();
    try std.testing.expectEqual(@as(u64, 1), parsed_query_version.value.version);
    try std.testing.expectEqual(query_types.QueryView.published, parsed_query_version.value.view);
    try std.testing.expectEqual(@as(usize, 1), parsed_query_version.value.documents.len);

    var query_artifact = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/namespaces/docs/query/head/artifacts/0",
    });
    defer query_artifact.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_artifact.status);
    var parsed_query_artifact = try parseJsonTestBody(query_types.QueryArtifactResult, alloc, query_artifact.body);
    defer parsed_query_artifact.deinit();
    try std.testing.expectEqual(@as(u64, 1), parsed_query_artifact.value.version);
    try std.testing.expectEqual(@as(usize, 0), parsed_query_artifact.value.artifact.index);
    try std.testing.expectEqual(manifest_mod.ArtifactKind.mutation_segment, parsed_query_artifact.value.artifact.kind);
    try std.testing.expect(parsed_query_artifact.value.artifact.byte_len > 0);
    try std.testing.expectEqual(@as(usize, 2), parsed_query_artifact.value.artifact.mutations.len);
    try std.testing.expectEqual(@as(usize, 0), parsed_query_artifact.value.artifact.documents.len);
    try std.testing.expectEqualStrings("doc-a", parsed_query_artifact.value.artifact.mutations[0].doc_id);
    try std.testing.expectEqualStrings("doc-b", parsed_query_artifact.value.artifact.mutations[1].doc_id);

    var document_artifact = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/namespaces/docs/query/head/artifacts/1",
    });
    defer document_artifact.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), document_artifact.status);
    var parsed_document_artifact = try parseJsonTestBody(query_types.QueryArtifactResult, alloc, document_artifact.body);
    defer parsed_document_artifact.deinit();
    try std.testing.expectEqual(manifest_mod.ArtifactKind.document_segment, parsed_document_artifact.value.artifact.kind);
    try std.testing.expectEqual(@as(usize, 0), parsed_document_artifact.value.artifact.mutations.len);
    try std.testing.expectEqual(@as(usize, 1), parsed_document_artifact.value.artifact.documents.len);
    try std.testing.expectEqualStrings("alpha", parsed_document_artifact.value.artifact.documents[0].body);

    const next_mutation = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-c", .body = "gamma" },
    };
    var next_ingest = try api.ingestBatch(.{
        .namespace = "docs",
        .timestamp_ns = 789,
        .mutations = &next_mutation,
    });
    defer next_ingest.deinit(alloc);

    var set_policy = try handler.handle(.{
        .method = .put,
        .path = "/internal/v1/namespaces/docs/policy",
        .body = "{\"default_query_view\":\"latest\",\"keep_latest_versions\":2,\"max_pending_records\":4,\"compaction_trigger_version_count\":2,\"vector_distance_metric\":\"l2_squared\"}",
    });
    defer set_policy.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), set_policy.status);
    var parsed_set_policy = try parseJsonTestBody(api_types.NamespacePolicyResult, alloc, set_policy.body);
    defer parsed_set_policy.deinit();
    try std.testing.expectEqualStrings("docs", parsed_set_policy.value.namespace);
    try std.testing.expectEqual(shared_vector.DistanceMetric.l2_squared, parsed_set_policy.value.policy.vector_distance_metric);
    try std.testing.expectEqual(catalog_types.DefaultQueryView.latest, parsed_set_policy.value.policy.default_query_view);

    var query_default = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/namespaces/docs/query",
    });
    defer query_default.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_default.status);
    var parsed_query_default = try parseJsonTestBody(query_types.QueryResult, alloc, query_default.body);
    defer parsed_query_default.deinit();
    try std.testing.expectEqual(query_types.QueryView.latest, parsed_query_default.value.view);
    try std.testing.expectEqual(@as(u64, 3), parsed_query_default.value.latest_wal_lsn);
    try std.testing.expectEqual(@as(u64, 0), parsed_query_default.value.freshness_lag_records);
    try std.testing.expectEqual(@as(usize, 1), parsed_query_default.value.overlay_mutation_count);
    try std.testing.expectEqual(@as(usize, 2), parsed_query_default.value.documents.len);
    try std.testing.expectEqualStrings("doc-a", parsed_query_default.value.documents[0].doc_id);
    try std.testing.expectEqualStrings("doc-c", parsed_query_default.value.documents[1].doc_id);

    var query_latest = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/namespaces/docs/query/latest",
    });
    defer query_latest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_latest.status);
    var parsed_query_latest = try parseJsonTestBody(query_types.QueryResult, alloc, query_latest.body);
    defer parsed_query_latest.deinit();
    try std.testing.expectEqual(query_types.QueryView.latest, parsed_query_latest.value.view);
    try std.testing.expectEqual(@as(u64, 3), parsed_query_latest.value.latest_wal_lsn);
    try std.testing.expectEqual(@as(u64, 0), parsed_query_latest.value.freshness_lag_records);
    try std.testing.expectEqual(@as(usize, 1), parsed_query_latest.value.overlay_mutation_count);
    try std.testing.expectEqual(@as(usize, 2), parsed_query_latest.value.documents.len);
    try std.testing.expectEqualStrings("doc-a", parsed_query_latest.value.documents[0].doc_id);
    try std.testing.expectEqualStrings("doc-c", parsed_query_latest.value.documents[1].doc_id);
    try std.testing.expectEqual(@as(bool, false), parsed_query_latest.value.publication.vector_compaction_recommended);
    try std.testing.expectEqual(@as(?shared_vector.DistanceMetric, null), parsed_query_latest.value.publication.vector_distance_metric);
    try std.testing.expectEqual(@as(?u32, null), parsed_query_latest.value.publication.vector_cluster_count);
    try std.testing.expectEqual(@as(?u32, null), parsed_query_latest.value.publication.vector_base_probe_count);
    try std.testing.expectEqual(@as(?u32, null), parsed_query_latest.value.publication.vector_shortlist_multiplier);
    try std.testing.expectEqual(@as(?u32, null), parsed_query_latest.value.publication.vector_target_cluster_count);
}

test "http handler serves public table joins on published heads" {
    const alloc = std.testing.allocator;

    const DummyForeign = struct {
        fn destroy(ptr: *anyopaque, inner_alloc: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            inner_alloc.destroy(self);
        }

        fn query(_: *anyopaque, inner_alloc: std.mem.Allocator, params: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            const rows = try inner_alloc.alloc(std.json.Value, 2);
            if (std.mem.eql(u8, params.table, "addresses")) {
                rows[0] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"id\":\"addr:a\",\"city\":\"Seattle\"}");
                rows[1] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"id\":\"addr:b\",\"city\":\"Portland\"}");
            } else {
                rows[0] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"id\":\"cust:a\",\"name\":\"Alice\",\"address_id\":\"addr:a\"}");
                rows[1] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"id\":\"cust:b\",\"name\":\"Bob\",\"address_id\":\"addr:b\"}");
            }
            return .{ .rows = rows, .total = 2 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 2, .size_bytes = 128 };
        }

        fn factory(inner_alloc: std.mem.Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned = config;
            defer owned.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                },
            };
        }
    };

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-http-table-joins");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-http-table-joins");
    const wal_root = tmpPath(&wal_root_buf, "wal-http-table-joins");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-http-table-joins");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    var foreign_registry = foreign_mod.Registry{};
    defer foreign_registry.deinit(alloc);
    try foreign_registry.register(alloc, .postgres, DummyForeign.factory);
    handler.foreign_registry = &foreign_registry;

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "orders",
        100,
        .{},
        "{\"version\":0}",
        "",
        tables_api.default_indexes_json,
    ));
    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "customers",
        101,
        .{},
        "{\"version\":0}",
        "",
        tables_api.default_indexes_json,
    ));
    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "addresses",
        102,
        .{},
        "{\"version\":0}",
        "",
        tables_api.default_indexes_json,
    ));

    const order_mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "order:1", .body = "{\"body\":\"alpha order\",\"title\":\"Alpha order\",\"customer_id\":\"cust:a\"}" },
        .{ .kind = .upsert, .doc_id = "order:2", .body = "{\"body\":\"orphan order\",\"title\":\"Orphan order\",\"customer_id\":\"cust:missing\"}" },
    };
    var order_ingest = try api.ingestBatch(.{ .namespace = "orders", .timestamp_ns = 100, .mutations = &order_mutations });
    defer order_ingest.deinit(alloc);
    var order_build = try catalog.buildTable("orders");
    defer order_build.deinit(alloc);

    const customer_mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "cust:a", .body = "{\"name\":\"Alice\",\"address_id\":\"addr:a\"}" },
        .{ .kind = .upsert, .doc_id = "cust:b", .body = "{\"name\":\"Bob\",\"address_id\":\"addr:b\"}" },
    };
    var customer_ingest = try api.ingestBatch(.{ .namespace = "customers", .timestamp_ns = 101, .mutations = &customer_mutations });
    defer customer_ingest.deinit(alloc);
    var customer_build = try catalog.buildTable("customers");
    defer customer_build.deinit(alloc);

    const address_mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "addr:a", .body = "{\"city\":\"Seattle\"}" },
        .{ .kind = .upsert, .doc_id = "addr:b", .body = "{\"city\":\"Portland\"}" },
    };
    var address_ingest = try api.ingestBatch(.{ .namespace = "addresses", .timestamp_ns = 102, .mutations = &address_mutations });
    defer address_ingest.deinit(alloc);
    var address_build = try catalog.buildTable("addresses");
    defer address_build.deinit(alloc);

    const inner_body =
        \\{"full_text_search":{"query":"body:order"},"fields":["title","customer_id"],"profile":true,"join":{"right_table":"customers","join_type":"inner","on":{"left_field":"customer_id","right_field":"_id","operator":"eq"}}}
    ;
    var inner = try handler.handle(.{
        .method = .post,
        .path = "/tables/orders/query",
        .body = inner_body,
    });
    defer inner.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), inner.status);

    var parsed_inner = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, inner.body, .{});
    defer parsed_inner.deinit();
    const inner_response = parsed_inner.value.responses.?[0];
    const inner_hits = inner_response.hits.?.hits.?;
    try std.testing.expectEqual(@as(usize, 1), inner_hits.len);
    try std.testing.expectEqual(@as(i64, 1), inner_response.hits.?.total.?);
    try std.testing.expectEqualStrings("Alice", testQueryHitSourcePathValue(inner_hits[0], "customers.name").?.string);
    try std.testing.expectEqualStrings("index_lookup", testJoinProfileFieldValue(inner_response, "strategy_used").?.string);

    const left_body =
        \\{"full_text_search":{"query":"body:order"},"fields":["title","customer_id"],"profile":true,"join":{"right_table":"customers","join_type":"left","on":{"left_field":"customer_id","right_field":"_id","operator":"eq"}}}
    ;
    var left = try handler.handle(.{
        .method = .post,
        .path = "/tables/orders/query",
        .body = left_body,
    });
    defer left.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), left.status);

    var parsed_left = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, left.body, .{});
    defer parsed_left.deinit();
    const left_hits = parsed_left.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqual(@as(usize, 2), left_hits.len);
    var found_left_unmatched = false;
    for (left_hits) |hit| {
        const source = hit._source.?;
        if (testQueryHitSourcePathValue(hit, "title")) |title| {
            if (title == .string and std.mem.eql(u8, title.string, "Orphan order")) {
                try std.testing.expect(source.object.get("customers.name") == null);
                found_left_unmatched = true;
            }
        }
    }
    try std.testing.expect(found_left_unmatched);

    const nested_body =
        \\{"full_text_search":{"query":"body:order"},"fields":["title","customer_id"],"profile":true,"join":{"right_table":"customers","join_type":"left","on":{"left_field":"customer_id","right_field":"_id","operator":"eq"},"right_fields":["name","addresses.city"],"nested_join":{"right_table":"addresses","join_type":"left","on":{"left_field":"address_id","right_field":"_id","operator":"eq"},"right_fields":["city"]}}}
    ;
    var nested = try handler.handle(.{
        .method = .post,
        .path = "/tables/orders/query",
        .body = nested_body,
    });
    defer nested.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), nested.status);

    var parsed_nested = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, nested.body, .{});
    defer parsed_nested.deinit();
    const nested_hits = parsed_nested.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqual(@as(usize, 2), nested_hits.len);
    var found_nested_city = false;
    for (nested_hits) |hit| {
        if (testQueryHitSourcePathValue(hit, "customers.name")) |name| {
            if (name == .string and std.mem.eql(u8, name.string, "Alice")) {
                try std.testing.expectEqualStrings("Seattle", testQueryHitSourcePathValue(hit, "customers.addresses.city").?.string);
                found_nested_city = true;
            }
        }
    }
    try std.testing.expect(found_nested_city);

    const right_body =
        \\{"full_text_search":{"query":"body:order"},"fields":["title","customer_id"],"profile":true,"join":{"right_table":"customers","join_type":"right","on":{"left_field":"customer_id","right_field":"_id","operator":"eq"}}}
    ;
    var right = try handler.handle(.{
        .method = .post,
        .path = "/tables/orders/query",
        .body = right_body,
    });
    defer right.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), right.status);

    var parsed_right = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, right.body, .{});
    defer parsed_right.deinit();
    const right_response = parsed_right.value.responses.?[0];
    const right_hits = right_response.hits.?.hits.?;
    try std.testing.expectEqual(@as(usize, 2), right_hits.len);
    var found_right_unmatched = false;
    for (right_hits) |hit| {
        if (testQueryHitSourcePathValue(hit, "customers.name")) |name| {
            if (name == .string and std.mem.eql(u8, name.string, "Bob")) {
                try std.testing.expect(testQueryHitSourcePathValue(hit, "title").? == .null);
                try std.testing.expect(testQueryHitSourcePathValue(hit, "customer_id").? == .null);
                found_right_unmatched = true;
            }
        }
    }
    try std.testing.expect(found_right_unmatched);
    try std.testing.expectEqualStrings("broadcast", testJoinProfileFieldValue(right_response, "strategy_used").?.string);

    const foreign_body =
        \\{"full_text_search":{"query":"body:order"},"fields":["title","customer_id"],"profile":true,"join":{"right_table":"pg_customers","join_type":"inner","on":{"left_field":"customer_id","right_field":"id","operator":"eq"}},"foreign_sources":{"pg_customers":{"type":"postgres","dsn":"postgres://db","postgres_table":"customers"}}}
    ;
    var foreign = try handler.handle(.{
        .method = .post,
        .path = "/tables/orders/query",
        .body = foreign_body,
    });
    defer foreign.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), foreign.status);

    var parsed_foreign = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, foreign.body, .{});
    defer parsed_foreign.deinit();
    const foreign_response = parsed_foreign.value.responses.?[0];
    const foreign_hits = foreign_response.hits.?.hits.?;
    try std.testing.expectEqual(@as(usize, 1), foreign_hits.len);
    try std.testing.expectEqualStrings("Alice", testQueryHitSourcePathValue(foreign_hits[0], "pg_customers.name").?.string);
    try std.testing.expectEqualStrings("broadcast", testJoinProfileFieldValue(foreign_response, "strategy_used").?.string);

    const nested_foreign_body =
        \\{"full_text_search":{"query":"body:order"},"fields":["title","customer_id"],"profile":true,"join":{"right_table":"customers","join_type":"left","on":{"left_field":"customer_id","right_field":"_id","operator":"eq"},"right_fields":["name","pg_addresses.city"],"nested_join":{"right_table":"pg_addresses","join_type":"left","on":{"left_field":"address_id","right_field":"id","operator":"eq"},"right_fields":["city"]}},"foreign_sources":{"pg_addresses":{"type":"postgres","dsn":"postgres://db","postgres_table":"addresses"}}}
    ;
    var nested_foreign = try handler.handle(.{
        .method = .post,
        .path = "/tables/orders/query",
        .body = nested_foreign_body,
    });
    defer nested_foreign.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), nested_foreign.status);

    var parsed_nested_foreign = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, nested_foreign.body, .{});
    defer parsed_nested_foreign.deinit();
    const nested_foreign_hits = parsed_nested_foreign.value.responses.?[0].hits.?.hits.?;
    try std.testing.expectEqual(@as(usize, 2), nested_foreign_hits.len);
    var found_nested_foreign_city = false;
    for (nested_foreign_hits) |hit| {
        const source = hit._source.?;
        if (source.object.get("customers.name")) |name| {
            if (name == .string and std.mem.eql(u8, name.string, "Alice")) {
                try std.testing.expectEqualStrings("Seattle", source.object.get("customers.pg_addresses.city").?.string);
                found_nested_foreign_city = true;
            }
        }
    }
    try std.testing.expect(found_nested_foreign_city);
}

test "http handler join planner uses foreign source statistics" {
    const alloc = std.testing.allocator;

    const DummyForeign = struct {
        fn destroy(ptr: *anyopaque, inner_alloc: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            inner_alloc.destroy(self);
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            return .{ .rows = &.{}, .total = 0 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 64, .size_bytes = 1024 };
        }

        fn factory(inner_alloc: std.mem.Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned = config;
            defer owned.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                },
            };
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try registry.register(alloc, .postgres, DummyForeign.factory);

    var handler: HttpHandler = .{
        .alloc = alloc,
        .api = undefined,
        .catalog = undefined,
        .manifests = undefined,
        .progress = undefined,
        .query = undefined,
        .runtime_status = undefined,
        .foreign_registry = null,
    };
    handler.setForeignRegistry(&registry);

    var parsed_hits = try parseTestQueryHitsAlloc(alloc,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:1"}},
        \\  {"_id":"doc:2","_source":{"customer_id":"cust:2"}}
        \\]
    );
    defer parsed_hits.deinit();

    const join: SupportedJoinRequest = .{
        .right_table = @constCast("pg_customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    var foreign_sources = foreign_mod.PostgresSourceMap{
        .entries = try alloc.alloc(foreign_mod.PostgresNamedConfig, 1),
    };
    defer foreign_sources.deinit(alloc);
    foreign_sources.entries[0] = .{
        .name = try alloc.dupe(u8, "pg_customers"),
        .config = .{
            .dsn = try alloc.dupe(u8, "postgres://db"),
            .postgres_table = try alloc.dupe(u8, "customers"),
            .columns = &.{},
        },
    };

    const plan = planSupportedJoinExecution(&handler, alloc, join, parsed_hits.values, foreign_sources);
    try std.testing.expect(plan.used_stats);
    try std.testing.expectEqual(PlannedJoinExecution.StrategyUsed.broadcast, plan.strategy);
}

test "http handler join parser accepts foreign source maps" {
    const alloc = std.testing.allocator;
    const body =
        \\{"fields":["title"],"join":{"right_table":"customers","join_type":"inner","on":{"left_field":"customer_id","right_field":"_id","operator":"eq"}},"foreign_sources":{"pg_customers":{"type":"postgres","dsn":"postgres://db","postgres_table":"customers"}}}
    ;
    const parsed = (try parseSupportedJoinRequest(alloc, body)).?;
    defer {
        var owned = parsed;
        owned.deinit(alloc);
    }

    try std.testing.expectEqualStrings("customers", parsed.join.right_table);
    try std.testing.expect(parsed.foreign_sources.contains("pg_customers"));
}

test "http handler executes foreign right join query through registry" {
    const alloc = std.testing.allocator;

    const DummyForeign = struct {
        fn destroy(ptr: *anyopaque, inner_alloc: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            inner_alloc.destroy(self);
        }

        fn query(_: *anyopaque, inner_alloc: std.mem.Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            const rows = try inner_alloc.alloc(std.json.Value, 2);
            rows[0] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"id\":\"cust:a\",\"name\":\"Alice\"}");
            rows[1] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"id\":\"cust:b\",\"name\":\"Bob\"}");
            return .{ .rows = rows, .total = 2 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 2, .size_bytes = 128 };
        }

        fn factory(inner_alloc: std.mem.Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned = config;
            defer owned.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                },
            };
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try registry.register(alloc, .postgres, DummyForeign.factory);

    var handler: HttpHandler = .{
        .alloc = alloc,
        .api = undefined,
        .catalog = undefined,
        .manifests = undefined,
        .progress = undefined,
        .query = undefined,
        .runtime_status = undefined,
        .foreign_registry = &registry,
    };

    var parsed_hits = try parseTestQueryHitsAlloc(alloc,
        \\[
        \\  {"_id":"order:1","_source":{"customer_id":"cust:a"}}
        \\]
    );
    defer parsed_hits.deinit();

    const join: SupportedJoinRequest = .{
        .right_table = try alloc.dupe(u8, "pg_customers"),
        .join_type = .inner,
        .left_field = try alloc.dupe(u8, "customer_id"),
        .right_field = try alloc.dupe(u8, "id"),
    };
    defer {
        var owned = join;
        owned.deinit(alloc);
    }

    var foreign_config: foreign_mod.PostgresConfig = .{
        .dsn = try alloc.dupe(u8, "postgres://db"),
        .postgres_table = try alloc.dupe(u8, "customers"),
        .columns = &.{},
    };
    defer foreign_config.deinit(alloc);

    var right = try handler.executeForeignRightJoinQuery(foreign_config, join, parsed_hits.values, .{});
    defer right.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), right.hits.len);
    try std.testing.expectEqualStrings("cust:a", right.hits[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("Alice", testOwnedHitSourcePathValue(right.hits[0], "name").?.string);
}

test "http handler executes direct foreign table query through registry" {
    const alloc = std.testing.allocator;
    const c = struct {
        extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern fn unsetenv(name: [*:0]const u8) c_int;
    };
    try std.testing.expectEqual(@as(c_int, 0), c.setenv("PG_DSN", "postgres://resolved", 1));
    defer _ = c.unsetenv("PG_DSN");

    const DummyForeign = struct {
        var last_dsn: ?[]u8 = null;

        fn destroy(ptr: *anyopaque, inner_alloc: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            inner_alloc.destroy(self);
        }

        fn query(_: *anyopaque, inner_alloc: std.mem.Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            const rows = try inner_alloc.alloc(std.json.Value, 2);
            rows[0] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"id\":\"cust:a\",\"name\":\"Alice\"}");
            rows[1] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"id\":\"cust:b\",\"name\":\"Bob\"}");
            return .{ .rows = rows, .total = 2 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 2, .size_bytes = 128 };
        }

        fn factory(inner_alloc: std.mem.Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            if (last_dsn) |value| inner_alloc.free(value);
            last_dsn = try inner_alloc.dupe(u8, config.dsn);
            var owned = config;
            defer owned.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                },
            };
        }
    };

    var registry = foreign_mod.Registry{};
    defer {
        if (DummyForeign.last_dsn) |value| alloc.free(value);
        registry.deinit(alloc);
    }
    try registry.register(alloc, .postgres, DummyForeign.factory);

    var handler: HttpHandler = .{
        .alloc = alloc,
        .api = undefined,
        .catalog = undefined,
        .manifests = undefined,
        .progress = undefined,
        .query = undefined,
        .runtime_status = undefined,
        .foreign_registry = null,
    };
    handler.setForeignRegistry(&registry);

    const body =
        \\{"fields":["name"],"limit":1,"offset":2,"order_by":[{"field":"name"}],"filter_query":{"term":"active","field":"status"},"foreign_sources":{"pg_customers":{"type":"postgres","dsn":"${secret:pg_dsn}","postgres_table":"customers","columns":[{"name":"status","type":"text"}]}}}
    ;

    const json = (try handler.executeForeignPublicTableQueryJsonAlloc("pg_customers", body)).?;
    defer alloc.free(json);

    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, json, .{});
    defer parsed.deinit();
    const responses = parsed.value.responses.?;
    try std.testing.expectEqual(@as(usize, 1), responses.len);
    const hits = responses[0].hits.?.hits.?;
    try std.testing.expectEqual(@as(usize, 2), hits.len);
    try std.testing.expectEqualStrings("cust:a", hits[0]._id);
    try std.testing.expectEqualStrings("Alice", testQueryHitSourcePathValue(hits[0], "name").?.string);
    try std.testing.expectEqualStrings("postgres://resolved", DummyForeign.last_dsn.?);
}

test "http handler executes direct foreign table aggregations through registry" {
    const alloc = std.testing.allocator;

    const DummyForeign = struct {
        fn destroy(ptr: *anyopaque, inner_alloc: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            inner_alloc.destroy(self);
        }

        fn query(ptr: *anyopaque, inner_alloc: std.mem.Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            _ = ptr;
            const rows = try inner_alloc.alloc(std.json.Value, 1);
            rows[0] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"id\":\"cust:a\",\"name\":\"Alice\",\"version\":1}");
            return .{ .rows = rows, .total = 1 };
        }

        fn aggregate(ptr: *anyopaque, inner_alloc: std.mem.Allocator, _: foreign_mod.AggregateParams) !foreign_mod.AggregateResult {
            _ = ptr;
            const results = try inner_alloc.alloc(foreign_mod.NamedValue, 2);
            results[0] = .{
                .name = try inner_alloc.dupe(u8, "version_stats"),
                .value = try parseOwnedJsonValueAlloc(inner_alloc, "{\"count\":2,\"min\":1,\"max\":2,\"avg\":1.5,\"sum\":3}"),
            };
            results[1] = .{
                .name = try inner_alloc.dupe(u8, "name_terms"),
                .value = try parseOwnedJsonValueAlloc(inner_alloc, "[{\"key\":\"Alice\",\"doc_count\":1},{\"key\":\"Bob\",\"doc_count\":1}]"),
            };
            return .{ .results = results };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 2, .size_bytes = 128 };
        }

        fn factory(inner_alloc: std.mem.Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned = config;
            defer owned.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .aggregate = aggregate,
                    .statistics = statistics,
                },
            };
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try registry.register(alloc, .postgres, DummyForeign.factory);

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-http-foreign-agg");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-http-foreign-agg");
    const wal_root = tmpPath(&wal_root_buf, "wal-http-foreign-agg");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-http-foreign-agg");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifacts_store = fs_artifacts.artifactStore();
    defer artifacts_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_progress = try catalog_mod.FsProgressStore.init(alloc, std.mem.span(catalog_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_catalog = try catalog_mod.FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifacts_store, &manifest_store, &progress_store, &wal_store);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifacts_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var query = query_mod.QueryRuntime.init(alloc, &artifacts_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);

    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);
    handler.setForeignRegistry(&registry);

    const body =
        \\{"fields":["name"],"aggregations":{"version_stats":{"type":"stats","field":"version"},"name_terms":{"type":"terms","field":"name","size":5}},"foreign_sources":{"pg_customers":{"type":"postgres","dsn":"postgres://db","postgres_table":"customers","columns":[{"name":"version","type":"bigint"},{"name":"name","type":"text"}]}}}
    ;

    var response = try handler.handle(.{
        .method = .post,
        .path = "/tables/pg_customers/query",
        .body = body,
    });
    defer response.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed_response = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, response.body);
    defer parsed_response.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_response.value.responses.?.len);
    const aggregations = parsed_response.value.responses.?[0].aggregations.?;
    try std.testing.expectEqual(@as(usize, 2), aggregations.map.count());
    const version_stats = aggregations.map.get("version_stats").?;
    try std.testing.expectEqual(@as(?i64, 2), version_stats.count);
    const name_terms = aggregations.map.get("name_terms").?;
    try std.testing.expectEqual(@as(usize, 2), name_terms.buckets.?.len);
    try std.testing.expectEqualStrings("Alice", name_terms.buckets.?[0].key);
}

test "http handler metrics expose deferred pending wal enrichment state" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-http-metrics-enrichment-state");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-http-metrics-enrichment-state");
    const wal_root = tmpPath(&wal_root_buf, "wal-http-metrics-enrichment-state");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-http-metrics-enrichment-state");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{
            .chunk_preview_enabled = true,
            .chunk_preview_publish_min_pending_records = 32,
        },
        "",
        "",
        tables_api.default_indexes_json,
    ));

    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo\",\"chunk_preview\":[\"alpha bravo\"],\"_enrichment\":{\"chunk_preview\":true,\"chunk_preview_version\":1}}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try catalog.buildTable("docs");
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"charlie delta\"}" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);

    var metrics = try handler.handle(.{
        .method = .get,
        .path = "/metrics",
    });
    defer metrics.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), metrics.status);
    var parsed_metrics = try parseJsonTestBody(api_types.MetricsResult, alloc, metrics.body);
    defer parsed_metrics.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_metrics.value.namespaces_with_enrichment_deferred_for_publish_threshold);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics.value.namespaces_with_enrichment_ready_for_publish);
    try std.testing.expectEqual(@as(usize, 1), parsed_metrics.value.namespaces_with_wal_enrichment_publish_pending);
    try std.testing.expectEqual(@as(usize, 1), parsed_metrics.value.namespaces_with_chunk_preview_materialization_pending);
    try std.testing.expectEqual(@as(usize, 0), parsed_metrics.value.namespaces_with_dense_vector_materialization_pending);
    try std.testing.expectEqual(@as(usize, 1), parsed_metrics.value.namespaces.len);
    try std.testing.expectEqual(@as(?catalog_types.EnrichmentStageSource, .pending_wal), parsed_metrics.value.namespaces[0].enrichment_stage_source);
    try std.testing.expectEqual(@as(?catalog_types.EnrichmentStageState, .deferred_for_publish_threshold), parsed_metrics.value.namespaces[0].enrichment_stage_state);
    try std.testing.expectEqual(@as(?catalog_types.NextPublishReason, .wal_enrichment), parsed_metrics.value.namespaces[0].next_publish_reason);
    try std.testing.expectEqual(@as(bool, false), parsed_metrics.value.namespaces[0].compaction_recommended);
    try std.testing.expectEqual(@as(bool, false), parsed_metrics.value.namespaces[0].mutation_tail_compaction_recommended);
    try std.testing.expectEqual(@as(bool, false), parsed_metrics.value.namespaces[0].vector_compaction_recommended);
    try std.testing.expectEqual(@as(?catalog_types.DocumentPublishMode, .append_mutation_tail), parsed_metrics.value.namespaces[0].head_document_publish_mode);
    try std.testing.expectEqual(@as(?catalog_types.DocumentPublishMode, .append_mutation_tail), parsed_metrics.value.namespaces[0].next_document_publish_mode);
    try std.testing.expectEqual(@as(bool, true), parsed_metrics.value.namespaces[0].pending_materialization_families.chunk_preview);
    try std.testing.expectEqual(catalog_types.DerivedOutputResolution.pending_materialization, parsed_metrics.value.namespaces[0].derived_output_resolutions.chunk_preview);
}

test "http handler index status exposes chunk preview blocker for chunk-backed full text" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-http-index-blocker");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-http-index-blocker");
    const wal_root = tmpPath(&wal_root_buf, "wal-http-index-blocker");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-http-index-blocker");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{
            .chunk_preview_enabled = true,
            .chunk_preview_publish_min_pending_records = 32,
        },
        "{\"version\":0}",
        "",
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"semantic_chunked_idx\":{\"field\":\"body\",\"dimension\":3,\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"full_text_index\":{},\"text\":{\"target_tokens\":4}}}}",
    ));

    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"body\":\"alpha bravo\",\"chunk_preview\":[\"alpha bravo\"],\"_enrichment\":{\"chunk_preview\":true,\"chunk_preview_version\":1}}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try catalog.buildTable("docs");
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "{\"body\":\"charlie delta echo foxtrot golf\"}" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);

    var full_text_index = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/indexes/full_text_index_v0",
    });
    defer full_text_index.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), full_text_index.status);
    var parsed_full_text_index = try parseServerlessIndexStatusTestResponse(alloc, full_text_index.body, "full_text_index_v0");
    defer parsed_full_text_index.deinit();
    try std.testing.expectEqual(@as(?bool, true), parsed_full_text_index.value.status.materialization_blocked);
    try std.testing.expectEqualStrings("chunk_preview", parsed_full_text_index.value.status.materialization_blocker.?);
    try std.testing.expectEqual(@as(?bool, true), parsed_full_text_index.value.status.chunked_full_text);
}

test "http handler index status exposes chunk embeddings blocker for chunked dense index" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-http-index-chunk-embeddings");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-http-index-chunk-embeddings");
    const wal_root = tmpPath(&wal_root_buf, "wal-http-index-chunk-embeddings");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-http-index-chunk-embeddings");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{
            .chunk_embeddings_enabled = true,
            .chunk_embeddings_publish_min_pending_records = 32,
        },
        "{\"version\":0}",
        "",
        "{\"semantic_chunked_idx\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"chunker\":{\"provider\":\"antfly\",\"store_chunks\":false,\"text\":{\"target_tokens\":4}}}}",
    ));

    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"body\":\"alpha bravo\",\"chunk_preview\":[\"alpha bravo\"],\"chunk_embeddings\":[{\"chunk\":\"alpha bravo\",\"embedding\":[1,0,0]}],\"_enrichment\":{\"chunk_preview\":true,\"chunk_preview_version\":1,\"chunk_embeddings\":true,\"chunk_embeddings_version\":1}}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try catalog.buildTable("docs");
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "{\"body\":\"charlie delta echo foxtrot golf\",\"chunk_preview\":[\"charlie delta\",\"echo foxtrot golf\"],\"_enrichment\":{\"chunk_preview\":true,\"chunk_preview_version\":1}}" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);

    var semantic_index = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/indexes/semantic_chunked_idx",
    });
    defer semantic_index.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), semantic_index.status);
    var parsed_semantic_index = try parseServerlessIndexStatusTestResponse(alloc, semantic_index.body, "semantic_chunked_idx");
    defer parsed_semantic_index.deinit();
    try std.testing.expectEqual(@as(?bool, true), parsed_semantic_index.value.status.materialization_blocked);
    try std.testing.expectEqualStrings("chunk_embeddings", parsed_semantic_index.value.status.materialization_blocker.?);
}

test "http handler index status exposes lexical sparse blocker for sparse index" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-http-index-lexical-sparse");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-http-index-lexical-sparse");
    const wal_root = tmpPath(&wal_root_buf, "wal-http-index-lexical-sparse");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-http-index-lexical-sparse");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{
            .enrichment_enabled = true,
            .enrichment_publish_min_pending_records = 32,
        },
        "{\"version\":0}",
        "",
        "{\"sparse_idx\":{\"type\":\"embeddings\",\"sparse\":true}}",
    ));

    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha bravo\",\"sparse_embedding\":{\"alpha\":0.5,\"bravo\":0.5},\"_enrichment\":{\"lexical_sparse\":true,\"lexical_sparse_version\":1}}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try catalog.buildTable("docs");
    defer build_first.deinit(alloc);

    const second = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "{\"text\":\"charlie delta echo foxtrot golf\"}" },
    };
    var ingest_second = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &second });
    defer ingest_second.deinit(alloc);

    var sparse_index = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/indexes/sparse_idx",
    });
    defer sparse_index.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), sparse_index.status);
    var parsed_sparse_index = try parseServerlessIndexStatusTestResponse(alloc, sparse_index.body, "sparse_idx");
    defer parsed_sparse_index.deinit();
    try std.testing.expectEqual(@as(?bool, true), parsed_sparse_index.value.status.materialization_blocked);
    try std.testing.expectEqualStrings("lexical_sparse", parsed_sparse_index.value.status.materialization_blocker.?);
}

test "http handler index status exposes graph publication actions" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-http-index-graph-actions");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-http-index-graph-actions");
    const wal_root = tmpPath(&wal_root_buf, "wal-http-index-graph-actions");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-http-index-graph-actions");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3}}",
    ));

    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"embedding\":[1,0,0],\"graph_edges\":[{\"type\":\"related\",\"to\":\"doc-b\"}]}" },
    };
    var ingest = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 123, .mutations = &mutations });
    defer ingest.deinit(alloc);

    var first_build = try catalog.buildTable("docs");
    defer first_build.deinit(alloc);
    try std.testing.expect(first_build.published);

    try std.testing.expect(try catalog.setTableDefinition(
        "docs",
        "{\"default_type\":\"doc\"}",
        "",
        "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3},\"graph_idx\":{\"type\":\"graph\"}}",
    ));

    var planned = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/indexes/graph_idx",
    });
    defer planned.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), planned.status);
    var parsed_planned = try parseServerlessIndexStatusTestResponse(alloc, planned.body, "graph_idx");
    defer parsed_planned.deinit();
    try std.testing.expectEqualStrings("rebuild", parsed_planned.value.status.planned_publication_action.?);

    var rebuild = try catalog.buildTable("docs");
    defer rebuild.deinit(alloc);
    try std.testing.expect(rebuild.published);

    var head = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/indexes/graph_idx",
    });
    defer head.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), head.status);
    var parsed_head = try parseServerlessIndexStatusTestResponse(alloc, head.body, "graph_idx");
    defer parsed_head.deinit();
    try std.testing.expectEqualStrings("rebuild", parsed_head.value.status.head_publication_action.?);
}

test "http handler index status predicts graph reuse and rebuild before publish" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-http-index-graph-predict");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-http-index-graph-predict");
    const wal_root = tmpPath(&wal_root_buf, "wal-http-index-graph-predict");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-http-index-graph-predict");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"default_type\":\"doc\"}",
        "",
        "{\"graph_idx\":{\"type\":\"graph\"}}",
    ));

    const first = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"alpha\",\"graph_edges\":[{\"edge_type\":\"related\",\"target\":\"doc-b\"}]}" },
    };
    var ingest_first = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 100, .mutations = &first });
    defer ingest_first.deinit(alloc);
    var build_first = try catalog.buildTable("docs");
    defer build_first.deinit(alloc);

    const text_only = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"bravo\",\"graph_edges\":[{\"edge_type\":\"related\",\"target\":\"doc-b\"}]}" },
    };
    var ingest_text = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 200, .mutations = &text_only });
    defer ingest_text.deinit(alloc);

    var graph_reuse = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/indexes/graph_idx",
    });
    defer graph_reuse.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), graph_reuse.status);
    var parsed_graph_reuse = try parseServerlessIndexStatusTestResponse(alloc, graph_reuse.body, "graph_idx");
    defer parsed_graph_reuse.deinit();
    try std.testing.expectEqualStrings("reuse", parsed_graph_reuse.value.status.planned_publication_action.?);

    var build_second = try catalog.buildTable("docs");
    defer build_second.deinit(alloc);
    try std.testing.expect(build_second.published);

    const graph_change = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "{\"text\":\"bravo\",\"graph_edges\":[{\"edge_type\":\"related\",\"target\":\"doc-c\"}]}" },
    };
    var ingest_graph = try api.ingestBatch(.{ .namespace = "docs", .timestamp_ns = 300, .mutations = &graph_change });
    defer ingest_graph.deinit(alloc);

    var graph_rebuild = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/indexes/graph_idx",
    });
    defer graph_rebuild.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), graph_rebuild.status);
    var parsed_graph_rebuild = try parseServerlessIndexStatusTestResponse(alloc, graph_rebuild.body, "graph_idx");
    defer parsed_graph_rebuild.deinit();
    try std.testing.expectEqualStrings("rebuild", parsed_graph_rebuild.value.status.planned_publication_action.?);
}

test "http handler create index expands schema-derived algebraic config" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-http-algebraic-create-index");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-http-algebraic-create-index");
    const wal_root = tmpPath(&wal_root_buf, "wal-http-algebraic-create-index");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-http-algebraic-create-index");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    try std.testing.expect(try catalog.ensureTableWithDefinition(
        "docs",
        100,
        .{},
        "{\"version\":1,\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"customer\":{\"type\":\"keyword\"},\"amount\":{\"type\":\"number\"},\"created_at\":{\"type\":\"datetime\"}}}}}}",
        "",
        "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
    ));

    var create = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/indexes/sales_rollup",
        .body =
        \\{"name":"sales_rollup","type":"algebraic","derive_from_schema":true}
        ,
    });
    defer create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), create.status);

    var table = (try catalog.getTableAlloc(alloc, "docs")).?;
    defer table.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, table.indexes_json, "\"derive_from_schema\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, table.indexes_json, "\"sales_rollup\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, table.indexes_json, "\"type\":\"algebraic\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, table.indexes_json, "\"group_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, table.indexes_json, "\"measure_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, table.indexes_json, "\"time_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, table.indexes_json, "\"materializations\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, table.indexes_json, "\"sum_by_customer\"") == null);

    var detail = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/indexes/sales_rollup",
    });
    defer detail.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), detail.status);
    try std.testing.expect(std.mem.indexOf(u8, detail.body, "\"derive_from_schema\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, detail.body, "\"group_fields\"") != null);
}

test "serverless algebraic index catalog validation rejects malformed configs" {
    const alloc = std.testing.allocator;

    try validateServerlessIndexCatalog(alloc,
        \\{"sales_rollup":{"type":"algebraic","version":1,"group_fields":[{"name":"customer","path":"customer","type":"keyword"}],"measure_fields":[{"name":"amount","path":"amount","type":"number"}],"time_fields":[{"name":"created_at","path":"created_at","type":"datetime"}],"materializations":[{"name":"sum_by_customer","op":"sum","group_by":["customer"],"measure":"amount","time":"created_at","bucket":"day"}]}}
    );

    try std.testing.expectError(error.InvalidTableIndexMetadata, validateServerlessIndexCatalog(alloc,
        \\{"sales_rollup":{"type":"algebraic","version":1,"group_fields":[{"name":"customer","path":"customer","type":"keyword"}],"materializations":[{"name":"bad","op":"median","group_by":["customer"]}]}}
    ));

    try std.testing.expectError(error.InvalidTableIndexMetadata, validateServerlessIndexCatalog(alloc,
        \\{"sales_rollup":{"type":"algebraic","version":1,"group_fields":[{"name":"customer","path":"customer","type":"keyword"}],"measure_fields":[{"name":"amount","path":"amount","type":"number"}],"materializations":[{"name":"bad","op":"sum","group_by":["missing"],"measure":"amount"}]}}
    ));

    try std.testing.expectError(error.InvalidTableIndexMetadata, validateServerlessIndexCatalog(alloc,
        \\{"sales_rollup":{"type":"algebraic","version":1,"time_fields":[{"name":"created_at","path":"created_at","type":"datetime"}],"materializations":[{"name":"bad","op":"count","time":"created_at","bucket":"week"}]}}
    ));
}

test "http handler serves the table public lifecycle and consistency routes" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-table-http");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-table-http");
    const wal_root = tmpPath(&wal_root_buf, "wal-table-http");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-table-http");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    var create = try handler.handle(.{
        .method = .put,
        .path = "/tables/docs",
        .body = "{\"created_at_ns\":123,\"policy\":{\"default_query_view\":\"latest\",\"keep_latest_versions\":3},\"schema\":{\"default_type\":\"doc\"},\"indexes\":{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3}}}",
    });
    defer create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), create.status);
    var parsed_create = try parseJsonTestBody(api_types.EnsureTableResult, alloc, create.body);
    defer parsed_create.deinit();
    try std.testing.expectEqualStrings("docs", parsed_create.value.table_name);
    try std.testing.expect(parsed_create.value.created);
    try std.testing.expectEqual(@as(u64, 123), parsed_create.value.created_at_ns);
    try std.testing.expectEqual(catalog_types.DefaultQueryView.latest, parsed_create.value.policy.default_query_view);

    var list = try handler.handle(.{
        .method = .get,
        .path = "/tables",
    });
    defer list.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), list.status);
    var parsed_list = try parseJsonTestBody([]api_types.TableRecord, alloc, list.body);
    defer parsed_list.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_list.value.len);
    try std.testing.expectEqualStrings("docs", parsed_list.value[0].table_name);

    var policy = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/tables/docs/policy",
    });
    defer policy.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), policy.status);
    var parsed_policy = try parseJsonTestBody(api_types.TablePolicyResult, alloc, policy.body);
    defer parsed_policy.deinit();
    try std.testing.expectEqualStrings("docs", parsed_policy.value.table_name);
    try std.testing.expectEqual(catalog_types.DefaultQueryView.latest, parsed_policy.value.policy.default_query_view);

    var ingest = try handler.handle(.{
        .method = .put,
        .path = "/tables/docs/ingest-batch",
        .body =
        \\{"timestamp_ns":456,"mutations":[{"kind":"upsert","doc_id":"doc-a","body":"{\"body\":\"alpha\",\"version\":1,\"embedding\":[1,0,0]}"}]}
        ,
    });
    defer ingest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), ingest.status);
    var parsed_ingest = try parseJsonTestBody(api_types.TableIngestBatchResult, alloc, ingest.body);
    defer parsed_ingest.deinit();
    try std.testing.expectEqualStrings("docs", parsed_ingest.value.table_name);
    try std.testing.expectEqual(@as(usize, 1), parsed_ingest.value.mutation_count);

    var build_status = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/tables/docs/build-status",
    });
    defer build_status.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), build_status.status);
    var parsed_build_status = try parseJsonTestBody(api_types.TableBuildStatus, alloc, build_status.body);
    defer parsed_build_status.deinit();
    try std.testing.expectEqualStrings("docs", parsed_build_status.value.table_name);
    try std.testing.expectEqual(@as(u64, 1), parsed_build_status.value.pending_records);
    try std.testing.expectEqual(@as(?catalog_types.NextPublishReason, .wal_artifact_update), parsed_build_status.value.next_publish_reason);
    try std.testing.expectEqual(@as(bool, false), parsed_build_status.value.compaction_recommended);
    try std.testing.expectEqual(@as(bool, false), parsed_build_status.value.mutation_tail_compaction_recommended);
    try std.testing.expectEqual(@as(bool, false), parsed_build_status.value.vector_compaction_recommended);
    try std.testing.expectEqual(catalog_types.MutationTailResolution.none, parsed_build_status.value.mutation_tail_resolution);
    try std.testing.expectEqual(@as(?[]u8, null), parsed_build_status.value.vector_compaction_driver_index_name);
    try std.testing.expectEqual(@as(?shared_vector.DistanceMetric, null), parsed_build_status.value.vector_compaction_distance_metric);
    try std.testing.expectEqual(@as(?catalog_types.DocumentPublishMode, null), parsed_build_status.value.head_document_publish_mode);
    try std.testing.expectEqual(@as(?catalog_types.DocumentPublishMode, .append_mutation_tail), parsed_build_status.value.next_document_publish_mode);

    var build = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/tables/docs/build",
    });
    defer build.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), build.status);
    var parsed_build = try parseJsonTestBody(api_types.TableBuildResult, alloc, build.body);
    defer parsed_build.deinit();
    try std.testing.expectEqualStrings("docs", parsed_build.value.table_name);
    try std.testing.expect(parsed_build.value.published);
    try std.testing.expectEqual(@as(u64, 1), parsed_build.value.version);

    var query_published = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/query/published",
    });
    defer query_published.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_published.status);
    var parsed_query_published = try parseJsonTestBody(query_types.TableQueryResult, alloc, query_published.body);
    defer parsed_query_published.deinit();
    try std.testing.expectEqualStrings("docs", parsed_query_published.value.table_name);
    try std.testing.expectEqual(query_types.QueryView.published, parsed_query_published.value.view);
    try std.testing.expectEqual(@as(usize, 1), parsed_query_published.value.documents.len);
    try std.testing.expectEqualStrings("doc-a", parsed_query_published.value.documents[0].doc_id);
    try std.testing.expectEqual(@as(bool, false), parsed_query_published.value.publication.publish_recommended);
    try std.testing.expectEqual(@as(bool, false), parsed_query_published.value.publication.mutation_tail_compaction_recommended);
    try std.testing.expectEqual(@as(?[]const u8, null), parsed_query_published.value.publication.vector_compaction_driver_index_name);
    try std.testing.expectEqual(@as(?shared_vector.DistanceMetric, null), parsed_query_published.value.publication.vector_distance_metric);
    try std.testing.expectEqual(@as(bool, false), parsed_query_published.value.publication.vector_compaction_recommended);
    try std.testing.expectEqual(@as(?u32, null), parsed_query_published.value.publication.vector_cluster_count);
    try std.testing.expectEqual(@as(?u32, null), parsed_query_published.value.publication.vector_base_probe_count);
    try std.testing.expectEqual(@as(?u32, null), parsed_query_published.value.publication.vector_shortlist_multiplier);
    try std.testing.expectEqual(@as(?u32, null), parsed_query_published.value.publication.vector_target_cluster_count);
    try std.testing.expectEqual(@as(?u32, null), parsed_query_published.value.publication.vector_target_base_probe_count);
    try std.testing.expectEqual(@as(?u32, null), parsed_query_published.value.publication.vector_target_shortlist_multiplier);

    var search = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/search",
        .body = "{\"text\":\"alpha\",\"limit\":5}",
    });
    defer search.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), search.status);
    var parsed_search = try parseJsonTestBody(query_types.TableQuerySearchResult, alloc, search.body);
    defer parsed_search.deinit();
    try std.testing.expectEqualStrings("docs", parsed_search.value.table_name);
    try std.testing.expectEqual(@as(usize, 1), parsed_search.value.hit_count);
    try std.testing.expectEqualStrings("doc-a", parsed_search.value.hits[0].doc_id);

    var public_search = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/search",
        .body = "{\"full_text_search\":{\"query\":\"body:alpha\"},\"limit\":5}",
    });
    defer public_search.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), public_search.status);
    var parsed_public_search = try parseJsonTestBody(query_types.TableQuerySearchResult, alloc, public_search.body);
    defer parsed_public_search.deinit();
    try std.testing.expectEqualStrings("docs", parsed_public_search.value.table_name);
    try std.testing.expectEqualStrings("alpha", parsed_public_search.value.query_text);
    try std.testing.expectEqual(@as(usize, 1), parsed_public_search.value.hit_count);

    var public_aggregated_search = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/search",
        .body =
        \\{"full_text_search":{"query":"body:alpha"},"limit":5,"aggregations":{"version_stats":{"type":"stats","field":"version"},"body_terms":{"type":"terms","field":"body","size":5}}}
        ,
    });
    defer public_aggregated_search.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), public_aggregated_search.status);
    var parsed_public_aggregated_search = try parseJsonTestBody(TestSearchAggregationsResponse, alloc, public_aggregated_search.body);
    defer parsed_public_aggregated_search.deinit();
    const public_search_aggs = parsed_public_aggregated_search.value.aggregations.?;
    try std.testing.expectEqual(@as(?i64, 1), public_search_aggs.map.get("version_stats").?.count);
    try std.testing.expectEqualStrings("alpha", public_search_aggs.map.get("body_terms").?.buckets.?[0].key);

    var public_significant_terms_search = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/search",
        .body =
        \\{"full_text_search":{"query":"body:alpha"},"limit":5,"aggregations":{"body_sig_bg":{"type":"significant_terms","field":"body","size":5,"background_filter":{"match":{"body":"alpha"}}}}}
        ,
    });
    defer public_significant_terms_search.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), public_significant_terms_search.status);
    var parsed_public_significant_terms_search = try parseJsonTestBody(TestSearchAggregationsResponse, alloc, public_significant_terms_search.body);
    defer parsed_public_significant_terms_search.deinit();
    const public_sig_search = parsed_public_significant_terms_search.value.aggregations.?.map.get("body_sig_bg").?;
    try std.testing.expectEqualStrings("alpha", public_sig_search.buckets.?[0].key);
    try std.testing.expect(public_sig_search.buckets.?[0].bg_count != null);

    var vector_search = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/search",
        .body = "{\"embeddings\":{\"semantic_idx\":[1,0,0]},\"indexes\":[\"semantic_idx\"],\"limit\":5}",
    });
    defer vector_search.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), vector_search.status);
    var parsed_vector_search = try parseJsonTestBody(query_types.TableQuerySearchResult, alloc, vector_search.body);
    defer parsed_vector_search.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_vector_search.value.hit_count);
    try std.testing.expectEqualStrings("doc-a", parsed_vector_search.value.hits[0].doc_id);

    var public_search_via_query = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query",
        .body = "{\"full_text_search\":{\"query\":\"body:alpha\"},\"limit\":5}",
    });
    defer public_search_via_query.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), public_search_via_query.status);
    var parsed_public_search_via_query = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, public_search_via_query.body);
    defer parsed_public_search_via_query.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_public_search_via_query.value.responses.?.len);
    try std.testing.expectEqualStrings("docs", parsed_public_search_via_query.value.responses.?[0].table.?);
    try std.testing.expectEqual(@as(?i64, 1), parsed_public_search_via_query.value.responses.?[0].hits.?.total);
    try std.testing.expectEqualStrings("doc-a", parsed_public_search_via_query.value.responses.?[0].hits.?.hits.?[0]._id);

    var public_aggregated_query = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query",
        .body =
        \\{"full_text_search":{"query":"body:alpha"},"limit":5,"aggregations":{"version_stats":{"type":"stats","field":"version"},"body_terms":{"type":"terms","field":"body","size":5}}}
        ,
    });
    defer public_aggregated_query.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), public_aggregated_query.status);
    var parsed_public_aggregated_query = try parseJsonTestBody(TestQueryAggregationsResponse, alloc, public_aggregated_query.body);
    defer parsed_public_aggregated_query.deinit();
    const public_aggregated_query_aggs = parsed_public_aggregated_query.value.responses[0].aggregations.?;
    try std.testing.expectEqual(@as(usize, 2), public_aggregated_query_aggs.map.count());
    const version_stats = public_aggregated_query_aggs.map.get("version_stats").?;
    try std.testing.expectEqual(@as(?i64, 1), version_stats.count);
    const body_terms = public_aggregated_query_aggs.map.get("body_terms").?;
    try std.testing.expectEqual(@as(usize, 1), body_terms.buckets.?.len);
    try std.testing.expectEqualStrings("alpha", body_terms.buckets.?[0].key);
    try std.testing.expectEqual(@as(i64, 1), body_terms.buckets.?[0].doc_count);

    var public_significant_terms_query = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query",
        .body =
        \\{"full_text_search":{"query":"body:alpha"},"limit":5,"aggregations":{"body_sig":{"type":"significant_terms","field":"body","size":5}}}
        ,
    });
    defer public_significant_terms_query.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), public_significant_terms_query.status);
    var parsed_public_significant_terms_query = try parseJsonTestBody(TestQueryAggregationsResponse, alloc, public_significant_terms_query.body);
    defer parsed_public_significant_terms_query.deinit();
    const body_sig = parsed_public_significant_terms_query.value.responses[0].aggregations.?.map.get("body_sig").?;
    try std.testing.expectEqual(@as(usize, 1), body_sig.buckets.?.len);
    try std.testing.expectEqualStrings("alpha", body_sig.buckets.?[0].key);
    try std.testing.expect(body_sig.buckets.?[0].score != null);

    var public_significant_terms_bg_query = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query",
        .body =
        \\{"full_text_search":{"query":"body:alpha"},"limit":5,"aggregations":{"body_sig_bg":{"type":"significant_terms","field":"body","size":5,"background_filter":{"match":{"body":"alpha"}}}}}
        ,
    });
    defer public_significant_terms_bg_query.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), public_significant_terms_bg_query.status);
    var parsed_public_significant_terms_bg_query = try parseJsonTestBody(TestQueryAggregationsResponse, alloc, public_significant_terms_bg_query.body);
    defer parsed_public_significant_terms_bg_query.deinit();
    const body_sig_bg = parsed_public_significant_terms_bg_query.value.responses[0].aggregations.?.map.get("body_sig_bg").?;
    try std.testing.expectEqual(@as(usize, 1), body_sig_bg.buckets.?.len);
    try std.testing.expectEqualStrings("alpha", body_sig_bg.buckets.?[0].key);
    try std.testing.expect(body_sig_bg.buckets.?[0].bg_count != null);

    var count_only_search = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/search",
        .body = "{\"full_text_search\":{\"query\":\"body:alpha\"},\"count\":true}",
    });
    defer count_only_search.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), count_only_search.status);
    var parsed_count_only_search = try parseJsonTestBody(query_types.TableQuerySearchResult, alloc, count_only_search.body);
    defer parsed_count_only_search.deinit();
    try std.testing.expect(parsed_count_only_search.value.count_only);
    try std.testing.expectEqual(@as(usize, 1), parsed_count_only_search.value.hit_count);
    try std.testing.expectEqual(@as(usize, 0), parsed_count_only_search.value.hits.len);

    var projected_fields_search = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/search",
        .body = "{\"full_text_search\":{\"query\":\"body:alpha\"},\"fields\":[\"body\",\"version\"]}",
    });
    defer projected_fields_search.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), projected_fields_search.status);
    var parsed_projected_fields_search = try parseJsonTestBody(query_types.TableQuerySearchResult, alloc, projected_fields_search.body);
    defer parsed_projected_fields_search.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_projected_fields_search.value.hits.len);
    try std.testing.expectEqualStrings("{\"body\":\"alpha\",\"version\":1}", parsed_projected_fields_search.value.hits[0].body);

    var unsupported_fields_search = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/search",
        .body = "{\"full_text_search\":{\"query\":\"body:alpha\"},\"fields\":[\"_chunks.*\"]}",
    });
    defer unsupported_fields_search.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), unsupported_fields_search.status);

    var text_only_update = try handler.handle(.{
        .method = .put,
        .path = "/tables/docs/ingest-batch",
        .body =
        \\{"timestamp_ns":600,"mutations":[{"kind":"upsert","doc_id":"doc-a","body":"{\"body\":\"bravo\",\"version\":1,\"embedding\":[1,0,0]}"}]}
        ,
    });
    defer text_only_update.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), text_only_update.status);

    var second_build = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/tables/docs/build",
    });
    defer second_build.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), second_build.status);

    var full_text_index = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/indexes/full_text_index_v0",
    });
    defer full_text_index.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), full_text_index.status);
    var parsed_full_text_index = try parseServerlessIndexStatusTestResponse(alloc, full_text_index.body, "full_text_index_v0");
    defer parsed_full_text_index.deinit();
    try std.testing.expectEqualStrings("reuse", parsed_full_text_index.value.status.planned_publication_action.?);
    try std.testing.expectEqualStrings("rebuild", parsed_full_text_index.value.status.head_publication_action.?);
    try std.testing.expectEqual(@as(?bool, false), parsed_full_text_index.value.status.materialization_blocked);
    try std.testing.expectEqual(@as(?bool, false), parsed_full_text_index.value.status.vector_compaction_driver);
    try std.testing.expectEqual(@as(?bool, false), parsed_full_text_index.value.status.vector_compaction_recommended);

    var semantic_index = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/indexes/semantic_idx",
    });
    defer semantic_index.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), semantic_index.status);
    var parsed_semantic_index = try parseServerlessIndexStatusTestResponse(alloc, semantic_index.body, "semantic_idx");
    defer parsed_semantic_index.deinit();
    try std.testing.expectEqualStrings("reuse", parsed_semantic_index.value.status.planned_publication_action.?);
    try std.testing.expectEqualStrings("reuse", parsed_semantic_index.value.status.head_publication_action.?);
    try std.testing.expectEqual(@as(?bool, false), parsed_semantic_index.value.status.materialization_blocked);
    try std.testing.expectEqual(@as(?bool, false), parsed_semantic_index.value.status.vector_compaction_driver);
    try std.testing.expectEqual(@as(?[]const u8, null), parsed_semantic_index.value.status.vector_distance_metric);
    try std.testing.expectEqual(@as(?bool, false), parsed_semantic_index.value.status.vector_compaction_recommended);
    try std.testing.expectEqual(@as(?u32, null), parsed_semantic_index.value.status.vector_cluster_count);
    try std.testing.expectEqual(@as(?u32, null), parsed_semantic_index.value.status.vector_base_probe_count);
    try std.testing.expectEqual(@as(?u32, null), parsed_semantic_index.value.status.vector_shortlist_multiplier);
    try std.testing.expectEqual(@as(?f32, null), parsed_semantic_index.value.status.vector_cluster_imbalance);
    try std.testing.expectEqual(@as(?f32, null), parsed_semantic_index.value.status.vector_cluster_distance_span_max);
    try std.testing.expectEqual(@as(?u32, null), parsed_semantic_index.value.status.vector_target_cluster_count);
    try std.testing.expectEqual(@as(?u32, null), parsed_semantic_index.value.status.vector_target_base_probe_count);
    try std.testing.expectEqual(@as(?u32, null), parsed_semantic_index.value.status.vector_target_shortlist_multiplier);

    var next_ingest = try handler.handle(.{
        .method = .put,
        .path = "/tables/docs/ingest-batch",
        .body =
        \\{"timestamp_ns":789,"mutations":[{"kind":"upsert","doc_id":"doc-b","body":"beta"}]}
        ,
    });
    defer next_ingest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), next_ingest.status);

    var query_default = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/query",
    });
    defer query_default.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_default.status);
    var parsed_query_default = try parseJsonTestBody(query_types.TableQueryResult, alloc, query_default.body);
    defer parsed_query_default.deinit();
    try std.testing.expectEqualStrings("docs", parsed_query_default.value.table_name);
    try std.testing.expectEqual(query_types.QueryView.latest, parsed_query_default.value.view);
    try std.testing.expectEqual(@as(bool, true), parsed_query_default.value.publication.publish_recommended);
    try std.testing.expectEqual(@as(?catalog_types.NextPublishReason, .wal_artifact_update), parsed_query_default.value.publication.next_publish_reason);
    try std.testing.expectEqual(@as(?catalog_types.DocumentPublishMode, .append_mutation_tail), parsed_query_default.value.publication.head_document_publish_mode);
    try std.testing.expectEqual(@as(?catalog_types.DocumentPublishMode, .append_mutation_tail), parsed_query_default.value.publication.next_document_publish_mode);
    try std.testing.expectEqual(@as(bool, false), parsed_query_default.value.publication.pending_materialization_families.full_text);
    try std.testing.expectEqualStrings("rebuild", @tagName(parsed_query_default.value.publication.artifact_actions.dense_vector));
    try std.testing.expectEqual(@as(usize, 1), parsed_query_default.value.publication.full_text_index_actions.len);
    try std.testing.expectEqualStrings("full_text_index_v0", parsed_query_default.value.publication.full_text_index_actions[0].name);
    try std.testing.expectEqual(@as(usize, 1), parsed_query_default.value.publication.vector_index_actions.len);
    try std.testing.expectEqualStrings("semantic_idx", parsed_query_default.value.publication.vector_index_actions[0].name);
    try std.testing.expectEqual(@as(usize, 2), parsed_query_default.value.documents.len);
    try std.testing.expectEqualStrings("doc-a", parsed_query_default.value.documents[0].doc_id);
    try std.testing.expectEqualStrings("doc-b", parsed_query_default.value.documents[1].doc_id);

    var query_latest = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/query/latest",
    });
    defer query_latest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_latest.status);
    var parsed_query_latest = try parseJsonTestBody(query_types.TableQueryResult, alloc, query_latest.body);
    defer parsed_query_latest.deinit();
    try std.testing.expectEqualStrings("docs", parsed_query_latest.value.table_name);
    try std.testing.expectEqual(query_types.QueryView.latest, parsed_query_latest.value.view);
    try std.testing.expectEqual(@as(usize, 1), parsed_query_latest.value.overlay_mutation_count);
    try std.testing.expectEqual(@as(bool, true), parsed_query_latest.value.publication.publish_recommended);
    try std.testing.expectEqual(@as(?catalog_types.NextPublishReason, .wal_artifact_update), parsed_query_latest.value.publication.next_publish_reason);
    try std.testing.expectEqual(@as(?catalog_types.DocumentPublishMode, .append_mutation_tail), parsed_query_latest.value.publication.head_document_publish_mode);
    try std.testing.expectEqual(@as(?catalog_types.DocumentPublishMode, .append_mutation_tail), parsed_query_latest.value.publication.next_document_publish_mode);
    try std.testing.expectEqual(@as(bool, false), parsed_query_latest.value.publication.pending_materialization_families.full_text);
    try std.testing.expectEqualStrings("rebuild", @tagName(parsed_query_latest.value.publication.artifact_actions.dense_vector));
    try std.testing.expectEqual(@as(usize, 1), parsed_query_latest.value.publication.vector_index_actions.len);
    try std.testing.expectEqualStrings("semantic_idx", parsed_query_latest.value.publication.vector_index_actions[0].name);
    try std.testing.expectEqual(@as(usize, 2), parsed_query_latest.value.documents.len);
    try std.testing.expectEqualStrings("doc-a", parsed_query_latest.value.documents[0].doc_id);
    try std.testing.expectEqualStrings("doc-b", parsed_query_latest.value.documents[1].doc_id);

    var transformed_batch = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/batch",
        .body =
        \\{"transforms":[{"key":"doc-a","operations":[{"op":"$set","path":"status","value":"updated"},{"op":"$max","path":"version","value":3}]}]}
        ,
    });
    defer transformed_batch.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), transformed_batch.status);
    var parsed_transformed_batch = try parseJsonTestBody(metadata_openapi.BatchResponse, alloc, transformed_batch.body);
    defer parsed_transformed_batch.deinit();
    try std.testing.expectEqual(@as(?i64, 1), parsed_transformed_batch.value.transformed);

    var transformed_latest = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/query/latest",
    });
    defer transformed_latest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), transformed_latest.status);
    var parsed_transformed_latest = try parseJsonTestBody(query_types.TableQueryResult, alloc, transformed_latest.body);
    defer parsed_transformed_latest.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_transformed_latest.value.documents.len);
    try std.testing.expectEqualStrings("doc-a", parsed_transformed_latest.value.documents[0].doc_id);
    try std.testing.expectEqualStrings("doc-b", parsed_transformed_latest.value.documents[1].doc_id);
    const TransformedDocument = struct {
        body: []const u8,
        version: i64,
        status: []const u8,
    };
    var parsed_transformed_document = try parseJsonTestBody(TransformedDocument, alloc, parsed_transformed_latest.value.documents[0].body);
    defer parsed_transformed_document.deinit();
    try std.testing.expectEqualStrings("bravo", parsed_transformed_document.value.body);
    try std.testing.expectEqual(@as(i64, 3), parsed_transformed_document.value.version);
    try std.testing.expectEqualStrings("updated", parsed_transformed_document.value.status);
}

test "http handler accepts structured table updates for metadata-only republish planning" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-http-update-table");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-http-update-table");
    const wal_root = tmpPath(&wal_root_buf, "wal-http-update-table");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-http-update-table");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    var create = try handler.handle(.{
        .method = .put,
        .path = "/tables/docs",
        .body = "{\"created_at_ns\":123,\"schema\":{\"version\":1,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"body\":{\"type\":\"string\"}}}}}},\"indexes\":{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3}}}",
    });
    defer create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), create.status);

    var ingest = try handler.handle(.{
        .method = .put,
        .path = "/tables/docs/ingest-batch",
        .body =
        \\{"timestamp_ns":456,"mutations":[{"kind":"upsert","doc_id":"doc-a","body":"{\"body\":\"alpha\",\"_embeddings\":{\"semantic_idx\":[1,0,0]}}"}]}
        ,
    });
    defer ingest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), ingest.status);

    var build = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/tables/docs/build",
    });
    defer build.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), build.status);

    var update = try handler.handle(.{
        .method = .put,
        .path = "/tables/docs",
        .body =
        \\{"schema":{"version":2,"document_schemas":{"doc":{"schema":{"type":"object","properties":{"body":{"type":"string"},"title":{"type":"string"}}}}}},"read_schema":{"version":1,"document_schemas":{"doc":{"schema":{"type":"object","properties":{"body":{"type":"string"}}}}}},"indexes":{"full_text_index_v0":{"type":"full_text"},"full_text_index_v1":{"type":"full_text"},"semantic_idx":{"type":"embeddings","dimension":3,"distance_metric":"inner_product"}}}
        ,
    });
    defer update.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), update.status);

    var build_status = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/tables/docs/build-status",
    });
    defer build_status.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), build_status.status);
    var parsed_build_status = try parseJsonTestBody(api_types.TableBuildStatus, alloc, build_status.body);
    defer parsed_build_status.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_build_status.value.full_text_index_actions.len);
    try std.testing.expectEqual(@as(usize, 1), parsed_build_status.value.vector_index_actions.len);
    try std.testing.expectEqualStrings("semantic_idx", parsed_build_status.value.vector_index_actions[0].name);
    try std.testing.expectEqual(catalog_types.ArtifactPublicationAction.rebuild, parsed_build_status.value.vector_index_actions[0].action);
}

test "http handler query publication exposes vector compaction targets" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-table-http-vector-compaction");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-table-http-vector-compaction");
    const wal_root = tmpPath(&wal_root_buf, "wal-table-http-vector-compaction");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-table-http-vector-compaction");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    var create = try handler.handle(.{
        .method = .put,
        .path = "/tables/docs",
        .body = "{\"created_at_ns\":123,\"policy\":{\"vector_compaction_max_cluster_imbalance\":0.0,\"vector_compaction_max_distance_span\":0.0},\"schema\":{\"default_type\":\"doc\"},\"indexes\":{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":2}}}",
    });
    defer create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), create.status);

    const ingest_body =
        \\{"timestamp_ns":456,"mutations":[
        \\{"kind":"upsert","doc_id":"doc-a","body":"{\"body\":\"alpha\",\"embedding\":[0,0]}"},
        \\{"kind":"upsert","doc_id":"doc-b","body":"{\"body\":\"bravo\",\"embedding\":[10,0]}"},
        \\{"kind":"upsert","doc_id":"doc-c","body":"{\"body\":\"charlie\",\"embedding\":[0,10]}"},
        \\{"kind":"upsert","doc_id":"doc-d","body":"{\"body\":\"delta\",\"embedding\":[10,10]}"}
        \\]}
    ;
    var ingest = try handler.handle(.{
        .method = .put,
        .path = "/tables/docs/ingest-batch",
        .body = ingest_body,
    });
    defer ingest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), ingest.status);

    var build = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/tables/docs/build",
    });
    defer build.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), build.status);

    var build_status = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/tables/docs/build-status",
    });
    defer build_status.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), build_status.status);
    var parsed_build_status = try parseJsonTestBody(api_types.TableBuildStatus, alloc, build_status.body);
    defer parsed_build_status.deinit();
    try std.testing.expectEqualStrings("docs", parsed_build_status.value.table_name);
    try std.testing.expectEqualStrings("semantic_idx", parsed_build_status.value.vector_compaction_driver_index_name.?);
    try std.testing.expectEqual(shared_vector.DistanceMetric.cosine, parsed_build_status.value.vector_compaction_distance_metric.?);
    try std.testing.expectEqual(@as(bool, true), parsed_build_status.value.compaction_recommended);
    try std.testing.expectEqual(@as(bool, false), parsed_build_status.value.mutation_tail_compaction_recommended);
    try std.testing.expectEqual(@as(bool, true), parsed_build_status.value.vector_compaction_recommended);

    var query_published = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/query/published",
    });
    defer query_published.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_published.status);
    var parsed_query_published = try parseJsonTestBody(query_types.TableQueryResult, alloc, query_published.body);
    defer parsed_query_published.deinit();
    try std.testing.expectEqualStrings("semantic_idx", parsed_query_published.value.publication.vector_compaction_driver_index_name.?);
    try std.testing.expectEqual(@as(bool, false), parsed_query_published.value.publication.mutation_tail_compaction_recommended);
    try std.testing.expectEqual(shared_vector.DistanceMetric.cosine, parsed_query_published.value.publication.vector_distance_metric.?);
    try std.testing.expectEqual(@as(bool, true), parsed_query_published.value.publication.vector_compaction_recommended);
    try std.testing.expect(parsed_query_published.value.publication.vector_cluster_count != null);
    try std.testing.expect(parsed_query_published.value.publication.vector_base_probe_count != null);
    try std.testing.expect(parsed_query_published.value.publication.vector_shortlist_multiplier != null);
    try std.testing.expect(parsed_query_published.value.publication.vector_cluster_imbalance != null);
    try std.testing.expect(parsed_query_published.value.publication.vector_cluster_distance_span_max != null);
    try std.testing.expect(parsed_query_published.value.publication.vector_target_cluster_count != null);
    try std.testing.expect(parsed_query_published.value.publication.vector_target_base_probe_count != null);
    try std.testing.expect(parsed_query_published.value.publication.vector_target_shortlist_multiplier != null);

    var semantic_index = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/indexes/semantic_idx",
    });
    defer semantic_index.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), semantic_index.status);
    var parsed_semantic_index = try parseServerlessIndexStatusTestResponse(alloc, semantic_index.body, "semantic_idx");
    defer parsed_semantic_index.deinit();
    try std.testing.expectEqual(@as(?bool, true), parsed_semantic_index.value.status.vector_compaction_driver);
    try std.testing.expectEqualStrings("cosine", parsed_semantic_index.value.status.vector_distance_metric.?);
    try std.testing.expectEqual(@as(?bool, true), parsed_semantic_index.value.status.vector_compaction_recommended);
    try std.testing.expect(parsed_semantic_index.value.status.vector_cluster_count != null);
    try std.testing.expect(parsed_semantic_index.value.status.vector_base_probe_count != null);
    try std.testing.expect(parsed_semantic_index.value.status.vector_shortlist_multiplier != null);
    try std.testing.expect(parsed_semantic_index.value.status.vector_cluster_imbalance != null);
    try std.testing.expect(parsed_semantic_index.value.status.vector_cluster_distance_span_max != null);
    try std.testing.expect(parsed_semantic_index.value.status.vector_target_cluster_count != null);
}

test "http handler resolves table routes through persisted serving namespace mappings" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-table-mapped");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-table-mapped");
    const wal_root = tmpPath(&wal_root_buf, "wal-table-mapped");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-table-mapped");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    try std.testing.expect(try catalog_store.ensureTable("docs", "docs-serving", 100, .{
        .default_query_view = .published,
    }, "", "", "{}"));

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    var ingest = try handler.handle(.{
        .method = .put,
        .path = "/tables/docs/ingest-batch",
        .body =
        \\{"timestamp_ns":456,"mutations":[{"kind":"upsert","doc_id":"doc-a","body":"alpha"}]}
        ,
    });
    defer ingest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), ingest.status);
    var parsed_ingest = try parseJsonTestBody(api_types.TableIngestBatchResult, alloc, ingest.body);
    defer parsed_ingest.deinit();
    try std.testing.expectEqualStrings("docs", parsed_ingest.value.table_name);

    var build = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/tables/docs/build",
    });
    defer build.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), build.status);
    var parsed_build = try parseJsonTestBody(api_types.TableBuildResult, alloc, build.body);
    defer parsed_build.deinit();
    try std.testing.expectEqualStrings("docs", parsed_build.value.table_name);

    var query_published = try handler.handle(.{
        .method = .get,
        .path = "/tables/docs/query/published",
    });
    defer query_published.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), query_published.status);
    var parsed_query_published = try parseJsonTestBody(query_types.TableQueryResult, alloc, query_published.body);
    defer parsed_query_published.deinit();
    try std.testing.expectEqualStrings("docs", parsed_query_published.value.table_name);
    try std.testing.expectEqual(@as(usize, 1), parsed_query_published.value.documents.len);
    try std.testing.expectEqualStrings("doc-a", parsed_query_published.value.documents[0].doc_id);
}

test "http handler honors public serverless sync levels on table batch writes" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-table-sync-levels");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-table-sync-levels");
    const wal_root = tmpPath(&wal_root_buf, "wal-table-sync-levels");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-table-sync-levels");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    var create_docs = try handler.handle(.{
        .method = .put,
        .path = "/tables/docs",
        .body = "{\"created_at_ns\":123}",
    });
    defer create_docs.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), create_docs.status);

    var full_text_batch = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/batch",
        .body =
        \\{"inserts":{"doc:a":{"body":"alpha sync level"}},"sync_level":"full_text"}
        ,
    });
    defer full_text_batch.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), full_text_batch.status);

    var search = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/search",
        .body = "{\"full_text_search\":{\"query\":\"body:alpha\"},\"limit\":5}",
    });
    defer search.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), search.status);
    var parsed_search = try parseJsonTestBody(query_types.TableQuerySearchResult, alloc, search.body);
    defer parsed_search.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_search.value.hit_count);
    try std.testing.expectEqualStrings("doc:a", parsed_search.value.hits[0].doc_id);

    var create_enriched = try handler.handle(.{
        .method = .put,
        .path = "/tables/enriched",
        .body =
        \\{"created_at_ns":124,"policy":{"chunk_embeddings_enabled":true},"indexes":{"semantic_idx":{"type":"embeddings","dimension":3}}}
        ,
    });
    defer create_enriched.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), create_enriched.status);

    var unsupported = try handler.handle(.{
        .method = .post,
        .path = "/tables/enriched/batch",
        .body =
        \\{"inserts":{"doc:a":{"body":"alpha sync level","_embeddings":{"semantic_idx":[1,0,0]}}},"sync_level":"enrichments"}
        ,
    });
    defer unsupported.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), unsupported.status);
    try std.testing.expect(std.mem.indexOf(u8, unsupported.body, "unsupported sync_level") != null);
}

test "http handler serves published graph query endpoints" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-graph");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-graph");
    const wal_root = tmpPath(&wal_root_buf, "wal-graph");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-graph");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    var create = try handler.handle(.{
        .method = .put,
        .path = "/internal/v1/namespaces/docs",
        .body = "{\"created_at_ns\":100}",
    });
    defer create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), create.status);

    var create_table = try handler.handle(.{
        .method = .put,
        .path = "/tables/docs",
        .body = "{\"created_at_ns\":100}",
    });
    defer create_table.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), create_table.status);

    var ingest = try handler.handle(.{
        .method = .put,
        .path = "/internal/v1/namespaces/docs/ingest-batch",
        .body =
        \\{"timestamp_ns":200,"mutations":[
        \\{"kind":"upsert","doc_id":"doc-a","body":"{\"title\":\"alpha\",\"text\":\"alpha\",\"graph_edges\":[{\"target\":\"doc-b\",\"edge_type\":\"cites\",\"weight\":1.5},{\"target\":\"doc-c\",\"edge_type\":\"related\",\"weight\":0.5}]}"},
        \\{"kind":"upsert","doc_id":"doc-b","body":"{\"title\":\"beta\",\"text\":\"beta\",\"graph_edges\":[{\"target\":\"doc-c\",\"edge_type\":\"cites\",\"weight\":2.0}]}"},
        \\{"kind":"upsert","doc_id":"doc-c","body":"{\"title\":\"gamma\",\"text\":\"gamma\"}"}
        \\]}
        ,
    });
    defer ingest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), ingest.status);
    var parsed_ingest = try parseJsonTestBody(api_types.IngestBatchResult, alloc, ingest.body);
    defer parsed_ingest.deinit();
    try std.testing.expectEqualStrings("docs", parsed_ingest.value.namespace);
    try std.testing.expectEqual(@as(usize, 3), parsed_ingest.value.mutation_count);

    var build = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/namespaces/docs/build",
    });
    defer build.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), build.status);
    var parsed_build = try parseJsonTestBody(build_mod.BuildResult, alloc, build.body);
    defer parsed_build.deinit();
    try std.testing.expect(parsed_build.value.published);
    try std.testing.expectEqual(@as(u64, 1), parsed_build.value.version);

    var neighbors = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/namespaces/docs/query/graph/neighbors",
        .body = "{\"doc_id\":\"doc-a\",\"direction\":\"out\",\"limit\":10}",
    });
    defer neighbors.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), neighbors.status);
    var parsed_neighbors = try parseJsonTestBody(query_types.GraphNeighborsResult, alloc, neighbors.body);
    defer parsed_neighbors.deinit();
    try std.testing.expectEqual(@as(u64, 1), parsed_neighbors.value.version);
    try std.testing.expectEqualStrings("doc-a", parsed_neighbors.value.node_id);
    try std.testing.expectEqual(@as(usize, 2), parsed_neighbors.value.neighbor_count);
    try std.testing.expectEqual(@as(usize, 2), parsed_neighbors.value.neighbors.len);
    try std.testing.expectEqualStrings("doc-b", parsed_neighbors.value.neighbors[0].doc_id);
    try std.testing.expectEqualStrings("cites", parsed_neighbors.value.neighbors[0].edge_type);
    try std.testing.expectEqualStrings("doc-c", parsed_neighbors.value.neighbors[1].doc_id);
    try std.testing.expectEqualStrings("related", parsed_neighbors.value.neighbors[1].edge_type);

    var version_neighbors = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/namespaces/docs/query/versions/1/graph/neighbors",
        .body = "{\"doc_id\":\"doc-a\",\"direction\":\"out\",\"edge_type\":\"cites\",\"limit\":10}",
    });
    defer version_neighbors.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), version_neighbors.status);
    var parsed_version_neighbors = try parseJsonTestBody(query_types.GraphNeighborsResult, alloc, version_neighbors.body);
    defer parsed_version_neighbors.deinit();
    try std.testing.expectEqual(@as(u64, 1), parsed_version_neighbors.value.version);
    try std.testing.expectEqualStrings("cites", parsed_version_neighbors.value.edge_type.?);
    try std.testing.expectEqual(@as(usize, 1), parsed_version_neighbors.value.neighbor_count);
    try std.testing.expectEqualStrings("doc-b", parsed_version_neighbors.value.neighbors[0].doc_id);

    var table_neighbors = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/graph/neighbors",
        .body = "{\"doc_id\":\"doc-a\",\"direction\":\"out\",\"limit\":10}",
    });
    defer table_neighbors.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), table_neighbors.status);
    var parsed_table_neighbors = try parseJsonTestBody(query_types.TableGraphNeighborsResult, alloc, table_neighbors.body);
    defer parsed_table_neighbors.deinit();
    try std.testing.expectEqualStrings("docs", parsed_table_neighbors.value.table_name);
    try std.testing.expectEqual(@as(usize, 2), parsed_table_neighbors.value.neighbor_count);

    var traverse = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/namespaces/docs/query/graph/traverse",
        .body = "{\"start_doc_id\":\"doc-a\",\"direction\":\"out\",\"edge_type\":\"cites\",\"max_depth\":2,\"limit\":10,\"include_start\":true}",
    });
    defer traverse.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), traverse.status);
    var parsed_traverse = try parseJsonTestBody(query_types.GraphTraverseResult, alloc, traverse.body);
    defer parsed_traverse.deinit();
    try std.testing.expectEqualStrings("doc-a", parsed_traverse.value.start_node_id);
    try std.testing.expectEqual(@as(usize, 3), parsed_traverse.value.node_count);
    try std.testing.expectEqual(@as(usize, 3), parsed_traverse.value.nodes.len);
    try std.testing.expectEqualStrings("doc-a", parsed_traverse.value.nodes[0].doc_id);
    try std.testing.expectEqualStrings("doc-b", parsed_traverse.value.nodes[1].doc_id);
    try std.testing.expectEqualStrings("doc-a", parsed_traverse.value.nodes[1].parent_doc_id.?);
    try std.testing.expectEqualStrings("doc-c", parsed_traverse.value.nodes[2].doc_id);
    try std.testing.expectEqualStrings("doc-b", parsed_traverse.value.nodes[2].parent_doc_id.?);
    try std.testing.expectEqualStrings("cites", parsed_traverse.value.nodes[2].via_edge_type.?);
    try std.testing.expectEqual(@as(usize, 3), parsed_traverse.value.nodes[2].path.?.len);
    try std.testing.expectEqualStrings("doc-c", parsed_traverse.value.nodes[2].path.?[2]);
    try std.testing.expect(parsed_traverse.value.nodes[2].edge_path != null);

    var table_traverse = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/graph/traverse",
        .body = "{\"start_doc_id\":\"doc-a\",\"direction\":\"out\",\"edge_type\":\"cites\",\"max_depth\":2,\"limit\":10,\"include_start\":true}",
    });
    defer table_traverse.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), table_traverse.status);
    var parsed_table_traverse = try parseJsonTestBody(query_types.TableGraphTraverseResult, alloc, table_traverse.body);
    defer parsed_table_traverse.deinit();
    try std.testing.expectEqualStrings("docs", parsed_table_traverse.value.table_name);
    try std.testing.expectEqual(@as(usize, 3), parsed_table_traverse.value.node_count);

    var version_traverse = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/namespaces/docs/query/versions/1/graph/traverse",
        .body = "{\"start_doc_id\":\"doc-a\",\"direction\":\"out\",\"edge_type\":\"cites\",\"max_depth\":2,\"limit\":10,\"include_start\":true}",
    });
    defer version_traverse.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), version_traverse.status);
    var parsed_version_traverse = try parseJsonTestBody(query_types.GraphTraverseResult, alloc, version_traverse.body);
    defer parsed_version_traverse.deinit();
    try std.testing.expectEqual(@as(u64, 1), parsed_version_traverse.value.version);
    try std.testing.expectEqual(@as(usize, 3), parsed_version_traverse.value.node_count);
    try std.testing.expectEqualStrings("doc-b", parsed_version_traverse.value.nodes[2].parent_doc_id.?);

    var shortest = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/namespaces/docs/query/graph/shortest-path",
        .body = "{\"start_doc_id\":\"doc-a\",\"end_doc_id\":\"doc-c\",\"direction\":\"out\",\"edge_type\":\"cites\",\"max_depth\":4}",
    });
    defer shortest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), shortest.status);
    var parsed_shortest = try parseJsonTestBody(query_types.GraphShortestPathResult, alloc, shortest.body);
    defer parsed_shortest.deinit();
    try std.testing.expect(parsed_shortest.value.found);
    try std.testing.expectEqual(@as(?u32, 2), parsed_shortest.value.depth);
    try std.testing.expectEqualStrings("doc-a", parsed_shortest.value.start_node_id);
    try std.testing.expectEqualStrings("doc-c", parsed_shortest.value.end_node_id);
    try std.testing.expectEqual(@as(usize, 3), parsed_shortest.value.node_path.?.len);
    try std.testing.expectEqualStrings("doc-b", parsed_shortest.value.node_path.?[1]);
    try std.testing.expect(parsed_shortest.value.edge_path != null);
    try std.testing.expectEqualStrings("doc-b", parsed_shortest.value.edge_path.?[0].to_doc_id);
    try std.testing.expectEqualStrings("doc-c", parsed_shortest.value.edge_path.?[1].to_doc_id);
    try std.testing.expectEqualStrings("cites", parsed_shortest.value.edge_path.?[0].edge_type);

    var table_shortest = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query/graph/shortest-path",
        .body = "{\"start_doc_id\":\"doc-a\",\"end_doc_id\":\"doc-c\",\"direction\":\"out\",\"edge_type\":\"cites\",\"max_depth\":4}",
    });
    defer table_shortest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), table_shortest.status);
    var parsed_table_shortest = try parseJsonTestBody(query_types.TableGraphShortestPathResult, alloc, table_shortest.body);
    defer parsed_table_shortest.deinit();
    try std.testing.expectEqualStrings("docs", parsed_table_shortest.value.table_name);
    try std.testing.expect(parsed_table_shortest.value.found);

    var version_shortest = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/namespaces/docs/query/versions/1/graph/shortest-path",
        .body = "{\"start_doc_id\":\"doc-a\",\"end_doc_id\":\"doc-c\",\"direction\":\"out\",\"edge_type\":\"cites\",\"max_depth\":4}",
    });
    defer version_shortest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), version_shortest.status);
    var parsed_version_shortest = try parseJsonTestBody(query_types.GraphShortestPathResult, alloc, version_shortest.body);
    defer parsed_version_shortest.deinit();
    try std.testing.expectEqual(@as(u64, 1), parsed_version_shortest.value.version);
    try std.testing.expect(parsed_version_shortest.value.found);
    try std.testing.expectEqual(@as(usize, 3), parsed_version_shortest.value.node_path.?.len);

    var from_search = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query",
        .body =
        \\{"full_text_search":{"query":"alpha"},"graph_searches":{"neighbors_from_search":{"type":"neighbors","index_name":"graph_idx","start_nodes":{"result_ref":"$full_text_results","limit":1},"params":{"edge_types":["cites","related"]}}},"limit":10}
        ,
    });
    defer from_search.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), from_search.status);
    var parsed_from_search = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, from_search.body);
    defer parsed_from_search.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_from_search.value.responses.?.len);
    try std.testing.expectEqual(@as(?i64, 1), parsed_from_search.value.responses.?[0].hits.?.total);
    const neighbors_from_search = parsed_from_search.value.responses.?[0].graph_results.?.map.get("neighbors_from_search").?;
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.neighbors, neighbors_from_search.type);
    try std.testing.expectEqual(@as(i64, 2), neighbors_from_search.total);
    try std.testing.expectEqualStrings("doc-b", neighbors_from_search.nodes.?[0].key);
    try std.testing.expectEqualStrings("doc-c", neighbors_from_search.nodes.?[1].key);

    var from_fused = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query",
        .body =
        \\{"full_text_search":{"query":"alpha"},"graph_searches":{"neighbors_from_fused":{"type":"neighbors","index_name":"graph_idx","start_nodes":{"result_ref":"$fused_results","limit":1},"params":{"edge_types":["cites","related"]}}},"limit":10}
        ,
    });
    defer from_fused.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), from_fused.status);
    var parsed_from_fused = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, from_fused.body);
    defer parsed_from_fused.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_from_fused.value.responses.?.len);
    try std.testing.expectEqual(@as(?i64, 1), parsed_from_fused.value.responses.?[0].hits.?.total);
    const neighbors_from_fused = parsed_from_fused.value.responses.?[0].graph_results.?.map.get("neighbors_from_fused").?;
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.neighbors, neighbors_from_fused.type);
    try std.testing.expectEqual(@as(i64, 2), neighbors_from_fused.total);
    try std.testing.expectEqualStrings("doc-b", neighbors_from_fused.nodes.?[0].key);
    try std.testing.expectEqualStrings("doc-c", neighbors_from_fused.nodes.?[1].key);

    var pattern = try handler.handle(.{
        .method = .post,
        .path = "/tables/docs/query",
        .body =
        \\{"graph_searches":{"two_hop":{"type":"pattern","index_name":"graph_idx","start_nodes":{"keys":["doc-a"]},"pattern":[{"alias":"a"},{"alias":"b","node_filter":{"filter_query":{"term":"beta","field":"title"}},"edge":{"types":["cites"],"direction":"out","min_hops":1,"max_hops":1}},{"alias":"c","node_filter":{"filter_query":{"prefix":"ga","field":"title"}},"edge":{"types":["cites"],"direction":"out","min_hops":1,"max_hops":1}}],"include_documents":true,"fields":["title"]}},"limit":10}
        ,
    });
    defer pattern.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), pattern.status);
    var parsed_pattern = try parseJsonTestBody(metadata_openapi.QueryResponses, alloc, pattern.body);
    defer parsed_pattern.deinit();
    const two_hop = parsed_pattern.value.responses.?[0].graph_results.?.map.get("two_hop").?;
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.pattern, two_hop.type);
    try std.testing.expectEqual(@as(i64, 1), two_hop.total);
    try std.testing.expectEqual(@as(usize, 1), two_hop.matches.?.len);
    const bindings = two_hop.matches.?[0].bindings.?;
    try std.testing.expect(bindings.map.get("a") != null);
    try std.testing.expect(bindings.map.get("b") != null);
    try std.testing.expect(bindings.map.get("c") != null);
    try std.testing.expectEqualStrings("alpha", bindings.map.get("a").?.document.?.object.get("title").?.string);
    try std.testing.expectEqualStrings("beta", bindings.map.get("b").?.document.?.object.get("title").?.string);
    try std.testing.expectEqualStrings("gamma", bindings.map.get("c").?.document.?.object.get("title").?.string);

    var invalid_version_neighbors = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/namespaces/docs/query/versions/1/graph/neighbors",
        .body = "{\"doc_id\":\"   \"}",
    });
    defer invalid_version_neighbors.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), invalid_version_neighbors.status);

    var invalid_version_traverse = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/namespaces/docs/query/versions/1/graph/traverse",
        .body = "{\"start_doc_id\":\"\"}",
    });
    defer invalid_version_traverse.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), invalid_version_traverse.status);

    var invalid_version_shortest = try handler.handle(.{
        .method = .post,
        .path = "/internal/v1/namespaces/docs/query/versions/1/graph/shortest-path",
        .body = "{\"start_doc_id\":\"doc-a\",\"end_doc_id\":\"\"}",
    });
    defer invalid_version_shortest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), invalid_version_shortest.status);
}

test "http handler returns 404 and 400 for missing or invalid requests" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-errors");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-errors");
    const wal_root = tmpPath(&wal_root_buf, "wal-errors");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-errors");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    var missing = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/namespaces/missing/head",
    });
    defer missing.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), missing.status);

    var invalid = try handler.handle(.{
        .method = .put,
        .path = "/internal/v1/namespaces/docs/ingest-batch",
        .body = "{\"timestamp_ns\":1,\"mutations\":[{\"kind\":\"bogus\",\"doc_id\":\"x\"}]}",
    });
    defer invalid.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), invalid.status);

    var missing_manifest = try handler.handle(.{
        .method = .put,
        .path = "/internal/v1/namespaces/docs/head",
        .body = "{\"version\":99}",
    });
    defer missing_manifest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), missing_manifest.status);

    var missing_artifact = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/namespaces/missing/query/head/artifacts/0",
    });
    defer missing_artifact.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), missing_artifact.status);

    var missing_policy = try handler.handle(.{
        .method = .get,
        .path = "/internal/v1/namespaces/missing/policy",
    });
    defer missing_policy.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), missing_policy.status);
}

test "http handler rejects ingest when namespace is backpressured" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-backpressure");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-backpressure");
    const wal_root = tmpPath(&wal_root_buf, "wal-backpressure");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-backpressure");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try manifest_mod.FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = build_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var api = api_service.Service.init(alloc, &wal_store, &builder);
    var catalog = catalog_mod.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    var query = query_mod.QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var runtime_status = api_types.RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 1,
        .validated = true,
        .targets = try alloc.alloc(api_types.RuntimeStorageTarget, 0),
    };
    defer runtime_status.deinit(alloc);
    var handler = HttpHandler.init(alloc, &api, &catalog, &manifest_store, &progress_store, &query, &runtime_status);

    var create = try handler.handle(.{
        .method = .put,
        .path = "/internal/v1/namespaces/docs",
        .body = "{\"created_at_ns\":1,\"policy\":{\"max_pending_records\":0}}",
    });
    defer create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), create.status);

    var first_ingest = try handler.handle(.{
        .method = .put,
        .path = "/internal/v1/namespaces/docs/ingest-batch",
        .body = "{\"timestamp_ns\":1,\"mutations\":[{\"kind\":\"upsert\",\"doc_id\":\"doc-a\",\"body\":\"alpha\"}]}",
    });
    defer first_ingest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), first_ingest.status);

    var second_ingest = try handler.handle(.{
        .method = .put,
        .path = "/internal/v1/namespaces/docs/ingest-batch",
        .body = "{\"timestamp_ns\":2,\"mutations\":[{\"kind\":\"upsert\",\"doc_id\":\"doc-b\",\"body\":\"beta\"}]}",
    });
    defer second_ingest.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 429), second_ingest.status);
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-http-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
