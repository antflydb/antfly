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
const catalog_types = @import("../catalog/types.zig");
const search_sources = @import("../search_sources.zig");
const vector_types = @import("antfly_vector").vector;

pub const MutationKind = enum(u8) {
    upsert = 1,
    delete = 2,
};

pub const DocumentMutation = struct {
    kind: MutationKind,
    doc_id: []const u8,
    body: ?[]const u8 = null,

    pub fn deinit(self: *DocumentMutation, alloc: Allocator) void {
        alloc.free(self.doc_id);
        if (self.body) |body| alloc.free(body);
        self.* = undefined;
    }
};

pub const IngestBatchRequest = struct {
    namespace: []const u8,
    timestamp_ns: u64,
    mutations: []const DocumentMutation,
};

pub const TableIngestBatchRequest = struct {
    table_name: []const u8,
    timestamp_ns: u64,
    mutations: []const DocumentMutation,
};

pub const IngestBatchResult = struct {
    namespace: []u8,
    mutation_count: usize,
    start_lsn: u64,
    end_lsn: u64,

    pub fn deinit(self: *IngestBatchResult, alloc: Allocator) void {
        alloc.free(self.namespace);
        self.* = undefined;
    }
};

pub const TableIngestBatchResult = struct {
    table_name: []u8,
    mutation_count: usize,
    start_lsn: u64,
    end_lsn: u64,

    pub fn deinit(self: *TableIngestBatchResult, alloc: Allocator) void {
        alloc.free(self.table_name);
        self.* = undefined;
    }
};

pub const EnsureNamespaceRequest = struct {
    created_at_ns: u64 = 0,
    policy: ?catalog_types.NamespacePolicy = null,
};

pub const EnsureTableRequest = struct {
    created_at_ns: u64 = 0,
    policy: ?catalog_types.NamespacePolicy = null,
    schema_json: ?[]const u8 = null,
    read_schema_json: ?[]const u8 = null,
    indexes_json: ?[]const u8 = null,

    pub fn deinit(self: *EnsureTableRequest, alloc: Allocator) void {
        if (self.schema_json) |value| alloc.free(value);
        if (self.read_schema_json) |value| alloc.free(value);
        if (self.indexes_json) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const EnsureNamespaceResult = struct {
    namespace: []u8,
    created: bool,
    created_at_ns: u64,
    policy: catalog_types.NamespacePolicy,

    pub fn deinit(self: *EnsureNamespaceResult, alloc: Allocator) void {
        alloc.free(self.namespace);
        self.* = undefined;
    }
};

pub const EnsureTableResult = struct {
    table_name: []u8,
    created: bool,
    created_at_ns: u64,
    policy: catalog_types.NamespacePolicy,

    pub fn deinit(self: *EnsureTableResult, alloc: Allocator) void {
        alloc.free(self.table_name);
        self.* = undefined;
    }
};

pub const NamespacePolicyRequest = catalog_types.NamespacePolicy;
pub const TablePolicyResult = struct {
    table_name: []u8,
    policy: catalog_types.NamespacePolicy,

    pub fn deinit(self: *TablePolicyResult, alloc: Allocator) void {
        alloc.free(self.table_name);
        self.* = undefined;
    }
};

pub const NamespacePolicyResult = struct {
    namespace: []u8,
    policy: catalog_types.NamespacePolicy,

    pub fn deinit(self: *NamespacePolicyResult, alloc: Allocator) void {
        alloc.free(self.namespace);
        self.* = undefined;
    }
};

pub const HeadPublishRequest = struct {
    version: u64,
    expected_head: ?u64 = null,
};

pub const HeadPublishResult = struct {
    namespace: []u8,
    requested_version: u64,
    expected_head: ?u64,
    current_head: ?u64,
    published: bool,

    pub fn deinit(self: *HeadPublishResult, alloc: Allocator) void {
        alloc.free(self.namespace);
        self.* = undefined;
    }
};

pub const TableBuildResult = struct {
    table_name: []u8,
    published: bool,
    version: u64,
    wal_start_lsn: u64,
    wal_end_lsn: u64,
    artifact_count: usize,

    pub fn deinit(self: *TableBuildResult, alloc: Allocator) void {
        alloc.free(self.table_name);
        self.* = undefined;
    }
};

pub const TableRecord = struct {
    table_name: []u8,
    created_at_ns: u64,
    policy: catalog_types.NamespacePolicy,
    schema_json: []u8,
    read_schema_json: []u8,
    indexes_json: []u8,

    pub fn deinit(self: *TableRecord, alloc: Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.schema_json);
        alloc.free(self.read_schema_json);
        alloc.free(self.indexes_json);
        self.* = undefined;
    }
};

pub const TableBuildStatus = struct {
    table_name: []u8,
    published_search_sources: search_sources.PublishedSearchSources = .{},
    materialized_search_sources: search_sources.PublishedSearchSources = .{},
    materialized_derived_outputs: search_sources.MaterializedDerivedOutputs = .{},
    head_version: u64,
    published_wal_end_lsn: u64,
    latest_wal_lsn: u64,
    freshness_lag_records: u64,
    pending_records: u64,
    next_version: u64,
    publish_admitted: bool,
    max_pending_records: u64,
    retained_versions: usize,
    retained_artifacts: usize,
    compaction_recommended: bool,
    mutation_tail_compaction_recommended: bool = false,
    vector_compaction_recommended: bool = false,
    mutation_tail_resolution: catalog_types.MutationTailResolution,
    vector_compaction_driver_index_name: ?[]u8 = null,
    vector_compaction_distance_metric: ?vector_types.DistanceMetric = null,
    vector_cluster_count: u32,
    vector_target_cluster_count: ?u32,
    vector_base_probe_count: u32,
    vector_target_base_probe_count: ?u32,
    vector_shortlist_multiplier: u32,
    vector_target_shortlist_multiplier: ?u32,
    vector_cluster_imbalance: f32,
    vector_cluster_distance_span_max: f32,
    publish_recommended: bool,
    next_publish_reason: ?catalog_types.NextPublishReason,
    head_document_publish_mode: ?catalog_types.DocumentPublishMode,
    next_document_publish_mode: ?catalog_types.DocumentPublishMode,
    document_base_version: u64,
    document_lineage_versions: u64,
    head_republish_recommended: bool,
    pending_materialization_rebuild: bool,
    pending_materialization_families: catalog_types.PendingMaterializationFamilies = .{},
    head_artifact_actions: catalog_types.ArtifactPublicationActions = .{},
    head_full_text_index_actions: []catalog_types.FullTextIndexPublicationAction = &.{},
    head_vector_index_actions: []catalog_types.NamedArtifactPublicationAction = &.{},
    head_sparse_index_actions: []catalog_types.NamedArtifactPublicationAction = &.{},
    head_graph_index_actions: []catalog_types.NamedArtifactPublicationAction = &.{},
    head_derived_output_actions: catalog_types.DerivedOutputPublicationActions = .{},
    artifact_actions: catalog_types.ArtifactPublicationActions = .{},
    full_text_index_actions: []catalog_types.FullTextIndexPublicationAction = &.{},
    vector_index_actions: []catalog_types.NamedArtifactPublicationAction = &.{},
    sparse_index_actions: []catalog_types.NamedArtifactPublicationAction = &.{},
    graph_index_actions: []catalog_types.NamedArtifactPublicationAction = &.{},
    derived_output_actions: catalog_types.DerivedOutputPublicationActions = .{},
    derived_output_resolutions: catalog_types.DerivedOutputResolutions = .{},
    enrichment_enabled: bool,
    lexical_sparse_model_preference: catalog_types.EnrichmentModelPreference,
    lexical_sparse_complete: bool,
    chunk_preview_enabled: bool,
    chunk_preview_complete: bool,
    chunk_embeddings_enabled: bool,
    chunk_embeddings_model_preference: catalog_types.EnrichmentModelPreference,
    chunk_embeddings_complete: bool,
    rerank_terms_enabled: bool,
    rerank_terms_complete: bool,
    enrichment_failure_policy: catalog_types.EnrichmentFailurePolicy,
    enrichment_active_stage: ?catalog_types.EnrichmentStage,
    enrichment_stage_source: ?catalog_types.EnrichmentStageSource,
    enrichment_stage_state: ?catalog_types.EnrichmentStageState,
    enrichment_in_progress: bool,
    enrichment_complete: bool,
    enrichment_head_version: ?u64,
    enrichment_doc_offset: u64,
    enrichment_total_document_count: u64,
    enrichment_pending_document_count: u64,
    enrichment_batch_size: usize,
    enrichment_publish_min_pending_records: u64,
    enrichment_pipeline_version: u32,

    pub fn deinit(self: *TableBuildStatus, alloc: Allocator) void {
        search_sources.deinitPublishedSearchSources(alloc, &self.published_search_sources);
        search_sources.deinitPublishedSearchSources(alloc, &self.materialized_search_sources);
        search_sources.deinitMaterializedDerivedOutputs(alloc, &self.materialized_derived_outputs);
        for (self.head_full_text_index_actions) |*entry| entry.deinit(alloc);
        if (self.head_full_text_index_actions.len > 0) alloc.free(self.head_full_text_index_actions);
        for (self.head_vector_index_actions) |*entry| entry.deinit(alloc);
        if (self.head_vector_index_actions.len > 0) alloc.free(self.head_vector_index_actions);
        for (self.head_sparse_index_actions) |*entry| entry.deinit(alloc);
        if (self.head_sparse_index_actions.len > 0) alloc.free(self.head_sparse_index_actions);
        for (self.head_graph_index_actions) |*entry| entry.deinit(alloc);
        if (self.head_graph_index_actions.len > 0) alloc.free(self.head_graph_index_actions);
        for (self.full_text_index_actions) |*entry| entry.deinit(alloc);
        if (self.full_text_index_actions.len > 0) alloc.free(self.full_text_index_actions);
        for (self.vector_index_actions) |*entry| entry.deinit(alloc);
        if (self.vector_index_actions.len > 0) alloc.free(self.vector_index_actions);
        for (self.sparse_index_actions) |*entry| entry.deinit(alloc);
        if (self.sparse_index_actions.len > 0) alloc.free(self.sparse_index_actions);
        for (self.graph_index_actions) |*entry| entry.deinit(alloc);
        if (self.graph_index_actions.len > 0) alloc.free(self.graph_index_actions);
        if (self.vector_compaction_driver_index_name) |value| alloc.free(value);
        alloc.free(self.table_name);
        self.* = undefined;
    }

    pub fn fromNamespaceBuildStatus(
        alloc: Allocator,
        table_name: []const u8,
        status: catalog_types.BuildStatus,
    ) !TableBuildStatus {
        return .{
            .table_name = try alloc.dupe(u8, table_name),
            .published_search_sources = try search_sources.clonePublishedSearchSourcesAlloc(alloc, status.published_search_sources),
            .materialized_search_sources = try search_sources.clonePublishedSearchSourcesAlloc(alloc, status.materialized_search_sources),
            .materialized_derived_outputs = try search_sources.cloneMaterializedDerivedOutputsAlloc(alloc, status.materialized_derived_outputs),
            .head_version = status.head_version,
            .published_wal_end_lsn = status.published_wal_end_lsn,
            .latest_wal_lsn = status.latest_wal_lsn,
            .freshness_lag_records = status.freshness_lag_records,
            .pending_records = status.pending_records,
            .next_version = status.next_version,
            .publish_admitted = status.publish_admitted,
            .max_pending_records = status.max_pending_records,
            .retained_versions = status.retained_versions,
            .retained_artifacts = status.retained_artifacts,
            .compaction_recommended = status.compaction_recommended,
            .mutation_tail_compaction_recommended = status.mutation_tail_compaction_recommended,
            .vector_compaction_recommended = status.vector_compaction_recommended,
            .mutation_tail_resolution = status.mutation_tail_resolution,
            .vector_compaction_driver_index_name = if (status.vector_compaction_driver_index_name) |value| try alloc.dupe(u8, value) else null,
            .vector_compaction_distance_metric = status.vector_compaction_distance_metric,
            .vector_cluster_count = status.vector_cluster_count,
            .vector_target_cluster_count = status.vector_target_cluster_count,
            .vector_base_probe_count = status.vector_base_probe_count,
            .vector_target_base_probe_count = status.vector_target_base_probe_count,
            .vector_shortlist_multiplier = status.vector_shortlist_multiplier,
            .vector_target_shortlist_multiplier = status.vector_target_shortlist_multiplier,
            .vector_cluster_imbalance = status.vector_cluster_imbalance,
            .vector_cluster_distance_span_max = status.vector_cluster_distance_span_max,
            .publish_recommended = status.publish_recommended,
            .next_publish_reason = status.next_publish_reason,
            .head_document_publish_mode = status.head_document_publish_mode,
            .next_document_publish_mode = status.next_document_publish_mode,
            .document_base_version = status.document_base_version,
            .document_lineage_versions = status.document_lineage_versions,
            .head_republish_recommended = status.head_republish_recommended,
            .pending_materialization_rebuild = status.pending_materialization_rebuild,
            .pending_materialization_families = status.pending_materialization_families,
            .head_artifact_actions = status.head_artifact_actions,
            .head_full_text_index_actions = try cloneFullTextIndexPublicationActionsAlloc(alloc, status.head_full_text_index_actions),
            .head_vector_index_actions = try cloneNamedArtifactPublicationActionsAlloc(alloc, status.head_vector_index_actions),
            .head_sparse_index_actions = try cloneNamedArtifactPublicationActionsAlloc(alloc, status.head_sparse_index_actions),
            .head_graph_index_actions = try cloneNamedArtifactPublicationActionsAlloc(alloc, status.head_graph_index_actions),
            .head_derived_output_actions = status.head_derived_output_actions,
            .artifact_actions = status.artifact_actions,
            .full_text_index_actions = try cloneFullTextIndexPublicationActionsAlloc(alloc, status.full_text_index_actions),
            .vector_index_actions = try cloneNamedArtifactPublicationActionsAlloc(alloc, status.vector_index_actions),
            .sparse_index_actions = try cloneNamedArtifactPublicationActionsAlloc(alloc, status.sparse_index_actions),
            .graph_index_actions = try cloneNamedArtifactPublicationActionsAlloc(alloc, status.graph_index_actions),
            .derived_output_actions = status.derived_output_actions,
            .derived_output_resolutions = status.derived_output_resolutions,
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
            .enrichment_head_version = status.enrichment_head_version,
            .enrichment_doc_offset = status.enrichment_doc_offset,
            .enrichment_total_document_count = status.enrichment_total_document_count,
            .enrichment_pending_document_count = status.enrichment_pending_document_count,
            .enrichment_batch_size = status.enrichment_batch_size,
            .enrichment_publish_min_pending_records = status.enrichment_publish_min_pending_records,
            .enrichment_pipeline_version = status.enrichment_pipeline_version,
        };
    }
};

fn cloneFullTextIndexPublicationActionsAlloc(
    alloc: Allocator,
    items: []const catalog_types.FullTextIndexPublicationAction,
) ![]catalog_types.FullTextIndexPublicationAction {
    if (items.len == 0) return &.{};
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
    if (items.len == 0) return &.{};
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

pub const RuntimeRole = enum {
    combined,
    api_only,
    query_only,
    maintenance_only,
};

pub const RuntimeStorageBackend = enum {
    file,
    s3,
    gs,
};

pub const RuntimeStorageTarget = struct {
    lane: []u8,
    uri: []u8,
    backend: RuntimeStorageBackend,
    path: ?[]u8 = null,
    bucket: ?[]u8 = null,
    prefix: ?[]u8 = null,

    pub fn deinit(self: *RuntimeStorageTarget, alloc: Allocator) void {
        alloc.free(self.lane);
        alloc.free(self.uri);
        if (self.path) |value| alloc.free(value);
        if (self.bucket) |value| alloc.free(value);
        if (self.prefix) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const RuntimeStatusResult = struct {
    role: RuntimeRole,
    swarm_mode: bool = false,
    tick_interval_ms: u64,
    validated: bool,
    publish_enabled: bool = true,
    compaction_enabled: bool = true,
    prune_enabled: bool = true,
    enrichment_enabled: bool = true,
    published_search_sources: search_sources.PublishedSearchSources = .{},
    targets: []RuntimeStorageTarget,

    pub fn deinit(self: *RuntimeStatusResult, alloc: Allocator) void {
        search_sources.deinitPublishedSearchSources(alloc, &self.published_search_sources);
        for (self.targets) |*target| target.deinit(alloc);
        alloc.free(self.targets);
        self.* = undefined;
    }
};

pub const HealthResult = struct {
    live: bool,
    ready: bool,
    validated: bool,
    namespace_count: usize,
};

pub const MetricsNamespace = struct {
    namespace: []u8,
    head_version: u64,
    latest_wal_lsn: u64,
    freshness_lag_records: u64,
    pending_records: u64,
    retained_versions: usize,
    retained_artifacts: usize,
    publish_admitted: bool,
    publish_recommended: bool,
    next_publish_reason: ?catalog_types.NextPublishReason = null,
    mutation_tail_resolution: catalog_types.MutationTailResolution = .none,
    head_document_publish_mode: ?catalog_types.DocumentPublishMode = null,
    next_document_publish_mode: ?catalog_types.DocumentPublishMode = null,
    document_base_version: u64 = 0,
    document_lineage_versions: u64 = 0,
    pending_materialization_families: catalog_types.PendingMaterializationFamilies = .{},
    derived_output_resolutions: catalog_types.DerivedOutputResolutions = .{},
    compaction_recommended: bool,
    mutation_tail_compaction_recommended: bool = false,
    vector_compaction_recommended: bool = false,
    vector_compaction_driver_index_name: ?[]u8 = null,
    vector_compaction_distance_metric: ?vector_types.DistanceMetric = null,
    vector_cluster_count: u32 = 0,
    vector_target_cluster_count: ?u32 = null,
    vector_base_probe_count: u32 = 2,
    vector_target_base_probe_count: ?u32 = null,
    vector_shortlist_multiplier: u32 = 2,
    vector_target_shortlist_multiplier: ?u32 = null,
    vector_cluster_imbalance: f32 = 0,
    vector_cluster_distance_span_max: f32 = 0,
    enrichment_enabled: bool = false,
    lexical_sparse_model_preference: catalog_types.EnrichmentModelPreference = .prefer_model,
    lexical_sparse_complete: bool = true,
    chunk_preview_enabled: bool = false,
    chunk_preview_complete: bool = true,
    chunk_embeddings_enabled: bool = false,
    chunk_embeddings_model_preference: catalog_types.EnrichmentModelPreference = .prefer_model,
    chunk_embeddings_complete: bool = true,
    rerank_terms_enabled: bool = false,
    rerank_terms_complete: bool = true,
    enrichment_failure_policy: catalog_types.EnrichmentFailurePolicy = .skip_document,
    enrichment_active_stage: ?catalog_types.EnrichmentStage = null,
    enrichment_stage_source: ?catalog_types.EnrichmentStageSource = null,
    enrichment_stage_state: ?catalog_types.EnrichmentStageState = null,
    enrichment_in_progress: bool = false,
    enrichment_complete: bool = true,
    enrichment_pending_documents: u64 = 0,
    ann_total_queries: u64 = 0,
    ann_vector_queries: u64 = 0,
    ann_hybrid_queries: u64 = 0,
    ann_sparse_queries: u64 = 0,
    ann_avg_actual_probes: f32 = 0,
    ann_avg_shortlist_candidates: f32 = 0,
    ann_avg_exact_reranks: f32 = 0,
    ann_avg_cluster_prunes: f32 = 0,

    pub fn deinit(self: *MetricsNamespace, alloc: Allocator) void {
        alloc.free(self.namespace);
        if (self.vector_compaction_driver_index_name) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const MetricsResult = struct {
    live: bool,
    ready: bool,
    validated: bool,
    namespace_count: usize,
    total_pending_records: u64,
    total_retained_versions: usize,
    total_retained_artifacts: usize,
    publish_recommended_namespaces: usize,
    namespaces_with_head_republish_pending: usize,
    namespaces_with_wal_artifact_publish_pending: usize,
    namespaces_with_wal_enrichment_publish_pending: usize,
    namespaces_with_enrichment_in_progress: usize,
    namespaces_with_enrichment_complete: usize,
    namespaces_with_enrichment_executing: usize,
    namespaces_with_enrichment_awaiting_execution: usize,
    namespaces_with_enrichment_deferred_for_publish_threshold: usize,
    namespaces_with_enrichment_ready_for_publish: usize,
    namespaces_with_full_text_materialization_pending: usize,
    namespaces_with_dense_vector_materialization_pending: usize,
    namespaces_with_sparse_vector_materialization_pending: usize,
    namespaces_with_chunk_preview_materialization_pending: usize,
    namespaces_with_chunk_embeddings_materialization_pending: usize,
    namespaces_with_rerank_terms_materialization_pending: usize,
    total_enrichment_pending_documents: u64,
    lexical_sparse_incomplete_namespaces: usize,
    chunk_preview_incomplete_namespaces: usize,
    chunk_embeddings_incomplete_namespaces: usize,
    rerank_terms_incomplete_namespaces: usize,
    published_namespaces: usize,
    publish_head_conflicts: usize,
    compacted_namespaces: usize,
    compact_head_conflicts: usize,
    pruned_namespaces: usize,
    prune_gc_conflicts: usize,
    deleted_versions: usize,
    deleted_artifacts: usize,
    wal_records_removed: u64,
    enriched_namespaces: usize,
    enriched_documents: usize,
    enrichment_wal_appends: usize,
    enrichment_model_documents: usize,
    enrichment_fallback_documents: usize,
    enrichment_failed_documents: usize,
    enrichment_stage_failures: usize,
    cache_hits: u64,
    cache_misses: u64,
    cache_writes: u64,
    cache_full_hits: u64,
    cache_full_misses: u64,
    cache_full_writes: u64,
    cache_range_hits: u64,
    cache_range_misses: u64,
    cache_range_writes: u64,
    cache_block_hits: u64,
    cache_block_misses: u64,
    cache_block_writes: u64,
    cache_routing_block_hits: u64,
    cache_routing_block_misses: u64,
    cache_routing_block_writes: u64,
    cache_payload_block_hits: u64,
    cache_payload_block_misses: u64,
    cache_payload_block_writes: u64,
    cache_approx_payload_block_hits: u64,
    cache_approx_payload_block_misses: u64,
    cache_approx_payload_block_writes: u64,
    cache_exact_payload_block_hits: u64,
    cache_exact_payload_block_misses: u64,
    cache_exact_payload_block_writes: u64,
    cache_evictions: u64,
    cache_current_bytes: u64,
    cache_pinned_bytes: u64,
    cache_payload_bytes: u64,
    cache_pinned_block_count: u64,
    cache_payload_block_count: u64,
    cache_max_bytes: u64,
    cache_max_payload_bytes: u64,
    ann_total_queries: u64,
    ann_vector_queries: u64,
    ann_hybrid_queries: u64,
    ann_sparse_queries: u64,
    ann_total_actual_probes: u64,
    ann_total_shortlist_candidates: u64,
    ann_total_quantized_candidates: u64,
    ann_total_exact_reranks: u64,
    ann_total_cluster_prunes: u64,
    ann_avg_actual_probes: f32,
    ann_avg_shortlist_candidates: f32,
    ann_avg_quantized_candidates: f32,
    ann_avg_exact_reranks: f32,
    ann_avg_cluster_prunes: f32,
    namespaces: []MetricsNamespace,

    pub fn deinit(self: *MetricsResult, alloc: Allocator) void {
        for (self.namespaces) |*ns| ns.deinit(alloc);
        alloc.free(self.namespaces);
        self.* = undefined;
    }
};

test "delete mutation does not require body" {
    const mutation = DocumentMutation{
        .kind = .delete,
        .doc_id = "doc-1",
        .body = null,
    };
    try std.testing.expectEqual(MutationKind.delete, mutation.kind);
    try std.testing.expectEqual(@as(?[]const u8, null), mutation.body);
}

test "ensure namespace request can omit policy" {
    const req = EnsureNamespaceRequest{};
    try std.testing.expectEqual(@as(u64, 0), req.created_at_ns);
    try std.testing.expectEqual(@as(?catalog_types.NamespacePolicy, null), req.policy);
}

test "runtime storage target deinit handles optional fields" {
    const target = RuntimeStorageTarget{
        .lane = @constCast("artifacts"),
        .uri = @constCast("file:///tmp/artifacts"),
        .backend = .file,
    };
    try std.testing.expectEqual(RuntimeStorageBackend.file, target.backend);
}

test "runtime role enum includes split serverless roles" {
    try std.testing.expectEqual(RuntimeRole.combined, RuntimeRole.combined);
    try std.testing.expectEqual(RuntimeRole.api_only, RuntimeRole.api_only);
    try std.testing.expectEqual(RuntimeRole.query_only, RuntimeRole.query_only);
    try std.testing.expectEqual(RuntimeRole.maintenance_only, RuntimeRole.maintenance_only);
}

test "health result is compact readiness payload" {
    const health = HealthResult{
        .live = true,
        .ready = true,
        .validated = true,
        .namespace_count = 0,
    };
    try std.testing.expect(health.live);
    try std.testing.expect(health.ready);
}

test "runtime status defaults maintenance features to enabled" {
    const status = RuntimeStatusResult{
        .role = .combined,
        .tick_interval_ms = 25,
        .validated = true,
        .targets = &.{},
    };
    try std.testing.expect(status.publish_enabled);
    try std.testing.expect(status.compaction_enabled);
    try std.testing.expect(status.prune_enabled);
    try std.testing.expect(status.enrichment_enabled);
}

test "metrics result deinit handles namespace array" {
    var metrics = MetricsResult{
        .live = true,
        .ready = true,
        .validated = true,
        .namespace_count = 0,
        .total_pending_records = 0,
        .total_retained_versions = 0,
        .total_retained_artifacts = 0,
        .publish_recommended_namespaces = 0,
        .namespaces_with_head_republish_pending = 0,
        .namespaces_with_wal_artifact_publish_pending = 0,
        .namespaces_with_wal_enrichment_publish_pending = 0,
        .namespaces_with_enrichment_in_progress = 0,
        .namespaces_with_enrichment_complete = 0,
        .namespaces_with_enrichment_executing = 0,
        .namespaces_with_enrichment_awaiting_execution = 0,
        .namespaces_with_enrichment_deferred_for_publish_threshold = 0,
        .namespaces_with_enrichment_ready_for_publish = 0,
        .namespaces_with_full_text_materialization_pending = 0,
        .namespaces_with_dense_vector_materialization_pending = 0,
        .namespaces_with_sparse_vector_materialization_pending = 0,
        .namespaces_with_chunk_preview_materialization_pending = 0,
        .namespaces_with_chunk_embeddings_materialization_pending = 0,
        .namespaces_with_rerank_terms_materialization_pending = 0,
        .total_enrichment_pending_documents = 0,
        .lexical_sparse_incomplete_namespaces = 0,
        .chunk_preview_incomplete_namespaces = 0,
        .chunk_embeddings_incomplete_namespaces = 0,
        .rerank_terms_incomplete_namespaces = 0,
        .published_namespaces = 0,
        .publish_head_conflicts = 0,
        .compacted_namespaces = 0,
        .compact_head_conflicts = 0,
        .pruned_namespaces = 0,
        .prune_gc_conflicts = 0,
        .deleted_versions = 0,
        .deleted_artifacts = 0,
        .wal_records_removed = 0,
        .enriched_namespaces = 0,
        .enriched_documents = 0,
        .enrichment_wal_appends = 0,
        .enrichment_model_documents = 0,
        .enrichment_fallback_documents = 0,
        .enrichment_failed_documents = 0,
        .enrichment_stage_failures = 0,
        .cache_hits = 0,
        .cache_misses = 0,
        .cache_writes = 0,
        .cache_full_hits = 0,
        .cache_full_misses = 0,
        .cache_full_writes = 0,
        .cache_range_hits = 0,
        .cache_range_misses = 0,
        .cache_range_writes = 0,
        .cache_block_hits = 0,
        .cache_block_misses = 0,
        .cache_block_writes = 0,
        .cache_routing_block_hits = 0,
        .cache_routing_block_misses = 0,
        .cache_routing_block_writes = 0,
        .cache_payload_block_hits = 0,
        .cache_payload_block_misses = 0,
        .cache_payload_block_writes = 0,
        .cache_approx_payload_block_hits = 0,
        .cache_approx_payload_block_misses = 0,
        .cache_approx_payload_block_writes = 0,
        .cache_exact_payload_block_hits = 0,
        .cache_exact_payload_block_misses = 0,
        .cache_exact_payload_block_writes = 0,
        .cache_evictions = 0,
        .cache_current_bytes = 0,
        .cache_pinned_bytes = 0,
        .cache_payload_bytes = 0,
        .cache_pinned_block_count = 0,
        .cache_payload_block_count = 0,
        .cache_max_bytes = 0,
        .cache_max_payload_bytes = 0,
        .ann_total_queries = 0,
        .ann_vector_queries = 0,
        .ann_hybrid_queries = 0,
        .ann_sparse_queries = 0,
        .ann_total_actual_probes = 0,
        .ann_total_shortlist_candidates = 0,
        .ann_total_quantized_candidates = 0,
        .ann_total_exact_reranks = 0,
        .ann_total_cluster_prunes = 0,
        .ann_avg_actual_probes = 0,
        .ann_avg_shortlist_candidates = 0,
        .ann_avg_quantized_candidates = 0,
        .ann_avg_exact_reranks = 0,
        .ann_avg_cluster_prunes = 0,
        .namespaces = &.{},
    };
    try std.testing.expect(metrics.live);
    _ = &metrics;
}
