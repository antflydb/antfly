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
const vector_types = @import("antfly_vector").vector;
const search_sources = @import("../search_sources.zig");
const full_text_indexes = @import("../../api/full_text_indexes.zig");

pub const FullTextSourceMode = full_text_indexes.FullTextSourceMode;

pub const DefaultQueryView = enum {
    published,
    latest,
};

pub const EnrichmentStage = enum(u8) {
    lexical_sparse = 1,
    chunk_preview = 2,
    chunk_embeddings = 3,
    rerank_terms = 4,
};

pub const EnrichmentStageSource = enum(u8) {
    current_head = 1,
    pending_wal = 2,
};

pub const EnrichmentStageState = enum(u8) {
    executing = 1,
    awaiting_execution = 2,
    deferred_for_publish_threshold = 3,
    ready_for_publish = 4,
};

pub const NextPublishReason = enum(u8) {
    head_republish = 1,
    wal_artifact_update = 2,
    wal_enrichment = 3,
};

pub const DocumentPublishMode = enum(u8) {
    append_mutation_tail = 1,
    inline_rebase = 2,
    head_republish = 3,
};

pub const MutationTailResolution = enum(u8) {
    none = 1,
    background_compaction = 2,
    next_publish_inline_rebase = 3,
};

pub const EnrichmentModelPreference = enum(u8) {
    deterministic_only = 1,
    prefer_model = 2,
    require_model = 3,
};

pub const EnrichmentFailurePolicy = enum(u8) {
    skip_document = 1,
    fail_stage = 2,
};

pub const ArtifactPublicationAction = enum {
    reuse,
    rebuild,
    drop,
};

pub const DerivedOutputPublicationAction = enum {
    reuse,
    recompute,
    drop,
};

pub const ArtifactPublicationActions = struct {
    document_segment: ArtifactPublicationAction = .rebuild,
    full_text: ArtifactPublicationAction = .rebuild,
    dense_vector: ArtifactPublicationAction = .rebuild,
    sparse_vector: ArtifactPublicationAction = .rebuild,
    graph: ArtifactPublicationAction = .rebuild,
};

pub const DerivedOutputPublicationActions = struct {
    chunk_preview: DerivedOutputPublicationAction = .reuse,
    chunk_embeddings: DerivedOutputPublicationAction = .reuse,
    rerank_terms: DerivedOutputPublicationAction = .reuse,
};

pub const DerivedOutputResolution = enum(u8) {
    disabled = 1,
    ready = 2,
    head_republish_reuse = 3,
    pending_materialization = 4,
    drop_on_republish = 5,
};

pub const DerivedOutputResolutions = struct {
    chunk_preview: DerivedOutputResolution = .disabled,
    chunk_embeddings: DerivedOutputResolution = .disabled,
    rerank_terms: DerivedOutputResolution = .disabled,
};

pub const PendingMaterializationFamilies = struct {
    full_text: bool = false,
    dense_vector: bool = false,
    sparse_vector: bool = false,
    chunk_preview: bool = false,
    chunk_embeddings: bool = false,
    rerank_terms: bool = false,

    pub fn any(self: PendingMaterializationFamilies) bool {
        return self.full_text or
            self.dense_vector or
            self.sparse_vector or
            self.chunk_preview or
            self.chunk_embeddings or
            self.rerank_terms;
    }
};

pub const FullTextIndexPublicationAction = struct {
    name: []u8,
    action: ArtifactPublicationAction,
    source_mode: FullTextSourceMode = .document,
    chunked_source_count: usize = 0,

    pub fn deinit(self: *FullTextIndexPublicationAction, alloc: Allocator) void {
        alloc.free(self.name);
        self.* = undefined;
    }
};

pub const NamedArtifactPublicationAction = struct {
    name: []u8,
    action: ArtifactPublicationAction,

    pub fn deinit(self: *NamedArtifactPublicationAction, alloc: Allocator) void {
        alloc.free(self.name);
        self.* = undefined;
    }
};

pub const NamespacePolicy = struct {
    default_query_view: DefaultQueryView = .published,
    keep_latest_versions: usize = 2,
    max_pending_records: u64 = 1024,
    compaction_enabled: bool = true,
    compaction_trigger_version_count: usize = 8,
    vector_compaction_max_cluster_imbalance: f32 = 0.5,
    vector_compaction_max_distance_span: f32 = 0.75,
    vector_distance_metric: vector_types.DistanceMetric = .cosine,
    enrichment_enabled: bool = false,
    lexical_sparse_model_preference: EnrichmentModelPreference = .prefer_model,
    enrichment_batch_size: usize = 32,
    enrichment_failure_policy: EnrichmentFailurePolicy = .skip_document,
    enrichment_publish_min_pending_records: u64 = 16,
    enrichment_pipeline_version: u32 = 1,
    chunk_preview_enabled: bool = false,
    chunk_preview_pipeline_version: u32 = 1,
    chunk_preview_publish_min_pending_records: u64 = 32,
    chunk_embeddings_enabled: bool = false,
    chunk_embeddings_model_preference: EnrichmentModelPreference = .prefer_model,
    chunk_embeddings_pipeline_version: u32 = 1,
    chunk_embeddings_publish_min_pending_records: u64 = 48,
    rerank_terms_enabled: bool = false,
    rerank_terms_pipeline_version: u32 = 1,
    rerank_terms_publish_min_pending_records: u64 = 64,
};

pub const NamespaceRecord = struct {
    name: []u8,
    created_at_ns: u64,
    policy: NamespacePolicy = .{},

    pub fn deinit(self: *NamespaceRecord, alloc: Allocator) void {
        alloc.free(self.name);
        self.* = undefined;
    }
};

pub const TableNamespaceRecord = struct {
    table_name: []u8,
    namespace: []u8,
    created_at_ns: u64,
    policy: NamespacePolicy = .{},
    schema_json: []u8,
    read_schema_json: []u8,
    indexes_json: []u8,

    pub fn deinit(self: *TableNamespaceRecord, alloc: Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.namespace);
        alloc.free(self.schema_json);
        alloc.free(self.read_schema_json);
        alloc.free(self.indexes_json);
        self.* = undefined;
    }
};

pub const BuildStatus = struct {
    namespace: []u8,
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
    mutation_tail_resolution: MutationTailResolution = .none,
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
    next_publish_reason: ?NextPublishReason = null,
    head_document_publish_mode: ?DocumentPublishMode = null,
    next_document_publish_mode: ?DocumentPublishMode = null,
    document_base_version: u64 = 0,
    document_lineage_versions: u64 = 0,
    head_republish_recommended: bool,
    pending_materialization_rebuild: bool,
    pending_materialization_families: PendingMaterializationFamilies = .{},
    head_artifact_actions: ArtifactPublicationActions = .{},
    head_full_text_index_actions: []FullTextIndexPublicationAction = &.{},
    head_vector_index_actions: []NamedArtifactPublicationAction = &.{},
    head_sparse_index_actions: []NamedArtifactPublicationAction = &.{},
    head_graph_index_actions: []NamedArtifactPublicationAction = &.{},
    head_derived_output_actions: DerivedOutputPublicationActions = .{},
    artifact_actions: ArtifactPublicationActions = .{},
    full_text_index_actions: []FullTextIndexPublicationAction = &.{},
    vector_index_actions: []NamedArtifactPublicationAction = &.{},
    sparse_index_actions: []NamedArtifactPublicationAction = &.{},
    graph_index_actions: []NamedArtifactPublicationAction = &.{},
    derived_output_actions: DerivedOutputPublicationActions = .{},
    derived_output_resolutions: DerivedOutputResolutions = .{},
    enrichment_enabled: bool,
    lexical_sparse_model_preference: EnrichmentModelPreference,
    lexical_sparse_complete: bool,
    chunk_preview_enabled: bool,
    chunk_preview_complete: bool,
    chunk_embeddings_enabled: bool,
    chunk_embeddings_model_preference: EnrichmentModelPreference,
    chunk_embeddings_complete: bool,
    rerank_terms_enabled: bool,
    rerank_terms_complete: bool,
    enrichment_failure_policy: EnrichmentFailurePolicy,
    enrichment_active_stage: ?EnrichmentStage,
    enrichment_stage_source: ?EnrichmentStageSource,
    enrichment_stage_state: ?EnrichmentStageState,
    enrichment_in_progress: bool,
    enrichment_complete: bool,
    enrichment_head_version: ?u64,
    enrichment_doc_offset: u64,
    enrichment_total_document_count: u64,
    enrichment_pending_document_count: u64,
    enrichment_batch_size: usize,
    enrichment_publish_min_pending_records: u64,
    enrichment_pipeline_version: u32,

    pub fn deinit(self: *BuildStatus, alloc: Allocator) void {
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
        alloc.free(self.namespace);
        self.* = undefined;
    }
};

// TablePublicationState is the preferred serverless serving-plane term. The
// current implementation still exposes BuildStatus as the concrete summary
// shape while the underlying state model is being cleaned up.
pub const TablePublicationState = BuildStatus;

test "build status next version follows head" {
    var status = BuildStatus{
        .namespace = undefined,
        .published_search_sources = .{},
        .materialized_search_sources = .{},
        .materialized_derived_outputs = .{},
        .head_version = 3,
        .published_wal_end_lsn = 7,
        .latest_wal_lsn = 9,
        .freshness_lag_records = 2,
        .pending_records = 2,
        .next_version = 4,
        .publish_admitted = true,
        .max_pending_records = 128,
        .retained_versions = 3,
        .retained_artifacts = 6,
        .compaction_recommended = false,
        .mutation_tail_resolution = .none,
        .vector_compaction_driver_index_name = null,
        .vector_compaction_distance_metric = null,
        .vector_cluster_count = 0,
        .vector_target_cluster_count = null,
        .vector_base_probe_count = 2,
        .vector_target_base_probe_count = null,
        .vector_shortlist_multiplier = 2,
        .vector_target_shortlist_multiplier = null,
        .vector_cluster_imbalance = 0,
        .vector_cluster_distance_span_max = 0,
        .publish_recommended = true,
        .next_publish_reason = .wal_artifact_update,
        .head_document_publish_mode = .append_mutation_tail,
        .next_document_publish_mode = .append_mutation_tail,
        .document_base_version = 3,
        .document_lineage_versions = 1,
        .head_republish_recommended = false,
        .pending_materialization_rebuild = false,
        .pending_materialization_families = .{},
        .head_artifact_actions = .{},
        .head_full_text_index_actions = &.{},
        .head_vector_index_actions = &.{},
        .head_sparse_index_actions = &.{},
        .head_graph_index_actions = &.{},
        .head_derived_output_actions = .{},
        .artifact_actions = .{},
        .vector_index_actions = &.{},
        .sparse_index_actions = &.{},
        .graph_index_actions = &.{},
        .derived_output_actions = .{},
        .derived_output_resolutions = .{},
        .enrichment_enabled = false,
        .lexical_sparse_model_preference = .prefer_model,
        .lexical_sparse_complete = true,
        .chunk_preview_enabled = false,
        .chunk_preview_complete = true,
        .chunk_embeddings_enabled = false,
        .chunk_embeddings_model_preference = .prefer_model,
        .chunk_embeddings_complete = true,
        .rerank_terms_enabled = false,
        .rerank_terms_complete = true,
        .enrichment_failure_policy = .skip_document,
        .enrichment_active_stage = null,
        .enrichment_stage_source = null,
        .enrichment_stage_state = null,
        .enrichment_in_progress = false,
        .enrichment_complete = true,
        .enrichment_head_version = null,
        .enrichment_doc_offset = 0,
        .enrichment_total_document_count = 0,
        .enrichment_pending_document_count = 0,
        .enrichment_batch_size = 32,
        .enrichment_publish_min_pending_records = 16,
        .enrichment_pipeline_version = 1,
    };
    try std.testing.expectEqual(@as(u64, 4), status.next_version);
    _ = &status;
}

test "namespace policy defaults to published queries and two retained versions" {
    const policy = NamespacePolicy{};
    try std.testing.expectEqual(DefaultQueryView.published, policy.default_query_view);
    try std.testing.expectEqual(@as(usize, 2), policy.keep_latest_versions);
    try std.testing.expectEqual(@as(u64, 1024), policy.max_pending_records);
    try std.testing.expectEqual(true, policy.compaction_enabled);
    try std.testing.expectEqual(@as(usize, 8), policy.compaction_trigger_version_count);
    try std.testing.expectEqual(@as(f32, 0.5), policy.vector_compaction_max_cluster_imbalance);
    try std.testing.expectEqual(@as(f32, 0.75), policy.vector_compaction_max_distance_span);
    try std.testing.expectEqual(vector_types.DistanceMetric.cosine, policy.vector_distance_metric);
    try std.testing.expectEqual(false, policy.enrichment_enabled);
    try std.testing.expectEqual(EnrichmentModelPreference.prefer_model, policy.lexical_sparse_model_preference);
    try std.testing.expectEqual(@as(usize, 32), policy.enrichment_batch_size);
    try std.testing.expectEqual(EnrichmentFailurePolicy.skip_document, policy.enrichment_failure_policy);
    try std.testing.expectEqual(@as(u64, 16), policy.enrichment_publish_min_pending_records);
    try std.testing.expectEqual(@as(u32, 1), policy.enrichment_pipeline_version);
    try std.testing.expectEqual(false, policy.chunk_preview_enabled);
    try std.testing.expectEqual(@as(u32, 1), policy.chunk_preview_pipeline_version);
    try std.testing.expectEqual(@as(u64, 32), policy.chunk_preview_publish_min_pending_records);
    try std.testing.expectEqual(false, policy.chunk_embeddings_enabled);
    try std.testing.expectEqual(EnrichmentModelPreference.prefer_model, policy.chunk_embeddings_model_preference);
    try std.testing.expectEqual(@as(u32, 1), policy.chunk_embeddings_pipeline_version);
    try std.testing.expectEqual(@as(u64, 48), policy.chunk_embeddings_publish_min_pending_records);
    try std.testing.expectEqual(false, policy.rerank_terms_enabled);
    try std.testing.expectEqual(@as(u32, 1), policy.rerank_terms_pipeline_version);
    try std.testing.expectEqual(@as(u64, 64), policy.rerank_terms_publish_min_pending_records);
}
