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
const manifest_mod = @import("../manifest/mod.zig");
const api_types = @import("types.zig");
const catalog_types = @import("../catalog/types.zig");
const query_mod = @import("../query/mod.zig");
const search_sources = @import("../search_sources.zig");
const vector_types = @import("antfly_vector").vector;

pub const QueryView = enum {
    published,
    latest,
};

pub const QueryArtifactSummary = struct {
    index: usize,
    kind: manifest_mod.ArtifactKind,
    artifact_id: []const u8,
    byte_len: u64,
    checksum: []const u8,
    search_sources: search_sources.PublishedSearchSources = .{},
    materialized_derived_outputs: search_sources.MaterializedDerivedOutputs = .{},
};

pub const QueryMutation = struct {
    lsn: u64,
    timestamp_ns: u64,
    kind: api_types.MutationKind,
    doc_id: []const u8,
    body: ?[]const u8 = null,
};

pub const QueryDocument = struct {
    doc_id: []const u8,
    body: []const u8,
};

pub const QueryEnrichmentStatus = struct {
    enabled: bool,
    in_progress: bool,
    complete: bool,
    stage_source: ?catalog_types.EnrichmentStageSource = null,
    stage_state: ?catalog_types.EnrichmentStageState = null,
    pipeline_version: u32,
    head_version: ?u64,
    processed_document_count: u64,
    total_document_count: u64,
};

pub const QueryPublicationStatus = struct {
    publish_recommended: bool,
    next_publish_reason: ?catalog_types.NextPublishReason = null,
    mutation_tail_resolution: catalog_types.MutationTailResolution = .none,
    mutation_tail_compaction_recommended: bool = false,
    vector_compaction_driver_index_name: ?[]const u8 = null,
    vector_distance_metric: ?vector_types.DistanceMetric = null,
    vector_compaction_recommended: bool = false,
    vector_cluster_count: ?u32 = null,
    vector_base_probe_count: ?u32 = null,
    vector_shortlist_multiplier: ?u32 = null,
    vector_cluster_imbalance: ?f32 = null,
    vector_cluster_distance_span_max: ?f32 = null,
    vector_target_cluster_count: ?u32 = null,
    vector_target_base_probe_count: ?u32 = null,
    vector_target_shortlist_multiplier: ?u32 = null,
    head_document_publish_mode: ?catalog_types.DocumentPublishMode = null,
    next_document_publish_mode: ?catalog_types.DocumentPublishMode = null,
    document_base_version: u64 = 0,
    document_lineage_versions: u64 = 0,
    head_republish_recommended: bool,
    pending_materialization_rebuild: bool,
    pending_materialization_families: catalog_types.PendingMaterializationFamilies = .{},
    head_artifact_actions: catalog_types.ArtifactPublicationActions = .{},
    head_full_text_index_actions: []const catalog_types.FullTextIndexPublicationAction = &.{},
    head_vector_index_actions: []const catalog_types.NamedArtifactPublicationAction = &.{},
    head_sparse_index_actions: []const catalog_types.NamedArtifactPublicationAction = &.{},
    head_graph_index_actions: []const catalog_types.NamedArtifactPublicationAction = &.{},
    artifact_actions: catalog_types.ArtifactPublicationActions = .{},
    full_text_index_actions: []const catalog_types.FullTextIndexPublicationAction = &.{},
    vector_index_actions: []const catalog_types.NamedArtifactPublicationAction = &.{},
    sparse_index_actions: []const catalog_types.NamedArtifactPublicationAction = &.{},
    graph_index_actions: []const catalog_types.NamedArtifactPublicationAction = &.{},
    head_derived_output_actions: catalog_types.DerivedOutputPublicationActions = .{},
    derived_output_actions: catalog_types.DerivedOutputPublicationActions = .{},
    derived_output_resolutions: catalog_types.DerivedOutputResolutions = .{},
    pending_records: u64,
};

pub const QueryArtifactContents = struct {
    index: usize,
    kind: manifest_mod.ArtifactKind,
    artifact_id: []const u8,
    byte_len: u64,
    checksum: []const u8,
    search_sources: search_sources.PublishedSearchSources = .{},
    materialized_derived_outputs: search_sources.MaterializedDerivedOutputs = .{},
    mutations: []QueryMutation = &.{},
    documents: []QueryDocument = &.{},
};

pub const QueryTailMutation = QueryMutation;

pub const QueryResult = struct {
    namespace: []const u8,
    version: u64,
    view: QueryView,
    materialized_search_sources: search_sources.PublishedSearchSources = .{},
    materialized_derived_outputs: search_sources.MaterializedDerivedOutputs = .{},
    published_wal_end_lsn: u64,
    visible_wal_end_lsn: u64,
    latest_wal_lsn: u64,
    freshness_lag_records: u64,
    artifact_count: usize,
    artifacts: []QueryArtifactSummary,
    document_count: usize,
    documents: []QueryDocument,
    overlay_mutation_count: usize,
    publication: QueryPublicationStatus,
    enrichment: QueryEnrichmentStatus,
};

pub const TableQueryResult = struct {
    table_name: []const u8,
    version: u64,
    view: QueryView,
    materialized_search_sources: search_sources.PublishedSearchSources = .{},
    materialized_derived_outputs: search_sources.MaterializedDerivedOutputs = .{},
    published_wal_end_lsn: u64,
    visible_wal_end_lsn: u64,
    latest_wal_lsn: u64,
    freshness_lag_records: u64,
    artifact_count: usize,
    artifacts: []QueryArtifactSummary,
    document_count: usize,
    documents: []QueryDocument,
    overlay_mutation_count: usize,
    publication: QueryPublicationStatus,
    enrichment: QueryEnrichmentStatus,
};

pub const QueryArtifactResult = struct {
    namespace: []const u8,
    version: u64,
    artifact: QueryArtifactContents,
};

pub const QueryRequest = query_mod.QueryRequest;
pub const QueryMode = query_mod.QueryMode;
pub const GraphQueryDirection = query_mod.GraphQueryDirection;
pub const QueryOperator = query_mod.QueryOperator;
pub const QueryFusionStrategy = query_mod.QueryFusionStrategy;
pub const SparseTermWeight = query_mod.SparseTermWeight;
pub const GraphNeighborsRequest = query_mod.GraphNeighborsRequest;
pub const GraphTraverseRequest = query_mod.GraphTraverseRequest;
pub const GraphShortestPathRequest = query_mod.GraphShortestPathRequest;
pub const QueryHit = struct {
    doc_id: []const u8,
    body: []const u8,
    score: u32,
};

pub const GraphNeighbor = struct {
    doc_id: []const u8,
    edge_type: []const u8,
    weight: f32,
    direction: GraphQueryDirection,
};

pub const GraphTraversalNode = struct {
    doc_id: []const u8,
    depth: u32,
    parent_doc_id: ?[]const u8 = null,
    via_edge_type: ?[]const u8 = null,
    path: ?[]const []const u8 = null,
    edge_path: ?[]GraphPathHop = null,
};

pub const GraphPathHop = struct {
    from_doc_id: []const u8,
    to_doc_id: []const u8,
    edge_type: []const u8,
    weight: f32,
    direction: GraphQueryDirection,
};

pub const GraphNeighborsResult = struct {
    namespace: []const u8,
    version: u64,
    published_wal_end_lsn: u64,
    latest_wal_lsn: u64,
    freshness_lag_records: u64,
    node_id: []const u8,
    direction: GraphQueryDirection,
    edge_type: ?[]const u8 = null,
    limit: usize,
    neighbor_count: usize,
    neighbors: []GraphNeighbor,
};

pub const TableGraphNeighborsResult = struct {
    table_name: []const u8,
    version: u64,
    published_wal_end_lsn: u64,
    latest_wal_lsn: u64,
    freshness_lag_records: u64,
    node_id: []const u8,
    direction: GraphQueryDirection,
    edge_type: ?[]const u8 = null,
    limit: usize,
    neighbor_count: usize,
    neighbors: []GraphNeighbor,
};

pub const GraphTraverseResult = struct {
    namespace: []const u8,
    version: u64,
    published_wal_end_lsn: u64,
    latest_wal_lsn: u64,
    freshness_lag_records: u64,
    start_node_id: []const u8,
    direction: GraphQueryDirection,
    edge_type: ?[]const u8 = null,
    max_depth: u32,
    limit: usize,
    node_count: usize,
    nodes: []GraphTraversalNode,
};

pub const TableGraphTraverseResult = struct {
    table_name: []const u8,
    version: u64,
    published_wal_end_lsn: u64,
    latest_wal_lsn: u64,
    freshness_lag_records: u64,
    start_node_id: []const u8,
    direction: GraphQueryDirection,
    edge_type: ?[]const u8 = null,
    max_depth: u32,
    limit: usize,
    node_count: usize,
    nodes: []GraphTraversalNode,
};

pub const GraphShortestPathResult = struct {
    namespace: []const u8,
    version: u64,
    published_wal_end_lsn: u64,
    latest_wal_lsn: u64,
    freshness_lag_records: u64,
    start_node_id: []const u8,
    end_node_id: []const u8,
    direction: GraphQueryDirection,
    edge_type: ?[]const u8 = null,
    max_depth: u32,
    found: bool,
    depth: ?u32 = null,
    node_path: ?[]const []const u8 = null,
    edge_path: ?[]GraphPathHop = null,
};

pub const TableGraphShortestPathResult = struct {
    table_name: []const u8,
    version: u64,
    published_wal_end_lsn: u64,
    latest_wal_lsn: u64,
    freshness_lag_records: u64,
    start_node_id: []const u8,
    end_node_id: []const u8,
    direction: GraphQueryDirection,
    edge_type: ?[]const u8 = null,
    max_depth: u32,
    found: bool,
    depth: ?u32 = null,
    node_path: ?[]const []const u8 = null,
    edge_path: ?[]GraphPathHop = null,
};

pub const QuerySearchResult = struct {
    namespace: []const u8,
    version: u64,
    view: QueryView,
    published_wal_end_lsn: u64,
    latest_wal_lsn: u64,
    freshness_lag_records: u64,
    query_text: []const u8,
    mode: QueryMode,
    operator: QueryOperator,
    fusion_strategy: QueryFusionStrategy,
    vector_dims: usize,
    sparse_term_count: usize,
    num_probes: u32,
    actual_probe_count: usize,
    actual_shortlist_count: usize,
    quantized_candidate_count: usize,
    exact_rerank_count: usize,
    cluster_prune_count: usize,
    count_only: bool,
    offset: usize,
    limit: usize,
    min_score: u32,
    hit_count: usize,
    hits: []QueryHit,
    aggregations: ?std.json.Value = null,
    enrichment: QueryEnrichmentStatus,
};

pub const TableQuerySearchResult = struct {
    table_name: []const u8,
    version: u64,
    view: QueryView,
    published_wal_end_lsn: u64,
    latest_wal_lsn: u64,
    freshness_lag_records: u64,
    query_text: []const u8,
    mode: QueryMode,
    operator: QueryOperator,
    fusion_strategy: QueryFusionStrategy,
    vector_dims: usize,
    sparse_term_count: usize,
    num_probes: u32,
    actual_probe_count: usize,
    actual_shortlist_count: usize,
    quantized_candidate_count: usize,
    exact_rerank_count: usize,
    cluster_prune_count: usize,
    count_only: bool,
    offset: usize,
    limit: usize,
    min_score: u32,
    hit_count: usize,
    hits: []QueryHit,
    aggregations: ?std.json.Value = null,
    enrichment: QueryEnrichmentStatus,
};

test "query result types compile" {
    _ = QueryView;
    _ = QueryArtifactSummary;
    _ = QueryMutation;
    _ = QueryDocument;
    _ = QueryEnrichmentStatus;
    _ = QueryArtifactContents;
    _ = QueryTailMutation;
    _ = QueryResult;
    _ = TableQueryResult;
    _ = QueryArtifactResult;
    _ = QueryRequest;
    _ = QueryMode;
    _ = GraphQueryDirection;
    _ = QueryOperator;
    _ = QueryFusionStrategy;
    _ = SparseTermWeight;
    _ = GraphNeighborsRequest;
    _ = GraphTraverseRequest;
    _ = GraphShortestPathRequest;
    _ = QueryHit;
    _ = QuerySearchResult;
    _ = TableQuerySearchResult;
    _ = GraphNeighbor;
    _ = GraphNeighborsResult;
    _ = TableGraphNeighborsResult;
    _ = GraphPathHop;
    _ = GraphTraversalNode;
    _ = GraphTraverseResult;
    _ = TableGraphTraverseResult;
    _ = GraphShortestPathResult;
    _ = TableGraphShortestPathResult;
}
