// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const antfly = @import("antfly-zig");
const builtin = @import("builtin");

const api = antfly.public_api;
const common = antfly.common;
const db_mod = antfly.db;
const metadata_api = antfly.metadata_api;
const metadata_table_manager = antfly.metadata.table_manager;
const schema_mod = antfly.schema;
const table_schema_api = antfly.table_schema;
const platform_time = antfly.platform_time;
const raft_mod = antfly.raft;
const http_common = antfly.common.http.http_common;
const std_http_executor = antfly.common.http.std_http_executor;
const std_http_listener = antfly.common.http.std_http_listener;

const table_name = "docs";
const index_name = "dense_idx";
const sparse_index_name = "sparse_idx";
const text_index_name = "full_text_index_v0";
const algebraic_index_name = "algebraic_idx";
const graph_index_name = "graph_idx";
const native_endian = builtin.target.cpu.arch.endian();

const benchmark_schema_json =
    \\{"version":1,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true,"properties":{"title":{"type":"text"},"body":{"type":"text"},"category":{"type":"keyword"},"status":{"type":"keyword"},"tenant":{"type":"keyword"},"score":{"type":"number"}}}}}}
;

const benchmark_algebraic_config_json =
    \\{"version":1,"table":"docs","group_fields":[{"name":"category","path":"category","type":"string"},{"name":"status","path":"status","type":"string"},{"name":"tenant","path":"tenant","type":"string"}],"measure_fields":[{"name":"score","path":"score","type":"number"}],"adaptive":{"observe":true,"lazy_materialization":true,"min_observations":2,"max_backfill_rows_per_tick":100000,"min_estimated_scan_rows_saved":1},"materializations":[]}
;

const Mode = enum {
    handler,
    local,
    swarm,
};

const ServerKind = enum {
    zig,
    go,
};

const QueryShape = enum {
    dense,
    full_text,
    dense_filter,
    sparse_filter,
    graph_expand,
    algebraic_filter,
    hybrid_composed,
    hybrid,
    hybrid_filter,
    hybrid_filter_exclude,
    hybrid_filter_exclude_project,

    fn parse(raw: []const u8) ?QueryShape {
        if (std.mem.eql(u8, raw, "dense")) return .dense;
        if (std.mem.eql(u8, raw, "full-text")) return .full_text;
        if (std.mem.eql(u8, raw, "dense-filter")) return .dense_filter;
        if (std.mem.eql(u8, raw, "sparse-filter")) return .sparse_filter;
        if (std.mem.eql(u8, raw, "graph-expand")) return .graph_expand;
        if (std.mem.eql(u8, raw, "algebraic-filter")) return .algebraic_filter;
        if (std.mem.eql(u8, raw, "hybrid-composed")) return .hybrid_composed;
        if (std.mem.eql(u8, raw, "hybrid")) return .hybrid;
        if (std.mem.eql(u8, raw, "hybrid-filter")) return .hybrid_filter;
        if (std.mem.eql(u8, raw, "hybrid-filter-exclude")) return .hybrid_filter_exclude;
        if (std.mem.eql(u8, raw, "hybrid-filter-exclude-project")) return .hybrid_filter_exclude_project;
        return null;
    }

    fn text(self: QueryShape) []const u8 {
        return switch (self) {
            .dense => "dense",
            .full_text => "full-text",
            .dense_filter => "dense-filter",
            .sparse_filter => "sparse-filter",
            .graph_expand => "graph-expand",
            .algebraic_filter => "algebraic-filter",
            .hybrid_composed => "hybrid-composed",
            .hybrid => "hybrid",
            .hybrid_filter => "hybrid-filter",
            .hybrid_filter_exclude => "hybrid-filter-exclude",
            .hybrid_filter_exclude_project => "hybrid-filter-exclude-project",
        };
    }

    fn usesFullText(self: QueryShape) bool {
        return switch (self) {
            .dense, .dense_filter, .sparse_filter, .graph_expand, .algebraic_filter => false,
            .full_text, .hybrid_composed, .hybrid, .hybrid_filter, .hybrid_filter_exclude, .hybrid_filter_exclude_project => true,
        };
    }

    fn usesDense(self: QueryShape) bool {
        return switch (self) {
            .full_text, .sparse_filter, .graph_expand => false,
            .dense, .dense_filter, .algebraic_filter, .hybrid_composed, .hybrid, .hybrid_filter, .hybrid_filter_exclude, .hybrid_filter_exclude_project => true,
        };
    }

    fn usesSparse(self: QueryShape) bool {
        return self == .sparse_filter or self == .hybrid_composed;
    }

    fn usesGraph(self: QueryShape) bool {
        return self == .graph_expand;
    }

    fn usesFilter(self: QueryShape) bool {
        return switch (self) {
            .dense, .full_text, .graph_expand, .hybrid => false,
            .dense_filter, .sparse_filter, .algebraic_filter, .hybrid_composed => true,
            .hybrid_filter, .hybrid_filter_exclude, .hybrid_filter_exclude_project => true,
        };
    }

    fn usesExclusion(self: QueryShape) bool {
        return switch (self) {
            .dense, .full_text, .dense_filter, .sparse_filter, .graph_expand, .algebraic_filter, .hybrid_composed, .hybrid, .hybrid_filter => false,
            .hybrid_filter_exclude, .hybrid_filter_exclude_project => true,
        };
    }

    fn needsDefaultFullTextIndex(self: QueryShape) bool {
        return self.usesFullText() or self.usesFilter() or self.usesExclusion();
    }

    fn projectsFields(self: QueryShape) bool {
        return self == .hybrid_filter_exclude_project;
    }
};

const Config = struct {
    mode: Mode = .local,
    server_kind: ServerKind = .zig,
    query_shape: QueryShape = .dense,
    with_schema: bool = false,
    with_algebraic: bool = false,
    with_sparse: bool = false,
    with_graph: bool = false,
    docs: usize = 5000,
    dims: usize = 384,
    queries: usize = 25,
    repeats: usize = 10,
    k: usize = 100,
    batch_size: usize = 250,
    search_threads: usize = 5,
    search_thread_sweep: bool = false,
    seed: u64 = 42,
    sync_level: db_mod.types.SyncLevel = .write,
    poll_interval_ms: u64 = 50,
    require_symbolic_profile: bool = false,
    max_health_latency_ms: u64 = 250,
    max_metrics_latency_ms: u64 = 500,
    max_status_latency_ms: u64 = 500,
    max_health_failures: u64 = 0,
    max_metrics_failures: u64 = 0,
    max_status_failures: u64 = 0,
    startup_timeout_ms: u64 = 30_000,
    index_ready_timeout_ms: u64 = 120_000,
    load_progress_interval: usize = 25_000,
    swarm_binary: []const u8 = "./zig-out/bin/antfly",
    bind_host: []const u8 = "127.0.0.1",
};

const InputDoc = struct {
    key: []const u8,
    value: []const u8,
};

const PollStats = struct {
    health_samples: u64 = 0,
    metrics_samples: u64 = 0,
    status_samples: u64 = 0,
    health_failures: u64 = 0,
    metrics_failures: u64 = 0,
    status_failures: u64 = 0,
    health_max_latency_ns: u64 = 0,
    metrics_max_latency_ns: u64 = 0,
    status_max_latency_ns: u64 = 0,
};

const PollKind = enum {
    health,
    metrics,
    status,
};
const slow_poll_log_threshold_ns: u64 = 500 * std.time.ns_per_ms;

fn isRetryableQueryConnectionError(err: anyerror) bool {
    return switch (err) {
        error.HttpConnectionClosing,
        error.EndOfStream,
        error.ConnectionResetByPeer,
        error.BrokenPipe,
        => true,
        else => false,
    };
}

const HbcCacheKindWire = struct {
    used_bytes: ?u64 = null,
    peak_bytes: ?u64 = null,
    insertions: ?u64 = null,
    admission_skips: ?u64 = null,
    evictions: ?u64 = null,
};

const HbcCacheWire = struct {
    total_bytes: ?u64 = null,
    accounted_bytes: ?u64 = null,
    node: ?HbcCacheKindWire = null,
    quantized: ?HbcCacheKindWire = null,
    vector: ?HbcCacheKindWire = null,
    metadata: ?HbcCacheKindWire = null,
};

const IndexStatusWire = struct {
    status: ?Status = null,

    const Status = struct {
        doc_count: ?u64 = null,
        total_indexed: ?u64 = null,
        node_count: ?u64 = null,
        total_nodes: ?u64 = null,
        root_node: ?u64 = null,
        query_visible_doc_count: ?u64 = null,
        published_doc_count: ?u64 = null,
        published_node_count: ?u64 = null,
        published_root_node: ?u64 = null,
        dense_publish_pending: ?bool = null,
        replay_target_sequence: ?u64 = null,
        replay_applied_sequence: ?u64 = null,
        replay_catch_up_required: ?bool = null,
        runtime_fresh: ?bool = null,
        backfill_progress: ?f64 = null,
        rebuilding: ?bool = null,
        backfill_active: ?bool = null,
        hbc_cache: ?HbcCacheWire = null,
    };
};

const VisibilitySnapshot = struct {
    doc_count: u64 = 0,
    total_indexed: u64 = 0,
    node_count: u64 = 0,
    root_node: u64 = 0,
    query_visible_doc_count: u64 = 0,
    published_doc_count: u64 = 0,
    published_node_count: u64 = 0,
    published_root_node: u64 = 0,
    dense_publish_pending: bool = false,
    replay_target_sequence: u64 = 0,
    replay_applied_sequence: u64 = 0,
    replay_catch_up_required: bool = false,
    runtime_fresh: bool = false,
    rebuilding: bool = false,
    backfill_active: bool = false,
    hbc_total_bytes: u64 = 0,
    hbc_accounted_bytes: u64 = 0,
    hbc_node_bytes: u64 = 0,
    hbc_quantized_bytes: u64 = 0,
    hbc_vector_bytes: u64 = 0,
    hbc_metadata_bytes: u64 = 0,

    fn publishedReady(self: VisibilitySnapshot, expected_docs: usize) bool {
        const visible_docs = @max(self.query_visible_doc_count, self.published_doc_count);
        return visible_docs >= expected_docs and
            self.total_indexed >= expected_docs and
            self.published_node_count > 0 and
            self.published_root_node > 0 and
            !self.dense_publish_pending;
    }

    fn textReady(self: VisibilitySnapshot, expected_docs: usize) bool {
        const visible_docs = @max(self.query_visible_doc_count, self.published_doc_count);
        return visible_docs >= expected_docs and
            self.total_indexed >= expected_docs and
            !self.rebuilding and
            !self.backfill_active;
    }

    fn replayCaughtUp(self: VisibilitySnapshot) bool {
        return !self.replay_catch_up_required and
            self.replay_applied_sequence >= self.replay_target_sequence;
    }
};

const MemoryBreakdown = struct {
    process_resident_bytes: u64 = 0,
    process_footprint_bytes: u64 = 0,
    process_wired_bytes: u64 = 0,
    lsm_cache_used_bytes: u64 = 0,
    rm_lsm_cache_used_bytes: u64 = 0,
    rm_lsm_cache_peak_bytes: u64 = 0,
    rm_lsm_compaction_used_bytes: u64 = 0,
    rm_lsm_compaction_peak_bytes: u64 = 0,
    rm_lsm_in_memory_used_bytes: u64 = 0,
    rm_lsm_in_memory_peak_bytes: u64 = 0,
    rm_lsm_wal_working_used_bytes: u64 = 0,
    rm_lsm_wal_working_peak_bytes: u64 = 0,
    rm_hbc_cache_used_bytes: u64 = 0,
    rm_hbc_cache_peak_bytes: u64 = 0,
    rm_dense_search_used_bytes: u64 = 0,
    rm_dense_search_peak_bytes: u64 = 0,
    rm_dense_apply_used_bytes: u64 = 0,
    rm_dense_apply_peak_bytes: u64 = 0,
    rm_dense_routing_used_bytes: u64 = 0,
    rm_dense_routing_peak_bytes: u64 = 0,
    rm_replay_window_used_bytes: u64 = 0,
    rm_replay_window_peak_bytes: u64 = 0,
    rm_full_text_pending_used_bytes: u64 = 0,
    rm_full_text_pending_peak_bytes: u64 = 0,
    hbc_total_bytes: u64 = 0,
    hbc_accounted_bytes: u64 = 0,
    hbc_node_used_bytes: u64 = 0,
    hbc_node_peak_bytes: u64 = 0,
    hbc_quantized_used_bytes: u64 = 0,
    hbc_quantized_peak_bytes: u64 = 0,
    hbc_vector_used_bytes: u64 = 0,
    hbc_vector_peak_bytes: u64 = 0,
    hbc_metadata_used_bytes: u64 = 0,
    hbc_metadata_peak_bytes: u64 = 0,
};

const QueryBenchStats = struct {
    total_ns: u64 = 0,
    first_pass_ns: u64 = 0,
    first_pass_queries: u64 = 0,
    later_pass_ns: u64 = 0,
    later_pass_queries: u64 = 0,
    response_hit_count: u64 = 0,
    profile_response_count: u64 = 0,
    profile_dense_search_count: u64 = 0,
    profile_total_ns: u64 = 0,
    profile_hbc_search_ns: u64 = 0,
    profile_hbc_runtime_txn_ns: u64 = 0,
    profile_hbc_scratch_acquire_ns: u64 = 0,
    profile_hbc_node_cache_lookup_ns: u64 = 0,
    profile_hbc_quantized_cache_lookup_ns: u64 = 0,
    profile_hbc_reranked_vectors: u64 = 0,
    profile_hbc_approx_candidate_count: u64 = 0,
    profile_hbc_rerank_candidate_count: u64 = 0,
    profile_hbc_ambiguous_top_k_pairs: u64 = 0,
    profile_hbc_ambiguous_boundary_pairs: u64 = 0,
    profile_hbc_ambiguous_distance_over_hits: u64 = 0,
    profile_hbc_ambiguous_distance_under_hits: u64 = 0,
    profile_hbc_full_rerank_due_to_threshold: u64 = 0,
    profile_hbc_top_k_count: u64 = 0,
    profile_returned_hit_count: u64 = 0,
    profile_hbc_boundary_pair_count: u64 = 0,
    profile_hbc_boundary_left_distance_sum: f64 = 0,
    profile_hbc_boundary_left_error_sum: f64 = 0,
    profile_hbc_boundary_left_lower_sum: f64 = 0,
    profile_hbc_boundary_left_upper_sum: f64 = 0,
    profile_hbc_boundary_right_distance_sum: f64 = 0,
    profile_hbc_boundary_right_error_sum: f64 = 0,
    profile_hbc_boundary_right_lower_sum: f64 = 0,
    profile_hbc_boundary_right_upper_sum: f64 = 0,
    profile_hbc_boundary_distance_gap_sum: f64 = 0,
    profile_hbc_boundary_interval_gap_sum: f64 = 0,
    profile_hbc_boundary_left_error_max: f64 = 0,
    profile_hbc_boundary_right_error_max: f64 = 0,
    profile_hbc_boundary_tail_error_avg_sum: f64 = 0,
    profile_hbc_boundary_tail_error_max: f64 = 0,
    profile_hbc_boundary_tail_distance_gap_avg_sum: f64 = 0,
    profile_hbc_boundary_tail_distance_gap_min: f64 = std.math.floatMax(f64),
    profile_hbc_boundary_tail_distance_gap_max: f64 = -std.math.floatMax(f64),
    profile_hbc_boundary_tail_interval_gap_avg_sum: f64 = 0,
    profile_hbc_boundary_tail_interval_gap_min: f64 = std.math.floatMax(f64),
    profile_hbc_boundary_tail_interval_gap_max: f64 = -std.math.floatMax(f64),
    profile_index_lookup_ns: u64 = 0,
    profile_doc_key_ns: u64 = 0,
    profile_project_ns: u64 = 0,
    profile_postprocess_ns: u64 = 0,
    profile_rerank_external_score_ns: u64 = 0,
    profile_rerank_vector_load_ns: u64 = 0,
    profile_rerank_metadata_lookup_ns: u64 = 0,
    profile_rerank_artifact_key_ns: u64 = 0,
    profile_rerank_artifact_read_ns: u64 = 0,
    profile_rerank_artifact_decode_ns: u64 = 0,
    profile_rerank_artifact_distance_ns: u64 = 0,
    profile_rerank_lsm_cache_hits: u64 = 0,
    profile_rerank_lsm_cache_misses: u64 = 0,
    profile_rerank_distance_ns: u64 = 0,
    profile_inline_metadata_hits: u64 = 0,
    profile_fetched_metadata_hits: u64 = 0,
    profile_lookup_doc_key_hits: u64 = 0,
    queries: u64 = 0,
    failures: u64 = 0,

    fn avgNs(self: QueryBenchStats) u64 {
        if (self.queries == 0) return 0;
        return self.total_ns / self.queries;
    }

    fn avgFirstPassNs(self: QueryBenchStats) u64 {
        if (self.first_pass_queries == 0) return 0;
        return self.first_pass_ns / self.first_pass_queries;
    }

    fn avgLaterPassNs(self: QueryBenchStats) u64 {
        if (self.later_pass_queries == 0) return 0;
        return self.later_pass_ns / self.later_pass_queries;
    }

    fn avgProfileNs(self: QueryBenchStats) u64 {
        if (self.queries == 0) return 0;
        return self.profile_total_ns / self.queries;
    }

    fn profileResponseRate(self: QueryBenchStats) f64 {
        if (self.queries == 0) return 0;
        return @as(f64, @floatFromInt(self.profile_response_count)) / @as(f64, @floatFromInt(self.queries));
    }

    fn denseProfileRate(self: QueryBenchStats) f64 {
        if (self.queries == 0) return 0;
        return @as(f64, @floatFromInt(self.profile_dense_search_count)) / @as(f64, @floatFromInt(self.queries));
    }

    fn avgBoundary(self: QueryBenchStats, value: f64) f64 {
        if (self.profile_hbc_boundary_pair_count == 0) return 0;
        return value / @as(f64, @floatFromInt(self.profile_hbc_boundary_pair_count));
    }
};

const HandlerPipelineStats = struct {
    parse_ns: u64 = 0,
    route_ns: u64 = 0,
    source_query_ns: u64 = 0,
    source_profile_total_ns: u64 = 0,
    source_profile_hbc_ns: u64 = 0,
    queries: u64 = 0,

    fn avgParseNs(self: HandlerPipelineStats) u64 {
        if (self.queries == 0) return 0;
        return self.parse_ns / self.queries;
    }

    fn avgRouteNs(self: HandlerPipelineStats) u64 {
        if (self.queries == 0) return 0;
        return self.route_ns / self.queries;
    }

    fn avgSourceQueryNs(self: HandlerPipelineStats) u64 {
        if (self.queries == 0) return 0;
        return self.source_query_ns / self.queries;
    }
};

const ConcurrentStats = struct {
    total_ns: u64 = 0,
    request_ns: u64 = 0,
    max_request_ns: u64 = 0,
    queries: u64 = 0,
    failures: u64 = 0,

    fn qps(self: ConcurrentStats) f64 {
        if (self.total_ns == 0) return 0;
        return (@as(f64, @floatFromInt(self.queries)) * @as(f64, @floatFromInt(std.time.ns_per_s))) / @as(f64, @floatFromInt(self.total_ns));
    }

    fn avgRequestNs(self: ConcurrentStats) u64 {
        if (self.queries == 0) return 0;
        return self.request_ns / self.queries;
    }
};

const QueryResponseWire = struct {
    responses: []const Response,

    const Response = struct {
        hits: Hits,
        profile: ?Profile = null,

        const Hits = struct {
            total: u32 = 0,
            hits: ?[]const Hit = null,

            const Hit = struct {
                _id: []const u8,
            };
        };

        const Profile = struct {
            dense_search: ?DenseSearch = null,

            const DenseSearch = struct {
                const DebugHit = struct {
                    id: u64 = 0,
                    distance: f32 = 0,
                    error_bound: f32 = 0,
                    lower_bound: f32 = 0,
                    upper_bound: f32 = 0,
                };

                const DebugPair = struct {
                    left: DebugHit = .{},
                    right: DebugHit = .{},
                    distance_gap: f32 = 0,
                    interval_gap: f32 = 0,
                    overlaps: bool = false,
                };

                total_ns: u64 = 0,
                index_lookup_ns: u64 = 0,
                hbc_search_ns: u64 = 0,
                hbc_runtime_txn_ns: u64 = 0,
                hbc_scratch_acquire_ns: u64 = 0,
                hbc_node_cache_lookup_ns: u64 = 0,
                hbc_quantized_cache_lookup_ns: u64 = 0,
                hbc_reranked_vectors: u64 = 0,
                hbc_approx_candidate_count: u64 = 0,
                hbc_rerank_candidate_count: u64 = 0,
                hbc_ambiguous_top_k_pairs: u64 = 0,
                hbc_ambiguous_boundary_pairs: u64 = 0,
                hbc_ambiguous_distance_over_hits: u64 = 0,
                hbc_ambiguous_distance_under_hits: u64 = 0,
                hbc_full_rerank_due_to_threshold: bool = false,
                hbc_top_k_count: u64 = 0,
                hbc_boundary_pair: ?DebugPair = null,
                hbc_boundary_tail_error_avg: f32 = 0,
                hbc_boundary_tail_error_max: f32 = 0,
                hbc_boundary_tail_distance_gap_avg: f32 = 0,
                hbc_boundary_tail_distance_gap_min: f32 = 0,
                hbc_boundary_tail_distance_gap_max: f32 = 0,
                hbc_boundary_tail_interval_gap_avg: f32 = 0,
                hbc_boundary_tail_interval_gap_min: f32 = 0,
                hbc_boundary_tail_interval_gap_max: f32 = 0,
                doc_key_resolve_ns: u64 = 0,
                load_projected_document_ns: u64 = 0,
                postprocess_ns: u64 = 0,
                hbc_rerank_external_score_ns: u64 = 0,
                hbc_rerank_vector_load_ns: u64 = 0,
                hbc_rerank_metadata_lookup_ns: u64 = 0,
                hbc_rerank_artifact_key_ns: u64 = 0,
                hbc_rerank_artifact_read_ns: u64 = 0,
                hbc_rerank_artifact_decode_ns: u64 = 0,
                hbc_rerank_artifact_distance_ns: u64 = 0,
                hbc_rerank_lsm_cache_hits: u64 = 0,
                hbc_rerank_lsm_cache_misses: u64 = 0,
                hbc_rerank_distance_ns: u64 = 0,
                returned_hit_count: u32 = 0,
                inline_metadata_hits: u32 = 0,
                fetched_metadata_hits: u32 = 0,
                lookup_doc_key_hits: u32 = 0,
            };
        };
    };
};

const FakeStatusSource = struct {
    table: metadata_table_manager.TableRecord,
    empty_ranges: [0]metadata_table_manager.RangeRecord = .{},
    empty_stores: [0]metadata_table_manager.StoreRecord = .{},
    empty_placements: [0]raft_mod.reconciler.PlacementIntent = .{},
    empty_splits: [0]antfly.metadata.transition_state.SplitTransitionRecord = .{},
    empty_merges: [0]antfly.metadata.transition_state.MergeTransitionRecord = .{},

    fn init(cfg: Config) !FakeStatusSource {
        const indexes_json = try benchmarkIndexesJsonAlloc(std.heap.c_allocator, cfg);
        return .{
            .table = .{
                .table_id = 1,
                .name = table_name,
                .description = "public query guardrail",
                .schema_json = if (cfg.with_schema or cfg.query_shape == .algebraic_filter) benchmark_schema_json else "",
                .read_schema_json = "",
                .indexes_json = indexes_json,
                .replication_sources_json = "[]",
                .placement_role = "data",
            },
        };
    }

    fn deinit(self: *FakeStatusSource) void {
        std.heap.c_allocator.free(self.table.indexes_json);
        self.* = undefined;
    }

    fn iface(self: *FakeStatusSource) api.http_server.StatusSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .status = status,
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn status(_: *anyopaque) !metadata_api.MetadataStatus {
        return .{
            .metadata_group_id = 1,
            .metrics = .{},
            .projected_stores = 1,
        };
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *FakeStatusSource = @ptrCast(@alignCast(ptr));
        const tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1];
        return .{
            .status = .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 },
            .tables = tables,
            .ranges = @constCast(self.empty_ranges[0..]),
            .stores = @constCast(self.empty_stores[0..]),
            .placement_intents = @constCast(self.empty_placements[0..]),
            .split_transitions = @constCast(self.empty_splits[0..]),
            .merge_transitions = @constCast(self.empty_merges[0..]),
        };
    }

    fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
};

const BenchMetricsSource = struct {
    alloc: std.mem.Allocator,
    server: *api.ApiHttpServer,
    db: *db_mod.DB,

    fn readiness(_: *BenchMetricsSource) common.health_server.ReadinessChecker {
        return .{
            .ptr = undefined,
            .vtable = &.{ .check = readyCheck },
        };
    }

    fn metricsWriter(self: *BenchMetricsSource) common.health_server.MetricsWriter {
        return .{
            .ptr = self,
            .vtable = &.{ .write_metrics = writeMetrics },
        };
    }

    fn readyCheck(_: *anyopaque) bool {
        return true;
    }

    fn writeMetrics(ptr: *anyopaque, writer: *std.Io.Writer) anyerror!void {
        const self: *BenchMetricsSource = @ptrCast(@alignCast(ptr));
        const append = common.health_server.appendPromMetric;
        const request_stats = self.server.requestStats();
        try append(writer, "antfly_guardrail_api_requests_total", "counter", "Requests handled by the public query guardrail server", request_stats.request_count);
        try append(writer, "antfly_guardrail_api_first_request_elapsed_ms", "gauge", "Milliseconds from API server initialization to first handled request", request_stats.first_request_elapsed_ms);

        const stats = try self.db.stats(self.alloc);
        defer db_mod.types.freeDBStats(self.alloc, stats);
        try append(writer, "antfly_guardrail_doc_count", "gauge", "Documents currently indexed in the guardrail DB", stats.doc_count);
        try append(writer, "antfly_guardrail_index_count", "gauge", "Indexes currently present in the guardrail DB", @intCast(stats.indexes.len));
    }
};

const PollerContext = struct {
    alloc: std.mem.Allocator,
    health_uri: []const u8,
    metrics_uri: []const u8,
    status_uri: ?[]const u8 = null,
    poll_interval_ms: u64,
    stop: *std.atomic.Value(bool),
    stats: PollStats = .{},
    err: ?anyerror = null,

    fn run(self: *PollerContext) void {
        var executor = std_http_executor.StdHttpExecutor.init(self.alloc, .{});
        defer executor.deinit();
        while (!self.stop.load(.acquire)) {
            self.pollOne(executor.executor(), self.health_uri, .health);
            self.pollOne(executor.executor(), self.metrics_uri, .metrics);
            if (self.status_uri) |uri| self.pollOne(executor.executor(), uri, .status);
            sleepMs(self.poll_interval_ms);
        }
    }

    fn pollOne(self: *PollerContext, executor: http_common.RequestExecutor, uri: []const u8, kind: PollKind) void {
        const started = nowNs();
        var resp = executor.execute(self.alloc, .{
            .method = .GET,
            .uri = uri,
        }) catch {
            self.recordPollFailure(kind);
            return;
        };
        defer resp.deinit(self.alloc);
        const elapsed = elapsedSince(started);
        switch (kind) {
            .health => {
                self.stats.health_samples += 1;
                self.stats.health_max_latency_ns = @max(self.stats.health_max_latency_ns, elapsed);
            },
            .metrics => {
                self.stats.metrics_samples += 1;
                self.stats.metrics_max_latency_ns = @max(self.stats.metrics_max_latency_ns, elapsed);
            },
            .status => {
                self.stats.status_samples += 1;
                self.stats.status_max_latency_ns = @max(self.stats.status_max_latency_ns, elapsed);
            },
        }
        if (elapsed >= slow_poll_log_threshold_ns) {
            std.debug.print("public_query_slow_poll kind={s} elapsed_ms={d:.2} uri={s} status={d}\n", .{
                @tagName(kind),
                nsToMs(elapsed),
                uri,
                resp.status,
            });
        }
        if (resp.status != 200) self.recordPollFailure(kind);
    }

    fn recordPollFailure(self: *PollerContext, kind: PollKind) void {
        switch (kind) {
            .health => self.stats.health_failures += 1,
            .metrics => self.stats.metrics_failures += 1,
            .status => self.stats.status_failures += 1,
        }
    }
};

const EndpointPollerContext = struct {
    alloc: std.mem.Allocator,
    uri: []const u8,
    kind: PollKind,
    poll_interval_ms: u64,
    stop: *std.atomic.Value(bool),
    stats: PollStats = .{},
    err: ?anyerror = null,

    fn run(self: *EndpointPollerContext) void {
        var executor = std_http_executor.StdHttpExecutor.init(self.alloc, .{});
        defer executor.deinit();
        while (!self.stop.load(.acquire)) {
            self.pollOne(executor.executor());
            sleepMs(self.poll_interval_ms);
        }
    }

    fn pollOne(self: *EndpointPollerContext, executor: http_common.RequestExecutor) void {
        const started = nowNs();
        var resp = executor.execute(self.alloc, .{
            .method = .GET,
            .uri = self.uri,
        }) catch {
            self.recordPollFailure();
            return;
        };
        defer resp.deinit(self.alloc);
        const elapsed = elapsedSince(started);
        switch (self.kind) {
            .health => {
                self.stats.health_samples += 1;
                self.stats.health_max_latency_ns = @max(self.stats.health_max_latency_ns, elapsed);
            },
            .metrics => {
                self.stats.metrics_samples += 1;
                self.stats.metrics_max_latency_ns = @max(self.stats.metrics_max_latency_ns, elapsed);
            },
            .status => {
                self.stats.status_samples += 1;
                self.stats.status_max_latency_ns = @max(self.stats.status_max_latency_ns, elapsed);
            },
        }
        if (elapsed >= slow_poll_log_threshold_ns) {
            std.debug.print("public_query_slow_poll kind={s} elapsed_ms={d:.2} uri={s} status={d}\n", .{
                @tagName(self.kind),
                nsToMs(elapsed),
                self.uri,
                resp.status,
            });
        }
        if (resp.status != 200) self.recordPollFailure();
    }

    fn recordPollFailure(self: *EndpointPollerContext) void {
        switch (self.kind) {
            .health => self.stats.health_failures += 1,
            .metrics => self.stats.metrics_failures += 1,
            .status => self.stats.status_failures += 1,
        }
    }
};

fn mergePollStats(parts: []const PollStats) PollStats {
    var out: PollStats = .{};
    for (parts) |part| {
        out.health_samples += part.health_samples;
        out.metrics_samples += part.metrics_samples;
        out.status_samples += part.status_samples;
        out.health_failures += part.health_failures;
        out.metrics_failures += part.metrics_failures;
        out.status_failures += part.status_failures;
        out.health_max_latency_ns = @max(out.health_max_latency_ns, part.health_max_latency_ns);
        out.metrics_max_latency_ns = @max(out.metrics_max_latency_ns, part.metrics_max_latency_ns);
        out.status_max_latency_ns = @max(out.status_max_latency_ns, part.status_max_latency_ns);
    }
    return out;
}

const RssPollerContext = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    pid: std.process.Child.Id,
    poll_interval_ms: u64,
    stop: *std.atomic.Value(bool),
    peak_rss_bytes: usize = 0,
    err: ?anyerror = null,

    fn run(self: *RssPollerContext) void {
        while (!self.stop.load(.acquire)) {
            const rss = sampleRssBytes(self.alloc, self.io, self.pid) catch |err| {
                self.err = err;
                return;
            };
            self.peak_rss_bytes = @max(self.peak_rss_bytes, rss);
            sleepMs(self.poll_interval_ms);
        }
    }
};

const HttpWorkerContext = struct {
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    query_bodies: []const []const u8,
    repeats: usize,
    stats: ConcurrentStats = .{},
    err: ?anyerror = null,

    fn run(self: *HttpWorkerContext) void {
        var executor = std_http_executor.StdHttpExecutor.init(self.alloc, .{});
        defer executor.deinit();
        var client = api.ApiHttpClient.init(self.alloc, executor.executor());
        const started = nowNs();
        var local_queries: u64 = 0;
        var local_request_ns: u64 = 0;
        var local_max_request_ns: u64 = 0;
        for (0..self.repeats) |_| {
            for (self.query_bodies) |body| {
                const request_started = nowNs();
                var resp = client.fetchQuery(self.base_uri, table_name, body) catch |err| retry: {
                    if (isRetryableQueryConnectionError(err)) {
                        break :retry client.fetchQuery(self.base_uri, table_name, body) catch |retry_err| {
                            self.err = retry_err;
                            self.stats.failures += 1;
                            self.stats.total_ns = elapsedSince(started);
                            return;
                        };
                    }
                    self.err = err;
                    self.stats.failures += 1;
                    self.stats.total_ns = elapsedSince(started);
                    return;
                };
                defer resp.deinit(self.alloc);
                var parsed = std.json.parseFromSlice(QueryResponseWire, self.alloc, resp.body, .{ .ignore_unknown_fields = true }) catch |err| {
                    self.err = err;
                    self.stats.failures += 1;
                    self.stats.total_ns = elapsedSince(started);
                    return;
                };
                defer parsed.deinit();
                _ = accumulateParsedResponseNoProfile(parsed.value, resp.body) catch |err| {
                    self.err = err;
                    self.stats.failures += 1;
                    self.stats.total_ns = elapsedSince(started);
                    return;
                };
                const request_elapsed = elapsedSince(request_started);
                local_request_ns += request_elapsed;
                local_max_request_ns = @max(local_max_request_ns, request_elapsed);
                local_queries += 1;
            }
        }
        self.stats.queries = local_queries;
        self.stats.request_ns = local_request_ns;
        self.stats.max_request_ns = local_max_request_ns;
        self.stats.total_ns = elapsedSince(started);
    }
};

const DirectHandlerWorkerContext = struct {
    alloc: std.mem.Allocator,
    executor: http_common.RequestExecutor,
    query_bodies: []const []const u8,
    repeats: usize,
    stats: ConcurrentStats = .{},
    err: ?anyerror = null,

    fn run(self: *DirectHandlerWorkerContext) void {
        const uri = "/tables/" ++ table_name ++ "/query";
        const started = nowNs();
        var local_queries: u64 = 0;
        var local_request_ns: u64 = 0;
        var local_max_request_ns: u64 = 0;
        for (0..self.repeats) |_| {
            for (self.query_bodies) |body| {
                const request_started = nowNs();
                var resp = self.executor.execute(self.alloc, .{
                    .method = .POST,
                    .uri = uri,
                    .content_type = "application/json",
                    .body = body,
                }) catch |err| {
                    self.err = err;
                    self.stats.failures += 1;
                    self.stats.total_ns = elapsedSince(started);
                    return;
                };
                defer resp.deinit(self.alloc);
                if (resp.status != 200) {
                    self.err = error.UnexpectedHttpStatus;
                    self.stats.failures += 1;
                    self.stats.total_ns = elapsedSince(started);
                    return;
                }
                const request_elapsed = elapsedSince(request_started);
                local_request_ns += request_elapsed;
                local_max_request_ns = @max(local_max_request_ns, request_elapsed);
                local_queries += 1;
            }
        }
        self.stats.queries = local_queries;
        self.stats.request_ns = local_request_ns;
        self.stats.max_request_ns = local_max_request_ns;
        self.stats.total_ns = elapsedSince(started);
    }
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);
    if (cfg.docs == 0 or cfg.dims == 0 or cfg.queries == 0 or cfg.repeats == 0 or cfg.k == 0 or cfg.batch_size == 0 or cfg.search_threads == 0) {
        return error.InvalidArgument;
    }

    const dataset = try makeDataset(alloc, cfg);
    defer alloc.free(dataset);
    const queries = try makeQueries(alloc, dataset, cfg);
    defer alloc.free(queries);
    const query_bodies = try makeQueryBodies(alloc, queries, cfg);
    defer freeOwnedStrings(alloc, query_bodies);

    switch (cfg.mode) {
        .handler => try runHandlerBench(alloc, cfg, dataset, queries, query_bodies),
        .local => try runLocalBench(alloc, init.io, cfg, dataset, queries, query_bodies),
        .swarm => {
            const cwd = try std.process.currentPathAlloc(init.io, alloc);
            defer alloc.free(cwd);
            try runSwarmBench(alloc, init.io, cwd, cfg, dataset, query_bodies);
        },
    }
}

fn runHandlerBench(
    alloc: std.mem.Allocator,
    cfg: Config,
    dataset: []const f32,
    queries: []const f32,
    query_bodies: []const []const u8,
) !void {
    _ = queries;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try openAndSeedDb(alloc, path[0..path.len], cfg, dataset);
    defer db.close();

    var read_source = api.BoundTableReadSource.init(table_name, 1, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var write_source = api.BoundTableWriteSource.init(table_name, &db);
    var status_source = try FakeStatusSource.init(cfg);
    defer status_source.deinit();

    var server = api.ApiHttpServer.init(
        alloc,
        .{},
        status_source.iface(),
        read_source.source(),
        write_source.source(),
    );
    defer server.deinit();

    std.debug.print("public-query guardrail stage=db-search\n", .{});
    const db_stats = try benchDbSearch(alloc, &db, query_bodies, cfg);
    std.debug.print("public-query guardrail stage=handler-pipeline\n", .{});
    const handler_pipeline = try benchHandlerPipeline(alloc, &server, read_source.source(), query_bodies, cfg);
    std.debug.print("public-query guardrail stage=direct-handler\n", .{});
    const handler_stats = try benchDirectHandler(alloc, server.executor(), query_bodies, cfg);
    try enforceSymbolicProfileGuardrail(cfg, handler_stats);
    try enforceSymbolicResultFillGuardrail(cfg, handler_stats);
    std.debug.print("public-query guardrail stage=handler-concurrent\n", .{});
    const handler_concurrent = try benchConcurrentDirectHandler(alloc, server.executor(), query_bodies, cfg);

    const avg_db_ns = db_stats.avgNs();
    const avg_handler_ns = handler_stats.avgNs();
    const avg_profile_ns = handler_stats.avgProfileNs();
    const handler_overhead_ns = avg_handler_ns -| avg_profile_ns;

    std.debug.print(
        "public_query_guardrail query_shape={s} with_schema={} with_algebraic={} docs={d} dims={d} queries={d} repeats={d} k={d} threads={d} db={d:.3}us handler={d:.3}us http={d:.3}us http_first_pass={d:.3}us http_later_pass={d:.3}us handler_over_profile={d:.3}us http_over_handler={d:.3}us profile_total={d:.3}us profile_hbc={d:.3}us concurrent_qps={d:.2} concurrent_avg={d:.3}us concurrent_max={d:.3}us health_max_ms={d:.2} metrics_max_ms={d:.2} health_failures={d} metrics_failures={d}\n",
        .{
            cfg.query_shape.text(),
            cfg.with_schema,
            cfg.with_algebraic,
            cfg.docs,
            cfg.dims,
            cfg.queries,
            cfg.repeats,
            cfg.k,
            cfg.search_threads,
            nsToUs(avg_db_ns),
            nsToUs(avg_handler_ns),
            nsToUs(avg_handler_ns),
            nsToUs(handler_stats.avgFirstPassNs()),
            nsToUs(handler_stats.avgLaterPassNs()),
            nsToUs(handler_overhead_ns),
            nsToUs(0),
            nsToUs(avg_profile_ns),
            nsToUs(if (handler_stats.queries == 0) 0 else handler_stats.profile_hbc_search_ns / handler_stats.queries),
            handler_concurrent.qps(),
            nsToUs(handler_concurrent.avgRequestNs()),
            nsToUs(handler_concurrent.max_request_ns),
            nsToMs(0),
            nsToMs(0),
            0,
            0,
        },
    );
    printPublicQueryGuardrailSummaryJson(
        "handler",
        @tagName(cfg.server_kind),
        cfg,
        avg_db_ns,
        avg_handler_ns,
        handler_stats,
        handler_concurrent,
        .{ .visibility = .{}, .polls = .{} },
        0,
        0,
        .{},
    );
    std.debug.print(
        "public_query_handler_breakdown parse={d:.3}us route={d:.3}us source_query={d:.3}us source_over_profile={d:.3}us handler_concurrent_qps={d:.2} http_concurrent_qps={d:.2}\n",
        .{
            nsToUs(handler_pipeline.avgParseNs()),
            nsToUs(handler_pipeline.avgRouteNs()),
            nsToUs(handler_pipeline.avgSourceQueryNs()),
            nsToUs(handler_pipeline.avgSourceQueryNs() -| (if (handler_pipeline.queries == 0) 0 else handler_pipeline.source_profile_total_ns / handler_pipeline.queries)),
            handler_concurrent.qps(),
            handler_concurrent.qps(),
        },
    );
    std.debug.print(
        "public_query_hbc_concurrency_profile runtime_txn={d:.3}us scratch_acquire={d:.3}us node_cache_lookup={d:.3}us quantized_cache_lookup={d:.3}us\n",
        .{
            nsToUs(if (handler_stats.queries == 0) 0 else handler_stats.profile_hbc_runtime_txn_ns / handler_stats.queries),
            nsToUs(if (handler_stats.queries == 0) 0 else handler_stats.profile_hbc_scratch_acquire_ns / handler_stats.queries),
            nsToUs(if (handler_stats.queries == 0) 0 else handler_stats.profile_hbc_node_cache_lookup_ns / handler_stats.queries),
            nsToUs(if (handler_stats.queries == 0) 0 else handler_stats.profile_hbc_quantized_cache_lookup_ns / handler_stats.queries),
        },
    );
    std.debug.print(
        "public_query_rerank_selection approx_candidates={d:.2} rerank_candidates={d:.2} reranked_vectors={d:.2} top_k_count={d:.2} ambiguous_top_k_pairs={d:.2} ambiguous_boundary_pairs={d:.2} ambiguous_distance_over_hits={d:.2} ambiguous_distance_under_hits={d:.2} full_rerank_threshold_rate={d:.4}\n",
        .{
            avgPerQuery(handler_stats, handler_stats.profile_hbc_approx_candidate_count),
            avgPerQuery(handler_stats, handler_stats.profile_hbc_rerank_candidate_count),
            avgPerQuery(handler_stats, handler_stats.profile_hbc_reranked_vectors),
            avgPerQuery(handler_stats, handler_stats.profile_hbc_top_k_count),
            avgPerQuery(handler_stats, handler_stats.profile_hbc_ambiguous_top_k_pairs),
            avgPerQuery(handler_stats, handler_stats.profile_hbc_ambiguous_boundary_pairs),
            avgPerQuery(handler_stats, handler_stats.profile_hbc_ambiguous_distance_over_hits),
            avgPerQuery(handler_stats, handler_stats.profile_hbc_ambiguous_distance_under_hits),
            if (handler_stats.queries == 0) 0 else @as(f64, @floatFromInt(handler_stats.profile_hbc_full_rerank_due_to_threshold)) / @as(f64, @floatFromInt(handler_stats.queries)),
        },
    );
    printPublicQuerySymbolicFilterProfile(cfg, handler_stats);
    std.debug.print(
        "public_query_rerank_boundary avg_left_distance={d:.6} avg_left_error={d:.6} avg_left_lower={d:.6} avg_left_upper={d:.6} avg_right_distance={d:.6} avg_right_error={d:.6} avg_right_lower={d:.6} avg_right_upper={d:.6} avg_distance_gap={d:.6} avg_interval_gap={d:.6} max_left_error={d:.6} max_right_error={d:.6} boundary_pair_rate={d:.4}\n",
        .{
            handler_stats.avgBoundary(handler_stats.profile_hbc_boundary_left_distance_sum),
            handler_stats.avgBoundary(handler_stats.profile_hbc_boundary_left_error_sum),
            handler_stats.avgBoundary(handler_stats.profile_hbc_boundary_left_lower_sum),
            handler_stats.avgBoundary(handler_stats.profile_hbc_boundary_left_upper_sum),
            handler_stats.avgBoundary(handler_stats.profile_hbc_boundary_right_distance_sum),
            handler_stats.avgBoundary(handler_stats.profile_hbc_boundary_right_error_sum),
            handler_stats.avgBoundary(handler_stats.profile_hbc_boundary_right_lower_sum),
            handler_stats.avgBoundary(handler_stats.profile_hbc_boundary_right_upper_sum),
            handler_stats.avgBoundary(handler_stats.profile_hbc_boundary_distance_gap_sum),
            handler_stats.avgBoundary(handler_stats.profile_hbc_boundary_interval_gap_sum),
            handler_stats.profile_hbc_boundary_left_error_max,
            handler_stats.profile_hbc_boundary_right_error_max,
            if (handler_stats.queries == 0) 0 else @as(f64, @floatFromInt(handler_stats.profile_hbc_boundary_pair_count)) / @as(f64, @floatFromInt(handler_stats.queries)),
        },
    );
    std.debug.print(
        "public_query_rerank_boundary_tail avg_error={d:.6} max_error={d:.6} avg_distance_gap={d:.6} min_distance_gap={d:.6} max_distance_gap={d:.6} avg_interval_gap={d:.6} min_interval_gap={d:.6} max_interval_gap={d:.6}\n",
        .{
            handler_stats.avgBoundary(handler_stats.profile_hbc_boundary_tail_error_avg_sum),
            handler_stats.profile_hbc_boundary_tail_error_max,
            handler_stats.avgBoundary(handler_stats.profile_hbc_boundary_tail_distance_gap_avg_sum),
            if (handler_stats.profile_hbc_boundary_tail_distance_gap_min == std.math.floatMax(f64)) 0 else handler_stats.profile_hbc_boundary_tail_distance_gap_min,
            if (handler_stats.profile_hbc_boundary_tail_distance_gap_max == -std.math.floatMax(f64)) 0 else handler_stats.profile_hbc_boundary_tail_distance_gap_max,
            handler_stats.avgBoundary(handler_stats.profile_hbc_boundary_tail_interval_gap_avg_sum),
            if (handler_stats.profile_hbc_boundary_tail_interval_gap_min == std.math.floatMax(f64)) 0 else handler_stats.profile_hbc_boundary_tail_interval_gap_min,
            if (handler_stats.profile_hbc_boundary_tail_interval_gap_max == -std.math.floatMax(f64)) 0 else handler_stats.profile_hbc_boundary_tail_interval_gap_max,
        },
    );
}

fn runLocalBench(
    alloc: std.mem.Allocator,
    io: std.Io,
    cfg: Config,
    dataset: []const f32,
    queries: []const f32,
    query_bodies: []const []const u8,
) !void {
    _ = queries;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try openAndSeedDb(alloc, path[0..path.len], cfg, dataset);
    defer db.close();

    var read_source = api.BoundTableReadSource.init(table_name, 1, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var write_source = api.BoundTableWriteSource.init(table_name, &db);
    var status_source = try FakeStatusSource.init(cfg);
    defer status_source.deinit();

    var server = api.ApiHttpServer.init(
        alloc,
        .{},
        status_source.iface(),
        read_source.source(),
        write_source.source(),
    );
    defer server.deinit();

    var listener = std_http_listener.StdHttpListener.init(alloc, .{
        .bind_host = "127.0.0.1",
        .bind_port = 0,
        .serve_in_connection_threads = true,
        .connection_thread_stack_size = 512 * 1024,
    }, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    var metrics_source = BenchMetricsSource{
        .alloc = alloc,
        .server = &server,
        .db = &db,
    };
    const health_server = try common.health_server.HealthServer.init(
        alloc,
        .{ .bind_port = 0 },
        metrics_source.readiness(),
        metrics_source.metricsWriter(),
    );
    defer health_server.deinit();
    try health_server.start();

    const health_base_uri = try health_server.baseUri(alloc);
    defer alloc.free(health_base_uri);
    const health_uri = try std.fmt.allocPrint(alloc, "{s}/healthz", .{health_base_uri});
    defer alloc.free(health_uri);
    const metrics_uri = try std.fmt.allocPrint(alloc, "{s}/metrics", .{health_base_uri});
    defer alloc.free(metrics_uri);

    std.debug.print("public-query guardrail stage=db-search\n", .{});
    const db_stats = try benchDbSearch(alloc, &db, query_bodies, cfg);
    std.debug.print("public-query guardrail stage=handler-pipeline\n", .{});
    const handler_pipeline = try benchHandlerPipeline(alloc, &server, read_source.source(), query_bodies, cfg);
    std.debug.print("public-query guardrail stage=direct-handler\n", .{});
    const handler_stats = try benchDirectHandler(alloc, server.executor(), query_bodies, cfg);
    std.debug.print("public-query guardrail stage=http-query\n", .{});
    const http_stats = try benchHttpQuery(alloc, base_uri, query_bodies, cfg);
    try enforceSymbolicProfileGuardrail(cfg, http_stats);
    try enforceSymbolicResultFillGuardrail(cfg, http_stats);
    std.debug.print("public-query guardrail stage=handler-concurrent\n", .{});
    const handler_concurrent = try benchConcurrentDirectHandler(alloc, server.executor(), query_bodies, cfg);
    std.debug.print("public-query guardrail stage=http-concurrent\n", .{});
    const concurrent = try benchConcurrentHttpWithPolling(alloc, io, base_uri, query_bodies, health_uri, metrics_uri, null, cfg, null);

    try enforceGuardrails(cfg, concurrent.polls);
    try maybeRunHttpSearchThreadSweep(alloc, io, base_uri, query_bodies, health_uri, metrics_uri, null, cfg, null);

    const avg_db_ns = db_stats.avgNs();
    const avg_handler_ns = handler_stats.avgNs();
    const avg_http_ns = http_stats.avgNs();
    const avg_profile_ns = http_stats.avgProfileNs();
    const handler_overhead_ns = avg_handler_ns -| avg_profile_ns;
    const http_transport_overhead_ns = avg_http_ns -| avg_handler_ns;

    std.debug.print(
        "public_query_guardrail query_shape={s} with_schema={} with_algebraic={} docs={d} dims={d} queries={d} repeats={d} k={d} threads={d} db={d:.3}us handler={d:.3}us http={d:.3}us http_first_pass={d:.3}us http_later_pass={d:.3}us handler_over_profile={d:.3}us http_over_handler={d:.3}us profile_total={d:.3}us profile_hbc={d:.3}us concurrent_qps={d:.2} concurrent_avg={d:.3}us concurrent_max={d:.3}us health_max_ms={d:.2} metrics_max_ms={d:.2} health_failures={d} metrics_failures={d}\n",
        .{
            cfg.query_shape.text(),
            cfg.with_schema,
            cfg.with_algebraic,
            cfg.docs,
            cfg.dims,
            cfg.queries,
            cfg.repeats,
            cfg.k,
            cfg.search_threads,
            nsToUs(avg_db_ns),
            nsToUs(avg_handler_ns),
            nsToUs(avg_http_ns),
            nsToUs(http_stats.avgFirstPassNs()),
            nsToUs(http_stats.avgLaterPassNs()),
            nsToUs(handler_overhead_ns),
            nsToUs(http_transport_overhead_ns),
            nsToUs(avg_profile_ns),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_hbc_search_ns / http_stats.queries),
            concurrent.http.qps(),
            nsToUs(concurrent.http.avgRequestNs()),
            nsToUs(concurrent.http.max_request_ns),
            nsToMs(concurrent.polls.health_max_latency_ns),
            nsToMs(concurrent.polls.metrics_max_latency_ns),
            concurrent.polls.health_failures,
            concurrent.polls.metrics_failures,
        },
    );
    printPublicQueryGuardrailSummaryJson(
        "local",
        @tagName(cfg.server_kind),
        cfg,
        avg_db_ns,
        avg_handler_ns,
        http_stats,
        concurrent.http,
        .{ .visibility = .{}, .polls = .{} },
        0,
        0,
        .{},
    );
    std.debug.print(
        "public_query_handler_breakdown parse={d:.3}us route={d:.3}us source_query={d:.3}us source_over_profile={d:.3}us handler_concurrent_qps={d:.2} http_concurrent_qps={d:.2}\n",
        .{
            nsToUs(handler_pipeline.avgParseNs()),
            nsToUs(handler_pipeline.avgRouteNs()),
            nsToUs(handler_pipeline.avgSourceQueryNs()),
            nsToUs(handler_pipeline.avgSourceQueryNs() -| (if (handler_pipeline.queries == 0) 0 else handler_pipeline.source_profile_total_ns / handler_pipeline.queries)),
            handler_concurrent.qps(),
            concurrent.http.qps(),
        },
    );
    std.debug.print(
        "public_query_hbc_concurrency_profile runtime_txn={d:.3}us scratch_acquire={d:.3}us node_cache_lookup={d:.3}us quantized_cache_lookup={d:.3}us\n",
        .{
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_hbc_runtime_txn_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_hbc_scratch_acquire_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_hbc_node_cache_lookup_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_hbc_quantized_cache_lookup_ns / http_stats.queries),
        },
    );
    std.debug.print(
        "public_query_rerank_selection approx_candidates={d:.2} rerank_candidates={d:.2} reranked_vectors={d:.2} top_k_count={d:.2} ambiguous_top_k_pairs={d:.2} ambiguous_boundary_pairs={d:.2} ambiguous_distance_over_hits={d:.2} ambiguous_distance_under_hits={d:.2} full_rerank_threshold_rate={d:.4}\n",
        .{
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_approx_candidate_count)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_rerank_candidate_count)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_reranked_vectors)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_top_k_count)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_ambiguous_top_k_pairs)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_ambiguous_boundary_pairs)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_ambiguous_distance_over_hits)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_ambiguous_distance_under_hits)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_full_rerank_due_to_threshold)) / @as(f64, @floatFromInt(http_stats.queries)),
        },
    );
    printPublicQuerySymbolicFilterProfile(cfg, http_stats);
    std.debug.print(
        "public_query_rerank_boundary avg_left_distance={d:.6} avg_left_error={d:.6} avg_left_lower={d:.6} avg_left_upper={d:.6} avg_right_distance={d:.6} avg_right_error={d:.6} avg_right_lower={d:.6} avg_right_upper={d:.6} avg_distance_gap={d:.6} avg_interval_gap={d:.6} max_left_error={d:.6} max_right_error={d:.6} boundary_pair_rate={d:.4}\n",
        .{
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_left_distance_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_left_error_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_left_lower_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_left_upper_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_right_distance_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_right_error_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_right_lower_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_right_upper_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_distance_gap_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_interval_gap_sum),
            http_stats.profile_hbc_boundary_left_error_max,
            http_stats.profile_hbc_boundary_right_error_max,
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_boundary_pair_count)) / @as(f64, @floatFromInt(http_stats.queries)),
        },
    );
    std.debug.print(
        "public_query_rerank_boundary_tail avg_error={d:.6} max_error={d:.6} avg_distance_gap={d:.6} min_distance_gap={d:.6} max_distance_gap={d:.6} avg_interval_gap={d:.6} min_interval_gap={d:.6} max_interval_gap={d:.6}\n",
        .{
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_tail_error_avg_sum),
            http_stats.profile_hbc_boundary_tail_error_max,
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_tail_distance_gap_avg_sum),
            if (http_stats.profile_hbc_boundary_tail_distance_gap_min == std.math.floatMax(f64)) 0 else http_stats.profile_hbc_boundary_tail_distance_gap_min,
            if (http_stats.profile_hbc_boundary_tail_distance_gap_max == -std.math.floatMax(f64)) 0 else http_stats.profile_hbc_boundary_tail_distance_gap_max,
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_tail_interval_gap_avg_sum),
            if (http_stats.profile_hbc_boundary_tail_interval_gap_min == std.math.floatMax(f64)) 0 else http_stats.profile_hbc_boundary_tail_interval_gap_min,
            if (http_stats.profile_hbc_boundary_tail_interval_gap_max == -std.math.floatMax(f64)) 0 else http_stats.profile_hbc_boundary_tail_interval_gap_max,
        },
    );
}

const ConcurrentRun = struct {
    http: ConcurrentStats,
    polls: PollStats,
    rss_peak_bytes: usize = 0,
};

const LoadRun = struct {
    visibility: VisibilitySnapshot,
    polls: PollStats,
    rss_peak_bytes: usize = 0,
    insert_ns: u64 = 0,
    visibility_wait_ns: u64 = 0,
    total_ns: u64 = 0,
};

fn maybeRunHttpSearchThreadSweep(
    alloc: std.mem.Allocator,
    io: std.Io,
    base_uri: []const u8,
    query_bodies: []const []const u8,
    health_uri: []const u8,
    metrics_uri: []const u8,
    status_uri: ?[]const u8,
    cfg: Config,
    rss_pid: ?std.process.Child.Id,
) !void {
    if (!cfg.search_thread_sweep) return;

    var next_threads: usize = 1;
    while (true) {
        const threads = @min(next_threads, cfg.search_threads);
        var run_cfg = cfg;
        run_cfg.search_threads = threads;
        const run = try benchConcurrentHttpWithPolling(
            alloc,
            io,
            base_uri,
            query_bodies,
            health_uri,
            metrics_uri,
            status_uri,
            run_cfg,
            rss_pid,
        );
        try enforceGuardrails(run_cfg, run.polls);
        std.debug.print(
            "public_query_thread_sweep threads={d} qps={d:.2} avg={d:.3}us max={d:.3}us rss_peak_mb={d:.2} health_max_ms={d:.2} metrics_max_ms={d:.2} status_max_ms={d:.2} failures={{health={d},metrics={d},status={d}}}\n",
            .{
                threads,
                run.http.qps(),
                nsToUs(run.http.avgRequestNs()),
                nsToUs(run.http.max_request_ns),
                bytesToMiB(run.rss_peak_bytes),
                nsToMs(run.polls.health_max_latency_ns),
                nsToMs(run.polls.metrics_max_latency_ns),
                nsToMs(run.polls.status_max_latency_ns),
                run.polls.health_failures,
                run.polls.metrics_failures,
                run.polls.status_failures,
            },
        );
        if (threads == cfg.search_threads) break;
        next_threads *= 2;
        if (next_threads == 0) break;
    }
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    if (args.next()) |first| {
        if (std.mem.startsWith(u8, first, "--")) try parseArg(&cfg, first, &args);
    }
    while (args.next()) |arg| {
        try parseArg(&cfg, arg, &args);
    }
    return cfg;
}

fn parseArg(cfg: *Config, arg: []const u8, args: *std.process.Args.Iterator) !void {
    if (std.mem.eql(u8, arg, "--mode")) {
        const raw = args.next() orelse return error.InvalidArgument;
        if (std.mem.eql(u8, raw, "handler")) {
            cfg.mode = .handler;
        } else if (std.mem.eql(u8, raw, "local")) {
            cfg.mode = .local;
        } else if (std.mem.eql(u8, raw, "swarm")) {
            cfg.mode = .swarm;
        } else {
            return error.InvalidArgument;
        }
    } else if (std.mem.eql(u8, arg, "--server-kind")) {
        const raw = args.next() orelse return error.InvalidArgument;
        if (std.mem.eql(u8, raw, "zig")) {
            cfg.server_kind = .zig;
        } else if (std.mem.eql(u8, raw, "go")) {
            cfg.server_kind = .go;
        } else {
            return error.InvalidArgument;
        }
    } else if (std.mem.eql(u8, arg, "--query-shape")) {
        const raw = args.next() orelse return error.InvalidArgument;
        cfg.query_shape = QueryShape.parse(raw) orelse return error.InvalidArgument;
    } else if (std.mem.eql(u8, arg, "--with-schema")) {
        cfg.with_schema = true;
    } else if (std.mem.eql(u8, arg, "--with-algebraic")) {
        cfg.with_algebraic = true;
        cfg.with_schema = true;
    } else if (std.mem.eql(u8, arg, "--with-sparse")) {
        cfg.with_sparse = true;
    } else if (std.mem.eql(u8, arg, "--with-graph")) {
        cfg.with_graph = true;
    } else if (std.mem.eql(u8, arg, "--require-symbolic-profile")) {
        cfg.require_symbolic_profile = true;
    } else if (std.mem.eql(u8, arg, "--docs")) {
        cfg.docs = try parseNextUsize(args, "--docs");
    } else if (std.mem.eql(u8, arg, "--dims")) {
        cfg.dims = try parseNextUsize(args, "--dims");
    } else if (std.mem.eql(u8, arg, "--queries")) {
        cfg.queries = try parseNextUsize(args, "--queries");
    } else if (std.mem.eql(u8, arg, "--repeats")) {
        cfg.repeats = try parseNextUsize(args, "--repeats");
    } else if (std.mem.eql(u8, arg, "--k")) {
        cfg.k = try parseNextUsize(args, "--k");
    } else if (std.mem.eql(u8, arg, "--batch-size")) {
        cfg.batch_size = try parseNextUsize(args, "--batch-size");
    } else if (std.mem.eql(u8, arg, "--search-threads")) {
        cfg.search_threads = try parseNextUsize(args, "--search-threads");
    } else if (std.mem.eql(u8, arg, "--search-thread-sweep")) {
        cfg.search_thread_sweep = true;
    } else if (std.mem.eql(u8, arg, "--seed")) {
        cfg.seed = try parseNextU64(args, "--seed");
    } else if (std.mem.eql(u8, arg, "--poll-interval-ms")) {
        cfg.poll_interval_ms = try parseNextU64(args, "--poll-interval-ms");
    } else if (std.mem.eql(u8, arg, "--max-health-latency-ms")) {
        cfg.max_health_latency_ms = try parseNextU64(args, "--max-health-latency-ms");
    } else if (std.mem.eql(u8, arg, "--max-metrics-latency-ms")) {
        cfg.max_metrics_latency_ms = try parseNextU64(args, "--max-metrics-latency-ms");
    } else if (std.mem.eql(u8, arg, "--max-status-latency-ms")) {
        cfg.max_status_latency_ms = try parseNextU64(args, "--max-status-latency-ms");
    } else if (std.mem.eql(u8, arg, "--max-health-failures")) {
        cfg.max_health_failures = try parseNextU64(args, "--max-health-failures");
    } else if (std.mem.eql(u8, arg, "--max-metrics-failures")) {
        cfg.max_metrics_failures = try parseNextU64(args, "--max-metrics-failures");
    } else if (std.mem.eql(u8, arg, "--max-status-failures")) {
        cfg.max_status_failures = try parseNextU64(args, "--max-status-failures");
    } else if (std.mem.eql(u8, arg, "--startup-timeout-ms")) {
        cfg.startup_timeout_ms = try parseNextU64(args, "--startup-timeout-ms");
    } else if (std.mem.eql(u8, arg, "--index-ready-timeout-ms")) {
        cfg.index_ready_timeout_ms = try parseNextU64(args, "--index-ready-timeout-ms");
    } else if (std.mem.eql(u8, arg, "--load-progress-interval")) {
        cfg.load_progress_interval = try parseNextUsize(args, "--load-progress-interval");
    } else if (std.mem.eql(u8, arg, "--sync-level")) {
        const raw = args.next() orelse return error.InvalidArgument;
        cfg.sync_level = db_mod.types.parsePublicSyncLevelText(raw) orelse return error.InvalidArgument;
    } else if (std.mem.eql(u8, arg, "--swarm-binary")) {
        cfg.swarm_binary = args.next() orelse return error.InvalidArgument;
    } else if (std.mem.eql(u8, arg, "--host")) {
        cfg.bind_host = args.next() orelse return error.InvalidArgument;
    } else {
        return error.InvalidArgument;
    }
}

fn openAndSeedDb(
    alloc: std.mem.Allocator,
    path: []const u8,
    cfg: Config,
    dataset: []const f32,
) !db_mod.DB {
    var db = try db_mod.DB.open(alloc, path, .{});
    errdefer db.close();

    if (cfg.with_schema or cfg.query_shape == .algebraic_filter) {
        var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, benchmark_schema_json);
        defer parsed_schema.deinit(alloc);
        const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
        defer schema_mod.freeSchema(alloc, runtime_schema);
        try db.setSchema(runtime_schema);
    }

    const index_cfg = try std.fmt.allocPrint(
        alloc,
        "{{\"field\":\"embedding\",\"dims\":{d},\"metric\":\"l2_squared\",\"external\":true}}",
        .{cfg.dims},
    );
    defer alloc.free(index_cfg);
    try db.addIndex(.{
        .name = index_name,
        .kind = .dense_vector,
        .config_json = index_cfg,
    });
    if (cfg.query_shape.needsDefaultFullTextIndex()) {
        try db.addIndex(.{
            .name = text_index_name,
            .kind = .full_text,
            .config_json = "{}",
        });
    }
    if (cfg.with_sparse or cfg.query_shape.usesSparse()) {
        try db.addIndex(.{
            .name = sparse_index_name,
            .kind = .sparse_vector,
            .config_json = "{\"field\":\"sparse\",\"external\":true}",
        });
    }
    if (cfg.with_graph or cfg.query_shape.usesGraph()) {
        try db.addIndex(.{
            .name = graph_index_name,
            .kind = .graph,
            .config_json = "{}",
        });
    }
    if (cfg.with_algebraic or cfg.query_shape == .algebraic_filter) {
        try db.addIndex(.{
            .name = algebraic_index_name,
            .kind = .algebraic,
            .config_json = benchmark_algebraic_config_json,
        });
    }

    const writes_buf = try alloc.alloc(db_mod.types.BatchWrite, cfg.batch_size);
    defer alloc.free(writes_buf);
    const docs_buf = try alloc.alloc(InputDoc, cfg.batch_size);
    defer {
        for (docs_buf) |doc| {
            if (doc.key.len > 0) alloc.free(doc.key);
            if (doc.value.len > 0) alloc.free(doc.value);
        }
        alloc.free(docs_buf);
    }
    for (docs_buf) |*doc| doc.* = .{ .key = &.{}, .value = &.{} };

    var start: usize = 0;
    while (start < cfg.docs) : (start += cfg.batch_size) {
        const end = @min(start + cfg.batch_size, cfg.docs);
        const docs = docs_buf[0 .. end - start];
        const writes = writes_buf[0 .. end - start];
        defer {
            for (docs) |*doc| {
                if (doc.key.len > 0) alloc.free(doc.key);
                if (doc.value.len > 0) alloc.free(doc.value);
                doc.* = .{ .key = &.{}, .value = &.{} };
            }
        }
        for (start..end, 0..) |doc_idx, i| {
            const vector = dataset[doc_idx * cfg.dims ..][0..cfg.dims];
            docs[i] = .{
                .key = try std.fmt.allocPrint(alloc, "doc:{d:0>8}", .{doc_idx}),
                .value = try encodeVectorDocJson(alloc, vector, doc_idx, cfg),
            };
            writes[i] = .{
                .key = docs[i].key,
                .value = docs[i].value,
            };
        }
        try db.batch(.{
            .writes = writes,
            .sync_level = cfg.sync_level,
        });
    }
    try db.runUntilIdle();
    return db;
}

fn benchmarkIndexesJsonAlloc(alloc: std.mem.Allocator, cfg: Config) ![]u8 {
    const text_entry = if (cfg.query_shape.needsDefaultFullTextIndex())
        try std.fmt.allocPrint(alloc, ",\"{s}\":{{\"type\":\"full_text\"}}", .{text_index_name})
    else
        try alloc.dupe(u8, "");
    defer alloc.free(text_entry);
    const sparse_entry = if (cfg.with_sparse or cfg.query_shape.usesSparse())
        try std.fmt.allocPrint(alloc, ",\"{s}\":{{\"type\":\"embeddings\",\"sparse\":true,\"external\":true}}", .{sparse_index_name})
    else
        try alloc.dupe(u8, "");
    defer alloc.free(sparse_entry);
    const graph_entry = if (cfg.with_graph or cfg.query_shape.usesGraph())
        try std.fmt.allocPrint(alloc, ",\"{s}\":{{\"type\":\"graph\"}}", .{graph_index_name})
    else
        try alloc.dupe(u8, "");
    defer alloc.free(graph_entry);
    const algebraic_entry = if (cfg.with_algebraic or cfg.query_shape == .algebraic_filter)
        try std.fmt.allocPrint(alloc, ",\"{s}\":{{\"type\":\"algebraic\",\"group_fields\":[{{\"name\":\"category\",\"path\":\"category\",\"type\":\"string\"}},{{\"name\":\"status\",\"path\":\"status\",\"type\":\"string\"}},{{\"name\":\"tenant\",\"path\":\"tenant\",\"type\":\"string\"}}],\"measure_fields\":[{{\"name\":\"score\",\"path\":\"score\",\"type\":\"number\"}}],\"materializations\":[]}}", .{algebraic_index_name})
    else
        try alloc.dupe(u8, "");
    defer alloc.free(algebraic_entry);
    return try std.fmt.allocPrint(alloc, "{{\"{s}\":{{\"type\":\"embeddings\",\"dimension\":{d},\"distance_metric\":\"l2_squared\"}}{s}{s}{s}{s}}}", .{
        index_name,
        cfg.dims,
        sparse_entry,
        graph_entry,
        text_entry,
        algebraic_entry,
    });
}

fn benchDbSearch(alloc: std.mem.Allocator, db: *db_mod.DB, query_bodies: []const []const u8, cfg: Config) !QueryBenchStats {
    var stats: QueryBenchStats = .{};
    for (0..cfg.repeats) |_| {
        for (query_bodies, 0..) |body, i| {
            var owned = try api.query.parseQueryRequest(alloc, null, table_name, body);
            defer owned.deinit(alloc);
            owned.req.include_stored = false;
            if (cfg.query_shape.needsDefaultFullTextIndex() and owned.req.primary_text_index_name == null) {
                owned.req.primary_text_index_name = try alloc.dupe(u8, text_index_name);
            }
            const started = nowNs();
            var result = try db.search(alloc, owned.req);
            defer result.deinit();
            stats.total_ns += elapsedSince(started);
            stats.queries += 1;
            if (result.hits.len == 0) {
                const db_stats = try db.stats(alloc);
                defer db_mod.types.freeDBStats(alloc, db_stats);
                std.debug.print("public-query guardrail empty db result idx={d} query={d}\n", .{
                    stats.queries,
                    i,
                });
                std.debug.print("public-query guardrail db_stats docs={d} indexes={d}\n", .{
                    db_stats.doc_count,
                    db_stats.indexes.len,
                });
                return error.EmptyQueryResult;
            }
        }
    }
    return stats;
}

fn benchDirectHandler(
    alloc: std.mem.Allocator,
    executor: http_common.RequestExecutor,
    query_bodies: []const []const u8,
    cfg: Config,
) !QueryBenchStats {
    var stats: QueryBenchStats = .{};
    const uri = "/tables/" ++ table_name ++ "/query";
    for (0..cfg.repeats) |_| {
        for (query_bodies) |body| {
            const started = nowNs();
            var resp = try executor.execute(alloc, .{
                .method = .POST,
                .uri = uri,
                .content_type = "application/json",
                .body = body,
            });
            defer resp.deinit(alloc);
            const elapsed = elapsedSince(started);
            if (resp.status != 200) {
                std.debug.print("public-query guardrail direct-handler status={d} body={s}\n", .{
                    resp.status,
                    resp.body,
                });
                return error.UnexpectedHttpStatus;
            }
            var parsed = std.json.parseFromSlice(QueryResponseWire, alloc, resp.body, .{ .ignore_unknown_fields = true }) catch |err| {
                std.debug.print("public-query guardrail direct-handler parse error={s} body={s}\n", .{
                    @errorName(err),
                    resp.body,
                });
                return err;
            };
            defer parsed.deinit();
            try accumulateParsedResponse(&stats, parsed.value, elapsed, resp.body);
        }
    }
    return stats;
}

fn benchHandlerPipeline(
    alloc: std.mem.Allocator,
    server: *api.ApiHttpServer,
    source: api.TableReadSource,
    query_bodies: []const []const u8,
    cfg: Config,
) !HandlerPipelineStats {
    var stats: HandlerPipelineStats = .{};
    for (0..cfg.repeats) |_| {
        for (query_bodies) |body| {
            const parse_start = nowNs();
            var owned = try api.query.parseQueryRequest(alloc, null, table_name, body);
            defer owned.deinit(alloc);
            stats.parse_ns += elapsedSince(parse_start);

            const route_start = nowNs();
            try server.maybeRouteQueryToReadSchema(table_name, &owned.req);
            stats.route_ns += elapsedSince(route_start);

            const source_start = nowNs();
            var resp = (try source.query(alloc, table_name, owned.req, .read_index)) orelse return error.TableNotFound;
            defer resp.deinit(alloc);
            const source_elapsed = elapsedSince(source_start);
            stats.source_query_ns += source_elapsed;

            var parsed = try std.json.parseFromSlice(QueryResponseWire, alloc, resp.json, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            if (parsed.value.responses.len == 0) return error.InvalidQueryResponse;
            if (parsed.value.responses[0].profile) |profile| {
                if (profile.dense_search) |dense| {
                    stats.source_profile_total_ns += dense.total_ns;
                    stats.source_profile_hbc_ns += dense.hbc_search_ns;
                }
            }
            stats.queries += 1;
        }
    }
    return stats;
}

fn benchHttpQuery(
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    query_bodies: []const []const u8,
    cfg: Config,
) !QueryBenchStats {
    var executor = std_http_executor.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    var client = api.ApiHttpClient.init(alloc, executor.executor());

    var stats: QueryBenchStats = .{};
    for (0..cfg.repeats) |repeat_index| {
        for (query_bodies) |body| {
            const started = nowNs();
            var resp = client.fetchQuery(base_uri, table_name, body) catch |err| retry: {
                if (isRetryableQueryConnectionError(err)) {
                    break :retry try client.fetchQuery(base_uri, table_name, body);
                }
                return err;
            };
            defer resp.deinit(alloc);
            const elapsed = elapsedSince(started);
            var parsed = std.json.parseFromSlice(QueryResponseWire, alloc, resp.body, .{ .ignore_unknown_fields = true }) catch |err| {
                std.debug.print(
                    "public-query guardrail http-query parse error={s} content_type={s} body={s}\n",
                    .{ @errorName(err), resp.content_type orelse "(none)", resp.body },
                );
                return err;
            };
            defer parsed.deinit();
            try accumulateParsedResponse(&stats, parsed.value, elapsed, resp.body);
            if (repeat_index == 0) {
                stats.first_pass_ns += elapsed;
                stats.first_pass_queries += 1;
            } else {
                stats.later_pass_ns += elapsed;
                stats.later_pass_queries += 1;
            }
        }
    }
    return stats;
}

fn benchConcurrentDirectHandler(
    alloc: std.mem.Allocator,
    executor: http_common.RequestExecutor,
    query_bodies: []const []const u8,
    cfg: Config,
) !ConcurrentStats {
    const workers = try alloc.alloc(DirectHandlerWorkerContext, cfg.search_threads);
    defer alloc.free(workers);
    const threads = try alloc.alloc(std.Thread, cfg.search_threads);
    defer alloc.free(threads);

    for (workers, 0..) |*worker, i| {
        worker.* = .{
            .alloc = alloc,
            .executor = executor,
            .query_bodies = query_bodies,
            .repeats = cfg.repeats,
        };
        threads[i] = try std.Thread.spawn(.{ .stack_size = 512 * 1024 }, DirectHandlerWorkerContext.run, .{worker});
    }

    var combined: ConcurrentStats = .{};
    for (threads, workers) |thread, *worker| {
        thread.join();
        if (worker.err) |err| return err;
        combined.total_ns = @max(combined.total_ns, worker.stats.total_ns);
        combined.request_ns += worker.stats.request_ns;
        combined.max_request_ns = @max(combined.max_request_ns, worker.stats.max_request_ns);
        combined.queries += worker.stats.queries;
        combined.failures += worker.stats.failures;
    }
    return combined;
}

fn benchConcurrentHttpWithPolling(
    alloc: std.mem.Allocator,
    io: std.Io,
    base_uri: []const u8,
    query_bodies: []const []const u8,
    health_uri: []const u8,
    metrics_uri: []const u8,
    status_uri: ?[]const u8,
    cfg: Config,
    rss_pid: ?std.process.Child.Id,
) !ConcurrentRun {
    var stop = std.atomic.Value(bool).init(false);
    var health_poller = EndpointPollerContext{
        .alloc = alloc,
        .uri = health_uri,
        .kind = .health,
        .poll_interval_ms = cfg.poll_interval_ms,
        .stop = &stop,
    };
    var metrics_poller = EndpointPollerContext{
        .alloc = alloc,
        .uri = metrics_uri,
        .kind = .metrics,
        .poll_interval_ms = cfg.poll_interval_ms,
        .stop = &stop,
    };
    var status_poller: ?EndpointPollerContext = if (status_uri) |uri| .{
        .alloc = alloc,
        .uri = uri,
        .kind = .status,
        .poll_interval_ms = cfg.poll_interval_ms,
        .stop = &stop,
    } else null;
    const health_thread = try std.Thread.spawn(.{ .stack_size = 512 * 1024 }, EndpointPollerContext.run, .{&health_poller});
    const metrics_thread = try std.Thread.spawn(.{ .stack_size = 512 * 1024 }, EndpointPollerContext.run, .{&metrics_poller});
    const status_thread: ?std.Thread = blk: {
        if (status_poller) |*poller| {
            break :blk try std.Thread.spawn(.{ .stack_size = 512 * 1024 }, EndpointPollerContext.run, .{poller});
        }
        break :blk null;
    };
    var rss_poller: ?RssPollerContext = if (rss_pid) |pid| .{
        .alloc = alloc,
        .io = io,
        .pid = pid,
        .poll_interval_ms = cfg.poll_interval_ms,
        .stop = &stop,
    } else null;
    const rss_thread: ?std.Thread = if (rss_poller != null)
        try std.Thread.spawn(.{ .stack_size = 512 * 1024 }, RssPollerContext.run, .{&rss_poller.?})
    else
        null;

    const workers = try alloc.alloc(HttpWorkerContext, cfg.search_threads);
    defer alloc.free(workers);
    const threads = try alloc.alloc(std.Thread, cfg.search_threads);
    defer alloc.free(threads);

    for (workers, 0..) |*worker, i| {
        worker.* = .{
            .alloc = alloc,
            .base_uri = base_uri,
            .query_bodies = query_bodies,
            .repeats = cfg.repeats,
        };
        threads[i] = try std.Thread.spawn(.{ .stack_size = 512 * 1024 }, HttpWorkerContext.run, .{worker});
    }

    var combined: ConcurrentStats = .{};
    for (threads, workers) |thread, *worker| {
        thread.join();
        if (worker.err) |err| return err;
        combined.total_ns = @max(combined.total_ns, worker.stats.total_ns);
        combined.request_ns += worker.stats.request_ns;
        combined.max_request_ns = @max(combined.max_request_ns, worker.stats.max_request_ns);
        combined.queries += worker.stats.queries;
        combined.failures += worker.stats.failures;
    }

    stop.store(true, .release);
    health_thread.join();
    metrics_thread.join();
    if (status_thread) |thread| thread.join();
    if (rss_thread) |thread| thread.join();
    if (health_poller.err) |err| return err;
    if (metrics_poller.err) |err| return err;
    if (status_poller) |ctx| if (ctx.err) |err| return err;
    if (rss_poller) |*ctx| if (ctx.err) |err| return err;

    const polls = if (status_poller) |ctx|
        mergePollStats(&.{ health_poller.stats, metrics_poller.stats, ctx.stats })
    else
        mergePollStats(&.{ health_poller.stats, metrics_poller.stats });

    return .{
        .http = combined,
        .polls = polls,
        .rss_peak_bytes = if (rss_poller) |ctx| ctx.peak_rss_bytes else 0,
    };
}

fn accumulateParsedResponse(stats: *QueryBenchStats, parsed: QueryResponseWire, elapsed_ns: u64, raw_body: []const u8) !void {
    _ = try accumulateParsedResponseNoProfile(parsed, raw_body);
    if (parsed.responses.len == 0) return error.InvalidQueryResponse;
    const first = parsed.responses[0];
    stats.total_ns += elapsed_ns;
    stats.queries += 1;
    if (first.hits.hits) |hits| stats.response_hit_count += @intCast(hits.len);
    if (graphResultHasNodes(raw_body)) stats.response_hit_count += 1;
    if (first.profile) |profile| {
        stats.profile_response_count += 1;
        if (profile.dense_search) |dense| {
            stats.profile_dense_search_count += 1;
            stats.profile_total_ns += dense.total_ns;
            stats.profile_hbc_search_ns += dense.hbc_search_ns;
            stats.profile_hbc_runtime_txn_ns += dense.hbc_runtime_txn_ns;
            stats.profile_hbc_scratch_acquire_ns += dense.hbc_scratch_acquire_ns;
            stats.profile_hbc_node_cache_lookup_ns += dense.hbc_node_cache_lookup_ns;
            stats.profile_hbc_quantized_cache_lookup_ns += dense.hbc_quantized_cache_lookup_ns;
            stats.profile_hbc_reranked_vectors += dense.hbc_reranked_vectors;
            stats.profile_hbc_approx_candidate_count += dense.hbc_approx_candidate_count;
            stats.profile_hbc_rerank_candidate_count += dense.hbc_rerank_candidate_count;
            stats.profile_hbc_ambiguous_top_k_pairs += dense.hbc_ambiguous_top_k_pairs;
            stats.profile_hbc_ambiguous_boundary_pairs += dense.hbc_ambiguous_boundary_pairs;
            stats.profile_hbc_ambiguous_distance_over_hits += dense.hbc_ambiguous_distance_over_hits;
            stats.profile_hbc_ambiguous_distance_under_hits += dense.hbc_ambiguous_distance_under_hits;
            stats.profile_hbc_top_k_count += dense.hbc_top_k_count;
            stats.profile_returned_hit_count += dense.returned_hit_count;
            if (dense.hbc_full_rerank_due_to_threshold) stats.profile_hbc_full_rerank_due_to_threshold += 1;
            if (dense.hbc_boundary_pair) |pair| {
                stats.profile_hbc_boundary_pair_count += 1;
                stats.profile_hbc_boundary_left_distance_sum += pair.left.distance;
                stats.profile_hbc_boundary_left_error_sum += pair.left.error_bound;
                stats.profile_hbc_boundary_left_lower_sum += pair.left.lower_bound;
                stats.profile_hbc_boundary_left_upper_sum += pair.left.upper_bound;
                stats.profile_hbc_boundary_right_distance_sum += pair.right.distance;
                stats.profile_hbc_boundary_right_error_sum += pair.right.error_bound;
                stats.profile_hbc_boundary_right_lower_sum += pair.right.lower_bound;
                stats.profile_hbc_boundary_right_upper_sum += pair.right.upper_bound;
                stats.profile_hbc_boundary_distance_gap_sum += pair.distance_gap;
                stats.profile_hbc_boundary_interval_gap_sum += pair.interval_gap;
                stats.profile_hbc_boundary_left_error_max = @max(stats.profile_hbc_boundary_left_error_max, pair.left.error_bound);
                stats.profile_hbc_boundary_right_error_max = @max(stats.profile_hbc_boundary_right_error_max, pair.right.error_bound);
            }
            if (dense.hbc_ambiguous_boundary_pairs > 0) {
                stats.profile_hbc_boundary_tail_error_avg_sum += dense.hbc_boundary_tail_error_avg;
                stats.profile_hbc_boundary_tail_error_max = @max(stats.profile_hbc_boundary_tail_error_max, dense.hbc_boundary_tail_error_max);
                stats.profile_hbc_boundary_tail_distance_gap_avg_sum += dense.hbc_boundary_tail_distance_gap_avg;
                stats.profile_hbc_boundary_tail_distance_gap_min = @min(stats.profile_hbc_boundary_tail_distance_gap_min, dense.hbc_boundary_tail_distance_gap_min);
                stats.profile_hbc_boundary_tail_distance_gap_max = @max(stats.profile_hbc_boundary_tail_distance_gap_max, dense.hbc_boundary_tail_distance_gap_max);
                stats.profile_hbc_boundary_tail_interval_gap_avg_sum += dense.hbc_boundary_tail_interval_gap_avg;
                stats.profile_hbc_boundary_tail_interval_gap_min = @min(stats.profile_hbc_boundary_tail_interval_gap_min, dense.hbc_boundary_tail_interval_gap_min);
                stats.profile_hbc_boundary_tail_interval_gap_max = @max(stats.profile_hbc_boundary_tail_interval_gap_max, dense.hbc_boundary_tail_interval_gap_max);
            }
            stats.profile_index_lookup_ns += dense.index_lookup_ns;
            stats.profile_doc_key_ns += dense.doc_key_resolve_ns;
            stats.profile_project_ns += dense.load_projected_document_ns;
            stats.profile_postprocess_ns += dense.postprocess_ns;
            stats.profile_rerank_external_score_ns += dense.hbc_rerank_external_score_ns;
            stats.profile_rerank_vector_load_ns += dense.hbc_rerank_vector_load_ns;
            stats.profile_rerank_metadata_lookup_ns += dense.hbc_rerank_metadata_lookup_ns;
            stats.profile_rerank_artifact_key_ns += dense.hbc_rerank_artifact_key_ns;
            stats.profile_rerank_artifact_read_ns += dense.hbc_rerank_artifact_read_ns;
            stats.profile_rerank_artifact_decode_ns += dense.hbc_rerank_artifact_decode_ns;
            stats.profile_rerank_artifact_distance_ns += dense.hbc_rerank_artifact_distance_ns;
            stats.profile_rerank_lsm_cache_hits += dense.hbc_rerank_lsm_cache_hits;
            stats.profile_rerank_lsm_cache_misses += dense.hbc_rerank_lsm_cache_misses;
            stats.profile_rerank_distance_ns += dense.hbc_rerank_distance_ns;
            stats.profile_inline_metadata_hits += dense.inline_metadata_hits;
            stats.profile_fetched_metadata_hits += dense.fetched_metadata_hits;
            stats.profile_lookup_doc_key_hits += dense.lookup_doc_key_hits;
            if (dense.returned_hit_count == 0) {
                std.debug.print("public-query guardrail zero returned_hit_count body={s}\n", .{raw_body});
                return error.EmptyQueryResult;
            }
        }
    }
}

fn enforceSymbolicProfileGuardrail(cfg: Config, stats: QueryBenchStats) !void {
    if (!cfg.require_symbolic_profile) return;
    if (!cfg.with_algebraic or !cfg.query_shape.usesFilter()) return;

    const expected_queries: u64 = @intCast(cfg.queries * cfg.repeats);
    if (stats.queries != expected_queries) {
        std.debug.print(
            "public-query guardrail failed: symbolic profile query_count={d} expected={d}\n",
            .{ stats.queries, expected_queries },
        );
        return error.SymbolicProfileGuardrailFailed;
    }
    if (stats.profile_response_count != stats.queries) {
        std.debug.print(
            "public-query guardrail failed: symbolic profile missing profile responses profile_count={d} queries={d}\n",
            .{ stats.profile_response_count, stats.queries },
        );
        return error.SymbolicProfileGuardrailFailed;
    }
    if (stats.profile_dense_search_count != stats.queries) {
        std.debug.print(
            "public-query guardrail failed: symbolic profile missing dense_search profile dense_profile_count={d} queries={d}\n",
            .{ stats.profile_dense_search_count, stats.queries },
        );
        return error.SymbolicProfileGuardrailFailed;
    }
    if (stats.profile_returned_hit_count == 0) {
        std.debug.print("public-query guardrail failed: symbolic profile returned_hit_count=0\n", .{});
        return error.SymbolicProfileGuardrailFailed;
    }
    if (stats.profile_hbc_approx_candidate_count == 0 and
        stats.profile_hbc_rerank_candidate_count == 0 and
        stats.profile_hbc_reranked_vectors == 0 and
        stats.profile_hbc_top_k_count == 0)
    {
        std.debug.print(
            "public-query guardrail failed: symbolic profile has no HBC candidate/rerank counters approx={d} rerank_candidates={d} reranked_vectors={d} top_k={d}\n",
            .{
                stats.profile_hbc_approx_candidate_count,
                stats.profile_hbc_rerank_candidate_count,
                stats.profile_hbc_reranked_vectors,
                stats.profile_hbc_top_k_count,
            },
        );
        return error.SymbolicProfileGuardrailFailed;
    }
}

fn accumulateParsedResponseNoProfile(parsed: QueryResponseWire, raw_body: []const u8) !void {
    if (parsed.responses.len == 0) return error.InvalidQueryResponse;
    const first = parsed.responses[0];
    if ((first.hits.hits == null or first.hits.hits.?.len == 0) and !graphResultHasNodes(raw_body)) {
        std.debug.print("public-query guardrail empty response body={s}\n", .{raw_body});
        return error.EmptyQueryResult;
    }
}

fn graphResultHasNodes(raw_body: []const u8) bool {
    return std.mem.indexOf(u8, raw_body, "\"graph_results\"") != null and
        std.mem.indexOf(u8, raw_body, "\"nodes\":[{") != null;
}

fn enforceSymbolicResultFillGuardrail(cfg: Config, stats: QueryBenchStats) !void {
    if (!cfg.query_shape.usesFilter()) return;
    if (cfg.k == 0 or cfg.queries == 0 or cfg.repeats == 0) return;
    const expected = expectedSymbolicMatchStats(cfg);
    if (expected.min < cfg.k) return;

    const expected_returned: u64 = @intCast(cfg.queries * cfg.repeats * cfg.k);
    if (stats.response_hit_count >= expected_returned) return;

    std.debug.print(
        "public-query guardrail failed: symbolic result underfilled query_shape={s} returned={d} expected_at_least={d} expected_match_min={d} k={d}\n",
        .{
            cfg.query_shape.text(),
            stats.response_hit_count,
            expected_returned,
            expected.min,
            cfg.k,
        },
    );
    return error.SymbolicResultFillGuardrailFailed;
}

fn enforceGuardrails(cfg: Config, polls: PollStats) !void {
    const health_ms = @divTrunc(polls.health_max_latency_ns, std.time.ns_per_ms);
    const metrics_ms = @divTrunc(polls.metrics_max_latency_ns, std.time.ns_per_ms);
    const status_ms = @divTrunc(polls.status_max_latency_ns, std.time.ns_per_ms);
    if (health_ms > cfg.max_health_latency_ms) {
        std.debug.print("public-query guardrail failed: health_max_latency_ms={d} > limit={d}\n", .{
            health_ms,
            cfg.max_health_latency_ms,
        });
        return error.GuardrailFailed;
    }
    if (metrics_ms > cfg.max_metrics_latency_ms) {
        std.debug.print("public-query guardrail failed: metrics_max_latency_ms={d} > limit={d}\n", .{
            metrics_ms,
            cfg.max_metrics_latency_ms,
        });
        return error.GuardrailFailed;
    }
    if (status_ms > cfg.max_status_latency_ms) {
        std.debug.print("public-query guardrail failed: status_max_latency_ms={d} > limit={d}\n", .{
            status_ms,
            cfg.max_status_latency_ms,
        });
        return error.GuardrailFailed;
    }
    if (polls.health_failures > cfg.max_health_failures) {
        std.debug.print("public-query guardrail failed: health_failures={d} > limit={d}\n", .{
            polls.health_failures,
            cfg.max_health_failures,
        });
        return error.GuardrailFailed;
    }
    if (polls.metrics_failures > cfg.max_metrics_failures) {
        std.debug.print("public-query guardrail failed: metrics_failures={d} > limit={d}\n", .{
            polls.metrics_failures,
            cfg.max_metrics_failures,
        });
        return error.GuardrailFailed;
    }
    if (polls.status_failures > cfg.max_status_failures) {
        std.debug.print("public-query guardrail failed: status_failures={d} > limit={d}\n", .{
            polls.status_failures,
            cfg.max_status_failures,
        });
        return error.GuardrailFailed;
    }
}

fn makeDataset(alloc: std.mem.Allocator, cfg: Config) ![]f32 {
    const data = try alloc.alloc(f32, cfg.docs * cfg.dims);
    for (0..cfg.docs) |doc_idx| {
        const cluster = @as(f32, @floatFromInt(doc_idx % 8)) * 0.25;
        for (0..cfg.dims) |dim_idx| {
            data[doc_idx * cfg.dims + dim_idx] = cluster + deterministicNoise(cfg.seed, doc_idx, dim_idx);
        }
        _ = antfly.vector.normalize(data[doc_idx * cfg.dims ..][0..cfg.dims]);
    }
    return data;
}

fn makeQueries(alloc: std.mem.Allocator, dataset: []const f32, cfg: Config) ![]f32 {
    const queries = try alloc.alloc(f32, cfg.queries * cfg.dims);
    for (0..cfg.queries) |i| {
        const src_idx = (i * 997) % cfg.docs;
        @memcpy(queries[i * cfg.dims ..][0..cfg.dims], dataset[src_idx * cfg.dims ..][0..cfg.dims]);
    }
    return queries;
}

fn makeQueryBodies(alloc: std.mem.Allocator, queries: []const f32, cfg: Config) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, cfg.queries);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| alloc.free(@constCast(value));
    }
    for (0..cfg.queries) |i| {
        out[i] = try encodeQueryJson(alloc, queries[i * cfg.dims ..][0..cfg.dims], querySourceDocIndex(i, cfg), cfg);
        initialized += 1;
    }
    return out;
}

fn runSwarmBench(
    alloc: std.mem.Allocator,
    io: std.Io,
    cwd: []const u8,
    cfg: Config,
    dataset: []const f32,
    query_bodies: []const []const u8,
) !void {
    var root_buf: [256]u8 = undefined;
    const root_path = tempPath(&root_buf);
    var completed = false;
    defer {
        if (completed) {
            cleanupTempDir(root_path);
        } else {
            std.debug.print("public-query guardrail preserved failed swarm root={s}\n", .{root_path});
        }
    }

    const bind_port = try reserveEphemeralPort(io);
    var health_port = try reserveEphemeralPort(io);
    if (health_port == bind_port) health_port +%= 1;
    var metadata_port = try reserveEphemeralPort(io);
    if (metadata_port == bind_port or metadata_port == health_port) metadata_port +%= 2;
    var metadata_admin_port = try reserveEphemeralPort(io);
    if (metadata_admin_port == bind_port or metadata_admin_port == health_port or metadata_admin_port == metadata_port) metadata_admin_port +%= 3;
    var store_raft_port = try reserveEphemeralPort(io);
    if (store_raft_port == bind_port or store_raft_port == health_port or store_raft_port == metadata_port or store_raft_port == metadata_admin_port) store_raft_port +%= 4;

    var child = try spawnSwarm(alloc, io, cwd, cfg, root_path[0..root_path.len], bind_port, health_port, metadata_port, metadata_admin_port, store_raft_port);
    defer child.kill(io);

    const base_uri = try std.fmt.allocPrint(alloc, "http://{s}:{d}/api/v1", .{ cfg.bind_host, bind_port });
    defer alloc.free(base_uri);
    const health_uri = try std.fmt.allocPrint(alloc, "http://{s}:{d}/healthz", .{ cfg.bind_host, health_port });
    defer alloc.free(health_uri);
    const metrics_uri = try std.fmt.allocPrint(alloc, "http://{s}:{d}/metrics", .{ cfg.bind_host, health_port });
    defer alloc.free(metrics_uri);
    const index_status_uri = try std.fmt.allocPrint(alloc, "{s}/tables/{s}/indexes/{s}", .{ base_uri, table_name, index_name });
    defer alloc.free(index_status_uri);

    std.debug.print("public-query guardrail stage=swarm-start\n", .{});
    try waitForHttpOk(alloc, health_uri, cfg.startup_timeout_ms);

    std.debug.print("public-query guardrail stage=swarm-load\n", .{});
    const child_pid = child.id orelse return error.UnexpectedProcessExit;
    const load = try seedSwarm(alloc, io, base_uri, health_uri, metrics_uri, index_status_uri, child_pid, dataset, cfg);
    const load_memory = fetchMemoryBreakdown(alloc, base_uri, metrics_uri) catch |err| blk: {
        std.debug.print("public-query guardrail memory_breakdown_err phase=post_load err={s}\n", .{@errorName(err)});
        break :blk MemoryBreakdown{};
    };
    printMemoryBreakdown("post_load", load_memory);

    std.debug.print("public-query guardrail stage=http-query\n", .{});
    const http_stats = try benchHttpQuery(alloc, base_uri, query_bodies, cfg);
    try enforceSymbolicProfileGuardrail(cfg, http_stats);
    try enforceSymbolicResultFillGuardrail(cfg, http_stats);
    std.debug.print("public-query guardrail stage=http-concurrent\n", .{});
    const concurrent = try benchConcurrentHttpWithPolling(alloc, io, base_uri, query_bodies, health_uri, metrics_uri, index_status_uri, cfg, child.id);
    try maybeRunHttpSearchThreadSweep(alloc, io, base_uri, query_bodies, health_uri, metrics_uri, index_status_uri, cfg, child.id);

    const search_memory = fetchMemoryBreakdown(alloc, base_uri, metrics_uri) catch |err| blk: {
        std.debug.print("public-query guardrail memory_breakdown_err phase=post_search err={s}\n", .{@errorName(err)});
        break :blk MemoryBreakdown{};
    };

    const avg_http_ns = http_stats.avgNs();
    const avg_profile_ns = http_stats.avgProfileNs();
    std.debug.print(
        "public_query_guardrail mode=swarm server={s} query_shape={s} with_schema={} with_algebraic={} docs={d} dims={d} queries={d} repeats={d} k={d} threads={d} load_insert_ms={d:.2} index_wait_ms={d:.2} load_total_ms={d:.2} http={d:.3}us http_first_pass={d:.3}us http_later_pass={d:.3}us http_over_profile={d:.3}us profile_total={d:.3}us concurrent_qps={d:.2} concurrent_avg={d:.3}us concurrent_max={d:.3}us load_rss_peak_mb={d:.2} search_rss_peak_mb={d:.2}\n",
        .{
            @tagName(cfg.server_kind),
            cfg.query_shape.text(),
            cfg.with_schema,
            cfg.with_algebraic,
            cfg.docs,
            cfg.dims,
            cfg.queries,
            cfg.repeats,
            cfg.k,
            cfg.search_threads,
            nsToMs(load.insert_ns),
            nsToMs(load.visibility_wait_ns),
            nsToMs(load.total_ns),
            nsToUs(avg_http_ns),
            nsToUs(http_stats.avgFirstPassNs()),
            nsToUs(http_stats.avgLaterPassNs()),
            nsToUs(avg_http_ns -| avg_profile_ns),
            nsToUs(avg_profile_ns),
            concurrent.http.qps(),
            nsToUs(concurrent.http.avgRequestNs()),
            nsToUs(concurrent.http.max_request_ns),
            bytesToMiB(load.rss_peak_bytes),
            bytesToMiB(concurrent.rss_peak_bytes),
        },
    );
    printPublicQueryGuardrailSummaryJson(
        "swarm",
        @tagName(cfg.server_kind),
        cfg,
        0,
        0,
        http_stats,
        concurrent.http,
        load,
        load.rss_peak_bytes,
        concurrent.rss_peak_bytes,
        search_memory,
    );
    std.debug.print(
        "public_query_profile_breakdown profile_index={d:.3}us profile_hbc={d:.3}us profile_hbc_runtime_txn={d:.3}us profile_hbc_scratch_acquire={d:.3}us profile_hbc_node_cache_lookup={d:.3}us profile_hbc_quantized_cache_lookup={d:.3}us profile_doc_key={d:.3}us profile_project={d:.3}us profile_postprocess={d:.3}us profile_rerank_external_score={d:.3}us profile_rerank_load_compat={d:.3}us profile_rerank_metadata={d:.3}us profile_rerank_artifact_key={d:.3}us profile_rerank_artifact_read={d:.3}us profile_rerank_artifact_decode={d:.3}us profile_rerank_artifact_distance={d:.3}us profile_rerank_lsm_cache_hits={d:.2} profile_rerank_lsm_cache_misses={d:.2} profile_rerank_distance={d:.3}us metadata_inline={d:.2} metadata_fetched={d:.2} metadata_lookup={d:.2}\n",
        .{
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_index_lookup_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_hbc_search_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_hbc_runtime_txn_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_hbc_scratch_acquire_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_hbc_node_cache_lookup_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_hbc_quantized_cache_lookup_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_doc_key_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_project_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_postprocess_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_rerank_external_score_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_rerank_vector_load_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_rerank_metadata_lookup_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_rerank_artifact_key_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_rerank_artifact_read_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_rerank_artifact_decode_ns / http_stats.queries),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_rerank_artifact_distance_ns / http_stats.queries),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_rerank_lsm_cache_hits)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_rerank_lsm_cache_misses)) / @as(f64, @floatFromInt(http_stats.queries)),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_rerank_distance_ns / http_stats.queries),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_inline_metadata_hits)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_fetched_metadata_hits)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_lookup_doc_key_hits)) / @as(f64, @floatFromInt(http_stats.queries)),
        },
    );
    std.debug.print(
        "public_query_rerank_selection approx_candidates={d:.2} rerank_candidates={d:.2} reranked_vectors={d:.2} top_k_count={d:.2} ambiguous_top_k_pairs={d:.2} ambiguous_boundary_pairs={d:.2} ambiguous_distance_over_hits={d:.2} ambiguous_distance_under_hits={d:.2} full_rerank_threshold_rate={d:.4}\n",
        .{
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_approx_candidate_count)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_rerank_candidate_count)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_reranked_vectors)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_top_k_count)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_ambiguous_top_k_pairs)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_ambiguous_boundary_pairs)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_ambiguous_distance_over_hits)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_ambiguous_distance_under_hits)) / @as(f64, @floatFromInt(http_stats.queries)),
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_full_rerank_due_to_threshold)) / @as(f64, @floatFromInt(http_stats.queries)),
        },
    );
    printPublicQuerySymbolicFilterProfile(cfg, http_stats);
    std.debug.print(
        "public_query_rerank_boundary avg_left_distance={d:.6} avg_left_error={d:.6} avg_left_lower={d:.6} avg_left_upper={d:.6} avg_right_distance={d:.6} avg_right_error={d:.6} avg_right_lower={d:.6} avg_right_upper={d:.6} avg_distance_gap={d:.6} avg_interval_gap={d:.6} max_left_error={d:.6} max_right_error={d:.6} boundary_pair_rate={d:.4}\n",
        .{
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_left_distance_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_left_error_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_left_lower_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_left_upper_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_right_distance_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_right_error_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_right_lower_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_right_upper_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_distance_gap_sum),
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_interval_gap_sum),
            http_stats.profile_hbc_boundary_left_error_max,
            http_stats.profile_hbc_boundary_right_error_max,
            if (http_stats.queries == 0) 0 else @as(f64, @floatFromInt(http_stats.profile_hbc_boundary_pair_count)) / @as(f64, @floatFromInt(http_stats.queries)),
        },
    );
    std.debug.print(
        "public_query_rerank_boundary_tail avg_error={d:.6} max_error={d:.6} avg_distance_gap={d:.6} min_distance_gap={d:.6} max_distance_gap={d:.6} avg_interval_gap={d:.6} min_interval_gap={d:.6} max_interval_gap={d:.6}\n",
        .{
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_tail_error_avg_sum),
            http_stats.profile_hbc_boundary_tail_error_max,
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_tail_distance_gap_avg_sum),
            if (http_stats.profile_hbc_boundary_tail_distance_gap_min == std.math.floatMax(f64)) 0 else http_stats.profile_hbc_boundary_tail_distance_gap_min,
            if (http_stats.profile_hbc_boundary_tail_distance_gap_max == -std.math.floatMax(f64)) 0 else http_stats.profile_hbc_boundary_tail_distance_gap_max,
            http_stats.avgBoundary(http_stats.profile_hbc_boundary_tail_interval_gap_avg_sum),
            if (http_stats.profile_hbc_boundary_tail_interval_gap_min == std.math.floatMax(f64)) 0 else http_stats.profile_hbc_boundary_tail_interval_gap_min,
            if (http_stats.profile_hbc_boundary_tail_interval_gap_max == -std.math.floatMax(f64)) 0 else http_stats.profile_hbc_boundary_tail_interval_gap_max,
        },
    );
    std.debug.print(
        "public_query_visibility published_doc_count={d} query_visible_doc_count={d} total_indexed={d} published_node_count={d} published_root_node={d} runtime_fresh={any} publish_pending={any} query_visible_ready={any} accepted_sequence={d} replay_applied_sequence={d} replay_caught_up={any} replay_catch_up_required={any} backfill_active={any} rebuilding={any}\n",
        .{
            load.visibility.published_doc_count,
            load.visibility.query_visible_doc_count,
            load.visibility.total_indexed,
            load.visibility.published_node_count,
            load.visibility.published_root_node,
            load.visibility.runtime_fresh,
            load.visibility.dense_publish_pending,
            load.visibility.publishedReady(cfg.docs),
            load.visibility.replay_target_sequence,
            load.visibility.replay_applied_sequence,
            load.visibility.replayCaughtUp(),
            load.visibility.replay_catch_up_required,
            load.visibility.backfill_active,
            load.visibility.rebuilding,
        },
    );
    std.debug.print(
        "public_query_health load_health_max_ms={d:.2} load_metrics_max_ms={d:.2} load_status_max_ms={d:.2} load_health_failures={d} load_metrics_failures={d} load_status_failures={d} health_max_ms={d:.2} metrics_max_ms={d:.2} status_max_ms={d:.2} health_failures={d} metrics_failures={d} status_failures={d}\n",
        .{
            nsToMs(load.polls.health_max_latency_ns),
            nsToMs(load.polls.metrics_max_latency_ns),
            nsToMs(load.polls.status_max_latency_ns),
            load.polls.health_failures,
            load.polls.metrics_failures,
            load.polls.status_failures,
            nsToMs(concurrent.polls.health_max_latency_ns),
            nsToMs(concurrent.polls.metrics_max_latency_ns),
            nsToMs(concurrent.polls.status_max_latency_ns),
            concurrent.polls.health_failures,
            concurrent.polls.metrics_failures,
            concurrent.polls.status_failures,
        },
    );
    printMemoryBreakdown("post_search", search_memory);
    try enforceGuardrails(cfg, load.polls);
    try enforceGuardrails(cfg, concurrent.polls);
    completed = true;
}

fn spawnSwarm(
    alloc: std.mem.Allocator,
    io: std.Io,
    cwd: []const u8,
    cfg: Config,
    root_path: []const u8,
    bind_port: u16,
    health_port: u16,
    metadata_port: u16,
    metadata_admin_port: u16,
    store_raft_port: u16,
) !std.process.Child {
    const bind_port_arg = try std.fmt.allocPrint(alloc, "{d}", .{bind_port});
    defer alloc.free(bind_port_arg);
    const health_port_arg = try std.fmt.allocPrint(alloc, "{d}", .{health_port});
    defer alloc.free(health_port_arg);
    const store_raft_port_arg = try std.fmt.allocPrint(alloc, "{d}", .{store_raft_port});
    defer alloc.free(store_raft_port_arg);
    const replica_root = try std.fmt.allocPrint(alloc, "{s}/replicas", .{root_path});
    defer alloc.free(replica_root);
    const replica_catalog = try std.fmt.allocPrint(alloc, "{s}/catalog.txt", .{root_path});
    defer alloc.free(replica_catalog);
    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}/snapshots", .{root_path});
    defer alloc.free(snapshot_root);
    const config_path = try std.fmt.allocPrint(alloc, "{s}/config.json", .{root_path});
    defer alloc.free(config_path);
    const config_json = try std.fmt.allocPrint(
        alloc,
        "{{\"metadata\":{{}},\"storage\":{{\"local\":{{\"base_dir\":\"{s}\"}}}},\"replication_factor\":1,\"default_shards_per_table\":1,\"max_shard_size_bytes\":1099511627776,\"max_shards_per_table\":1}}",
        .{root_path},
    );
    defer alloc.free(config_json);
    try std.Io.Dir.cwd().createDirPath(io, root_path);
    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = config_path,
        .data = config_json,
    });

    switch (cfg.server_kind) {
        .zig => {
            return try std.process.spawn(io, .{
                .argv = &.{
                    cfg.swarm_binary,
                    "swarm",
                    "--config",
                    config_path,
                    "--host",
                    cfg.bind_host,
                    "--port",
                    bind_port_arg,
                    "--health-port",
                    health_port_arg,
                    "--tick-ms",
                    "5",
                    "--replica-root-dir",
                    replica_root,
                    "--replica-catalog-path",
                    replica_catalog,
                    "--snapshot-root-dir",
                    snapshot_root,
                },
                .cwd = .{ .path = cwd },
                .stdin = .ignore,
                .stdout = .inherit,
                .stderr = .inherit,
            });
        },
        .go => {
            const metadata_api_url = try std.fmt.allocPrint(alloc, "http://{s}:{d}", .{ cfg.bind_host, metadata_admin_port });
            defer alloc.free(metadata_api_url);
            const metadata_raft = try std.fmt.allocPrint(alloc, "http://{s}:{d}", .{ cfg.bind_host, metadata_port });
            defer alloc.free(metadata_raft);
            const store_api = try std.fmt.allocPrint(alloc, "http://{s}:{d}", .{ cfg.bind_host, bind_port });
            defer alloc.free(store_api);
            const store_raft = try std.fmt.allocPrint(alloc, "http://{s}:{d}", .{ cfg.bind_host, store_raft_port });
            defer alloc.free(store_raft);
            const metadata_cluster = try std.fmt.allocPrint(alloc, "{{\"1\":\"{s}\"}}", .{metadata_raft});
            defer alloc.free(metadata_cluster);
            var env_map = std.process.Environ.Map.init(alloc);
            defer env_map.deinit();
            try env_map.put("ANTFLY_DATA_DIR", root_path);
            try env_map.put("ANTFLY_STORAGE_LOCAL_BASE_DIR", root_path);
            try env_map.put("ANTFLY_HEALTH_PORT", health_port_arg);
            try env_map.put("ANTFLY_LOG_STYLE", "logfmt");
            try env_map.put("ANTFLY_SWARM_TERMITE", "false");

            return try std.process.spawn(io, .{
                .argv = &.{
                    cfg.swarm_binary,
                    "--data-dir",
                    root_path,
                    "--log-style",
                    "logfmt",
                    "swarm",
                    "--metadata-api",
                    metadata_api_url,
                    "--metadata-raft",
                    metadata_raft,
                    "--metadata-cluster",
                    metadata_cluster,
                    "--store-api",
                    store_api,
                    "--store-raft",
                    store_raft,
                    "--health-port",
                    health_port_arg,
                    "--termite=false",
                },
                .cwd = .{ .path = cwd },
                .environ_map = &env_map,
                .stdin = .ignore,
                .stdout = .inherit,
                .stderr = .inherit,
            });
        },
    }
}

fn seedSwarm(
    alloc: std.mem.Allocator,
    io: std.Io,
    base_uri: []const u8,
    health_uri: []const u8,
    metrics_uri: []const u8,
    status_uri: []const u8,
    rss_pid: std.process.Child.Id,
    dataset: []const f32,
    cfg: Config,
) !LoadRun {
    var executor = std_http_executor.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    var client = api.ApiHttpClient.init(alloc, executor.executor());

    const create_table_body = if (cfg.with_schema or cfg.query_shape == .algebraic_filter)
        try std.fmt.allocPrint(alloc, "{{\"num_shards\":1,\"description\":\"public query swarm guardrail\",\"schema\":{s}}}", .{benchmark_schema_json})
    else
        try std.fmt.allocPrint(alloc, "{{\"num_shards\":1,\"description\":\"public query swarm guardrail\"}}", .{});
    defer alloc.free(create_table_body);
    const create_table_path = try std.fmt.allocPrint(alloc, "/tables/{s}", .{table_name});
    defer alloc.free(create_table_path);
    const created = try postJsonExpect(alloc, executor.executor(), base_uri, create_table_path, create_table_body, &.{ 200, 201 });
    defer alloc.free(created);

    const create_index_body = try std.fmt.allocPrint(
        alloc,
        "{{\"name\":\"{s}\",\"type\":\"embeddings\",\"external\":true,\"dimension\":{d}}}",
        .{ index_name, cfg.dims },
    );
    defer alloc.free(create_index_body);
    const create_index_path = try std.fmt.allocPrint(alloc, "/tables/{s}/indexes/{s}", .{ table_name, index_name });
    defer alloc.free(create_index_path);
    const created_index = try postJsonExpect(alloc, executor.executor(), base_uri, create_index_path, create_index_body, &.{ 200, 201 });
    defer alloc.free(created_index);

    if (cfg.query_shape.usesFullText()) {
        const create_text_index_body = try std.fmt.allocPrint(alloc, "{{\"name\":\"{s}\",\"type\":\"full_text\"}}", .{text_index_name});
        defer alloc.free(create_text_index_body);
        const create_text_index_path = try std.fmt.allocPrint(alloc, "/tables/{s}/indexes/{s}", .{ table_name, text_index_name });
        defer alloc.free(create_text_index_path);
        const created_text_index = try postJsonExpect(alloc, executor.executor(), base_uri, create_text_index_path, create_text_index_body, &.{ 200, 201 });
        defer alloc.free(created_text_index);
    }

    if (cfg.with_sparse or cfg.query_shape.usesSparse()) {
        const create_sparse_index_body = try std.fmt.allocPrint(alloc, "{{\"name\":\"{s}\",\"type\":\"embeddings\",\"sparse\":true,\"external\":true}}", .{sparse_index_name});
        defer alloc.free(create_sparse_index_body);
        const create_sparse_index_path = try std.fmt.allocPrint(alloc, "/tables/{s}/indexes/{s}", .{ table_name, sparse_index_name });
        defer alloc.free(create_sparse_index_path);
        const created_sparse_index = try postJsonExpect(alloc, executor.executor(), base_uri, create_sparse_index_path, create_sparse_index_body, &.{ 200, 201 });
        defer alloc.free(created_sparse_index);
    }

    if (cfg.with_graph or cfg.query_shape.usesGraph()) {
        const create_graph_index_body = try std.fmt.allocPrint(alloc, "{{\"name\":\"{s}\",\"type\":\"graph\"}}", .{graph_index_name});
        defer alloc.free(create_graph_index_body);
        const create_graph_index_path = try std.fmt.allocPrint(alloc, "/tables/{s}/indexes/{s}", .{ table_name, graph_index_name });
        defer alloc.free(create_graph_index_path);
        const created_graph_index = try postJsonExpect(alloc, executor.executor(), base_uri, create_graph_index_path, create_graph_index_body, &.{ 200, 201 });
        defer alloc.free(created_graph_index);
    }

    if (cfg.with_algebraic or cfg.query_shape == .algebraic_filter) {
        const create_algebraic_index_body = try std.fmt.allocPrint(alloc, "{{\"name\":\"{s}\",\"type\":\"algebraic\",\"derive_from_schema\":true}}", .{algebraic_index_name});
        defer alloc.free(create_algebraic_index_body);
        const create_algebraic_index_path = try std.fmt.allocPrint(alloc, "/tables/{s}/indexes/{s}", .{ table_name, algebraic_index_name });
        defer alloc.free(create_algebraic_index_path);
        const created_algebraic_index = try postJsonExpect(alloc, executor.executor(), base_uri, create_algebraic_index_path, create_algebraic_index_body, &.{ 200, 201 });
        defer alloc.free(created_algebraic_index);
    }

    var stop = std.atomic.Value(bool).init(false);
    var health_poller = EndpointPollerContext{
        .alloc = alloc,
        .uri = health_uri,
        .kind = .health,
        .poll_interval_ms = cfg.poll_interval_ms,
        .stop = &stop,
    };
    var metrics_poller = EndpointPollerContext{
        .alloc = alloc,
        .uri = metrics_uri,
        .kind = .metrics,
        .poll_interval_ms = cfg.poll_interval_ms,
        .stop = &stop,
    };
    var status_poller = EndpointPollerContext{
        .alloc = alloc,
        .uri = status_uri,
        .kind = .status,
        .poll_interval_ms = cfg.poll_interval_ms,
        .stop = &stop,
    };
    const health_thread = try std.Thread.spawn(.{ .stack_size = 512 * 1024 }, EndpointPollerContext.run, .{&health_poller});
    const metrics_thread = try std.Thread.spawn(.{ .stack_size = 512 * 1024 }, EndpointPollerContext.run, .{&metrics_poller});
    const status_thread = try std.Thread.spawn(.{ .stack_size = 512 * 1024 }, EndpointPollerContext.run, .{&status_poller});
    var rss_poller = RssPollerContext{
        .alloc = alloc,
        .io = io,
        .pid = rss_pid,
        .poll_interval_ms = cfg.poll_interval_ms,
        .stop = &stop,
    };
    const rss_thread = try std.Thread.spawn(.{ .stack_size = 512 * 1024 }, RssPollerContext.run, .{&rss_poller});
    var pollers_joined = false;
    defer {
        if (!pollers_joined) {
            stop.store(true, .release);
            health_thread.join();
            metrics_thread.join();
            status_thread.join();
            rss_thread.join();
        }
    }

    const load_started_ns = nowNs();
    const insert_started_ns = load_started_ns;
    var start: usize = 0;
    var window_docs: usize = 0;
    var window_ns: u64 = 0;
    var window_max_batch_ns: u64 = 0;
    var batch_count: usize = 0;
    while (start < cfg.docs) : (start += cfg.batch_size) {
        {
            const end = @min(start + cfg.batch_size, cfg.docs);
            const batch_body = try encodeBatchInsertJson(alloc, dataset, start, end, cfg);
            defer alloc.free(batch_body);
            const batch_path = try std.fmt.allocPrint(alloc, "/tables/{s}/batch", .{table_name});
            defer alloc.free(batch_path);
            const batch_started_ns = nowNs();
            const batch = postJsonExpect(alloc, executor.executor(), base_uri, batch_path, batch_body, &.{ 200, 201 }) catch |err| {
                std.debug.print("public-query guardrail swarm batch failed start={d} end={d} err={s}\n", .{
                    start,
                    end,
                    @errorName(err),
                });
                std.debug.print("public-query guardrail swarm load poll status health_samples={d} metrics_samples={d} status_samples={d} health_failures={d} metrics_failures={d} status_failures={d} health_max_ms={d:.2} metrics_max_ms={d:.2} status_max_ms={d:.2} rss_peak_mb={d:.2}\n", .{
                    health_poller.stats.health_samples,
                    metrics_poller.stats.metrics_samples,
                    status_poller.stats.status_samples,
                    health_poller.stats.health_failures,
                    metrics_poller.stats.metrics_failures,
                    status_poller.stats.status_failures,
                    nsToMs(health_poller.stats.health_max_latency_ns),
                    nsToMs(metrics_poller.stats.metrics_max_latency_ns),
                    nsToMs(status_poller.stats.status_max_latency_ns),
                    bytesToMiB(rss_poller.peak_rss_bytes),
                });
                try printSwarmLoadFailureDiagnostics(alloc, &client, base_uri, health_uri);
                return err;
            };
            const batch_ns = elapsedSince(batch_started_ns);
            batch_count += 1;
            window_docs += end - start;
            window_ns += batch_ns;
            window_max_batch_ns = @max(window_max_batch_ns, batch_ns);
            defer alloc.free(batch);
            if (cfg.load_progress_interval > 0 and (end == cfg.docs or end % cfg.load_progress_interval == 0)) {
                try printSwarmLoadSample(
                    alloc,
                    &client,
                    base_uri,
                    metrics_uri,
                    end,
                    cfg.docs,
                    batch_count,
                    window_docs,
                    window_ns,
                    window_max_batch_ns,
                );
                window_docs = 0;
                window_ns = 0;
                window_max_batch_ns = 0;
            }
        }
    }
    const insert_ns = elapsedSince(insert_started_ns);

    const visibility_started_ns = nowNs();
    const visibility = try waitForQueryIndexesReady(alloc, base_uri, cfg.docs, cfg.index_ready_timeout_ms, cfg);
    const visibility_wait_ns = elapsedSince(visibility_started_ns);
    stop.store(true, .release);
    health_thread.join();
    metrics_thread.join();
    status_thread.join();
    rss_thread.join();
    pollers_joined = true;
    if (health_poller.err) |err| return err;
    if (metrics_poller.err) |err| return err;
    if (status_poller.err) |err| return err;
    if (rss_poller.err) |err| return err;
    const polls = mergePollStats(&.{ health_poller.stats, metrics_poller.stats, status_poller.stats });
    return .{
        .visibility = visibility,
        .polls = polls,
        .rss_peak_bytes = rss_poller.peak_rss_bytes,
        .insert_ns = insert_ns,
        .visibility_wait_ns = visibility_wait_ns,
        .total_ns = elapsedSince(load_started_ns),
    };
}

fn postJsonExpect(
    alloc: std.mem.Allocator,
    executor: http_common.RequestExecutor,
    base_uri: []const u8,
    path: []const u8,
    body: []const u8,
    expected_statuses: []const u16,
) ![]u8 {
    const uri = try std.fmt.allocPrint(alloc, "{s}{s}", .{ base_uri, path });
    defer alloc.free(uri);
    var resp = try executor.execute(alloc, .{
        .method = .POST,
        .uri = uri,
        .content_type = "application/json",
        .body = body,
    });
    defer resp.deinit(alloc);
    for (expected_statuses) |status| {
        if (resp.status == status) return try alloc.dupe(u8, resp.body);
    }
    std.debug.print("public-query guardrail unexpected status={d} uri={s} body={s}\n", .{ resp.status, uri, resp.body });
    return error.UnexpectedHttpStatus;
}

fn printSwarmLoadFailureDiagnostics(
    alloc: std.mem.Allocator,
    client: *api.ApiHttpClient,
    base_uri: []const u8,
    health_uri: []const u8,
) !void {
    var health_executor = std_http_executor.StdHttpExecutor.init(alloc, .{});
    defer health_executor.deinit();
    var health = health_executor.executor().execute(alloc, .{
        .method = .GET,
        .uri = health_uri,
    }) catch |err| {
        std.debug.print("public-query guardrail swarm failure health_err={s}\n", .{@errorName(err)});
        return;
    };
    defer health.deinit(alloc);
    std.debug.print("public-query guardrail swarm failure health_status={d} body={s}\n", .{ health.status, health.body });

    const visibility = fetchDenseVisibilitySnapshot(alloc, client, base_uri) catch |err| {
        std.debug.print("public-query guardrail swarm failure index_status_err={s}\n", .{@errorName(err)});
        return;
    };
    std.debug.print("public-query guardrail swarm failure index_status doc_count={d} total_indexed={d} node_count={d} root_node={d} replay_applied={d} replay_target={d} replay_required={any} backfill_active={any} rebuilding={any}\n", .{
        visibility.doc_count,
        visibility.total_indexed,
        visibility.node_count,
        visibility.root_node,
        visibility.replay_applied_sequence,
        visibility.replay_target_sequence,
        visibility.replay_catch_up_required,
        visibility.backfill_active,
        visibility.rebuilding,
    });
}

fn encodeBatchInsertJson(
    alloc: std.mem.Allocator,
    dataset: []const f32,
    start: usize,
    end: usize,
    cfg: Config,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.print(alloc, "{{\"sync_level\":\"{s}\",\"inserts\":{{", .{db_mod.types.publicSyncLevelText(cfg.sync_level)});
    for (start..end) |doc_idx| {
        if (doc_idx != start) try out.append(alloc, ',');
        try out.print(alloc, "\"doc:{d:0>8}\":", .{doc_idx});
        try appendVectorDocJson(&out, alloc, dataset[doc_idx * cfg.dims ..][0..cfg.dims], doc_idx, cfg);
    }
    try out.appendSlice(alloc, "}}");
    return out.toOwnedSlice(alloc);
}

fn appendVectorDocJson(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, vector: []const f32, doc_idx: usize, cfg: Config) !void {
    try out.appendSlice(alloc, "{\"_embeddings\":{\"" ++ index_name ++ "\":\"");
    try appendPackedF32Base64(out, alloc, vector);
    try out.append(alloc, '"');
    if (cfg.with_sparse or cfg.query_shape.usesSparse()) {
        try out.appendSlice(alloc, ",\"" ++ sparse_index_name ++ "\":{");
        try appendSparseEmbeddingObject(out, alloc, doc_idx);
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "},\"title\":\"Document ");
    try out.print(alloc, "{d:0>8}", .{doc_idx});
    try out.appendSlice(alloc, "\",\"body\":\"");
    try appendDocBody(out, alloc, doc_idx);
    try out.appendSlice(alloc, "\",\"category\":\"");
    try out.appendSlice(alloc, docCategory(doc_idx));
    try out.appendSlice(alloc, "\",\"status\":\"");
    try out.appendSlice(alloc, docStatus(doc_idx));
    try out.appendSlice(alloc, "\",\"tenant\":\"");
    try out.appendSlice(alloc, docTenant(doc_idx));
    try out.appendSlice(alloc, "\",\"score\":");
    try out.print(alloc, "{d}", .{docScore(doc_idx)});
    if (cfg.with_graph or cfg.query_shape.usesGraph()) {
        try out.appendSlice(alloc, ",\"_edges\":{\"" ++ graph_index_name ++ "\":{\"cites\":[{\"target\":\"");
        try out.print(alloc, "doc:{d:0>8}", .{(doc_idx + 1) % cfg.docs});
        try out.appendSlice(alloc, "\",\"weight\":1.0}]}}");
    }
    try out.append(alloc, '}');
}

fn waitForHttpOk(alloc: std.mem.Allocator, uri: []const u8, timeout_ms: u64) !void {
    var executor = std_http_executor.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    const deadline = nowNs() + timeout_ms * std.time.ns_per_ms;
    var successes: usize = 0;
    while (nowNs() < deadline) {
        var resp = executor.executor().execute(alloc, .{
            .method = .GET,
            .uri = uri,
        }) catch {
            successes = 0;
            sleepMs(100);
            continue;
        };
        defer resp.deinit(alloc);
        if (resp.status == 200) {
            successes += 1;
            if (successes >= 2) return;
        } else {
            successes = 0;
        }
        sleepMs(100);
    }
    return error.Timeout;
}

fn fetchDenseVisibilitySnapshot(
    alloc: std.mem.Allocator,
    client: *api.ApiHttpClient,
    base_uri: []const u8,
) !VisibilitySnapshot {
    return fetchIndexVisibilitySnapshot(alloc, client, base_uri, index_name);
}

fn fetchIndexVisibilitySnapshot(
    alloc: std.mem.Allocator,
    client: *api.ApiHttpClient,
    base_uri: []const u8,
    target_index_name: []const u8,
) !VisibilitySnapshot {
    var detail = try client.fetchTableIndex(base_uri, table_name, target_index_name);
    defer detail.deinit(alloc);
    var parsed = try std.json.parseFromSlice(IndexStatusWire, alloc, detail.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value.status) |status| {
        const total_indexed = status.total_indexed orelse 0;
        const doc_count = status.doc_count orelse total_indexed;
        const query_visible_doc_count = status.query_visible_doc_count orelse doc_count;
        const published_doc_count = status.published_doc_count orelse query_visible_doc_count;
        const node_count = status.node_count orelse (status.total_nodes orelse 0);
        const published_node_count = status.published_node_count orelse node_count;
        const root_node = status.root_node orelse 0;
        const published_root_node = status.published_root_node orelse root_node;
        return .{
            .doc_count = doc_count,
            .total_indexed = total_indexed,
            .node_count = node_count,
            .root_node = root_node,
            .query_visible_doc_count = query_visible_doc_count,
            .published_doc_count = published_doc_count,
            .published_node_count = published_node_count,
            .published_root_node = published_root_node,
            .dense_publish_pending = status.dense_publish_pending orelse false,
            .replay_target_sequence = status.replay_target_sequence orelse 0,
            .replay_applied_sequence = status.replay_applied_sequence orelse 0,
            .replay_catch_up_required = status.replay_catch_up_required orelse false,
            .runtime_fresh = status.runtime_fresh orelse false,
            .rebuilding = status.rebuilding orelse false,
            .backfill_active = status.backfill_active orelse false,
            .hbc_total_bytes = if (status.hbc_cache) |cache| cache.total_bytes orelse 0 else 0,
            .hbc_accounted_bytes = if (status.hbc_cache) |cache| cache.accounted_bytes orelse 0 else 0,
            .hbc_node_bytes = if (status.hbc_cache) |cache| if (cache.node) |kind| kind.used_bytes orelse 0 else 0 else 0,
            .hbc_quantized_bytes = if (status.hbc_cache) |cache| if (cache.quantized) |kind| kind.used_bytes orelse 0 else 0 else 0,
            .hbc_vector_bytes = if (status.hbc_cache) |cache| if (cache.vector) |kind| kind.used_bytes orelse 0 else 0 else 0,
            .hbc_metadata_bytes = if (status.hbc_cache) |cache| if (cache.metadata) |kind| kind.used_bytes orelse 0 else 0 else 0,
        };
    }
    return .{};
}

fn printSwarmLoadSample(
    alloc: std.mem.Allocator,
    client: *api.ApiHttpClient,
    base_uri: []const u8,
    metrics_uri: []const u8,
    loaded_docs: usize,
    total_docs: usize,
    batch_count: usize,
    window_docs: usize,
    window_ns: u64,
    window_max_batch_ns: u64,
) !void {
    const status_started_ns = nowNs();
    const visibility = try fetchDenseVisibilitySnapshot(alloc, client, base_uri);
    const status_ns = elapsedSince(status_started_ns);
    const metrics_started_ns = nowNs();
    const metrics = try fetchMemoryBreakdownFromMetrics(alloc, metrics_uri);
    const metrics_ns = elapsedSince(metrics_started_ns);
    std.debug.print(
        "public_query_load_sample docs={d}/{d} batches={d} window_docs={d} window_ms={d:.2} window_ns_per_doc={d} max_batch_ms={d:.2} status_ms={d:.2} metrics_ms={d:.2} published_docs={d} query_visible_docs={d} total_indexed={d} published_nodes={d} root={d} pending={any} replay={d}/{d} replay_required={any} runtime_fresh={any}\n",
        .{
            loaded_docs,
            total_docs,
            batch_count,
            window_docs,
            nsToMs(window_ns),
            if (window_docs == 0) 0 else @divTrunc(window_ns, window_docs),
            nsToMs(window_max_batch_ns),
            nsToMs(status_ns),
            nsToMs(metrics_ns),
            visibility.published_doc_count,
            visibility.query_visible_doc_count,
            visibility.total_indexed,
            visibility.published_node_count,
            visibility.published_root_node,
            visibility.dense_publish_pending,
            visibility.replay_applied_sequence,
            visibility.replay_target_sequence,
            visibility.replay_catch_up_required,
            visibility.runtime_fresh,
        },
    );
    std.debug.print(
        "public_query_load_resources docs={d}/{d} hbc_total_mb={d:.2} hbc_accounted_mb={d:.2} hbc_node_mb={d:.2} hbc_quantized_mb={d:.2} hbc_vector_mb={d:.2} hbc_metadata_mb={d:.2} rss_mb={d:.2} lsm_cache_mb={d:.2} rm_lsm_compaction_mb={d:.2}/{d:.2} rm_dense_apply_mb={d:.2}/{d:.2} rm_replay_window_mb={d:.2}/{d:.2}\n",
        .{
            loaded_docs,
            total_docs,
            bytesToMiB64(visibility.hbc_total_bytes),
            bytesToMiB64(visibility.hbc_accounted_bytes),
            bytesToMiB64(visibility.hbc_node_bytes),
            bytesToMiB64(visibility.hbc_quantized_bytes),
            bytesToMiB64(visibility.hbc_vector_bytes),
            bytesToMiB64(visibility.hbc_metadata_bytes),
            bytesToMiB64(metrics.process_resident_bytes),
            bytesToMiB64(metrics.lsm_cache_used_bytes),
            bytesToMiB64(metrics.rm_lsm_compaction_used_bytes),
            bytesToMiB64(metrics.rm_lsm_compaction_peak_bytes),
            bytesToMiB64(metrics.rm_dense_apply_used_bytes),
            bytesToMiB64(metrics.rm_dense_apply_peak_bytes),
            bytesToMiB64(metrics.rm_replay_window_used_bytes),
            bytesToMiB64(metrics.rm_replay_window_peak_bytes),
        },
    );
}

fn fetchMemoryBreakdownFromMetrics(
    alloc: std.mem.Allocator,
    metrics_uri: []const u8,
) !MemoryBreakdown {
    var out = MemoryBreakdown{};
    const metrics = try fetchMetricsBody(alloc, metrics_uri);
    defer alloc.free(metrics);

    out.process_resident_bytes = promMetricValue(metrics, "antfly_process_resident_bytes") orelse 0;
    out.process_footprint_bytes = promMetricValue(metrics, "antfly_process_footprint_bytes") orelse 0;
    out.process_wired_bytes = promMetricValue(metrics, "antfly_process_wired_bytes") orelse 0;
    out.lsm_cache_used_bytes = promMetricValue(metrics, "antfly_lsm_cache_used_bytes") orelse 0;
    out.rm_lsm_compaction_used_bytes = promResourceSliceValue(metrics, "antfly_resource_used_bytes", "lsm.compaction_work") orelse 0;
    out.rm_lsm_compaction_peak_bytes = promResourceSliceValue(metrics, "antfly_resource_peak_bytes", "lsm.compaction_work") orelse 0;
    out.rm_dense_apply_used_bytes = promResourceSliceValue(metrics, "antfly_resource_used_bytes", "dense.apply_working_set") orelse 0;
    out.rm_dense_apply_peak_bytes = promResourceSliceValue(metrics, "antfly_resource_peak_bytes", "dense.apply_working_set") orelse 0;
    out.rm_replay_window_used_bytes = promResourceSliceValue(metrics, "antfly_resource_used_bytes", "derived.replay_window") orelse 0;
    out.rm_replay_window_peak_bytes = promResourceSliceValue(metrics, "antfly_resource_peak_bytes", "derived.replay_window") orelse 0;
    return out;
}

fn fetchMemoryBreakdown(
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    metrics_uri: []const u8,
) !MemoryBreakdown {
    var out = MemoryBreakdown{};
    const metrics = try fetchMetricsBody(alloc, metrics_uri);
    defer alloc.free(metrics);

    out.process_resident_bytes = promMetricValue(metrics, "antfly_process_resident_bytes") orelse 0;
    out.process_footprint_bytes = promMetricValue(metrics, "antfly_process_footprint_bytes") orelse 0;
    out.process_wired_bytes = promMetricValue(metrics, "antfly_process_wired_bytes") orelse 0;
    out.lsm_cache_used_bytes = promMetricValue(metrics, "antfly_lsm_cache_used_bytes") orelse 0;
    out.rm_lsm_cache_used_bytes = promResourceSliceValue(metrics, "antfly_resource_used_bytes", "lsm.block_table_cache") orelse 0;
    out.rm_lsm_cache_peak_bytes = promResourceSliceValue(metrics, "antfly_resource_peak_bytes", "lsm.block_table_cache") orelse 0;
    out.rm_lsm_compaction_used_bytes = promResourceSliceValue(metrics, "antfly_resource_used_bytes", "lsm.compaction_work") orelse 0;
    out.rm_lsm_compaction_peak_bytes = promResourceSliceValue(metrics, "antfly_resource_peak_bytes", "lsm.compaction_work") orelse 0;
    out.rm_lsm_in_memory_used_bytes = promResourceSliceValue(metrics, "antfly_resource_used_bytes", "lsm.in_memory_state") orelse 0;
    out.rm_lsm_in_memory_peak_bytes = promResourceSliceValue(metrics, "antfly_resource_peak_bytes", "lsm.in_memory_state") orelse 0;
    out.rm_lsm_wal_working_used_bytes = promResourceSliceValue(metrics, "antfly_resource_used_bytes", "lsm.wal_write_working_set") orelse 0;
    out.rm_lsm_wal_working_peak_bytes = promResourceSliceValue(metrics, "antfly_resource_peak_bytes", "lsm.wal_write_working_set") orelse 0;
    out.rm_hbc_cache_used_bytes = promResourceSliceValue(metrics, "antfly_resource_used_bytes", "hbc.node_metadata_cache") orelse 0;
    out.rm_hbc_cache_peak_bytes = promResourceSliceValue(metrics, "antfly_resource_peak_bytes", "hbc.node_metadata_cache") orelse 0;
    out.rm_dense_search_used_bytes = promResourceSliceValue(metrics, "antfly_resource_used_bytes", "dense.search_working_set") orelse 0;
    out.rm_dense_search_peak_bytes = promResourceSliceValue(metrics, "antfly_resource_peak_bytes", "dense.search_working_set") orelse 0;
    out.rm_dense_apply_used_bytes = promResourceSliceValue(metrics, "antfly_resource_used_bytes", "dense.apply_working_set") orelse 0;
    out.rm_dense_apply_peak_bytes = promResourceSliceValue(metrics, "antfly_resource_peak_bytes", "dense.apply_working_set") orelse 0;
    out.rm_dense_routing_used_bytes = promResourceSliceValue(metrics, "antfly_resource_used_bytes", "dense.routing_working_set") orelse 0;
    out.rm_dense_routing_peak_bytes = promResourceSliceValue(metrics, "antfly_resource_peak_bytes", "dense.routing_working_set") orelse 0;
    out.rm_replay_window_used_bytes = promResourceSliceValue(metrics, "antfly_resource_used_bytes", "derived.replay_window") orelse 0;
    out.rm_replay_window_peak_bytes = promResourceSliceValue(metrics, "antfly_resource_peak_bytes", "derived.replay_window") orelse 0;
    out.rm_full_text_pending_used_bytes = promResourceSliceValue(metrics, "antfly_resource_used_bytes", "full_text.pending_segments") orelse 0;
    out.rm_full_text_pending_peak_bytes = promResourceSliceValue(metrics, "antfly_resource_peak_bytes", "full_text.pending_segments") orelse 0;

    var executor = std_http_executor.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    var client = api.ApiHttpClient.init(alloc, executor.executor());
    var detail = try client.fetchTableIndex(base_uri, table_name, index_name);
    defer detail.deinit(alloc);
    var parsed = try std.json.parseFromSlice(IndexStatusWire, alloc, detail.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value.status) |status| {
        if (status.hbc_cache) |cache| {
            out.hbc_total_bytes = cache.total_bytes orelse 0;
            out.hbc_accounted_bytes = cache.accounted_bytes orelse 0;
            applyHbcCacheKind(&out.hbc_node_used_bytes, &out.hbc_node_peak_bytes, cache.node);
            applyHbcCacheKind(&out.hbc_quantized_used_bytes, &out.hbc_quantized_peak_bytes, cache.quantized);
            applyHbcCacheKind(&out.hbc_vector_used_bytes, &out.hbc_vector_peak_bytes, cache.vector);
            applyHbcCacheKind(&out.hbc_metadata_used_bytes, &out.hbc_metadata_peak_bytes, cache.metadata);
        }
    }
    return out;
}

fn applyHbcCacheKind(used: *u64, peak: *u64, maybe_kind: ?HbcCacheKindWire) void {
    if (maybe_kind) |kind| {
        used.* = kind.used_bytes orelse 0;
        peak.* = kind.peak_bytes orelse 0;
    }
}

fn printMemoryBreakdown(phase: []const u8, memory: MemoryBreakdown) void {
    std.debug.print(
        "public_query_memory phase={s} process_rss_mb={d:.2} process_footprint_mb={d:.2} process_wired_mb={d:.2} lsm_cache_mb={d:.2} rm_lsm_cache_mb={d:.2}/{d:.2} rm_lsm_compaction_mb={d:.2}/{d:.2} rm_lsm_in_memory_mb={d:.2}/{d:.2} rm_lsm_wal_working_mb={d:.2}/{d:.2} rm_hbc_cache_mb={d:.2}/{d:.2} rm_dense_search_mb={d:.2}/{d:.2} rm_dense_apply_mb={d:.2}/{d:.2} rm_dense_routing_mb={d:.2}/{d:.2} rm_replay_window_mb={d:.2}/{d:.2} rm_full_text_pending_mb={d:.2}/{d:.2}",
        .{
            phase,
            bytesToMiB64(memory.process_resident_bytes),
            bytesToMiB64(memory.process_footprint_bytes),
            bytesToMiB64(memory.process_wired_bytes),
            bytesToMiB64(memory.lsm_cache_used_bytes),
            bytesToMiB64(memory.rm_lsm_cache_used_bytes),
            bytesToMiB64(memory.rm_lsm_cache_peak_bytes),
            bytesToMiB64(memory.rm_lsm_compaction_used_bytes),
            bytesToMiB64(memory.rm_lsm_compaction_peak_bytes),
            bytesToMiB64(memory.rm_lsm_in_memory_used_bytes),
            bytesToMiB64(memory.rm_lsm_in_memory_peak_bytes),
            bytesToMiB64(memory.rm_lsm_wal_working_used_bytes),
            bytesToMiB64(memory.rm_lsm_wal_working_peak_bytes),
            bytesToMiB64(memory.rm_hbc_cache_used_bytes),
            bytesToMiB64(memory.rm_hbc_cache_peak_bytes),
            bytesToMiB64(memory.rm_dense_search_used_bytes),
            bytesToMiB64(memory.rm_dense_search_peak_bytes),
            bytesToMiB64(memory.rm_dense_apply_used_bytes),
            bytesToMiB64(memory.rm_dense_apply_peak_bytes),
            bytesToMiB64(memory.rm_dense_routing_used_bytes),
            bytesToMiB64(memory.rm_dense_routing_peak_bytes),
            bytesToMiB64(memory.rm_replay_window_used_bytes),
            bytesToMiB64(memory.rm_replay_window_peak_bytes),
            bytesToMiB64(memory.rm_full_text_pending_used_bytes),
            bytesToMiB64(memory.rm_full_text_pending_peak_bytes),
        },
    );
    std.debug.print(
        " hbc_total_mb={d:.2} hbc_accounted_mb={d:.2} hbc_node_mb={d:.2}/{d:.2} hbc_quantized_mb={d:.2}/{d:.2} hbc_vector_mb={d:.2}/{d:.2} hbc_metadata_mb={d:.2}/{d:.2}\n",
        .{
            bytesToMiB64(memory.hbc_total_bytes),
            bytesToMiB64(memory.hbc_accounted_bytes),
            bytesToMiB64(memory.hbc_node_used_bytes),
            bytesToMiB64(memory.hbc_node_peak_bytes),
            bytesToMiB64(memory.hbc_quantized_used_bytes),
            bytesToMiB64(memory.hbc_quantized_peak_bytes),
            bytesToMiB64(memory.hbc_vector_used_bytes),
            bytesToMiB64(memory.hbc_vector_peak_bytes),
            bytesToMiB64(memory.hbc_metadata_used_bytes),
            bytesToMiB64(memory.hbc_metadata_peak_bytes),
        },
    );
}

fn fetchMetricsBody(alloc: std.mem.Allocator, metrics_uri: []const u8) ![]u8 {
    var executor = std_http_executor.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    var resp = try executor.executor().execute(alloc, .{
        .method = .GET,
        .uri = metrics_uri,
    });
    defer resp.deinit(alloc);
    if (resp.status != 200) return error.UnexpectedHttpStatus;
    return try alloc.dupe(u8, resp.body);
}

fn promMetricValue(metrics: []const u8, name: []const u8) ?u64 {
    var lines = std.mem.splitScalar(u8, metrics, '\n');
    while (lines.next()) |line| {
        if (line.len == 0 or line[0] == '#') continue;
        if (!std.mem.startsWith(u8, line, name)) continue;
        if (line.len > name.len and line[name.len] != ' ') continue;
        return parsePromLineValue(line);
    }
    return null;
}

fn promResourceSliceValue(metrics: []const u8, name: []const u8, slice: []const u8) ?u64 {
    var label_buf: [128]u8 = undefined;
    const label = std.fmt.bufPrint(&label_buf, "slice=\"{s}\"", .{slice}) catch return null;
    var lines = std.mem.splitScalar(u8, metrics, '\n');
    while (lines.next()) |line| {
        if (line.len == 0 or line[0] == '#') continue;
        if (!std.mem.startsWith(u8, line, name)) continue;
        if (std.mem.indexOf(u8, line, label) == null) continue;
        return parsePromLineValue(line);
    }
    return null;
}

fn parsePromLineValue(line: []const u8) ?u64 {
    const pos = std.mem.lastIndexOfScalar(u8, line, ' ') orelse return null;
    const raw = std.mem.trim(u8, line[pos + 1 ..], " \t\r");
    if (raw.len == 0) return null;
    const value = std.fmt.parseFloat(f64, raw) catch return null;
    if (!std.math.isFinite(value) or value < 0) return null;
    return @intFromFloat(value);
}

fn waitForDenseIndexReady(
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    expected_docs: usize,
    timeout_ms: u64,
) !VisibilitySnapshot {
    var executor = std_http_executor.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    var client = api.ApiHttpClient.init(alloc, executor.executor());
    const deadline = nowNs() + timeout_ms * std.time.ns_per_ms;
    var last: VisibilitySnapshot = .{};
    while (nowNs() < deadline) {
        last = fetchDenseVisibilitySnapshot(alloc, &client, base_uri) catch {
            sleepMs(100);
            continue;
        };
        if (last.publishedReady(expected_docs)) return last;
        sleepMs(100);
    }
    std.debug.print("public-query guardrail dense index not ready doc_count={d} total_indexed={d} published_doc_count={d} query_visible_doc_count={d} node_count={d} published_node_count={d} root_node={d} published_root_node={d} expected={d} runtime_fresh={any} publish_pending={any} replay_applied={d} replay_target={d} replay_required={any} backfill_active={any} rebuilding={any}\n", .{
        last.doc_count,
        last.total_indexed,
        last.published_doc_count,
        last.query_visible_doc_count,
        last.node_count,
        last.published_node_count,
        last.root_node,
        last.published_root_node,
        expected_docs,
        last.runtime_fresh,
        last.dense_publish_pending,
        last.replay_applied_sequence,
        last.replay_target_sequence,
        last.replay_catch_up_required,
        last.backfill_active,
        last.rebuilding,
    });
    return error.Timeout;
}

fn waitForQueryIndexesReady(
    alloc: std.mem.Allocator,
    base_uri: []const u8,
    expected_docs: usize,
    timeout_ms: u64,
    cfg: Config,
) !VisibilitySnapshot {
    const dense = try waitForDenseIndexReady(alloc, base_uri, expected_docs, timeout_ms);
    if (!cfg.query_shape.usesFullText()) return dense;

    var executor = std_http_executor.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    var client = api.ApiHttpClient.init(alloc, executor.executor());
    const deadline = nowNs() + timeout_ms * std.time.ns_per_ms;
    var last: VisibilitySnapshot = .{};
    while (nowNs() < deadline) {
        last = fetchIndexVisibilitySnapshot(alloc, &client, base_uri, text_index_name) catch {
            sleepMs(100);
            continue;
        };
        if (last.textReady(expected_docs)) return dense;
        sleepMs(100);
    }
    std.debug.print("public-query guardrail text index not ready doc_count={d} total_indexed={d} published_doc_count={d} query_visible_doc_count={d} expected={d} runtime_fresh={any} publish_pending={any} replay_applied={d} replay_target={d} replay_required={any} backfill_active={any} rebuilding={any}\n", .{
        last.doc_count,
        last.total_indexed,
        last.published_doc_count,
        last.query_visible_doc_count,
        expected_docs,
        last.runtime_fresh,
        last.dense_publish_pending,
        last.replay_applied_sequence,
        last.replay_target_sequence,
        last.replay_catch_up_required,
        last.backfill_active,
        last.rebuilding,
    });
    return error.Timeout;
}

fn encodeQueryJson(alloc: std.mem.Allocator, vector: []const f32, source_doc_idx: usize, cfg: Config) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '{');
    var wrote_field = false;
    if (cfg.query_shape.usesDense() or cfg.query_shape.usesSparse()) {
        try out.appendSlice(alloc, "\"embeddings\":{");
        var wrote_embedding = false;
        if (cfg.query_shape.usesDense()) {
            try out.appendSlice(alloc, "\"" ++ index_name ++ "\":");
            try appendF32JsonArray(&out, alloc, vector);
            wrote_embedding = true;
        }
        if (cfg.query_shape.usesSparse()) {
            if (wrote_embedding) try out.append(alloc, ',');
            try out.appendSlice(alloc, "\"" ++ sparse_index_name ++ "\":{\"indices\":[");
            try appendSparseIndices(&out, alloc, source_doc_idx);
            try out.appendSlice(alloc, "],\"values\":[");
            try appendSparseValues(&out, alloc, source_doc_idx);
            try out.appendSlice(alloc, "]}");
        }
        try out.appendSlice(alloc, "}");
        wrote_field = true;
    }
    if (cfg.server_kind == .zig) {
        if (wrote_field) try out.append(alloc, ',');
        try appendQueryIndexes(&out, alloc, cfg);
        wrote_field = true;
    }
    if (cfg.query_shape.usesFullText()) {
        if (wrote_field) try out.append(alloc, ',');
        try out.appendSlice(alloc, "\"full_text_search\":{\"match\":\"");
        try out.appendSlice(alloc, docBodyTerm(source_doc_idx));
        try out.appendSlice(alloc, "\",\"field\":\"body\"}");
        wrote_field = true;
    }
    if (cfg.query_shape.usesGraph()) {
        if (wrote_field) try out.append(alloc, ',');
        try out.appendSlice(alloc, "\"graph_searches\":{\"neighbors\":{\"type\":\"neighbors\",\"index_name\":\"" ++ graph_index_name ++ "\",\"start_nodes\":{\"keys\":[\"");
        try out.print(alloc, "doc:{d:0>8}", .{source_doc_idx});
        try out.appendSlice(alloc, "\"]},\"params\":{\"edge_types\":[\"cites\"]}}}");
        wrote_field = true;
    }
    if (cfg.query_shape.usesFilter()) {
        if (wrote_field) try out.append(alloc, ',');
        try appendMetadataFilterQuery(&out, alloc, source_doc_idx);
        wrote_field = true;
    }
    if (cfg.query_shape.usesExclusion()) {
        if (wrote_field) try out.append(alloc, ',');
        try appendMetadataExclusionQuery(&out, alloc, source_doc_idx);
        wrote_field = true;
    }
    if (cfg.query_shape.projectsFields()) {
        if (wrote_field) try out.append(alloc, ',');
        try out.appendSlice(alloc, "\"fields\":[\"title\",\"body\",\"category\",\"status\",\"tenant\",\"score\"],");
    } else if (wrote_field) {
        try out.append(alloc, ',');
    }
    try out.appendSlice(alloc, "\"limit\":");
    try out.print(alloc, "{d}", .{cfg.k});
    if (cfg.server_kind == .zig) {
        try out.appendSlice(alloc, ",\"profile\":true");
    }
    try out.append(alloc, '}');
    return out.toOwnedSlice(alloc);
}

fn appendF32JsonArray(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, vector: []const f32) !void {
    try out.append(alloc, '[');
    for (vector, 0..) |value, i| {
        if (i != 0) try out.append(alloc, ',');
        try out.print(alloc, "{d:.8}", .{value});
    }
    try out.append(alloc, ']');
}

fn appendQueryIndexes(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, cfg: Config) !void {
    try out.appendSlice(alloc, "\"indexes\":[");
    var wrote = false;
    if (cfg.query_shape.usesDense()) {
        try out.appendSlice(alloc, "\"" ++ index_name ++ "\"");
        wrote = true;
    }
    if (cfg.query_shape.usesSparse()) {
        if (wrote) try out.append(alloc, ',');
        try out.appendSlice(alloc, "\"" ++ sparse_index_name ++ "\"");
        wrote = true;
    }
    if (!wrote and cfg.query_shape.usesFullText()) {
        try out.appendSlice(alloc, "\"" ++ text_index_name ++ "\"");
        wrote = true;
    }
    if (!wrote and cfg.query_shape.usesGraph()) {
        try out.appendSlice(alloc, "\"" ++ graph_index_name ++ "\"");
    }
    try out.append(alloc, ']');
}

fn appendPackedF32Base64(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, vector: []const f32) !void {
    const raw_len = vector.len * @sizeOf(f32);
    const encoded_len = std.base64.standard.Encoder.calcSize(raw_len);
    const start = out.items.len;
    try out.resize(alloc, start + encoded_len);
    if (native_endian == .little) {
        _ = std.base64.standard.Encoder.encode(out.items[start..][0..encoded_len], std.mem.sliceAsBytes(vector));
        return;
    }

    const raw = try alloc.alloc(u8, raw_len);
    defer alloc.free(raw);
    for (vector, 0..) |value, i| {
        const offset = i * @sizeOf(f32);
        std.mem.writeInt(u32, raw[offset..][0..4], @bitCast(value), .little);
    }
    _ = std.base64.standard.Encoder.encode(out.items[start..][0..encoded_len], raw);
}

fn encodeVectorDocJson(alloc: std.mem.Allocator, vector: []const f32, doc_idx: usize, cfg: Config) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendVectorDocJson(&out, alloc, vector, doc_idx, cfg);
    return out.toOwnedSlice(alloc);
}

fn querySourceDocIndex(query_idx: usize, cfg: Config) usize {
    return (query_idx * 997) % cfg.docs;
}

fn printPublicQuerySymbolicFilterProfile(cfg: Config, stats: QueryBenchStats) void {
    const expected = expectedSymbolicMatchStats(cfg);
    const query_count = if (stats.queries == 0) 1 else stats.queries;
    const guardrail_required = cfg.require_symbolic_profile and cfg.with_algebraic and cfg.query_shape.usesFilter();
    const profile_complete = stats.profile_response_count == stats.queries and
        stats.profile_dense_search_count == stats.queries and
        stats.profile_returned_hit_count > 0;
    const hbc_counters_present = stats.profile_hbc_approx_candidate_count > 0 or
        stats.profile_hbc_rerank_candidate_count > 0 or
        stats.profile_hbc_reranked_vectors > 0 or
        stats.profile_hbc_top_k_count > 0;
    std.debug.print(
        "public_query_symbolic_filter query_shape={s} with_schema={} with_algebraic={} uses_full_text={} uses_filter={} uses_exclusion={} guardrail_required={} profile_complete={} hbc_counters_present={} profile_response_rate={d:.4} dense_profile_rate={d:.4} expected_match_avg={d:.2} expected_match_min={d} expected_match_max={d} approx_candidates={d:.2} rerank_candidates={d:.2} reranked_vectors={d:.2} profile_returned_hits={d:.2} actual_returned_hits={d:.2}\n",
        .{
            cfg.query_shape.text(),
            cfg.with_schema,
            cfg.with_algebraic,
            cfg.query_shape.usesFullText(),
            cfg.query_shape.usesFilter(),
            cfg.query_shape.usesExclusion(),
            guardrail_required,
            profile_complete,
            hbc_counters_present,
            stats.profileResponseRate(),
            stats.denseProfileRate(),
            expected.avg,
            expected.min,
            expected.max,
            @as(f64, @floatFromInt(stats.profile_hbc_approx_candidate_count)) / @as(f64, @floatFromInt(query_count)),
            @as(f64, @floatFromInt(stats.profile_hbc_rerank_candidate_count)) / @as(f64, @floatFromInt(query_count)),
            @as(f64, @floatFromInt(stats.profile_hbc_reranked_vectors)) / @as(f64, @floatFromInt(query_count)),
            @as(f64, @floatFromInt(stats.profile_returned_hit_count)) / @as(f64, @floatFromInt(query_count)),
            @as(f64, @floatFromInt(stats.response_hit_count)) / @as(f64, @floatFromInt(query_count)),
        },
    );
    std.debug.print(
        "{{\"event\":\"public_query_symbolic_filter\",\"query_shape\":\"{s}\",\"with_schema\":{},\"with_algebraic\":{},\"uses_full_text\":{},\"uses_filter\":{},\"uses_exclusion\":{},\"guardrail_required\":{},\"profile_complete\":{},\"hbc_counters_present\":{},\"profile_response_rate\":{d:.6},\"dense_profile_rate\":{d:.6},\"expected_match_avg\":{d:.3},\"expected_match_min\":{d},\"expected_match_max\":{d},\"approx_candidates_avg\":{d:.3},\"rerank_candidates_avg\":{d:.3},\"reranked_vectors_avg\":{d:.3},\"profile_returned_hits_avg\":{d:.3},\"actual_returned_hits_avg\":{d:.3}}}\n",
        .{
            cfg.query_shape.text(),
            cfg.with_schema,
            cfg.with_algebraic,
            cfg.query_shape.usesFullText(),
            cfg.query_shape.usesFilter(),
            cfg.query_shape.usesExclusion(),
            guardrail_required,
            profile_complete,
            hbc_counters_present,
            stats.profileResponseRate(),
            stats.denseProfileRate(),
            expected.avg,
            expected.min,
            expected.max,
            avgPerQuery(stats, stats.profile_hbc_approx_candidate_count),
            avgPerQuery(stats, stats.profile_hbc_rerank_candidate_count),
            avgPerQuery(stats, stats.profile_hbc_reranked_vectors),
            avgPerQuery(stats, stats.profile_returned_hit_count),
            avgPerQuery(stats, stats.response_hit_count),
        },
    );
}

fn printPublicQueryGuardrailSummaryJson(
    mode: []const u8,
    server: []const u8,
    cfg: Config,
    db_avg_ns: u64,
    handler_avg_ns: u64,
    http_stats: QueryBenchStats,
    concurrent: ConcurrentStats,
    load: LoadRun,
    load_rss_peak_bytes: usize,
    search_rss_peak_bytes: usize,
    search_memory: MemoryBreakdown,
) void {
    std.debug.print(
        "{{\"event\":\"public_query_guardrail_summary\",\"mode\":\"{s}\",\"server\":\"{s}\",\"query_shape\":\"{s}\",\"with_schema\":{},\"with_algebraic\":{},\"with_sparse\":{},\"with_graph\":{},\"docs\":{d},\"dims\":{d},\"queries\":{d},\"repeats\":{d},\"k\":{d},\"threads\":{d}",
        .{
            mode,
            server,
            cfg.query_shape.text(),
            cfg.with_schema,
            cfg.with_algebraic,
            cfg.with_sparse,
            cfg.with_graph,
            cfg.docs,
            cfg.dims,
            cfg.queries,
            cfg.repeats,
            cfg.k,
            cfg.search_threads,
        },
    );
    std.debug.print(
        ",\"db_avg_us\":{d:.3},\"handler_avg_us\":{d:.3},\"http_avg_us\":{d:.3},\"http_first_pass_us\":{d:.3},\"http_later_pass_us\":{d:.3},\"profile_total_us\":{d:.3},\"profile_hbc_us\":{d:.3},\"concurrent_qps\":{d:.3},\"concurrent_avg_us\":{d:.3},\"concurrent_max_us\":{d:.3},\"load_insert_ms\":{d:.3},\"index_wait_ms\":{d:.3},\"load_total_ms\":{d:.3}",
        .{
            nsToUs(db_avg_ns),
            nsToUs(handler_avg_ns),
            nsToUs(http_stats.avgNs()),
            nsToUs(http_stats.avgFirstPassNs()),
            nsToUs(http_stats.avgLaterPassNs()),
            nsToUs(http_stats.avgProfileNs()),
            nsToUs(if (http_stats.queries == 0) 0 else http_stats.profile_hbc_search_ns / http_stats.queries),
            concurrent.qps(),
            nsToUs(concurrent.avgRequestNs()),
            nsToUs(concurrent.max_request_ns),
            nsToMs(load.insert_ns),
            nsToMs(load.visibility_wait_ns),
            nsToMs(load.total_ns),
        },
    );
    std.debug.print(
        ",\"load_rss_peak_bytes\":{d},\"search_rss_peak_bytes\":{d},\"hbc_total_bytes\":{d},\"hbc_accounted_bytes\":{d},\"lsm_cache_bytes\":{d},\"full_text_pending_bytes\":{d},\"replay_window_bytes\":{d},\"approx_candidates_avg\":{d:.3},\"rerank_candidates_avg\":{d:.3},\"reranked_vectors_avg\":{d:.3},\"top_k_count_avg\":{d:.3},\"profile_response_rate\":{d:.6},\"dense_profile_rate\":{d:.6},\"returned_hits_avg\":{d:.3}}}\n",
        .{
            load_rss_peak_bytes,
            search_rss_peak_bytes,
            search_memory.hbc_total_bytes,
            search_memory.hbc_accounted_bytes,
            search_memory.lsm_cache_used_bytes,
            search_memory.rm_full_text_pending_used_bytes,
            search_memory.rm_replay_window_used_bytes,
            avgPerQuery(http_stats, http_stats.profile_hbc_approx_candidate_count),
            avgPerQuery(http_stats, http_stats.profile_hbc_rerank_candidate_count),
            avgPerQuery(http_stats, http_stats.profile_hbc_reranked_vectors),
            avgPerQuery(http_stats, http_stats.profile_hbc_top_k_count),
            http_stats.profileResponseRate(),
            http_stats.denseProfileRate(),
            avgPerQuery(http_stats, http_stats.response_hit_count),
        },
    );
}

fn avgPerQuery(stats: QueryBenchStats, value: u64) f64 {
    const query_count = if (stats.queries == 0) 1 else stats.queries;
    return @as(f64, @floatFromInt(value)) / @as(f64, @floatFromInt(query_count));
}

const ExpectedSymbolicMatchStats = struct {
    avg: f64 = 0,
    min: usize = 0,
    max: usize = 0,
};

fn expectedSymbolicMatchStats(cfg: Config) ExpectedSymbolicMatchStats {
    if (cfg.queries == 0) return .{};
    var total: usize = 0;
    var min_count: usize = std.math.maxInt(usize);
    var max_count: usize = 0;
    for (0..cfg.queries) |query_idx| {
        const source_doc_idx = querySourceDocIndex(query_idx, cfg);
        const count = expectedSymbolicMatchCount(source_doc_idx, cfg);
        total += count;
        min_count = @min(min_count, count);
        max_count = @max(max_count, count);
    }
    return .{
        .avg = @as(f64, @floatFromInt(total)) / @as(f64, @floatFromInt(cfg.queries)),
        .min = if (min_count == std.math.maxInt(usize)) 0 else min_count,
        .max = max_count,
    };
}

fn expectedSymbolicMatchCount(source_doc_idx: usize, cfg: Config) usize {
    if (cfg.query_shape == .dense) return cfg.docs;
    var count: usize = 0;
    for (0..cfg.docs) |doc_idx| {
        if (cfg.query_shape.usesFullText() and !std.mem.eql(u8, docBodyTerm(doc_idx), docBodyTerm(source_doc_idx))) continue;
        if (cfg.query_shape.usesFilter()) {
            if (!std.mem.eql(u8, docStatus(doc_idx), docStatus(source_doc_idx))) continue;
            if (!std.mem.eql(u8, docTenant(doc_idx), docTenant(source_doc_idx))) continue;
        }
        if (cfg.query_shape.usesExclusion() and std.mem.eql(u8, docCategory(doc_idx), docExcludedCategory(source_doc_idx))) continue;
        count += 1;
    }
    return count;
}

fn docStatus(doc_idx: usize) []const u8 {
    return if (doc_idx % 2 == 0) "active" else "archived";
}

fn docTenant(doc_idx: usize) []const u8 {
    return switch (doc_idx % 3) {
        0 => "tenanta",
        1 => "tenantb",
        else => "tenantc",
    };
}

fn docCategory(doc_idx: usize) []const u8 {
    return switch (doc_idx % 4) {
        0 => "science",
        1 => "systems",
        2 => "history",
        else => "archive",
    };
}

fn docExcludedCategory(source_doc_idx: usize) []const u8 {
    return docCategory(source_doc_idx + 1);
}

fn docBodyTerm(doc_idx: usize) []const u8 {
    return switch (doc_idx % 8) {
        0 => "alpha",
        1 => "beta",
        2 => "gamma",
        3 => "delta",
        4 => "epsilon",
        5 => "zeta",
        6 => "eta",
        else => "theta",
    };
}

fn docScore(doc_idx: usize) usize {
    return doc_idx % 1000;
}

fn appendSparseIndices(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, doc_idx: usize) !void {
    try out.print(alloc, "{d},{d},{d}", .{
        7 + (doc_idx % 32),
        10_000 + (doc_idx % 64),
        20_000 + (doc_idx % 128),
    });
}

fn appendSparseValues(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, doc_idx: usize) !void {
    try out.print(alloc, "{d:.3},{d:.3},{d:.3}", .{
        1.0 + @as(f64, @floatFromInt(doc_idx % 5)) * 0.1,
        0.5 + @as(f64, @floatFromInt(doc_idx % 7)) * 0.05,
        0.25 + @as(f64, @floatFromInt(doc_idx % 11)) * 0.02,
    });
}

fn appendSparseEmbeddingObject(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, doc_idx: usize) !void {
    const indices = [_]usize{
        7 + (doc_idx % 32),
        10_000 + (doc_idx % 64),
        20_000 + (doc_idx % 128),
    };
    const values = [_]f64{
        1.0 + @as(f64, @floatFromInt(doc_idx % 5)) * 0.1,
        0.5 + @as(f64, @floatFromInt(doc_idx % 7)) * 0.05,
        0.25 + @as(f64, @floatFromInt(doc_idx % 11)) * 0.02,
    };
    for (indices, values, 0..) |idx, value, i| {
        if (i != 0) try out.append(alloc, ',');
        try out.print(alloc, "\"{d}\":{d:.3}", .{ idx, value });
    }
}

fn appendDocBody(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, doc_idx: usize) !void {
    try out.print(alloc, "public query benchmark {s} {s} {s} {s}", .{
        docBodyTerm(doc_idx),
        docCategory(doc_idx),
        docStatus(doc_idx),
        docTenant(doc_idx),
    });
}

fn appendMetadataFilterQuery(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, source_doc_idx: usize) !void {
    try out.appendSlice(alloc, "\"filter_query\":{\"conjuncts\":[");
    try appendGeneratedTermQuery(out, alloc, "status", docStatus(source_doc_idx));
    try out.append(alloc, ',');
    try appendGeneratedTermQuery(out, alloc, "tenant", docTenant(source_doc_idx));
    try out.appendSlice(alloc, "]}");
}

fn appendMetadataExclusionQuery(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, source_doc_idx: usize) !void {
    try out.appendSlice(alloc, "\"exclusion_query\":");
    try appendGeneratedTermQuery(out, alloc, "category", docExcludedCategory(source_doc_idx));
}

fn appendGeneratedTermQuery(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, field: []const u8, term: []const u8) !void {
    try out.appendSlice(alloc, "{\"term\":{\"");
    try out.appendSlice(alloc, field);
    try out.appendSlice(alloc, "\":\"");
    try out.appendSlice(alloc, term);
    try out.appendSlice(alloc, "\"}}");
}

fn freeOwnedStrings(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
    alloc.free(@constCast(values));
}

fn deterministicNoise(seed: u64, doc_idx: usize, dim_idx: usize) f32 {
    var x = seed ^
        (@as(u64, @intCast(doc_idx + 1)) *% 0x9E3779B97F4A7C15) ^
        (@as(u64, @intCast(dim_idx + 1)) *% 0xC2B2AE3D27D4EB4F);
    x ^= x >> 33;
    x *%= 0xFF51AFD7ED558CCD;
    x ^= x >> 33;
    x *%= 0xC4CEB9FE1A85EC53;
    x ^= x >> 33;
    const scaled = @as(f32, @floatFromInt(x & 1023)) / 1024.0;
    return scaled * 0.01;
}

fn parseNextUsize(args: *std.process.Args.Iterator, flag: []const u8) !usize {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(usize, raw, 10);
}

fn parseNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return try std.fmt.parseInt(u64, raw, 10);
}

fn tempPath(buf: []u8) [:0]u8 {
    return std.fmt.bufPrintZ(buf, "/tmp/antfly-public-query-{d}", .{platform_time.monotonicNs()}) catch unreachable;
}

fn reserveEphemeralPort(_: std.Io) !u16 {
    const span: u16 = 20_000;
    const base: u16 = 20_000;
    return base + @as(u16, @intCast(platform_time.monotonicNs() % span));
}

fn sampleRssBytes(alloc: std.mem.Allocator, io: std.Io, pid: std.process.Child.Id) !usize {
    const pid_arg = try std.fmt.allocPrint(alloc, "{d}", .{@as(i64, @intCast(pid))});
    defer alloc.free(pid_arg);
    const result = try std.process.run(alloc, io, .{
        .argv = &.{ "ps", "-p", pid_arg, "-o", "rss=" },
    });
    defer alloc.free(result.stdout);
    defer alloc.free(result.stderr);
    switch (result.term) {
        .exited => |code| if (code != 0) return error.UnexpectedProcessExit,
        else => return error.UnexpectedProcessExit,
    }
    const trimmed = std.mem.trim(u8, result.stdout, " \t\r\n");
    if (trimmed.len == 0) return 0;
    const rss_kib = try std.fmt.parseInt(usize, trimmed, 10);
    return rss_kib * 1024;
}

fn cleanupTempDir(path: [:0]u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path[0..path.len]) catch {};
}

fn nowNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(started: u64) u64 {
    return platform_time.monotonicNs() -| started;
}

fn sleepMs(ms: u64) void {
    var req = std.posix.timespec{
        .sec = @intCast(ms / std.time.ms_per_s),
        .nsec = @intCast((ms % std.time.ms_per_s) * std.time.ns_per_ms),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

fn nsToUs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1e3;
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1e6;
}

fn bytesToMiB(bytes: usize) f64 {
    return @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0);
}

fn bytesToMiB64(bytes: u64) f64 {
    return @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0);
}
