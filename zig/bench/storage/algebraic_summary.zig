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

const churn_row_families = [_][]const u8{
    "materialized_expr",
    "docfact",
    "pathfact",
    "path_lookup",
    "path_profile",
    "joinfact",
    "docjf",
    "minmax",
    "sym",
};

const DatasetRecord = struct {
    case_name: []u8,
    algebraic_backend: []u8,
    algebraic_profile: []u8,
    algebraic_bulk_ingest: bool,
    algebraic_bulk_compact: bool,
    algebraic_bulk_flush: bool,
    docs: u64,
    fanout: u64,
    materialization_count: u64,
    algebraic_bytes: u64,
    algebraic_backend_path_bytes: u64,
    full_text_bytes: u64,
    algebraic_build_ms: f64,
    full_text_build_ms: f64,
    support_entries: u64,
    support_bytes: u64,
    symbol_entries: u64,
    symbol_bytes: u64,
    minmax_cache_entries: u64,
    minmax_cache_bytes: u64,
    join_facts_scanned: u64,
    join_facts_matched: u64,
    join_facts_pruned: u64,
    accumulator_flush_count: u64,
    minmax_cache_hits: u64,
    minmax_cache_misses: u64,
    minmax_support_scans: u64,
    planner_selected: u64,
    planner_fallback_count: u64,
    distributed_partial_validation_proven_count: u64,
    distributed_partial_validation_rejected_count: u64,
    distributed_partial_rows_exported_count: u64,
    path_dictionary_fst_rebuild_count: u64,
    observed_query_shape_count: u64,
    recommendation_count: u64,
    adaptive_maintenance_plan_build_count: u64,
    adaptive_maintenance_cached_spec_count: u64,
    adaptive_maintenance_disabled_count: u64,
    lsm_flushes: u64,
    lsm_flush_output_runs: u64,
    lsm_sorted_ingest_runs: u64,
    lsm_sorted_ingest_bytes: u64,
    lsm_write_pressure_compactions: u64,

    fn deinit(self: *DatasetRecord, alloc: std.mem.Allocator) void {
        alloc.free(self.case_name);
        alloc.free(self.algebraic_backend);
        alloc.free(self.algebraic_profile);
        self.* = undefined;
    }
};

const QueryRecord = struct {
    case_name: []u8,
    algebraic_backend: []u8,
    algebraic_profile: []u8,
    query: []u8,
    engine: []u8,
    avg_ms: f64,
    checksum: u64,

    fn deinit(self: *QueryRecord, alloc: std.mem.Allocator) void {
        alloc.free(self.case_name);
        alloc.free(self.algebraic_backend);
        alloc.free(self.algebraic_profile);
        alloc.free(self.query);
        alloc.free(self.engine);
        self.* = undefined;
    }
};

const CorrectnessRecord = struct {
    case_name: []u8,
    query: []u8,
    expected_equal: bool,
    all_match: bool,

    fn deinit(self: *CorrectnessRecord, alloc: std.mem.Allocator) void {
        alloc.free(self.case_name);
        alloc.free(self.query);
        self.* = undefined;
    }
};

const ChurnRecord = struct {
    case_name: []u8,
    docs: u64,
    ops: u64,
    batch_size: u64,
    algebraic_update_ms: f64,
    full_text_update_ms: f64,
    algebraic_sidecar_entries: u64,
    algebraic_sidecar_bytes: u64,
    adaptive_maintenance_plan_build_count: u64,
    adaptive_maintenance_cached_spec_count: u64,
    adaptive_maintenance_disabled_count: u64,

    fn deinit(self: *ChurnRecord, alloc: std.mem.Allocator) void {
        alloc.free(self.case_name);
        self.* = undefined;
    }
};

const ChurnRowFamilyRecord = struct {
    case_name: []u8,
    family: []u8,
    docs: u64,
    ops: u64,
    batch_size: u64,
    algebraic_bulk_ingest: bool,
    algebraic_update_ms: f64,
    entries_before: u64,
    entries_after: u64,
    entries_delta: i64,
    bytes_before: u64,
    bytes_after: u64,
    bytes_delta: i64,

    fn deinit(self: *ChurnRowFamilyRecord, alloc: std.mem.Allocator) void {
        alloc.free(self.case_name);
        alloc.free(self.family);
        self.* = undefined;
    }
};

const WarmupRecord = struct {
    case_name: []u8,
    algebraic_backend: []u8,
    algebraic_profile: []u8,
    adaptive_warmup_ms: f64,
    adaptive_backfill_ticks: u64,
    adaptive_ready_count: u64,
    adaptive_candidate_count: u64,
    adaptive_progress_count: u64,
    adaptive_backfilling_count: u64,
    adaptive_rebuild_required_count: u64,
    adaptive_stale_count: u64,
    adaptive_cleanup_recommended_count: u64,
    adaptive_decision_history_count: u64,
    adaptive_policy_drift_count: u64,
    adaptive_recommendation_count: u64,
    observed_query_shape_count: u64,

    fn deinit(self: *WarmupRecord, alloc: std.mem.Allocator) void {
        alloc.free(self.case_name);
        alloc.free(self.algebraic_backend);
        alloc.free(self.algebraic_profile);
        self.* = undefined;
    }
};

const GraphTraversalRecord = struct {
    case_name: []u8,
    mode: []u8,
    docs: u64,
    edges: u64,
    fanout: u64,
    repeats: u64,
    path_bytes: u64,
    build_ms: f64,
    traverse_avg_ms: f64,
    shortest_avg_ms: f64,
    rejected_avg_ms: f64,
    attempted: u64,
    proven: u64,
    rejected: u64,
    fallback: u64,
    result_nodes: u64,

    fn deinit(self: *GraphTraversalRecord, alloc: std.mem.Allocator) void {
        alloc.free(self.case_name);
        alloc.free(self.mode);
        self.* = undefined;
    }
};

const PublicQueryRecord = struct {
    mode: []u8,
    server: []u8,
    query_shape: []u8,
    with_schema: bool,
    with_algebraic: bool,
    docs: u64,
    dims: u64,
    queries: u64,
    repeats: u64,
    k: u64,
    threads: u64,
    db_avg_us: f64,
    handler_avg_us: f64,
    http_avg_us: f64,
    profile_total_us: f64,
    profile_hbc_us: f64,
    concurrent_qps: f64,
    concurrent_avg_us: f64,
    load_insert_ms: f64,
    index_wait_ms: f64,
    load_total_ms: f64,
    load_rss_peak_bytes: u64,
    search_rss_peak_bytes: u64,
    hbc_total_bytes: u64,
    hbc_accounted_bytes: u64,
    lsm_cache_bytes: u64,
    full_text_pending_bytes: u64,
    replay_window_bytes: u64,
    approx_candidates_avg: f64,
    rerank_candidates_avg: f64,
    reranked_vectors_avg: f64,
    returned_hits_avg: f64,

    fn deinit(self: *PublicQueryRecord, alloc: std.mem.Allocator) void {
        alloc.free(self.mode);
        alloc.free(self.server);
        alloc.free(self.query_shape);
        self.* = undefined;
    }
};

const CliConfig = struct {
    input_path: []const u8,
    baseline_path: []const u8 = "",
    guardrail: GuardrailConfig = .{},
};

const GuardrailConfig = struct {
    enabled: bool = false,
    min_dataset_cases: u64 = 0,
    min_lsm_dataset_cases: u64 = 0,
    min_algebraic_query_records: u64 = 0,
    min_doc_scan_query_records: u64 = 0,
    min_full_text_query_records: u64 = 0,
    min_lsm_query_records: u64 = 0,
    min_cold_query_records: u64 = 0,
    min_warm_query_records: u64 = 0,
    min_constrained_query_records: u64 = 0,
    min_wide_query_records: u64 = 0,
    min_stats_query_records: u64 = 0,
    min_cardinality_query_records: u64 = 0,
    min_range_query_records: u64 = 0,
    min_histogram_query_records: u64 = 0,
    min_fanout_dataset_cases: u64 = 0,
    min_churn_records: u64 = 0,
    min_public_query_comparison_pairs: u64 = 0,
    min_lsm_sorted_ingest_runs: u64 = 0,
    max_lsm_flushes: u64 = std.math.maxInt(u64),
    max_lsm_write_pressure_compactions: u64 = std.math.maxInt(u64),
    max_correctness_failures: u64 = std.math.maxInt(u64),
    max_unclassified_algebraic_comparisons: u64 = std.math.maxInt(u64),
    max_algebraic_bytes_per_doc: f64 = std.math.inf(f64),
    max_algebraic_bytes_per_materialization: f64 = std.math.inf(f64),
    max_symbol_bytes_per_doc: f64 = std.math.inf(f64),
    max_support_bytes_per_doc: f64 = std.math.inf(f64),
    max_accumulator_flush_count: u64 = std.math.maxInt(u64),
    max_path_dictionary_fst_rebuild_count: u64 = std.math.maxInt(u64),
    max_public_query_http_us: f64 = std.math.inf(f64),
    max_public_query_load_rss_peak_bytes: u64 = std.math.maxInt(u64),
    max_public_query_search_rss_peak_bytes: u64 = std.math.maxInt(u64),
    min_public_query_http_speedup: f64 = 0,
    max_churn_algebraic_update_ms: f64 = std.math.inf(f64),
    max_churn_sidecar_bytes: u64 = std.math.maxInt(u64),
    max_algebraic_query_ms: f64 = std.math.inf(f64),
    max_algebraic_query_ms_ratio_vs_baseline: f64 = std.math.inf(f64),
    max_public_query_http_us_ratio_vs_baseline: f64 = std.math.inf(f64),
    max_algebraic_bytes_per_doc_ratio_vs_baseline: f64 = std.math.inf(f64),
    max_churn_algebraic_update_ms_ratio_vs_baseline: f64 = std.math.inf(f64),
};

const PerformanceEvidenceMetrics = struct {
    dataset_cases: u64 = 0,
    lsm_dataset_cases: u64 = 0,
    total_materializations: u64 = 0,
    total_algebraic_bytes: u64 = 0,
    total_algebraic_backend_path_bytes: u64 = 0,
    total_full_text_bytes: u64 = 0,
    total_symbol_bytes: u64 = 0,
    total_support_bytes: u64 = 0,
    max_algebraic_bytes_per_doc: f64 = 0,
    max_algebraic_bytes_per_materialization: f64 = 0,
    max_symbol_bytes_per_doc: f64 = 0,
    max_support_bytes_per_doc: f64 = 0,
    max_accumulator_flush_count: u64 = 0,
    algebraic_query_records: u64 = 0,
    doc_scan_query_records: u64 = 0,
    full_text_query_records: u64 = 0,
    lsm_query_records: u64 = 0,
    cold_query_records: u64 = 0,
    warm_query_records: u64 = 0,
    constrained_query_records: u64 = 0,
    wide_query_records: u64 = 0,
    stats_query_records: u64 = 0,
    cardinality_query_records: u64 = 0,
    range_query_records: u64 = 0,
    histogram_query_records: u64 = 0,
    fanout_dataset_cases: u64 = 0,
    max_algebraic_query_ms: f64 = 0,
    correctness_failures: u64 = 0,
    unclassified_algebraic_comparisons: u64 = 0,
    unclassified_algebraic_checksum_mismatches: u64 = 0,
    churn_records: u64 = 0,
    churn_ops: u64 = 0,
    total_churn_sidecar_bytes: u64 = 0,
    max_churn_sidecar_bytes: u64 = 0,
    total_algebraic_update_ms: f64 = 0,
    total_full_text_update_ms: f64 = 0,
    max_churn_algebraic_update_ms: f64 = 0,
    churn_row_family_records: u64 = 0,
    total_churn_row_family_bytes_after: u64 = 0,
    max_churn_row_family_bytes_after: u64 = 0,
    total_adaptive_maintenance_plan_build_count: u64 = 0,
    total_adaptive_maintenance_cached_spec_count: u64 = 0,
    total_adaptive_maintenance_disabled_count: u64 = 0,
    max_adaptive_maintenance_plan_build_count: u64 = 0,
    max_adaptive_maintenance_cached_spec_count: u64 = 0,
    total_path_dictionary_fst_rebuild_count: u64 = 0,
    max_path_dictionary_fst_rebuild_count: u64 = 0,
    total_lsm_flushes: u64 = 0,
    total_lsm_flush_output_runs: u64 = 0,
    total_lsm_sorted_ingest_runs: u64 = 0,
    total_lsm_sorted_ingest_bytes: u64 = 0,
    total_lsm_write_pressure_compactions: u64 = 0,
    max_lsm_flushes: u64 = 0,
    max_lsm_write_pressure_compactions: u64 = 0,
    max_lsm_sorted_ingest_runs: u64 = 0,
    public_query_records: u64 = 0,
    public_query_algebraic_records: u64 = 0,
    public_query_schema_only_records: u64 = 0,
    public_query_no_schema_records: u64 = 0,
    public_query_comparison_pairs: u64 = 0,
    max_public_query_http_us: f64 = 0,
    max_public_query_load_rss_peak_bytes: u64 = 0,
    max_public_query_search_rss_peak_bytes: u64 = 0,
    min_public_query_http_speedup: f64 = std.math.inf(f64),
};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);
    const raw = try std.Io.Dir.cwd().readFileAlloc(init.io, cfg.input_path, alloc, .limited(1024 * 1024 * 1024));
    defer alloc.free(raw);

    var datasets = std.ArrayListUnmanaged(DatasetRecord).empty;
    defer {
        for (datasets.items) |*item| item.deinit(alloc);
        datasets.deinit(alloc);
    }
    var queries = std.ArrayListUnmanaged(QueryRecord).empty;
    defer {
        for (queries.items) |*item| item.deinit(alloc);
        queries.deinit(alloc);
    }
    var correctness = std.ArrayListUnmanaged(CorrectnessRecord).empty;
    defer {
        for (correctness.items) |*item| item.deinit(alloc);
        correctness.deinit(alloc);
    }
    var churn = std.ArrayListUnmanaged(ChurnRecord).empty;
    defer {
        for (churn.items) |*item| item.deinit(alloc);
        churn.deinit(alloc);
    }
    var churn_row_families_records = std.ArrayListUnmanaged(ChurnRowFamilyRecord).empty;
    defer {
        for (churn_row_families_records.items) |*item| item.deinit(alloc);
        churn_row_families_records.deinit(alloc);
    }
    var warmups = std.ArrayListUnmanaged(WarmupRecord).empty;
    defer {
        for (warmups.items) |*item| item.deinit(alloc);
        warmups.deinit(alloc);
    }
    var graph_traversals = std.ArrayListUnmanaged(GraphTraversalRecord).empty;
    defer {
        for (graph_traversals.items) |*item| item.deinit(alloc);
        graph_traversals.deinit(alloc);
    }
    var public_queries = std.ArrayListUnmanaged(PublicQueryRecord).empty;
    defer {
        for (public_queries.items) |*item| item.deinit(alloc);
        public_queries.deinit(alloc);
    }

    var lines = std.mem.splitScalar(u8, raw, '\n');
    while (lines.next()) |line| {
        if (std.mem.trim(u8, line, " \t\r").len == 0) continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{}) catch continue;
        defer parsed.deinit();
        const obj = switch (parsed.value) {
            .object => |object| object,
            else => continue,
        };
        const event = jsonString(obj, "event") orelse continue;
        if (std.mem.eql(u8, event, "dataset")) {
            try datasets.append(alloc, .{
                .case_name = try alloc.dupe(u8, jsonString(obj, "case") orelse "unknown"),
                .algebraic_backend = try alloc.dupe(u8, jsonString(obj, "algebraic_backend") orelse "unknown"),
                .algebraic_profile = try alloc.dupe(u8, jsonString(obj, "algebraic_profile") orelse "default"),
                .algebraic_bulk_ingest = jsonBool(obj, "algebraic_bulk_ingest") orelse false,
                .algebraic_bulk_compact = jsonBool(obj, "algebraic_bulk_compact") orelse false,
                .algebraic_bulk_flush = jsonBool(obj, "algebraic_bulk_flush") orelse false,
                .docs = jsonU64(obj, "docs") orelse 0,
                .fanout = jsonU64(obj, "fanout") orelse 0,
                .materialization_count = jsonU64(obj, "materialization_count") orelse 0,
                .algebraic_bytes = jsonU64(obj, "algebraic_sidecar_bytes_estimate") orelse 0,
                .algebraic_backend_path_bytes = jsonU64(obj, "algebraic_backend_path_bytes") orelse 0,
                .full_text_bytes = jsonU64(obj, "full_text_db_path_bytes") orelse 0,
                .algebraic_build_ms = jsonF64(obj, "algebraic_build_ms") orelse 0,
                .full_text_build_ms = jsonF64(obj, "full_text_build_ms") orelse 0,
                .support_entries = jsonU64(obj, "algebraic_support_entries") orelse 0,
                .support_bytes = jsonU64(obj, "algebraic_support_bytes") orelse 0,
                .symbol_entries = jsonU64(obj, "algebraic_symbol_entries") orelse 0,
                .symbol_bytes = jsonU64(obj, "algebraic_symbol_bytes") orelse 0,
                .minmax_cache_entries = jsonU64(obj, "algebraic_minmax_cache_entries") orelse 0,
                .minmax_cache_bytes = jsonU64(obj, "algebraic_minmax_cache_bytes") orelse 0,
                .join_facts_scanned = jsonU64(obj, "algebraic_join_facts_scanned") orelse 0,
                .join_facts_matched = jsonU64(obj, "algebraic_join_facts_matched") orelse 0,
                .join_facts_pruned = jsonU64(obj, "algebraic_join_facts_pruned") orelse 0,
                .accumulator_flush_count = jsonU64(obj, "algebraic_accumulator_flush_count") orelse 0,
                .minmax_cache_hits = jsonU64(obj, "algebraic_minmax_cache_hits") orelse 0,
                .minmax_cache_misses = jsonU64(obj, "algebraic_minmax_cache_misses") orelse 0,
                .minmax_support_scans = jsonU64(obj, "algebraic_minmax_support_scans") orelse 0,
                .planner_selected = jsonU64(obj, "algebraic_planner_selected") orelse 0,
                .planner_fallback_count = jsonU64(obj, "algebraic_planner_fallback_count") orelse 0,
                .distributed_partial_validation_proven_count = 0,
                .distributed_partial_validation_rejected_count = 0,
                .distributed_partial_rows_exported_count = 0,
                .path_dictionary_fst_rebuild_count = jsonU64(obj, "algebraic_path_dictionary_fst_rebuild_count") orelse 0,
                .observed_query_shape_count = 0,
                .recommendation_count = 0,
                .adaptive_maintenance_plan_build_count = 0,
                .adaptive_maintenance_cached_spec_count = 0,
                .adaptive_maintenance_disabled_count = 0,
                .lsm_flushes = jsonU64(obj, "algebraic_lsm_flushes") orelse 0,
                .lsm_flush_output_runs = jsonU64(obj, "algebraic_lsm_flush_output_runs") orelse 0,
                .lsm_sorted_ingest_runs = jsonU64(obj, "algebraic_lsm_sorted_ingest_runs") orelse 0,
                .lsm_sorted_ingest_bytes = jsonU64(obj, "algebraic_lsm_sorted_ingest_bytes") orelse 0,
                .lsm_write_pressure_compactions = jsonU64(obj, "algebraic_lsm_write_pressure_compactions") orelse 0,
            });
        } else if (std.mem.eql(u8, event, "dataset_algebraic_status")) {
            const case_name = jsonString(obj, "case") orelse "unknown";
            const backend = jsonString(obj, "algebraic_backend") orelse "unknown";
            const profile = jsonString(obj, "algebraic_profile") orelse "default";
            if (findDatasetMutable(datasets.items, case_name, backend, profile)) |dataset| {
                dataset.join_facts_scanned = jsonU64(obj, "algebraic_join_facts_scanned") orelse dataset.join_facts_scanned;
                dataset.join_facts_matched = jsonU64(obj, "algebraic_join_facts_matched") orelse dataset.join_facts_matched;
                dataset.join_facts_pruned = jsonU64(obj, "algebraic_join_facts_pruned") orelse dataset.join_facts_pruned;
                dataset.accumulator_flush_count = jsonU64(obj, "algebraic_accumulator_flush_count") orelse dataset.accumulator_flush_count;
                dataset.minmax_cache_hits = jsonU64(obj, "algebraic_minmax_cache_hits") orelse dataset.minmax_cache_hits;
                dataset.minmax_cache_misses = jsonU64(obj, "algebraic_minmax_cache_misses") orelse dataset.minmax_cache_misses;
                dataset.minmax_support_scans = jsonU64(obj, "algebraic_minmax_support_scans") orelse dataset.minmax_support_scans;
                dataset.planner_selected = jsonU64(obj, "algebraic_planner_selected") orelse dataset.planner_selected;
                dataset.planner_fallback_count = jsonU64(obj, "algebraic_planner_fallback_count") orelse dataset.planner_fallback_count;
                dataset.distributed_partial_validation_proven_count = jsonU64(obj, "algebraic_distributed_partial_validation_proven_count") orelse dataset.distributed_partial_validation_proven_count;
                dataset.distributed_partial_validation_rejected_count = jsonU64(obj, "algebraic_distributed_partial_validation_rejected_count") orelse dataset.distributed_partial_validation_rejected_count;
                dataset.distributed_partial_rows_exported_count = jsonU64(obj, "algebraic_distributed_partial_rows_exported_count") orelse dataset.distributed_partial_rows_exported_count;
                dataset.path_dictionary_fst_rebuild_count = jsonU64(obj, "algebraic_path_dictionary_fst_rebuild_count") orelse dataset.path_dictionary_fst_rebuild_count;
                dataset.observed_query_shape_count = jsonU64(obj, "algebraic_observed_query_shape_count") orelse dataset.observed_query_shape_count;
                dataset.recommendation_count = jsonU64(obj, "algebraic_recommendation_count") orelse dataset.recommendation_count;
                dataset.adaptive_maintenance_plan_build_count = jsonU64(obj, "algebraic_adaptive_maintenance_plan_build_count") orelse dataset.adaptive_maintenance_plan_build_count;
                dataset.adaptive_maintenance_cached_spec_count = jsonU64(obj, "algebraic_adaptive_maintenance_cached_spec_count") orelse dataset.adaptive_maintenance_cached_spec_count;
                dataset.adaptive_maintenance_disabled_count = jsonU64(obj, "algebraic_adaptive_maintenance_disabled_count") orelse dataset.adaptive_maintenance_disabled_count;
            }
        } else if (std.mem.eql(u8, event, "query")) {
            try queries.append(alloc, .{
                .case_name = try alloc.dupe(u8, jsonString(obj, "case") orelse "unknown"),
                .algebraic_backend = try alloc.dupe(u8, jsonString(obj, "algebraic_backend") orelse "unknown"),
                .algebraic_profile = try alloc.dupe(u8, jsonString(obj, "algebraic_profile") orelse "default"),
                .query = try alloc.dupe(u8, jsonString(obj, "query") orelse "unknown"),
                .engine = try alloc.dupe(u8, jsonString(obj, "engine") orelse "unknown"),
                .avg_ms = jsonF64(obj, "avg_ms") orelse 0,
                .checksum = jsonU64(obj, "checksum") orelse 0,
            });
        } else if (std.mem.eql(u8, event, "correctness")) {
            try correctness.append(alloc, .{
                .case_name = try alloc.dupe(u8, jsonString(obj, "case") orelse "unknown"),
                .query = try alloc.dupe(u8, jsonString(obj, "query") orelse "unknown"),
                .expected_equal = jsonBool(obj, "expected_equal") orelse false,
                .all_match = jsonBool(obj, "all_match") orelse false,
            });
        } else if (std.mem.eql(u8, event, "churn")) {
            try churn.append(alloc, .{
                .case_name = try alloc.dupe(u8, jsonString(obj, "case") orelse "unknown"),
                .docs = jsonU64(obj, "docs") orelse 0,
                .ops = jsonU64(obj, "ops") orelse 0,
                .batch_size = jsonU64(obj, "batch_size") orelse 0,
                .algebraic_update_ms = jsonF64(obj, "algebraic_update_ms") orelse 0,
                .full_text_update_ms = jsonF64(obj, "full_text_update_ms") orelse 0,
                .algebraic_sidecar_entries = jsonU64(obj, "algebraic_sidecar_entries") orelse 0,
                .algebraic_sidecar_bytes = jsonU64(obj, "algebraic_sidecar_bytes_estimate") orelse 0,
                .adaptive_maintenance_plan_build_count = jsonU64(obj, "algebraic_adaptive_maintenance_plan_build_count") orelse 0,
                .adaptive_maintenance_cached_spec_count = jsonU64(obj, "algebraic_adaptive_maintenance_cached_spec_count") orelse 0,
                .adaptive_maintenance_disabled_count = jsonU64(obj, "algebraic_adaptive_maintenance_disabled_count") orelse 0,
            });
        } else if (std.mem.eql(u8, event, "churn_row_family")) {
            try churn_row_families_records.append(alloc, .{
                .case_name = try alloc.dupe(u8, jsonString(obj, "case") orelse "unknown"),
                .family = try alloc.dupe(u8, jsonString(obj, "family") orelse "unknown"),
                .docs = jsonU64(obj, "docs") orelse 0,
                .ops = jsonU64(obj, "ops") orelse 0,
                .batch_size = jsonU64(obj, "batch_size") orelse 0,
                .algebraic_bulk_ingest = jsonBool(obj, "algebraic_bulk_ingest") orelse false,
                .algebraic_update_ms = jsonF64(obj, "algebraic_update_ms") orelse 0,
                .entries_before = jsonU64(obj, "entries_before") orelse 0,
                .entries_after = jsonU64(obj, "entries_after") orelse 0,
                .entries_delta = jsonI64(obj, "entries_delta") orelse 0,
                .bytes_before = jsonU64(obj, "bytes_before") orelse 0,
                .bytes_after = jsonU64(obj, "bytes_after") orelse 0,
                .bytes_delta = jsonI64(obj, "bytes_delta") orelse 0,
            });
        } else if (std.mem.eql(u8, event, "adaptive_warmup")) {
            try warmups.append(alloc, .{
                .case_name = try alloc.dupe(u8, jsonString(obj, "case") orelse "unknown"),
                .algebraic_backend = try alloc.dupe(u8, jsonString(obj, "algebraic_backend") orelse "unknown"),
                .algebraic_profile = try alloc.dupe(u8, jsonString(obj, "algebraic_profile") orelse "default"),
                .adaptive_warmup_ms = jsonF64(obj, "adaptive_warmup_ms") orelse 0,
                .adaptive_backfill_ticks = jsonU64(obj, "adaptive_backfill_ticks") orelse 0,
                .adaptive_ready_count = jsonU64(obj, "adaptive_ready_count") orelse 0,
                .adaptive_candidate_count = jsonU64(obj, "adaptive_candidate_count") orelse 0,
                .adaptive_progress_count = jsonU64(obj, "adaptive_progress_count") orelse 0,
                .adaptive_backfilling_count = jsonU64(obj, "adaptive_backfilling_count") orelse 0,
                .adaptive_rebuild_required_count = jsonU64(obj, "adaptive_rebuild_required_count") orelse 0,
                .adaptive_stale_count = jsonU64(obj, "adaptive_stale_count") orelse 0,
                .adaptive_cleanup_recommended_count = jsonU64(obj, "adaptive_cleanup_recommended_count") orelse 0,
                .adaptive_decision_history_count = jsonU64(obj, "adaptive_decision_history_count") orelse 0,
                .adaptive_policy_drift_count = jsonU64(obj, "adaptive_policy_drift_count") orelse 0,
                .adaptive_recommendation_count = jsonU64(obj, "adaptive_recommendation_count") orelse 0,
                .observed_query_shape_count = jsonU64(obj, "observed_query_shape_count") orelse 0,
            });
        } else if (std.mem.eql(u8, event, "graph_algebraic_traversal")) {
            try graph_traversals.append(alloc, .{
                .case_name = try alloc.dupe(u8, jsonString(obj, "case") orelse "unknown"),
                .mode = try alloc.dupe(u8, jsonString(obj, "mode") orelse "unknown"),
                .docs = jsonU64(obj, "docs") orelse 0,
                .edges = jsonU64(obj, "edges") orelse 0,
                .fanout = jsonU64(obj, "fanout") orelse 0,
                .repeats = jsonU64(obj, "repeats") orelse 0,
                .path_bytes = jsonU64(obj, "path_bytes") orelse 0,
                .build_ms = jsonF64(obj, "build_ms") orelse 0,
                .traverse_avg_ms = jsonF64(obj, "traverse_avg_ms") orelse 0,
                .shortest_avg_ms = jsonF64(obj, "shortest_avg_ms") orelse 0,
                .rejected_avg_ms = jsonF64(obj, "rejected_avg_ms") orelse 0,
                .attempted = jsonU64(obj, "attempted") orelse 0,
                .proven = jsonU64(obj, "proven") orelse 0,
                .rejected = jsonU64(obj, "rejected") orelse 0,
                .fallback = jsonU64(obj, "fallback") orelse 0,
                .result_nodes = jsonU64(obj, "result_nodes") orelse 0,
            });
        } else if (std.mem.eql(u8, event, "public_query_guardrail_summary")) {
            try public_queries.append(alloc, .{
                .mode = try alloc.dupe(u8, jsonString(obj, "mode") orelse "unknown"),
                .server = try alloc.dupe(u8, jsonString(obj, "server") orelse "unknown"),
                .query_shape = try alloc.dupe(u8, jsonString(obj, "query_shape") orelse "unknown"),
                .with_schema = jsonBool(obj, "with_schema") orelse false,
                .with_algebraic = jsonBool(obj, "with_algebraic") orelse false,
                .docs = jsonU64(obj, "docs") orelse 0,
                .dims = jsonU64(obj, "dims") orelse 0,
                .queries = jsonU64(obj, "queries") orelse 0,
                .repeats = jsonU64(obj, "repeats") orelse 0,
                .k = jsonU64(obj, "k") orelse 0,
                .threads = jsonU64(obj, "threads") orelse 0,
                .db_avg_us = jsonF64(obj, "db_avg_us") orelse 0,
                .handler_avg_us = jsonF64(obj, "handler_avg_us") orelse 0,
                .http_avg_us = jsonF64(obj, "http_avg_us") orelse 0,
                .profile_total_us = jsonF64(obj, "profile_total_us") orelse 0,
                .profile_hbc_us = jsonF64(obj, "profile_hbc_us") orelse 0,
                .concurrent_qps = jsonF64(obj, "concurrent_qps") orelse 0,
                .concurrent_avg_us = jsonF64(obj, "concurrent_avg_us") orelse 0,
                .load_insert_ms = jsonF64(obj, "load_insert_ms") orelse 0,
                .index_wait_ms = jsonF64(obj, "index_wait_ms") orelse 0,
                .load_total_ms = jsonF64(obj, "load_total_ms") orelse 0,
                .load_rss_peak_bytes = jsonU64(obj, "load_rss_peak_bytes") orelse 0,
                .search_rss_peak_bytes = jsonU64(obj, "search_rss_peak_bytes") orelse 0,
                .hbc_total_bytes = jsonU64(obj, "hbc_total_bytes") orelse 0,
                .hbc_accounted_bytes = jsonU64(obj, "hbc_accounted_bytes") orelse 0,
                .lsm_cache_bytes = jsonU64(obj, "lsm_cache_bytes") orelse 0,
                .full_text_pending_bytes = jsonU64(obj, "full_text_pending_bytes") orelse 0,
                .replay_window_bytes = jsonU64(obj, "replay_window_bytes") orelse 0,
                .approx_candidates_avg = jsonF64(obj, "approx_candidates_avg") orelse 0,
                .rerank_candidates_avg = jsonF64(obj, "rerank_candidates_avg") orelse 0,
                .reranked_vectors_avg = jsonF64(obj, "reranked_vectors_avg") orelse 0,
                .returned_hits_avg = jsonF64(obj, "returned_hits_avg") orelse 0,
            });
        }
    }

    for (datasets.items) |dataset| {
        const docs = if (dataset.docs == 0) 1 else dataset.docs;
        std.debug.print(
            "{{\"event\":\"dataset_summary\",\"case\":\"{s}\",\"algebraic_backend\":\"{s}\",\"algebraic_profile\":\"{s}\",\"algebraic_bulk_ingest\":{},\"algebraic_bulk_compact\":{},\"algebraic_bulk_flush\":{},\"docs\":{d},\"materialization_count\":{d},\"algebraic_bytes_per_doc\":{d:.3},\"algebraic_bytes_per_materialization\":{d:.3},\"algebraic_backend_path_bytes_per_doc\":{d:.3},\"full_text_bytes_per_doc\":{d:.3},\"support_entries\":{d},\"support_bytes\":{d},\"symbol_entries\":{d},\"symbol_bytes\":{d},\"minmax_cache_entries\":{d},\"minmax_cache_bytes\":{d},\"algebraic_build_ms\":{d:.3},\"full_text_build_ms\":{d:.3},\"build_speedup_vs_full_text\":{d:.3}",
            .{
                dataset.case_name,
                dataset.algebraic_backend,
                dataset.algebraic_profile,
                dataset.algebraic_bulk_ingest,
                dataset.algebraic_bulk_compact,
                dataset.algebraic_bulk_flush,
                dataset.docs,
                dataset.materialization_count,
                @as(f64, @floatFromInt(dataset.algebraic_bytes)) / @as(f64, @floatFromInt(docs)),
                @as(f64, @floatFromInt(dataset.algebraic_bytes)) / @as(f64, @floatFromInt(if (dataset.materialization_count == 0) 1 else dataset.materialization_count)),
                @as(f64, @floatFromInt(dataset.algebraic_backend_path_bytes)) / @as(f64, @floatFromInt(docs)),
                @as(f64, @floatFromInt(dataset.full_text_bytes)) / @as(f64, @floatFromInt(docs)),
                dataset.support_entries,
                dataset.support_bytes,
                dataset.symbol_entries,
                dataset.symbol_bytes,
                dataset.minmax_cache_entries,
                dataset.minmax_cache_bytes,
                dataset.algebraic_build_ms,
                dataset.full_text_build_ms,
                ratio(dataset.full_text_build_ms, dataset.algebraic_build_ms),
            },
        );
        std.debug.print(
            ",\"join_facts_scanned_per_doc\":{d:.3},\"join_match_rate\":{d:.3},\"join_prune_rate\":{d:.3},\"accumulator_flush_count\":{d},\"adaptive_maintenance_plan_build_count\":{d},\"adaptive_maintenance_cached_spec_count\":{d},\"adaptive_maintenance_disabled_count\":{d},\"path_dictionary_fst_rebuild_count\":{d},\"minmax_cache_hit_rate\":{d:.3},\"minmax_support_scans\":{d},\"planner_selected\":{d},\"planner_fallback_count\":{d},\"distributed_partial_validation_proven_count\":{d},\"distributed_partial_validation_rejected_count\":{d},\"distributed_partial_rows_exported_count\":{d}}}\n",
            .{
                @as(f64, @floatFromInt(dataset.join_facts_scanned)) / @as(f64, @floatFromInt(docs)),
                ratio(@as(f64, @floatFromInt(dataset.join_facts_matched)), @as(f64, @floatFromInt(dataset.join_facts_scanned))),
                ratio(@as(f64, @floatFromInt(dataset.join_facts_pruned)), @as(f64, @floatFromInt(dataset.join_facts_scanned))),
                dataset.accumulator_flush_count,
                dataset.adaptive_maintenance_plan_build_count,
                dataset.adaptive_maintenance_cached_spec_count,
                dataset.adaptive_maintenance_disabled_count,
                dataset.path_dictionary_fst_rebuild_count,
                ratio(@as(f64, @floatFromInt(dataset.minmax_cache_hits)), @as(f64, @floatFromInt(dataset.minmax_cache_hits + dataset.minmax_cache_misses))),
                dataset.minmax_support_scans,
                dataset.planner_selected,
                dataset.planner_fallback_count,
                dataset.distributed_partial_validation_proven_count,
                dataset.distributed_partial_validation_rejected_count,
                dataset.distributed_partial_rows_exported_count,
            },
        );
    }
    printBackendDatasetComparisons(datasets.items);
    printBackendTuningDatasetComparisons(datasets.items);
    printAdaptiveDatasetComparisons(datasets.items);
    printAdaptiveCoverageSummary(datasets.items, queries.items, warmups.items);
    printLsmAnalyticsSummary(datasets.items, queries.items, churn.items);
    printChurnRowFamilySummaries(churn_row_families_records.items);
    printGraphTraversalSummary(graph_traversals.items);
    printPublicQueryComparisonSummary(public_queries.items);
    const evidence = summarizePerformanceEvidence(datasets.items, queries.items, correctness.items, churn.items, churn_row_families_records.items, public_queries.items);
    printPerformanceEvidenceSummary(evidence);
    if (cfg.guardrail.enabled) try enforcePerformanceGuardrail(cfg.guardrail, evidence);
    if (cfg.baseline_path.len > 0) {
        const baseline_raw = try std.Io.Dir.cwd().readFileAlloc(init.io, cfg.baseline_path, alloc, .limited(1024 * 1024 * 1024));
        defer alloc.free(baseline_raw);
        const baseline = try parseBaselinePerformanceEvidence(alloc, baseline_raw);
        printPerformanceBaselineComparison(evidence, baseline);
        try enforcePerformanceBaselineGuardrail(cfg.guardrail, evidence, baseline);
    }

    for (queries.items) |query| {
        if (!isAlgebraicEngine(query.engine)) continue;
        const expected_equal = queryExpectedEqual(correctness.items, query.case_name, query.query);
        if (findBaseline(queries.items, query.case_name, query.algebraic_backend, query.algebraic_profile, query.query, "doc_scan")) |doc_scan| {
            printQuerySummary(query, doc_scan, expected_equal);
        }
        if (findBaseline(queries.items, query.case_name, query.algebraic_backend, query.algebraic_profile, query.query, "full_text_index")) |full_text| {
            printQuerySummary(query, full_text, expected_equal);
        }
    }
    printBackendQueryComparisons(queries.items);
    printBackendTuningQueryComparisons(queries.items);
    printAdaptiveQueryComparisons(queries.items);
    printAdaptiveChurnComparisons(churn.items);
    printAdaptiveWarmupComparisons(warmups.items, datasets.items);

    for (correctness.items) |item| {
        if (!item.expected_equal or item.all_match) continue;
        std.debug.print(
            "{{\"event\":\"correctness_failure\",\"case\":\"{s}\",\"query\":\"{s}\"}}\n",
            .{ item.case_name, item.query },
        );
    }
}

fn printBackendDatasetComparisons(items: []const DatasetRecord) void {
    for (items) |dataset| {
        if (std.mem.eql(u8, dataset.algebraic_backend, "mem")) continue;
        const mem = findDataset(items, dataset.case_name, "mem", dataset.algebraic_profile) orelse continue;
        const logical_denominator = if (mem.algebraic_bytes == 0) 1 else mem.algebraic_bytes;
        const dataset_logical_denominator = if (dataset.algebraic_bytes == 0) 1 else dataset.algebraic_bytes;
        const full_text_denominator = if (dataset.full_text_bytes == 0) 1 else dataset.full_text_bytes;
        std.debug.print(
            "{{\"event\":\"backend_dataset_compare\",\"case\":\"{s}\",\"algebraic_profile\":\"{s}\",\"baseline_backend\":\"mem\",\"algebraic_backend\":\"{s}\",\"build_ms_ratio_vs_mem\":{d:.3},\"logical_bytes_ratio_vs_mem\":{d:.3},\"path_bytes_ratio_vs_logical_sidecar\":{d:.3},\"path_bytes_ratio_vs_full_text\":{d:.3}}}\n",
            .{
                dataset.case_name,
                dataset.algebraic_profile,
                dataset.algebraic_backend,
                ratio(dataset.algebraic_build_ms, mem.algebraic_build_ms),
                @as(f64, @floatFromInt(dataset.algebraic_bytes)) / @as(f64, @floatFromInt(logical_denominator)),
                @as(f64, @floatFromInt(dataset.algebraic_backend_path_bytes)) / @as(f64, @floatFromInt(dataset_logical_denominator)),
                @as(f64, @floatFromInt(dataset.algebraic_backend_path_bytes)) / @as(f64, @floatFromInt(full_text_denominator)),
            },
        );
    }
}

fn printBackendTuningDatasetComparisons(items: []const DatasetRecord) void {
    for (items) |dataset| {
        if (!std.mem.eql(u8, dataset.algebraic_backend, "lsm")) continue;
        const baseline_profile = tuningBaselineProfile(dataset.algebraic_profile);
        if (std.mem.eql(u8, dataset.algebraic_profile, baseline_profile)) continue;
        const normal = findDataset(items, dataset.case_name, "lsm", baseline_profile) orelse continue;
        const normal_path_denominator = if (normal.algebraic_backend_path_bytes == 0) 1 else normal.algebraic_backend_path_bytes;
        std.debug.print(
            "{{\"event\":\"backend_tuning_dataset_compare\",\"case\":\"{s}\",\"algebraic_backend\":\"lsm\",\"baseline_profile\":\"{s}\",\"algebraic_profile\":\"{s}\",\"build_ms_ratio_vs_normal\":{d:.3},\"path_bytes_ratio_vs_normal\":{d:.3}}}\n",
            .{
                dataset.case_name,
                baseline_profile,
                dataset.algebraic_profile,
                ratio(dataset.algebraic_build_ms, normal.algebraic_build_ms),
                @as(f64, @floatFromInt(dataset.algebraic_backend_path_bytes)) / @as(f64, @floatFromInt(normal_path_denominator)),
            },
        );
    }
}

fn printBackendQueryComparisons(items: []const QueryRecord) void {
    for (items) |query| {
        if (!isAlgebraicEngine(query.engine)) continue;
        if (std.mem.eql(u8, query.algebraic_backend, "mem")) continue;
        const mem = findQuery(items, query.case_name, "mem", query.algebraic_profile, query.query, query.engine) orelse continue;
        std.debug.print(
            "{{\"event\":\"backend_query_compare\",\"case\":\"{s}\",\"algebraic_profile\":\"{s}\",\"query\":\"{s}\",\"engine\":\"{s}\",\"baseline_backend\":\"mem\",\"algebraic_backend\":\"{s}\",\"avg_ms_ratio_vs_mem\":{d:.3},\"speedup_vs_mem\":{d:.3},\"checksum_match\":{}}}\n",
            .{
                query.case_name,
                query.algebraic_profile,
                query.query,
                query.engine,
                query.algebraic_backend,
                ratio(query.avg_ms, mem.avg_ms),
                ratio(mem.avg_ms, query.avg_ms),
                query.checksum == mem.checksum,
            },
        );
    }
}

fn printBackendTuningQueryComparisons(items: []const QueryRecord) void {
    for (items) |query| {
        if (!isAlgebraicEngine(query.engine)) continue;
        if (!std.mem.eql(u8, query.algebraic_backend, "lsm")) continue;
        const baseline_profile = tuningBaselineProfile(query.algebraic_profile);
        if (std.mem.eql(u8, query.algebraic_profile, baseline_profile)) continue;
        const normal = findQuery(items, query.case_name, "lsm", baseline_profile, query.query, query.engine) orelse continue;
        std.debug.print(
            "{{\"event\":\"backend_tuning_query_compare\",\"case\":\"{s}\",\"algebraic_backend\":\"lsm\",\"baseline_profile\":\"{s}\",\"algebraic_profile\":\"{s}\",\"query\":\"{s}\",\"engine\":\"{s}\",\"avg_ms_ratio_vs_normal\":{d:.3},\"speedup_vs_normal\":{d:.3},\"checksum_match\":{}}}\n",
            .{
                query.case_name,
                baseline_profile,
                query.algebraic_profile,
                query.query,
                query.engine,
                ratio(query.avg_ms, normal.avg_ms),
                ratio(normal.avg_ms, query.avg_ms),
                query.checksum == normal.checksum,
            },
        );
    }
}

fn printAdaptiveDatasetComparisons(items: []const DatasetRecord) void {
    for (items) |materialized| {
        if (!isAdaptiveMaterializedCase(materialized.case_name)) continue;
        const static_case = adaptiveBaselineCase(materialized.case_name, "static");
        const fallback_case = adaptiveBaselineCase(materialized.case_name, "fallback");
        if (findDataset(items, static_case, materialized.algebraic_backend, materialized.algebraic_profile)) |static| {
            printAdaptiveDatasetCompare(materialized, static, static_case);
        }
        if (findDataset(items, fallback_case, materialized.algebraic_backend, materialized.algebraic_profile)) |fallback| {
            printAdaptiveDatasetCompare(materialized, fallback, fallback_case);
        }
    }
}

fn printAdaptiveDatasetCompare(materialized: DatasetRecord, baseline: DatasetRecord, baseline_case: []const u8) void {
    const baseline_bytes = if (baseline.algebraic_bytes == 0) 1 else baseline.algebraic_bytes;
    const baseline_path_bytes = if (baseline.algebraic_backend_path_bytes == 0) 1 else baseline.algebraic_backend_path_bytes;
    std.debug.print(
        "{{\"event\":\"adaptive_dataset_compare\",\"algebraic_backend\":\"{s}\",\"algebraic_profile\":\"{s}\",\"baseline_case\":\"{s}\",\"materialized_case\":\"{s}\",\"docs\":{d},\"build_ms_ratio_vs_baseline\":{d:.3},\"logical_bytes_ratio_vs_baseline\":{d:.3},\"path_bytes_ratio_vs_baseline\":{d:.3},\"baseline_declared_materialization_count\":{d},\"materialized_declared_materialization_count\":{d},\"baseline_observed_query_shape_count\":{d},\"materialized_observed_query_shape_count\":{d},\"baseline_recommendation_count\":{d},\"materialized_recommendation_count\":{d},\"baseline_planner_selected\":{d},\"materialized_planner_selected\":{d},\"baseline_planner_fallback_count\":{d},\"materialized_planner_fallback_count\":{d}}}\n",
        .{
            materialized.algebraic_backend,
            materialized.algebraic_profile,
            baseline_case,
            materialized.case_name,
            materialized.docs,
            ratio(materialized.algebraic_build_ms, baseline.algebraic_build_ms),
            @as(f64, @floatFromInt(materialized.algebraic_bytes)) / @as(f64, @floatFromInt(baseline_bytes)),
            @as(f64, @floatFromInt(materialized.algebraic_backend_path_bytes)) / @as(f64, @floatFromInt(baseline_path_bytes)),
            baseline.materialization_count,
            materialized.materialization_count,
            baseline.observed_query_shape_count,
            materialized.observed_query_shape_count,
            baseline.recommendation_count,
            materialized.recommendation_count,
            baseline.planner_selected,
            materialized.planner_selected,
            baseline.planner_fallback_count,
            materialized.planner_fallback_count,
        },
    );
}

fn printAdaptiveQueryComparisons(items: []const QueryRecord) void {
    for (items) |materialized| {
        if (!isAdaptiveMaterializedCase(materialized.case_name)) continue;
        if (!isAlgebraicEngine(materialized.engine)) continue;
        const static_case = adaptiveBaselineCase(materialized.case_name, "static");
        const fallback_case = adaptiveBaselineCase(materialized.case_name, "fallback");
        if (findQuery(items, static_case, materialized.algebraic_backend, materialized.algebraic_profile, materialized.query, materialized.engine)) |static| {
            printAdaptiveQueryCompare(materialized, static, static_case);
        }
        if (findQuery(items, fallback_case, materialized.algebraic_backend, materialized.algebraic_profile, materialized.query, materialized.engine)) |fallback| {
            printAdaptiveQueryCompare(materialized, fallback, fallback_case);
        }
    }
}

fn printAdaptiveQueryCompare(materialized: QueryRecord, baseline: QueryRecord, baseline_case: []const u8) void {
    std.debug.print(
        "{{\"event\":\"adaptive_query_compare\",\"algebraic_backend\":\"{s}\",\"algebraic_profile\":\"{s}\",\"query\":\"{s}\",\"engine\":\"{s}\",\"baseline_case\":\"{s}\",\"materialized_case\":\"{s}\",\"materialized_avg_ms\":{d:.3},\"baseline_avg_ms\":{d:.3},\"speedup_vs_baseline\":{d:.3},\"avg_ms_ratio_vs_baseline\":{d:.3},\"checksum_match\":{}}}\n",
        .{
            materialized.algebraic_backend,
            materialized.algebraic_profile,
            materialized.query,
            materialized.engine,
            baseline_case,
            materialized.case_name,
            materialized.avg_ms,
            baseline.avg_ms,
            ratio(baseline.avg_ms, materialized.avg_ms),
            ratio(materialized.avg_ms, baseline.avg_ms),
            materialized.checksum == baseline.checksum,
        },
    );
}

fn printAdaptiveChurnComparisons(items: []const ChurnRecord) void {
    for (items) |materialized| {
        if (!isAdaptiveMaterializedCase(materialized.case_name)) continue;
        const static_case = adaptiveBaselineCase(materialized.case_name, "static");
        const fallback_case = adaptiveBaselineCase(materialized.case_name, "fallback");
        if (findChurn(items, static_case)) |static| {
            printAdaptiveChurnCompare(materialized, static, static_case);
        }
        if (findChurn(items, fallback_case)) |fallback| {
            printAdaptiveChurnCompare(materialized, fallback, fallback_case);
        }
    }
}

fn printAdaptiveChurnCompare(materialized: ChurnRecord, baseline: ChurnRecord, baseline_case: []const u8) void {
    const baseline_sidecar_bytes = if (baseline.algebraic_sidecar_bytes == 0) 1 else baseline.algebraic_sidecar_bytes;
    std.debug.print(
        "{{\"event\":\"adaptive_churn_compare\",\"baseline_case\":\"{s}\",\"materialized_case\":\"{s}\",\"docs\":{d},\"ops\":{d},\"batch_size\":{d},\"materialized_update_ms\":{d:.3},\"baseline_update_ms\":{d:.3},\"speedup_vs_baseline\":{d:.3},\"update_ms_ratio_vs_baseline\":{d:.3},\"full_text_update_ms\":{d:.3},\"materialized_sidecar_entries\":{d},\"baseline_sidecar_entries\":{d},\"sidecar_bytes_ratio_vs_baseline\":{d:.3},\"adaptive_maintenance_plan_build_count\":{d},\"adaptive_maintenance_cached_spec_count\":{d},\"adaptive_maintenance_disabled_count\":{d}}}\n",
        .{
            baseline_case,
            materialized.case_name,
            materialized.docs,
            materialized.ops,
            materialized.batch_size,
            materialized.algebraic_update_ms,
            baseline.algebraic_update_ms,
            ratio(baseline.algebraic_update_ms, materialized.algebraic_update_ms),
            ratio(materialized.algebraic_update_ms, baseline.algebraic_update_ms),
            materialized.full_text_update_ms,
            materialized.algebraic_sidecar_entries,
            baseline.algebraic_sidecar_entries,
            @as(f64, @floatFromInt(materialized.algebraic_sidecar_bytes)) / @as(f64, @floatFromInt(baseline_sidecar_bytes)),
            materialized.adaptive_maintenance_plan_build_count,
            materialized.adaptive_maintenance_cached_spec_count,
            materialized.adaptive_maintenance_disabled_count,
        },
    );
}

fn printAdaptiveWarmupComparisons(warmups: []const WarmupRecord, datasets: []const DatasetRecord) void {
    for (warmups) |warmup| {
        const dataset = findDataset(datasets, warmup.case_name, warmup.algebraic_backend, warmup.algebraic_profile);
        std.debug.print(
            "{{\"event\":\"adaptive_warmup_compare\",\"case\":\"{s}\",\"algebraic_backend\":\"{s}\",\"algebraic_profile\":\"{s}\",\"docs\":{d},\"adaptive_warmup_ms\":{d:.3},\"adaptive_warmup_ms_per_doc\":{d:.6},\"adaptive_backfill_ticks\":{d},\"adaptive_ready_count\":{d},\"adaptive_candidate_count\":{d},\"adaptive_progress_count\":{d},\"adaptive_backfilling_count\":{d},\"adaptive_rebuild_required_count\":{d},\"adaptive_stale_count\":{d},\"adaptive_cleanup_recommended_count\":{d},\"adaptive_decision_history_count\":{d},\"adaptive_policy_drift_count\":{d},\"adaptive_recommendation_count\":{d},\"observed_query_shape_count\":{d},\"sidecar_bytes_after_warmup\":{d}}}\n",
            .{
                warmup.case_name,
                warmup.algebraic_backend,
                warmup.algebraic_profile,
                if (dataset) |item| item.docs else 0,
                warmup.adaptive_warmup_ms,
                ratio(warmup.adaptive_warmup_ms, @as(f64, @floatFromInt(if (dataset) |item| item.docs else 0))),
                warmup.adaptive_backfill_ticks,
                warmup.adaptive_ready_count,
                warmup.adaptive_candidate_count,
                warmup.adaptive_progress_count,
                warmup.adaptive_backfilling_count,
                warmup.adaptive_rebuild_required_count,
                warmup.adaptive_stale_count,
                warmup.adaptive_cleanup_recommended_count,
                warmup.adaptive_decision_history_count,
                warmup.adaptive_policy_drift_count,
                warmup.adaptive_recommendation_count,
                warmup.observed_query_shape_count,
                if (dataset) |item| item.algebraic_bytes else 0,
            },
        );
    }
}

fn printAdaptiveCoverageSummary(datasets: []const DatasetRecord, queries: []const QueryRecord, warmups: []const WarmupRecord) void {
    for (datasets) |dataset| {
        if (!isAdaptiveCoverageCase(dataset.case_name)) continue;
        const query_count = countQueriesForCase(queries, dataset.case_name, dataset.algebraic_backend, dataset.algebraic_profile);
        const algebraic_query_count = countAlgebraicQueriesForCase(queries, dataset.case_name, dataset.algebraic_backend, dataset.algebraic_profile);
        const warmup = findWarmup(warmups, dataset.case_name, dataset.algebraic_backend, dataset.algebraic_profile);
        std.debug.print(
            "{{\"event\":\"adaptive_coverage_summary\",\"case\":\"{s}\",\"algebraic_backend\":\"{s}\",\"algebraic_profile\":\"{s}\",\"docs\":{d},\"queries_recorded\":{d},\"algebraic_queries_recorded\":{d},\"materialization_count\":{d},\"observed_query_shape_count\":{d},\"recommendation_count\":{d},\"adaptive_warmup_ms\":{d:.3},\"adaptive_backfilling_count\":{d},\"adaptive_rebuild_required_count\":{d},\"adaptive_stale_count\":{d},\"adaptive_cleanup_recommended_count\":{d},\"adaptive_decision_history_count\":{d},\"adaptive_policy_drift_count\":{d}}}\n",
            .{
                dataset.case_name,
                dataset.algebraic_backend,
                dataset.algebraic_profile,
                dataset.docs,
                query_count,
                algebraic_query_count,
                dataset.materialization_count,
                dataset.observed_query_shape_count,
                dataset.recommendation_count,
                if (warmup) |item| item.adaptive_warmup_ms else 0,
                if (warmup) |item| item.adaptive_backfilling_count else 0,
                if (warmup) |item| item.adaptive_rebuild_required_count else 0,
                if (warmup) |item| item.adaptive_stale_count else 0,
                if (warmup) |item| item.adaptive_cleanup_recommended_count else 0,
                if (warmup) |item| item.adaptive_decision_history_count else 0,
                if (warmup) |item| item.adaptive_policy_drift_count else 0,
            },
        );
    }
}

fn isAdaptiveCoverageCase(case_name: []const u8) bool {
    return std.mem.startsWith(u8, case_name, "adaptive_coverage_") or
        std.mem.eql(u8, case_name, "lsm_analytics_smoke_adaptive");
}

fn isLsmAnalyticsProfile(profile: []const u8) bool {
    return std.mem.eql(u8, profile, "lsm_analytics") or
        std.mem.eql(u8, profile, "lsm_analytics_smoke");
}

fn printLsmAnalyticsSummary(datasets: []const DatasetRecord, queries: []const QueryRecord, churn: []const ChurnRecord) void {
    var dataset_count: u64 = 0;
    var query_count: u64 = 0;
    var churn_count: u64 = 0;
    var total_path_bytes: u64 = 0;
    var total_build_ms: f64 = 0;
    var total_distributed_partial_validation_proven_count: u64 = 0;
    var total_distributed_partial_validation_rejected_count: u64 = 0;
    var total_distributed_partial_rows_exported_count: u64 = 0;
    for (datasets) |dataset| {
        if (!std.mem.eql(u8, dataset.algebraic_backend, "lsm")) continue;
        if (!isLsmAnalyticsProfile(dataset.algebraic_profile)) continue;
        dataset_count += 1;
        total_path_bytes += dataset.algebraic_backend_path_bytes;
        total_build_ms += dataset.algebraic_build_ms;
        total_distributed_partial_validation_proven_count += dataset.distributed_partial_validation_proven_count;
        total_distributed_partial_validation_rejected_count += dataset.distributed_partial_validation_rejected_count;
        total_distributed_partial_rows_exported_count += dataset.distributed_partial_rows_exported_count;
    }
    for (queries) |query| {
        if (!std.mem.eql(u8, query.algebraic_backend, "lsm")) continue;
        if (!isLsmAnalyticsProfile(query.algebraic_profile)) continue;
        query_count += 1;
    }
    for (churn) |item| {
        if (std.mem.indexOf(u8, item.case_name, "lsm_analytics") != null or
            std.mem.startsWith(u8, item.case_name, "adaptive_coverage_"))
        {
            churn_count += 1;
        }
    }
    if (dataset_count == 0) return;
    std.debug.print(
        "{{\"event\":\"lsm_analytics_summary\",\"dataset_cases\":{d},\"query_records\":{d},\"churn_records\":{d},\"total_path_bytes\":{d},\"total_build_ms\":{d:.3},\"distributed_partial_validation_proven_count\":{d},\"distributed_partial_validation_rejected_count\":{d},\"distributed_partial_rows_exported_count\":{d}}}\n",
        .{ dataset_count, query_count, churn_count, total_path_bytes, total_build_ms, total_distributed_partial_validation_proven_count, total_distributed_partial_validation_rejected_count, total_distributed_partial_rows_exported_count },
    );
}

fn printChurnRowFamilySummaries(items: []const ChurnRowFamilyRecord) void {
    for (churn_row_families) |family| {
        var records: u64 = 0;
        var bulk_records: u64 = 0;
        var total_update_ms: f64 = 0;
        var total_entries_before: u64 = 0;
        var total_entries_after: u64 = 0;
        var total_entries_delta: i64 = 0;
        var total_bytes_before: u64 = 0;
        var total_bytes_after: u64 = 0;
        var total_bytes_delta: i64 = 0;
        var max_bytes_after: u64 = 0;
        for (items) |item| {
            if (!std.mem.eql(u8, item.family, family)) continue;
            records += 1;
            if (item.algebraic_bulk_ingest) bulk_records += 1;
            total_update_ms += item.algebraic_update_ms;
            total_entries_before += item.entries_before;
            total_entries_after += item.entries_after;
            total_entries_delta += item.entries_delta;
            total_bytes_before += item.bytes_before;
            total_bytes_after += item.bytes_after;
            total_bytes_delta += item.bytes_delta;
            max_bytes_after = @max(max_bytes_after, item.bytes_after);
        }
        if (records == 0) continue;
        std.debug.print(
            "{{\"event\":\"churn_row_family_summary\",\"family\":\"{s}\",\"records\":{d},\"bulk_records\":{d},\"total_update_ms\":{d:.3},\"total_entries_before\":{d},\"total_entries_after\":{d},\"total_entries_delta\":{d},\"total_bytes_before\":{d},\"total_bytes_after\":{d},\"total_bytes_delta\":{d},\"max_bytes_after\":{d}}}\n",
            .{
                family,
                records,
                bulk_records,
                total_update_ms,
                total_entries_before,
                total_entries_after,
                total_entries_delta,
                total_bytes_before,
                total_bytes_after,
                total_bytes_delta,
                max_bytes_after,
            },
        );
    }
}

fn printGraphTraversalSummary(items: []const GraphTraversalRecord) void {
    for (items) |item| {
        const docs = if (item.docs == 0) 1 else item.docs;
        std.debug.print(
            "{{\"event\":\"graph_algebraic_traversal_summary\",\"case\":\"{s}\",\"mode\":\"{s}\",\"docs\":{d},\"edges\":{d},\"fanout\":{d},\"repeats\":{d},\"path_bytes_per_doc\":{d:.3},\"build_ms\":{d:.3},\"traverse_avg_ms\":{d:.3},\"shortest_avg_ms\":{d:.3},\"rejected_avg_ms\":{d:.3},\"attempted\":{d},\"proven\":{d},\"rejected\":{d},\"fallback\":{d},\"result_nodes\":{d},\"proof_rate\":{d:.3},\"reject_rate\":{d:.3},\"fallback_rate\":{d:.3},\"result_nodes_per_proven\":{d:.3}}}\n",
            .{
                item.case_name,
                item.mode,
                item.docs,
                item.edges,
                item.fanout,
                item.repeats,
                @as(f64, @floatFromInt(item.path_bytes)) / @as(f64, @floatFromInt(docs)),
                item.build_ms,
                item.traverse_avg_ms,
                item.shortest_avg_ms,
                item.rejected_avg_ms,
                item.attempted,
                item.proven,
                item.rejected,
                item.fallback,
                item.result_nodes,
                ratio(@as(f64, @floatFromInt(item.proven)), @as(f64, @floatFromInt(item.attempted))),
                ratio(@as(f64, @floatFromInt(item.rejected)), @as(f64, @floatFromInt(item.attempted))),
                ratio(@as(f64, @floatFromInt(item.fallback)), @as(f64, @floatFromInt(item.attempted))),
                ratio(@as(f64, @floatFromInt(item.result_nodes)), @as(f64, @floatFromInt(item.proven))),
            },
        );
    }
}

fn printPublicQueryComparisonSummary(items: []const PublicQueryRecord) void {
    for (items) |item| {
        if (!item.with_algebraic) continue;
        if (findPublicQueryBaseline(items, item, false, false)) |baseline| {
            printPublicQueryCompare(item, baseline, "no_schema");
        }
        if (findPublicQueryBaseline(items, item, true, false)) |schema_only| {
            printPublicQueryCompare(item, schema_only, "schema_only");
        }
    }
}

fn printPublicQueryCompare(algebraic: PublicQueryRecord, baseline: PublicQueryRecord, baseline_kind: []const u8) void {
    std.debug.print(
        "{{\"event\":\"public_query_comparison_summary\",\"baseline\":\"{s}\",\"mode\":\"{s}\",\"server\":\"{s}\",\"query_shape\":\"{s}\",\"docs\":{d},\"dims\":{d},\"queries\":{d},\"repeats\":{d},\"k\":{d},\"threads\":{d}",
        .{
            baseline_kind,
            algebraic.mode,
            algebraic.server,
            algebraic.query_shape,
            algebraic.docs,
            algebraic.dims,
            algebraic.queries,
            algebraic.repeats,
            algebraic.k,
            algebraic.threads,
        },
    );
    std.debug.print(
        ",\"algebraic_http_avg_us\":{d:.3},\"baseline_http_avg_us\":{d:.3},\"http_speedup\":{d:.3},\"algebraic_concurrent_qps\":{d:.3},\"baseline_concurrent_qps\":{d:.3},\"qps_ratio\":{d:.3},\"algebraic_profile_total_us\":{d:.3},\"baseline_profile_total_us\":{d:.3},\"profile_speedup\":{d:.3},\"algebraic_hbc_us\":{d:.3},\"baseline_hbc_us\":{d:.3},\"hbc_speedup\":{d:.3}",
        .{
            algebraic.http_avg_us,
            baseline.http_avg_us,
            ratio(baseline.http_avg_us, algebraic.http_avg_us),
            algebraic.concurrent_qps,
            baseline.concurrent_qps,
            ratio(algebraic.concurrent_qps, baseline.concurrent_qps),
            algebraic.profile_total_us,
            baseline.profile_total_us,
            ratio(baseline.profile_total_us, algebraic.profile_total_us),
            algebraic.profile_hbc_us,
            baseline.profile_hbc_us,
            ratio(baseline.profile_hbc_us, algebraic.profile_hbc_us),
        },
    );
    std.debug.print(
        ",\"algebraic_approx_candidates_avg\":{d:.3},\"baseline_approx_candidates_avg\":{d:.3},\"approx_candidate_ratio\":{d:.3},\"algebraic_rerank_candidates_avg\":{d:.3},\"baseline_rerank_candidates_avg\":{d:.3},\"rerank_candidate_ratio\":{d:.3},\"algebraic_reranked_vectors_avg\":{d:.3},\"baseline_reranked_vectors_avg\":{d:.3},\"reranked_vector_ratio\":{d:.3},\"algebraic_returned_hits_avg\":{d:.3},\"baseline_returned_hits_avg\":{d:.3}",
        .{
            algebraic.approx_candidates_avg,
            baseline.approx_candidates_avg,
            ratio(algebraic.approx_candidates_avg, baseline.approx_candidates_avg),
            algebraic.rerank_candidates_avg,
            baseline.rerank_candidates_avg,
            ratio(algebraic.rerank_candidates_avg, baseline.rerank_candidates_avg),
            algebraic.reranked_vectors_avg,
            baseline.reranked_vectors_avg,
            ratio(algebraic.reranked_vectors_avg, baseline.reranked_vectors_avg),
            algebraic.returned_hits_avg,
            baseline.returned_hits_avg,
        },
    );
    std.debug.print(
        ",\"algebraic_hbc_total_bytes\":{d},\"baseline_hbc_total_bytes\":{d},\"hbc_total_bytes_ratio\":{d:.3},\"algebraic_lsm_cache_bytes\":{d},\"baseline_lsm_cache_bytes\":{d},\"algebraic_full_text_pending_bytes\":{d},\"baseline_full_text_pending_bytes\":{d},\"algebraic_replay_window_bytes\":{d},\"baseline_replay_window_bytes\":{d},\"algebraic_load_total_ms\":{d:.3},\"baseline_load_total_ms\":{d:.3},\"load_total_ratio\":{d:.3}}}\n",
        .{
            algebraic.hbc_total_bytes,
            baseline.hbc_total_bytes,
            ratio(@as(f64, @floatFromInt(algebraic.hbc_total_bytes)), @as(f64, @floatFromInt(baseline.hbc_total_bytes))),
            algebraic.lsm_cache_bytes,
            baseline.lsm_cache_bytes,
            algebraic.full_text_pending_bytes,
            baseline.full_text_pending_bytes,
            algebraic.replay_window_bytes,
            baseline.replay_window_bytes,
            algebraic.load_total_ms,
            baseline.load_total_ms,
            ratio(algebraic.load_total_ms, baseline.load_total_ms),
        },
    );
}

fn findPublicQueryBaseline(items: []const PublicQueryRecord, algebraic: PublicQueryRecord, with_schema: bool, with_algebraic: bool) ?PublicQueryRecord {
    for (items) |item| {
        if (item.with_schema != with_schema) continue;
        if (item.with_algebraic != with_algebraic) continue;
        if (!std.mem.eql(u8, item.mode, algebraic.mode)) continue;
        if (!std.mem.eql(u8, item.server, algebraic.server)) continue;
        if (!std.mem.eql(u8, item.query_shape, algebraic.query_shape)) continue;
        if (item.docs != algebraic.docs) continue;
        if (item.dims != algebraic.dims) continue;
        if (item.queries != algebraic.queries) continue;
        if (item.repeats != algebraic.repeats) continue;
        if (item.k != algebraic.k) continue;
        if (item.threads != algebraic.threads) continue;
        return item;
    }
    return null;
}

fn summarizePerformanceEvidence(
    datasets: []const DatasetRecord,
    queries: []const QueryRecord,
    correctness: []const CorrectnessRecord,
    churn: []const ChurnRecord,
    churn_row_family_records: []const ChurnRowFamilyRecord,
    public_queries: []const PublicQueryRecord,
) PerformanceEvidenceMetrics {
    var metrics = PerformanceEvidenceMetrics{};
    for (datasets) |dataset| {
        metrics.dataset_cases += 1;
        if (std.mem.eql(u8, dataset.algebraic_backend, "lsm")) metrics.lsm_dataset_cases += 1;
        metrics.total_algebraic_bytes += dataset.algebraic_bytes;
        metrics.total_algebraic_backend_path_bytes += dataset.algebraic_backend_path_bytes;
        metrics.total_full_text_bytes += dataset.full_text_bytes;
        metrics.total_symbol_bytes += dataset.symbol_bytes;
        metrics.total_support_bytes += dataset.support_bytes;
        metrics.total_materializations += dataset.materialization_count;
        if (dataset.fanout > 1) metrics.fanout_dataset_cases += 1;
        metrics.max_algebraic_bytes_per_doc = @max(metrics.max_algebraic_bytes_per_doc, ratio(@as(f64, @floatFromInt(dataset.algebraic_bytes)), @as(f64, @floatFromInt(dataset.docs))));
        metrics.max_algebraic_bytes_per_materialization = @max(metrics.max_algebraic_bytes_per_materialization, ratio(@as(f64, @floatFromInt(dataset.algebraic_bytes)), @as(f64, @floatFromInt(dataset.materialization_count))));
        metrics.max_symbol_bytes_per_doc = @max(metrics.max_symbol_bytes_per_doc, ratio(@as(f64, @floatFromInt(dataset.symbol_bytes)), @as(f64, @floatFromInt(dataset.docs))));
        metrics.max_support_bytes_per_doc = @max(metrics.max_support_bytes_per_doc, ratio(@as(f64, @floatFromInt(dataset.support_bytes)), @as(f64, @floatFromInt(dataset.docs))));
        metrics.max_accumulator_flush_count = @max(metrics.max_accumulator_flush_count, dataset.accumulator_flush_count);
        metrics.total_path_dictionary_fst_rebuild_count += dataset.path_dictionary_fst_rebuild_count;
        metrics.max_path_dictionary_fst_rebuild_count = @max(metrics.max_path_dictionary_fst_rebuild_count, dataset.path_dictionary_fst_rebuild_count);
        metrics.total_lsm_flushes += dataset.lsm_flushes;
        metrics.total_lsm_flush_output_runs += dataset.lsm_flush_output_runs;
        metrics.total_lsm_sorted_ingest_runs += dataset.lsm_sorted_ingest_runs;
        metrics.total_lsm_sorted_ingest_bytes += dataset.lsm_sorted_ingest_bytes;
        metrics.total_lsm_write_pressure_compactions += dataset.lsm_write_pressure_compactions;
        metrics.max_lsm_flushes = @max(metrics.max_lsm_flushes, dataset.lsm_flushes);
        metrics.max_lsm_write_pressure_compactions = @max(metrics.max_lsm_write_pressure_compactions, dataset.lsm_write_pressure_compactions);
        metrics.max_lsm_sorted_ingest_runs = @max(metrics.max_lsm_sorted_ingest_runs, dataset.lsm_sorted_ingest_runs);
    }
    for (queries) |query| {
        if (isAlgebraicEngine(query.engine)) {
            metrics.algebraic_query_records += 1;
            metrics.max_algebraic_query_ms = @max(metrics.max_algebraic_query_ms, query.avg_ms);
            const expected_equal = queryExpectedEqual(correctness, query.case_name, query.query);
            if (findBaseline(queries, query.case_name, query.algebraic_backend, query.algebraic_profile, query.query, "doc_scan")) |baseline| {
                updateQueryClassificationMetrics(&metrics, query, baseline, expected_equal);
            }
            if (findBaseline(queries, query.case_name, query.algebraic_backend, query.algebraic_profile, query.query, "full_text_index")) |baseline| {
                updateQueryClassificationMetrics(&metrics, query, baseline, expected_equal);
            }
        }
        if (std.mem.startsWith(u8, query.engine, "doc_scan")) metrics.doc_scan_query_records += 1;
        if (std.mem.startsWith(u8, query.engine, "full_text_index")) metrics.full_text_query_records += 1;
        if (std.mem.eql(u8, query.algebraic_backend, "lsm")) metrics.lsm_query_records += 1;
        if (std.mem.eql(u8, queryTemperature(query.engine), "cold")) metrics.cold_query_records += 1;
        if (std.mem.eql(u8, queryTemperature(query.engine), "warm")) metrics.warm_query_records += 1;
        if (isConstrainedQueryRecord(query)) metrics.constrained_query_records += 1;
        if (isWideQueryRecord(query)) metrics.wide_query_records += 1;
        if (isStatsQueryRecord(query)) metrics.stats_query_records += 1;
        if (isCardinalityQueryRecord(query)) metrics.cardinality_query_records += 1;
        if (isRangeQueryRecord(query)) metrics.range_query_records += 1;
        if (isHistogramQueryRecord(query)) metrics.histogram_query_records += 1;
    }
    for (correctness) |item| {
        if (item.expected_equal and !item.all_match) metrics.correctness_failures += 1;
    }
    for (churn) |item| {
        metrics.churn_records += 1;
        metrics.churn_ops += item.ops;
        metrics.total_churn_sidecar_bytes += item.algebraic_sidecar_bytes;
        metrics.max_churn_sidecar_bytes = @max(metrics.max_churn_sidecar_bytes, item.algebraic_sidecar_bytes);
        metrics.total_algebraic_update_ms += item.algebraic_update_ms;
        metrics.total_full_text_update_ms += item.full_text_update_ms;
        metrics.max_churn_algebraic_update_ms = @max(metrics.max_churn_algebraic_update_ms, item.algebraic_update_ms);
        metrics.total_adaptive_maintenance_plan_build_count += item.adaptive_maintenance_plan_build_count;
        metrics.total_adaptive_maintenance_cached_spec_count += item.adaptive_maintenance_cached_spec_count;
        metrics.total_adaptive_maintenance_disabled_count += item.adaptive_maintenance_disabled_count;
        metrics.max_adaptive_maintenance_plan_build_count = @max(metrics.max_adaptive_maintenance_plan_build_count, item.adaptive_maintenance_plan_build_count);
        metrics.max_adaptive_maintenance_cached_spec_count = @max(metrics.max_adaptive_maintenance_cached_spec_count, item.adaptive_maintenance_cached_spec_count);
    }
    for (churn_row_family_records) |item| {
        metrics.churn_row_family_records += 1;
        metrics.total_churn_row_family_bytes_after += item.bytes_after;
        metrics.max_churn_row_family_bytes_after = @max(metrics.max_churn_row_family_bytes_after, item.bytes_after);
    }
    for (public_queries) |item| {
        metrics.public_query_records += 1;
        if (item.with_algebraic) {
            metrics.public_query_algebraic_records += 1;
            metrics.max_public_query_http_us = @max(metrics.max_public_query_http_us, item.http_avg_us);
            metrics.max_public_query_load_rss_peak_bytes = @max(metrics.max_public_query_load_rss_peak_bytes, item.load_rss_peak_bytes);
            metrics.max_public_query_search_rss_peak_bytes = @max(metrics.max_public_query_search_rss_peak_bytes, item.search_rss_peak_bytes);
            if (findPublicQueryBaseline(public_queries, item, false, false)) |baseline| {
                metrics.public_query_comparison_pairs += 1;
                metrics.min_public_query_http_speedup = @min(metrics.min_public_query_http_speedup, ratio(baseline.http_avg_us, item.http_avg_us));
            }
            if (findPublicQueryBaseline(public_queries, item, true, false)) |baseline| {
                metrics.public_query_comparison_pairs += 1;
                metrics.min_public_query_http_speedup = @min(metrics.min_public_query_http_speedup, ratio(baseline.http_avg_us, item.http_avg_us));
            }
        } else if (item.with_schema) {
            metrics.public_query_schema_only_records += 1;
        } else {
            metrics.public_query_no_schema_records += 1;
        }
    }
    if (metrics.min_public_query_http_speedup == std.math.inf(f64)) metrics.min_public_query_http_speedup = 0;
    return metrics;
}

fn updateQueryClassificationMetrics(
    metrics: *PerformanceEvidenceMetrics,
    algebraic: QueryRecord,
    baseline: QueryRecord,
    expected_equal: ?bool,
) void {
    if (expected_equal != null) return;
    metrics.unclassified_algebraic_comparisons += 1;
    if (algebraic.checksum != baseline.checksum) metrics.unclassified_algebraic_checksum_mismatches += 1;
}

fn printPerformanceEvidenceSummary(metrics: PerformanceEvidenceMetrics) void {
    std.debug.print(
        "{{\"event\":\"performance_evidence_summary\",\"dataset_cases\":{d},\"lsm_dataset_cases\":{d},\"fanout_dataset_cases\":{d},\"total_materializations\":{d},\"total_algebraic_bytes\":{d},\"total_algebraic_backend_path_bytes\":{d},\"total_full_text_bytes\":{d},\"total_symbol_bytes\":{d},\"total_support_bytes\":{d},\"max_algebraic_bytes_per_doc\":{d:.3},\"max_algebraic_bytes_per_materialization\":{d:.3},\"max_symbol_bytes_per_doc\":{d:.3},\"max_support_bytes_per_doc\":{d:.3},\"max_accumulator_flush_count\":{d},\"total_path_dictionary_fst_rebuild_count\":{d},\"max_path_dictionary_fst_rebuild_count\":{d},\"total_adaptive_maintenance_plan_build_count\":{d},\"total_adaptive_maintenance_cached_spec_count\":{d},\"total_adaptive_maintenance_disabled_count\":{d},\"max_adaptive_maintenance_plan_build_count\":{d},\"max_adaptive_maintenance_cached_spec_count\":{d}",
        .{
            metrics.dataset_cases,
            metrics.lsm_dataset_cases,
            metrics.fanout_dataset_cases,
            metrics.total_materializations,
            metrics.total_algebraic_bytes,
            metrics.total_algebraic_backend_path_bytes,
            metrics.total_full_text_bytes,
            metrics.total_symbol_bytes,
            metrics.total_support_bytes,
            metrics.max_algebraic_bytes_per_doc,
            metrics.max_algebraic_bytes_per_materialization,
            metrics.max_symbol_bytes_per_doc,
            metrics.max_support_bytes_per_doc,
            metrics.max_accumulator_flush_count,
            metrics.total_path_dictionary_fst_rebuild_count,
            metrics.max_path_dictionary_fst_rebuild_count,
            metrics.total_adaptive_maintenance_plan_build_count,
            metrics.total_adaptive_maintenance_cached_spec_count,
            metrics.total_adaptive_maintenance_disabled_count,
            metrics.max_adaptive_maintenance_plan_build_count,
            metrics.max_adaptive_maintenance_cached_spec_count,
        },
    );
    std.debug.print(
        ",\"total_lsm_flushes\":{d},\"total_lsm_flush_output_runs\":{d},\"total_lsm_sorted_ingest_runs\":{d},\"total_lsm_sorted_ingest_bytes\":{d},\"total_lsm_write_pressure_compactions\":{d},\"max_lsm_flushes\":{d},\"max_lsm_write_pressure_compactions\":{d},\"max_lsm_sorted_ingest_runs\":{d},\"algebraic_query_records\":{d},\"doc_scan_query_records\":{d},\"full_text_query_records\":{d},\"lsm_query_records\":{d},\"cold_query_records\":{d},\"warm_query_records\":{d},\"constrained_query_records\":{d},\"wide_query_records\":{d},\"stats_query_records\":{d},\"cardinality_query_records\":{d},\"range_query_records\":{d},\"histogram_query_records\":{d},\"max_algebraic_query_ms\":{d:.3},\"correctness_failures\":{d},\"unclassified_algebraic_comparisons\":{d},\"unclassified_algebraic_checksum_mismatches\":{d}",
        .{
            metrics.total_lsm_flushes,
            metrics.total_lsm_flush_output_runs,
            metrics.total_lsm_sorted_ingest_runs,
            metrics.total_lsm_sorted_ingest_bytes,
            metrics.total_lsm_write_pressure_compactions,
            metrics.max_lsm_flushes,
            metrics.max_lsm_write_pressure_compactions,
            metrics.max_lsm_sorted_ingest_runs,
            metrics.algebraic_query_records,
            metrics.doc_scan_query_records,
            metrics.full_text_query_records,
            metrics.lsm_query_records,
            metrics.cold_query_records,
            metrics.warm_query_records,
            metrics.constrained_query_records,
            metrics.wide_query_records,
            metrics.stats_query_records,
            metrics.cardinality_query_records,
            metrics.range_query_records,
            metrics.histogram_query_records,
            metrics.max_algebraic_query_ms,
            metrics.correctness_failures,
            metrics.unclassified_algebraic_comparisons,
            metrics.unclassified_algebraic_checksum_mismatches,
        },
    );
    std.debug.print(
        ",\"churn_records\":{d},\"churn_ops\":{d},\"total_churn_sidecar_bytes\":{d},\"max_churn_sidecar_bytes\":{d},\"total_algebraic_update_ms\":{d:.3},\"total_full_text_update_ms\":{d:.3},\"max_churn_algebraic_update_ms\":{d:.3},\"churn_row_family_records\":{d},\"total_churn_row_family_bytes_after\":{d},\"max_churn_row_family_bytes_after\":{d},\"public_query_records\":{d},\"public_query_no_schema_records\":{d},\"public_query_schema_only_records\":{d},\"public_query_algebraic_records\":{d},\"public_query_comparison_pairs\":{d},\"max_public_query_http_us\":{d:.3},\"max_public_query_load_rss_peak_bytes\":{d},\"max_public_query_search_rss_peak_bytes\":{d},\"min_public_query_http_speedup\":{d:.3}}}\n",
        .{
            metrics.churn_records,
            metrics.churn_ops,
            metrics.total_churn_sidecar_bytes,
            metrics.max_churn_sidecar_bytes,
            metrics.total_algebraic_update_ms,
            metrics.total_full_text_update_ms,
            metrics.max_churn_algebraic_update_ms,
            metrics.churn_row_family_records,
            metrics.total_churn_row_family_bytes_after,
            metrics.max_churn_row_family_bytes_after,
            metrics.public_query_records,
            metrics.public_query_no_schema_records,
            metrics.public_query_schema_only_records,
            metrics.public_query_algebraic_records,
            metrics.public_query_comparison_pairs,
            metrics.max_public_query_http_us,
            metrics.max_public_query_load_rss_peak_bytes,
            metrics.max_public_query_search_rss_peak_bytes,
            metrics.min_public_query_http_speedup,
        },
    );
}

fn enforcePerformanceGuardrail(cfg: GuardrailConfig, metrics: PerformanceEvidenceMetrics) !void {
    var failed = false;
    failed = checkMinU64("dataset_cases", metrics.dataset_cases, cfg.min_dataset_cases) or failed;
    failed = checkMinU64("lsm_dataset_cases", metrics.lsm_dataset_cases, cfg.min_lsm_dataset_cases) or failed;
    failed = checkMinU64("algebraic_query_records", metrics.algebraic_query_records, cfg.min_algebraic_query_records) or failed;
    failed = checkMinU64("doc_scan_query_records", metrics.doc_scan_query_records, cfg.min_doc_scan_query_records) or failed;
    failed = checkMinU64("full_text_query_records", metrics.full_text_query_records, cfg.min_full_text_query_records) or failed;
    failed = checkMinU64("lsm_query_records", metrics.lsm_query_records, cfg.min_lsm_query_records) or failed;
    failed = checkMinU64("cold_query_records", metrics.cold_query_records, cfg.min_cold_query_records) or failed;
    failed = checkMinU64("warm_query_records", metrics.warm_query_records, cfg.min_warm_query_records) or failed;
    failed = checkMinU64("constrained_query_records", metrics.constrained_query_records, cfg.min_constrained_query_records) or failed;
    failed = checkMinU64("wide_query_records", metrics.wide_query_records, cfg.min_wide_query_records) or failed;
    failed = checkMinU64("stats_query_records", metrics.stats_query_records, cfg.min_stats_query_records) or failed;
    failed = checkMinU64("cardinality_query_records", metrics.cardinality_query_records, cfg.min_cardinality_query_records) or failed;
    failed = checkMinU64("range_query_records", metrics.range_query_records, cfg.min_range_query_records) or failed;
    failed = checkMinU64("histogram_query_records", metrics.histogram_query_records, cfg.min_histogram_query_records) or failed;
    failed = checkMinU64("fanout_dataset_cases", metrics.fanout_dataset_cases, cfg.min_fanout_dataset_cases) or failed;
    failed = checkMinU64("churn_records", metrics.churn_records, cfg.min_churn_records) or failed;
    failed = checkMinU64("public_query_comparison_pairs", metrics.public_query_comparison_pairs, cfg.min_public_query_comparison_pairs) or failed;
    failed = checkMinU64("total_lsm_sorted_ingest_runs", metrics.total_lsm_sorted_ingest_runs, cfg.min_lsm_sorted_ingest_runs) or failed;
    failed = checkMaxU64("max_lsm_flushes", metrics.max_lsm_flushes, cfg.max_lsm_flushes) or failed;
    failed = checkMaxU64("max_lsm_write_pressure_compactions", metrics.max_lsm_write_pressure_compactions, cfg.max_lsm_write_pressure_compactions) or failed;
    failed = checkMaxU64("correctness_failures", metrics.correctness_failures, cfg.max_correctness_failures) or failed;
    failed = checkMaxU64("unclassified_algebraic_comparisons", metrics.unclassified_algebraic_comparisons, cfg.max_unclassified_algebraic_comparisons) or failed;
    failed = checkMaxF64("max_algebraic_bytes_per_doc", metrics.max_algebraic_bytes_per_doc, cfg.max_algebraic_bytes_per_doc) or failed;
    failed = checkMaxF64("max_algebraic_bytes_per_materialization", metrics.max_algebraic_bytes_per_materialization, cfg.max_algebraic_bytes_per_materialization) or failed;
    failed = checkMaxF64("max_symbol_bytes_per_doc", metrics.max_symbol_bytes_per_doc, cfg.max_symbol_bytes_per_doc) or failed;
    failed = checkMaxF64("max_support_bytes_per_doc", metrics.max_support_bytes_per_doc, cfg.max_support_bytes_per_doc) or failed;
    failed = checkMaxU64("max_accumulator_flush_count", metrics.max_accumulator_flush_count, cfg.max_accumulator_flush_count) or failed;
    failed = checkMaxU64("max_path_dictionary_fst_rebuild_count", metrics.max_path_dictionary_fst_rebuild_count, cfg.max_path_dictionary_fst_rebuild_count) or failed;
    failed = checkMaxF64("max_public_query_http_us", metrics.max_public_query_http_us, cfg.max_public_query_http_us) or failed;
    failed = checkMaxU64("max_public_query_load_rss_peak_bytes", metrics.max_public_query_load_rss_peak_bytes, cfg.max_public_query_load_rss_peak_bytes) or failed;
    failed = checkMaxU64("max_public_query_search_rss_peak_bytes", metrics.max_public_query_search_rss_peak_bytes, cfg.max_public_query_search_rss_peak_bytes) or failed;
    failed = checkMinF64("min_public_query_http_speedup", metrics.min_public_query_http_speedup, cfg.min_public_query_http_speedup) or failed;
    failed = checkMaxF64("max_churn_algebraic_update_ms", metrics.max_churn_algebraic_update_ms, cfg.max_churn_algebraic_update_ms) or failed;
    failed = checkMaxU64("max_churn_sidecar_bytes", metrics.max_churn_sidecar_bytes, cfg.max_churn_sidecar_bytes) or failed;
    failed = checkMaxF64("max_algebraic_query_ms", metrics.max_algebraic_query_ms, cfg.max_algebraic_query_ms) or failed;
    if (failed) return error.PerformanceGuardrailFailed;
}

fn printPerformanceBaselineComparison(current: PerformanceEvidenceMetrics, baseline: PerformanceEvidenceMetrics) void {
    std.debug.print(
        "{{\"event\":\"performance_baseline_comparison\",\"dataset_cases\":{d},\"baseline_dataset_cases\":{d},\"algebraic_query_ms_ratio\":{d:.6},\"public_query_http_us_ratio\":{d:.6},\"algebraic_bytes_per_doc_ratio\":{d:.6},\"churn_algebraic_update_ms_ratio\":{d:.6},\"public_query_comparison_pairs\":{d},\"baseline_public_query_comparison_pairs\":{d},\"correctness_failures\":{d},\"baseline_correctness_failures\":{d}}}\n",
        .{
            current.dataset_cases,
            baseline.dataset_cases,
            regressionRatio(current.max_algebraic_query_ms, baseline.max_algebraic_query_ms),
            regressionRatio(current.max_public_query_http_us, baseline.max_public_query_http_us),
            regressionRatio(current.max_algebraic_bytes_per_doc, baseline.max_algebraic_bytes_per_doc),
            regressionRatio(current.max_churn_algebraic_update_ms, baseline.max_churn_algebraic_update_ms),
            current.public_query_comparison_pairs,
            baseline.public_query_comparison_pairs,
            current.correctness_failures,
            baseline.correctness_failures,
        },
    );
}

fn enforcePerformanceBaselineGuardrail(cfg: GuardrailConfig, current: PerformanceEvidenceMetrics, baseline: PerformanceEvidenceMetrics) !void {
    var failed = false;
    failed = checkMaxF64(
        "max_algebraic_query_ms_ratio_vs_baseline",
        regressionRatio(current.max_algebraic_query_ms, baseline.max_algebraic_query_ms),
        cfg.max_algebraic_query_ms_ratio_vs_baseline,
    ) or failed;
    failed = checkMaxF64(
        "max_public_query_http_us_ratio_vs_baseline",
        regressionRatio(current.max_public_query_http_us, baseline.max_public_query_http_us),
        cfg.max_public_query_http_us_ratio_vs_baseline,
    ) or failed;
    failed = checkMaxF64(
        "max_algebraic_bytes_per_doc_ratio_vs_baseline",
        regressionRatio(current.max_algebraic_bytes_per_doc, baseline.max_algebraic_bytes_per_doc),
        cfg.max_algebraic_bytes_per_doc_ratio_vs_baseline,
    ) or failed;
    failed = checkMaxF64(
        "max_churn_algebraic_update_ms_ratio_vs_baseline",
        regressionRatio(current.max_churn_algebraic_update_ms, baseline.max_churn_algebraic_update_ms),
        cfg.max_churn_algebraic_update_ms_ratio_vs_baseline,
    ) or failed;
    if (failed) return error.PerformanceGuardrailFailed;
}

fn parseBaselinePerformanceEvidence(alloc: std.mem.Allocator, raw: []const u8) !PerformanceEvidenceMetrics {
    var found: ?PerformanceEvidenceMetrics = null;
    var lines = std.mem.splitScalar(u8, raw, '\n');
    while (lines.next()) |line| {
        if (std.mem.trim(u8, line, " \t\r").len == 0) continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, line, .{}) catch continue;
        defer parsed.deinit();
        const obj = switch (parsed.value) {
            .object => |object| object,
            else => continue,
        };
        const event = jsonString(obj, "event") orelse continue;
        if (!std.mem.eql(u8, event, "performance_evidence_summary")) continue;
        found = .{
            .dataset_cases = jsonU64(obj, "dataset_cases") orelse 0,
            .lsm_dataset_cases = jsonU64(obj, "lsm_dataset_cases") orelse 0,
            .fanout_dataset_cases = jsonU64(obj, "fanout_dataset_cases") orelse 0,
            .total_materializations = jsonU64(obj, "total_materializations") orelse 0,
            .total_algebraic_bytes = jsonU64(obj, "total_algebraic_bytes") orelse 0,
            .total_algebraic_backend_path_bytes = jsonU64(obj, "total_algebraic_backend_path_bytes") orelse 0,
            .total_full_text_bytes = jsonU64(obj, "total_full_text_bytes") orelse 0,
            .total_symbol_bytes = jsonU64(obj, "total_symbol_bytes") orelse 0,
            .total_support_bytes = jsonU64(obj, "total_support_bytes") orelse 0,
            .max_algebraic_bytes_per_doc = jsonF64(obj, "max_algebraic_bytes_per_doc") orelse 0,
            .max_algebraic_bytes_per_materialization = jsonF64(obj, "max_algebraic_bytes_per_materialization") orelse 0,
            .max_symbol_bytes_per_doc = jsonF64(obj, "max_symbol_bytes_per_doc") orelse 0,
            .max_support_bytes_per_doc = jsonF64(obj, "max_support_bytes_per_doc") orelse 0,
            .max_accumulator_flush_count = jsonU64(obj, "max_accumulator_flush_count") orelse 0,
            .total_path_dictionary_fst_rebuild_count = jsonU64(obj, "total_path_dictionary_fst_rebuild_count") orelse 0,
            .max_path_dictionary_fst_rebuild_count = jsonU64(obj, "max_path_dictionary_fst_rebuild_count") orelse 0,
            .total_adaptive_maintenance_plan_build_count = jsonU64(obj, "total_adaptive_maintenance_plan_build_count") orelse 0,
            .total_adaptive_maintenance_cached_spec_count = jsonU64(obj, "total_adaptive_maintenance_cached_spec_count") orelse 0,
            .total_adaptive_maintenance_disabled_count = jsonU64(obj, "total_adaptive_maintenance_disabled_count") orelse 0,
            .max_adaptive_maintenance_plan_build_count = jsonU64(obj, "max_adaptive_maintenance_plan_build_count") orelse 0,
            .max_adaptive_maintenance_cached_spec_count = jsonU64(obj, "max_adaptive_maintenance_cached_spec_count") orelse 0,
            .total_lsm_flushes = jsonU64(obj, "total_lsm_flushes") orelse 0,
            .total_lsm_flush_output_runs = jsonU64(obj, "total_lsm_flush_output_runs") orelse 0,
            .total_lsm_sorted_ingest_runs = jsonU64(obj, "total_lsm_sorted_ingest_runs") orelse 0,
            .total_lsm_sorted_ingest_bytes = jsonU64(obj, "total_lsm_sorted_ingest_bytes") orelse 0,
            .total_lsm_write_pressure_compactions = jsonU64(obj, "total_lsm_write_pressure_compactions") orelse 0,
            .max_lsm_flushes = jsonU64(obj, "max_lsm_flushes") orelse 0,
            .max_lsm_write_pressure_compactions = jsonU64(obj, "max_lsm_write_pressure_compactions") orelse 0,
            .max_lsm_sorted_ingest_runs = jsonU64(obj, "max_lsm_sorted_ingest_runs") orelse 0,
            .algebraic_query_records = jsonU64(obj, "algebraic_query_records") orelse 0,
            .doc_scan_query_records = jsonU64(obj, "doc_scan_query_records") orelse 0,
            .full_text_query_records = jsonU64(obj, "full_text_query_records") orelse 0,
            .lsm_query_records = jsonU64(obj, "lsm_query_records") orelse 0,
            .cold_query_records = jsonU64(obj, "cold_query_records") orelse 0,
            .warm_query_records = jsonU64(obj, "warm_query_records") orelse 0,
            .constrained_query_records = jsonU64(obj, "constrained_query_records") orelse 0,
            .wide_query_records = jsonU64(obj, "wide_query_records") orelse 0,
            .stats_query_records = jsonU64(obj, "stats_query_records") orelse 0,
            .cardinality_query_records = jsonU64(obj, "cardinality_query_records") orelse 0,
            .range_query_records = jsonU64(obj, "range_query_records") orelse 0,
            .histogram_query_records = jsonU64(obj, "histogram_query_records") orelse 0,
            .max_algebraic_query_ms = jsonF64(obj, "max_algebraic_query_ms") orelse 0,
            .correctness_failures = jsonU64(obj, "correctness_failures") orelse 0,
            .unclassified_algebraic_comparisons = jsonU64(obj, "unclassified_algebraic_comparisons") orelse 0,
            .unclassified_algebraic_checksum_mismatches = jsonU64(obj, "unclassified_algebraic_checksum_mismatches") orelse 0,
            .churn_records = jsonU64(obj, "churn_records") orelse 0,
            .churn_ops = jsonU64(obj, "churn_ops") orelse 0,
            .total_churn_sidecar_bytes = jsonU64(obj, "total_churn_sidecar_bytes") orelse 0,
            .max_churn_sidecar_bytes = jsonU64(obj, "max_churn_sidecar_bytes") orelse 0,
            .total_algebraic_update_ms = jsonF64(obj, "total_algebraic_update_ms") orelse 0,
            .total_full_text_update_ms = jsonF64(obj, "total_full_text_update_ms") orelse 0,
            .max_churn_algebraic_update_ms = jsonF64(obj, "max_churn_algebraic_update_ms") orelse 0,
            .churn_row_family_records = jsonU64(obj, "churn_row_family_records") orelse 0,
            .total_churn_row_family_bytes_after = jsonU64(obj, "total_churn_row_family_bytes_after") orelse 0,
            .max_churn_row_family_bytes_after = jsonU64(obj, "max_churn_row_family_bytes_after") orelse 0,
            .public_query_records = jsonU64(obj, "public_query_records") orelse 0,
            .public_query_algebraic_records = jsonU64(obj, "public_query_algebraic_records") orelse 0,
            .public_query_schema_only_records = jsonU64(obj, "public_query_schema_only_records") orelse 0,
            .public_query_no_schema_records = jsonU64(obj, "public_query_no_schema_records") orelse 0,
            .public_query_comparison_pairs = jsonU64(obj, "public_query_comparison_pairs") orelse 0,
            .max_public_query_http_us = jsonF64(obj, "max_public_query_http_us") orelse 0,
            .max_public_query_load_rss_peak_bytes = jsonU64(obj, "max_public_query_load_rss_peak_bytes") orelse 0,
            .max_public_query_search_rss_peak_bytes = jsonU64(obj, "max_public_query_search_rss_peak_bytes") orelse 0,
            .min_public_query_http_speedup = jsonF64(obj, "min_public_query_http_speedup") orelse 0,
        };
    }
    return found orelse {
        std.debug.print("baseline file does not contain performance_evidence_summary\n", .{});
        return error.InvalidArgument;
    };
}

fn checkMinU64(name: []const u8, actual: u64, expected: u64) bool {
    if (actual >= expected) return false;
    std.debug.print("algebraic_summary_guardrail_failed metric={s} actual={d} min={d}\n", .{ name, actual, expected });
    return true;
}

fn checkMaxU64(name: []const u8, actual: u64, expected: u64) bool {
    if (actual <= expected) return false;
    std.debug.print("algebraic_summary_guardrail_failed metric={s} actual={d} max={d}\n", .{ name, actual, expected });
    return true;
}

fn checkMinF64(name: []const u8, actual: f64, expected: f64) bool {
    if (actual >= expected) return false;
    std.debug.print("algebraic_summary_guardrail_failed metric={s} actual={d:.6} min={d:.6}\n", .{ name, actual, expected });
    return true;
}

fn checkMaxF64(name: []const u8, actual: f64, expected: f64) bool {
    if (actual <= expected) return false;
    std.debug.print("algebraic_summary_guardrail_failed metric={s} actual={d:.6} max={d:.6}\n", .{ name, actual, expected });
    return true;
}

fn tuningBaselineProfile(profile: []const u8) []const u8 {
    if (std.mem.startsWith(u8, profile, "direct_")) return "direct_normal";
    return "normal";
}

fn parseArgs(args_in: std.process.Args) !CliConfig {
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    var cfg = CliConfig{ .input_path = "" };
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--input")) {
            cfg.input_path = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--baseline")) {
            cfg.baseline_path = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--require-performance-evidence")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_dataset_cases = @max(cfg.guardrail.min_dataset_cases, 1);
            cfg.guardrail.min_algebraic_query_records = @max(cfg.guardrail.min_algebraic_query_records, 1);
            cfg.guardrail.min_doc_scan_query_records = @max(cfg.guardrail.min_doc_scan_query_records, 1);
            cfg.guardrail.min_full_text_query_records = @max(cfg.guardrail.min_full_text_query_records, 1);
            cfg.guardrail.min_churn_records = @max(cfg.guardrail.min_churn_records, 1);
            cfg.guardrail.max_correctness_failures = @min(cfg.guardrail.max_correctness_failures, 0);
            cfg.guardrail.max_unclassified_algebraic_comparisons = @min(cfg.guardrail.max_unclassified_algebraic_comparisons, 0);
        } else if (std.mem.eql(u8, arg, "--min-dataset-cases")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_dataset_cases = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-lsm-dataset-cases")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_lsm_dataset_cases = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-algebraic-query-records")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_algebraic_query_records = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-doc-scan-query-records")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_doc_scan_query_records = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-full-text-query-records")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_full_text_query_records = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-lsm-query-records")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_lsm_query_records = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-cold-query-records")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_cold_query_records = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-warm-query-records")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_warm_query_records = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-constrained-query-records")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_constrained_query_records = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-wide-query-records")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_wide_query_records = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-stats-query-records")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_stats_query_records = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-cardinality-query-records")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_cardinality_query_records = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-range-query-records")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_range_query_records = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-histogram-query-records")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_histogram_query_records = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-fanout-dataset-cases")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_fanout_dataset_cases = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-churn-records")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_churn_records = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-public-query-comparison-pairs")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_public_query_comparison_pairs = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-lsm-sorted-ingest-runs")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_lsm_sorted_ingest_runs = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-lsm-flushes")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_lsm_flushes = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-lsm-write-pressure-compactions")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_lsm_write_pressure_compactions = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-correctness-failures")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_correctness_failures = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-unclassified-algebraic-comparisons")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_unclassified_algebraic_comparisons = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-algebraic-bytes-per-doc")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_algebraic_bytes_per_doc = try parseNextF64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-algebraic-bytes-per-materialization")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_algebraic_bytes_per_materialization = try parseNextF64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-symbol-bytes-per-doc")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_symbol_bytes_per_doc = try parseNextF64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-support-bytes-per-doc")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_support_bytes_per_doc = try parseNextF64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-accumulator-flush-count")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_accumulator_flush_count = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-path-dictionary-fst-rebuild-count")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_path_dictionary_fst_rebuild_count = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-public-query-http-us")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_public_query_http_us = try parseNextF64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-public-query-load-rss-peak-bytes")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_public_query_load_rss_peak_bytes = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-public-query-search-rss-peak-bytes")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_public_query_search_rss_peak_bytes = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--min-public-query-http-speedup")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.min_public_query_http_speedup = try parseNextF64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-churn-algebraic-update-ms")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_churn_algebraic_update_ms = try parseNextF64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-churn-sidecar-bytes")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_churn_sidecar_bytes = try parseNextU64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-algebraic-query-ms")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_algebraic_query_ms = try parseNextF64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-algebraic-query-ms-ratio-vs-baseline")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_algebraic_query_ms_ratio_vs_baseline = try parseNextF64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-public-query-http-us-ratio-vs-baseline")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_public_query_http_us_ratio_vs_baseline = try parseNextF64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-algebraic-bytes-per-doc-ratio-vs-baseline")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_algebraic_bytes_per_doc_ratio_vs_baseline = try parseNextF64(&args, arg);
        } else if (std.mem.eql(u8, arg, "--max-churn-algebraic-update-ms-ratio-vs-baseline")) {
            cfg.guardrail.enabled = true;
            cfg.guardrail.max_churn_algebraic_update_ms_ratio_vs_baseline = try parseNextF64(&args, arg);
        } else {
            std.debug.print("unknown argument: {s}\n", .{arg});
            printUsage();
            return error.InvalidArgument;
        }
    }
    if (cfg.input_path.len == 0) {
        printUsage();
        return error.InvalidArgument;
    }
    return cfg;
}

fn parseNextU64(args: *std.process.Args.Iterator, flag: []const u8) !u64 {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return std.fmt.parseInt(u64, raw, 10) catch {
        std.debug.print("invalid integer for {s}: {s}\n", .{ flag, raw });
        return error.InvalidArgument;
    };
}

fn parseNextF64(args: *std.process.Args.Iterator, flag: []const u8) !f64 {
    const raw = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return std.fmt.parseFloat(f64, raw) catch {
        std.debug.print("invalid float for {s}: {s}\n", .{ flag, raw });
        return error.InvalidArgument;
    };
}

fn printUsage() void {
    std.debug.print(
        \\usage: algebraic-summary --input <jsonl> [--baseline <summary-jsonl>] [guardrail thresholds]
        \\
        \\guardrail thresholds:
        \\  --require-performance-evidence
        \\  --min-dataset-cases <n>
        \\  --min-lsm-dataset-cases <n>
        \\  --min-algebraic-query-records <n>
        \\  --min-doc-scan-query-records <n>
        \\  --min-full-text-query-records <n>
        \\  --min-lsm-query-records <n>
        \\  --min-cold-query-records <n>
        \\  --min-warm-query-records <n>
        \\  --min-constrained-query-records <n>
        \\  --min-wide-query-records <n>
        \\  --min-stats-query-records <n>
        \\  --min-cardinality-query-records <n>
        \\  --min-range-query-records <n>
        \\  --min-histogram-query-records <n>
        \\  --min-fanout-dataset-cases <n>
        \\  --min-churn-records <n>
        \\  --min-public-query-comparison-pairs <n>
        \\  --min-lsm-sorted-ingest-runs <n>
        \\  --max-lsm-flushes <n>
        \\  --max-lsm-write-pressure-compactions <n>
        \\  --max-correctness-failures <n>
        \\  --max-unclassified-algebraic-comparisons <n>
        \\  --max-algebraic-bytes-per-doc <n>
        \\  --max-algebraic-bytes-per-materialization <n>
        \\  --max-symbol-bytes-per-doc <n>
        \\  --max-support-bytes-per-doc <n>
        \\  --max-accumulator-flush-count <n>
        \\  --max-public-query-http-us <n>
        \\  --max-public-query-load-rss-peak-bytes <n>
        \\  --max-public-query-search-rss-peak-bytes <n>
        \\  --min-public-query-http-speedup <n>
        \\  --max-churn-algebraic-update-ms <n>
        \\  --max-churn-sidecar-bytes <n>
        \\  --max-algebraic-query-ms <n>
        \\  --max-algebraic-query-ms-ratio-vs-baseline <n>
        \\  --max-public-query-http-us-ratio-vs-baseline <n>
        \\  --max-algebraic-bytes-per-doc-ratio-vs-baseline <n>
        \\  --max-churn-algebraic-update-ms-ratio-vs-baseline <n>
        \\
    , .{});
}

fn printQuerySummary(algebraic: QueryRecord, baseline: QueryRecord, expected_equal: ?bool) void {
    const has_correctness_record = expected_equal != null;
    const expected_equal_value = expected_equal orelse false;
    std.debug.print(
        "{{\"event\":\"query_summary\",\"case\":\"{s}\",\"algebraic_backend\":\"{s}\",\"algebraic_profile\":\"{s}\",\"query\":\"{s}\",\"temperature\":\"{s}\",\"algebraic_engine\":\"{s}\",\"baseline_engine\":\"{s}\",\"algebraic_avg_ms\":{d:.3},\"baseline_avg_ms\":{d:.3},\"speedup\":{d:.3},\"correctness_record\":{},\"expected_equal\":{},\"checksum_match\":{}}}\n",
        .{
            algebraic.case_name,
            algebraic.algebraic_backend,
            algebraic.algebraic_profile,
            algebraic.query,
            queryTemperature(algebraic.engine),
            algebraic.engine,
            baseline.engine,
            algebraic.avg_ms,
            baseline.avg_ms,
            ratio(baseline.avg_ms, algebraic.avg_ms),
            has_correctness_record,
            expected_equal_value,
            algebraic.checksum == baseline.checksum,
        },
    );
}

fn queryTemperature(engine: []const u8) []const u8 {
    if (std.mem.indexOf(u8, engine, "cold") != null) return "cold";
    if (std.mem.indexOf(u8, engine, "warm") != null) return "warm";
    return "unspecified";
}

fn isConstrainedQueryRecord(query: QueryRecord) bool {
    return std.mem.indexOf(u8, query.case_name, "constrained") != null or
        std.mem.indexOf(u8, query.query, "constrained") != null or
        std.mem.indexOf(u8, query.engine, "constrained") != null or
        std.mem.indexOf(u8, query.engine, "filtered") != null;
}

fn isWideQueryRecord(query: QueryRecord) bool {
    return std.mem.indexOf(u8, query.case_name, "wide") != null or
        std.mem.indexOf(u8, query.query, "wide") != null or
        std.mem.indexOf(u8, query.query, "composite") != null;
}

fn isStatsQueryRecord(query: QueryRecord) bool {
    return std.mem.indexOf(u8, query.query, "stats") != null;
}

fn isCardinalityQueryRecord(query: QueryRecord) bool {
    return std.mem.indexOf(u8, query.query, "cardinality") != null;
}

fn isRangeQueryRecord(query: QueryRecord) bool {
    return std.mem.indexOf(u8, query.query, "range") != null;
}

fn isHistogramQueryRecord(query: QueryRecord) bool {
    return std.mem.indexOf(u8, query.query, "histogram") != null and
        std.mem.indexOf(u8, query.query, "date_histogram") == null;
}

fn queryExpectedEqual(items: []const CorrectnessRecord, case_name: []const u8, query: []const u8) ?bool {
    for (items) |item| {
        if (!std.mem.eql(u8, item.case_name, case_name)) continue;
        if (!std.mem.eql(u8, item.query, query)) continue;
        return item.expected_equal;
    }
    return null;
}

fn findBaseline(
    items: []const QueryRecord,
    case_name: []const u8,
    algebraic_backend: []const u8,
    algebraic_profile: []const u8,
    query: []const u8,
    engine_prefix: []const u8,
) ?QueryRecord {
    for (items) |item| {
        if (!std.mem.eql(u8, item.case_name, case_name)) continue;
        if (!std.mem.eql(u8, item.algebraic_backend, algebraic_backend)) continue;
        if (!std.mem.eql(u8, item.algebraic_profile, algebraic_profile)) continue;
        if (!std.mem.eql(u8, item.query, query)) continue;
        if (!std.mem.startsWith(u8, item.engine, engine_prefix)) continue;
        return item;
    }
    return null;
}

fn findDataset(items: []const DatasetRecord, case_name: []const u8, algebraic_backend: []const u8, algebraic_profile: []const u8) ?DatasetRecord {
    for (items) |item| {
        if (!std.mem.eql(u8, item.case_name, case_name)) continue;
        if (!std.mem.eql(u8, item.algebraic_backend, algebraic_backend)) continue;
        if (!std.mem.eql(u8, item.algebraic_profile, algebraic_profile)) continue;
        return item;
    }
    return null;
}

fn findDatasetMutable(items: []DatasetRecord, case_name: []const u8, algebraic_backend: []const u8, algebraic_profile: []const u8) ?*DatasetRecord {
    for (items) |*item| {
        if (!std.mem.eql(u8, item.case_name, case_name)) continue;
        if (!std.mem.eql(u8, item.algebraic_backend, algebraic_backend)) continue;
        if (!std.mem.eql(u8, item.algebraic_profile, algebraic_profile)) continue;
        return item;
    }
    return null;
}

fn findQuery(
    items: []const QueryRecord,
    case_name: []const u8,
    algebraic_backend: []const u8,
    algebraic_profile: []const u8,
    query: []const u8,
    engine: []const u8,
) ?QueryRecord {
    for (items) |item| {
        if (!std.mem.eql(u8, item.case_name, case_name)) continue;
        if (!std.mem.eql(u8, item.algebraic_backend, algebraic_backend)) continue;
        if (!std.mem.eql(u8, item.algebraic_profile, algebraic_profile)) continue;
        if (!std.mem.eql(u8, item.query, query)) continue;
        if (!std.mem.eql(u8, item.engine, engine)) continue;
        return item;
    }
    return null;
}

fn findChurn(items: []const ChurnRecord, case_name: []const u8) ?ChurnRecord {
    for (items) |item| {
        if (std.mem.eql(u8, item.case_name, case_name)) return item;
    }
    return null;
}

fn findWarmup(items: []const WarmupRecord, case_name: []const u8, algebraic_backend: []const u8, algebraic_profile: []const u8) ?WarmupRecord {
    for (items) |item| {
        if (!std.mem.eql(u8, item.case_name, case_name)) continue;
        if (!std.mem.eql(u8, item.algebraic_backend, algebraic_backend)) continue;
        if (!std.mem.eql(u8, item.algebraic_profile, algebraic_profile)) continue;
        return item;
    }
    return null;
}

fn countQueriesForCase(items: []const QueryRecord, case_name: []const u8, algebraic_backend: []const u8, algebraic_profile: []const u8) u64 {
    var count: u64 = 0;
    for (items) |item| {
        if (!std.mem.eql(u8, item.case_name, case_name)) continue;
        if (!std.mem.eql(u8, item.algebraic_backend, algebraic_backend)) continue;
        if (!std.mem.eql(u8, item.algebraic_profile, algebraic_profile)) continue;
        count += 1;
    }
    return count;
}

fn countAlgebraicQueriesForCase(items: []const QueryRecord, case_name: []const u8, algebraic_backend: []const u8, algebraic_profile: []const u8) u64 {
    var count: u64 = 0;
    for (items) |item| {
        if (!std.mem.eql(u8, item.case_name, case_name)) continue;
        if (!std.mem.eql(u8, item.algebraic_backend, algebraic_backend)) continue;
        if (!std.mem.eql(u8, item.algebraic_profile, algebraic_profile)) continue;
        if (!isAlgebraicEngine(item.engine)) continue;
        count += 1;
    }
    return count;
}

fn isAdaptiveMaterializedCase(case_name: []const u8) bool {
    return std.mem.eql(u8, case_name, "adaptive_materialized") or
        std.mem.eql(u8, case_name, "adaptive_coverage_materialized");
}

fn adaptiveBaselineCase(materialized_case: []const u8, kind: []const u8) []const u8 {
    if (std.mem.eql(u8, materialized_case, "adaptive_coverage_materialized")) {
        if (std.mem.eql(u8, kind, "static")) return "adaptive_coverage_static";
        return "adaptive_coverage_fallback";
    }
    if (std.mem.eql(u8, kind, "static")) return "adaptive_static";
    return "adaptive_fallback";
}

fn isAlgebraicEngine(engine: []const u8) bool {
    return std.mem.startsWith(u8, engine, "algebraic");
}

fn ratio(numerator: f64, denominator: f64) f64 {
    if (denominator == 0) return 0;
    return numerator / denominator;
}

fn regressionRatio(current: f64, baseline: f64) f64 {
    if (baseline == 0) return if (current == 0) 0 else std.math.inf(f64);
    return current / baseline;
}

fn jsonString(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .string => |text| text,
        else => null,
    };
}

fn jsonBool(obj: std.json.ObjectMap, key: []const u8) ?bool {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .bool => |item| item,
        else => null,
    };
}

fn jsonU64(obj: std.json.ObjectMap, key: []const u8) ?u64 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .integer => |item| if (item >= 0) @intCast(item) else null,
        .float => |item| if (item >= 0) @intFromFloat(item) else null,
        else => null,
    };
}

fn jsonI64(obj: std.json.ObjectMap, key: []const u8) ?i64 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .integer => |item| @intCast(item),
        .float => |item| @intFromFloat(item),
        else => null,
    };
}

fn jsonF64(obj: std.json.ObjectMap, key: []const u8) ?f64 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .integer => |item| @floatFromInt(item),
        .float => |item| item,
        else => null,
    };
}

test "algebraic summary guardrail rejects LSM flush regressions" {
    try std.testing.expectError(error.PerformanceGuardrailFailed, enforcePerformanceGuardrail(.{
        .enabled = true,
        .max_lsm_flushes = 0,
    }, .{
        .max_lsm_flushes = 1,
    }));
}

test "algebraic summary guardrail rejects LSM write pressure regressions" {
    try std.testing.expectError(error.PerformanceGuardrailFailed, enforcePerformanceGuardrail(.{
        .enabled = true,
        .max_lsm_write_pressure_compactions = 0,
    }, .{
        .max_lsm_write_pressure_compactions = 1,
    }));
}
