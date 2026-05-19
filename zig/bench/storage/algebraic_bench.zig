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

const db_mod = antfly.db;
const aggregations = db_mod.aggregations;
const algebraic_mod = db_mod.algebraic;
const docstore_mod = db_mod.docstore;
const paths_mod = antfly.paths;
const platform_time = antfly.platform_time;

const algebraic_namespace_prefix = "\x00\x00__algebraic__:";

const AlgebraicBackendKind = enum {
    all,
    mem,
    lmdb,
    lsm,

    fn label(self: AlgebraicBackendKind) []const u8 {
        return switch (self) {
            .all => "all",
            .mem => "mem",
            .lmdb => "lmdb",
            .lsm => "lsm",
        };
    }
};

const Config = struct {
    mode: []const u8 = "baseline",
    algebraic_backend: AlgebraicBackendKind = .mem,
    algebraic_profile: []const u8 = "default",
    algebraic_bulk_ingest: bool = false,
    algebraic_bulk_compact: bool = false,
    algebraic_bulk_flush: bool = true,
    algebraic_bulk_max_deferred_l0_runs: ?usize = null,
    algebraic_bulk_max_foreground_compaction_steps: usize = 0,
    algebraic_bulk_max_foreground_compaction_input_bytes: ?u64 = null,
    algebraic_bulk_max_foreground_compaction_ns: ?u64 = null,
    lsm_flush_threshold: usize = 512,
    lsm_flush_threshold_bytes: u64 = 0,
    lsm_bulk_ingest_flush_threshold_multiplier: usize = 8,
    lsm_bulk_ingest_flush_threshold_bytes_multiplier: usize = 8,
    lsm_direct_bulk_ingest: bool = true,
    lsm_compact_threshold_runs: usize = 16,
    lsm_level_target_runs_base: usize = 4,
    lsm_level_target_runs_multiplier: usize = 4,
    lsm_level_target_bytes_base: usize = 128 * 1024,
    lsm_level_target_bytes_multiplier: usize = 8,
    docs: usize = 20_000,
    repeats: usize = 25,
    batch_size: usize = 500,
    region_cardinality: usize = 16,
    product_cardinality: usize = 128,
    customer_cardinality: usize = 4096,
    segment_cardinality: usize = 8,
    tenant_cardinality: usize = 8,
    store_cardinality: usize = 256,
    channel_cardinality: usize = 4,
    days: usize = 30,
    fanout: usize = 1,
    churn_ops: usize = 1000,
};

const Dataset = struct {
    hits: []db_mod.types.SearchHit,
    derived_docs: []db_mod.derived_types.DerivedDocument,
    doc_json_bytes: usize,
    derived_json_bytes: usize,

    fn deinit(self: *Dataset, alloc: std.mem.Allocator) void {
        for (self.hits) |hit| {
            alloc.free(hit.id);
            if (hit.stored_data) |stored| alloc.free(stored);
        }
        for (self.derived_docs[self.hits.len..]) |doc| {
            alloc.free(doc.key);
            if (doc.cleaned_value) |value| alloc.free(value);
        }
        if (self.hits.len > 0) alloc.free(self.hits);
        if (self.derived_docs.len > 0) alloc.free(self.derived_docs);
        self.* = undefined;
    }
};

const QueryStats = struct {
    total_ns: u64 = 0,
    min_ns: u64 = std.math.maxInt(u64),
    max_ns: u64 = 0,
    checksum: u64 = 0,
};

const AdaptiveWarmupCounts = struct {
    candidate_count: u64 = 0,
    progress_count: u64 = 0,
    ready_count: u64 = 0,
    backfilling_count: u64 = 0,
    rebuild_required_count: u64 = 0,
    stale_count: u64 = 0,
    dematerialize_recommended_count: u64 = 0,
    decision_history_count: u64 = 0,
    policy_drift_count: u64 = 0,
};

const SidecarStats = struct {
    entries: usize = 0,
    key_bytes: usize = 0,
    value_bytes: usize = 0,
    kinds: []SidecarKindStats = &.{},

    fn deinit(self: *SidecarStats, alloc: std.mem.Allocator) void {
        for (self.kinds) |kind| alloc.free(kind.kind);
        if (self.kinds.len > 0) alloc.free(self.kinds);
        self.* = undefined;
    }
};

const SidecarKindStats = struct {
    kind: []u8,
    entries: usize = 0,
    key_bytes: usize = 0,
    value_bytes: usize = 0,
};

const RowFamilyStats = struct {
    entries: usize = 0,
    key_bytes: usize = 0,
    value_bytes: usize = 0,
};

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

const TextIndexStats = struct {
    build_ns: u64 = 0,
    path_bytes: u64 = 0,
};

const AlgebraicBackend = union(enum) {
    mem: struct {
        backend: antfly.mem_backend.Backend,
    },
    lmdb: struct {
        backend: antfly.lmdb_backend.Backend,
        path: []u8,
    },
    lsm: struct {
        backend: antfly.lsm_backend.Backend,
        path: []u8,
    },

    fn runtimeStore(self: *AlgebraicBackend, alloc: std.mem.Allocator) !antfly.storage_backend_erased.Store {
        return switch (self.*) {
            .mem => |*opened| try opened.backend.runtimeStore(alloc, .{}),
            .lmdb => |*opened| try opened.backend.runtimeStore(alloc, .{}),
            .lsm => |*opened| try opened.backend.runtimeStore(alloc, .{}),
        };
    }

    fn path(self: *const AlgebraicBackend) ?[]const u8 {
        return switch (self.*) {
            .mem => null,
            .lmdb => |*opened| opened.path,
            .lsm => |*opened| opened.path,
        };
    }

    fn lsmWriteStats(self: *const AlgebraicBackend) antfly.lsm_backend.Backend.WriteStats {
        return switch (self.*) {
            .mem, .lmdb => .{},
            .lsm => |*opened| opened.backend.snapshotWriteStats(),
        };
    }

    fn close(self: *AlgebraicBackend, io: std.Io, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .mem => |*opened| opened.backend.close(),
            .lmdb => |*opened| {
                opened.backend.close();
                cleanupTempPath(io, opened.path);
                alloc.free(opened.path);
            },
            .lsm => |*opened| {
                opened.backend.close();
                cleanupTempPath(io, opened.path);
                alloc.free(opened.path);
            },
        }
        self.* = undefined;
    }

    fn closeKeepPath(self: *AlgebraicBackend, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .mem => |*opened| opened.backend.close(),
            .lmdb => |*opened| {
                opened.backend.close();
                alloc.free(opened.path);
            },
            .lsm => |*opened| {
                opened.backend.close();
                alloc.free(opened.path);
            },
        }
        self.* = undefined;
    }
};

const CaseOptions = struct {
    standard_queries: bool = true,
    standard_join_query: bool = true,
    join_queries: bool = false,
    wide_queries: bool = false,
    constrained_queries: bool = false,
    cold_warm_queries: bool = false,
    churn: bool = false,
    adaptive_compare_queries: bool = false,
    adaptive_coverage_queries: bool = false,
    adaptive_warmup: bool = false,
    config_json: []const u8 = algebraic_config,
};

const algebraic_config =
    \\{
    \\  "version": 1,
    \\  "table": "orders",
    \\  "group_fields": [
    \\    {"name":"kind","path":"kind","type":"string"},
    \\    {"name":"tenant","path":"tenant_id","type":"string"},
    \\    {"name":"region","path":"region","type":"string"},
    \\    {"name":"product","path":"product","type":"string"},
    \\    {"name":"customer","path":"customer_id","type":"integer"},
    \\    {"name":"segment","path":"segment","type":"string"},
    \\    {"name":"store","path":"store_id","type":"string"},
    \\    {"name":"channel","path":"channel","type":"string"}
    \\  ],
    \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
    \\  "time_fields": [{"name":"created","path":"created_at","type":"timestamp"}],
    \\  "joins": [
    \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer"}
    \\  ],
    \\  "materializations": [
    \\    {"name":"amount_total","op":"sum","group_by":[],"measure":"amount"},
    \\    {"name":"regions","op":"count","group_by":["region"]},
    \\    {"name":"amount_by_region","op":"sum","group_by":["region"],"measure":"amount"},
    \\    {"name":"avg_amount_by_region","op":"avg","group_by":["region"],"measure":"amount"},
    \\    {"name":"min_amount_by_region","op":"min","group_by":["region"],"measure":"amount"},
    \\    {"name":"max_amount_by_region","op":"max","group_by":["region"],"measure":"amount"},
    \\    {"name":"sumsquares_amount_by_region","op":"sumsquares","group_by":["region"],"measure":"amount"},
    \\    {"name":"products","op":"count","group_by":["product"]},
    \\    {"name":"amount_by_product","op":"sum","group_by":["product"],"measure":"amount"},
    \\    {"name":"wide_orders","op":"count","group_by":["tenant","region","product","customer"]},
    \\    {"name":"wide_amount","op":"sum","group_by":["tenant","region","product","customer"],"measure":"amount"},
    \\    {"name":"products_by_tenant","op":"count","group_by":["tenant","product"]},
    \\    {"name":"amount_by_tenant_product","op":"sum","group_by":["tenant","product"],"measure":"amount"},
    \\    {"name":"orders_by_day","op":"count","group_by":["region"],"time":"created","bucket":"day"},
    \\    {"name":"amount_by_day","op":"sum","group_by":["region"],"measure":"amount","time":"created","bucket":"day"},
    \\    {"name":"count_by_segment","op":"count","join":"orders_customers","group_by":["segment"],"group_side":"right","measure_side":"left"},
    \\    {"name":"amount_by_segment","op":"sum","join":"orders_customers","group_by":["segment"],"measure":"amount","group_side":"right","measure_side":"left"}
    \\  ]
    \\}
;

const algebraic_direct_config =
    \\{
    \\  "version": 1,
    \\  "table": "orders",
    \\  "group_fields": [
    \\    {"name":"kind","path":"kind","type":"string"},
    \\    {"name":"tenant","path":"tenant_id","type":"string"},
    \\    {"name":"region","path":"region","type":"string"},
    \\    {"name":"product","path":"product","type":"string"},
    \\    {"name":"customer","path":"customer_id","type":"integer"},
    \\    {"name":"segment","path":"segment","type":"string"},
    \\    {"name":"store","path":"store_id","type":"string"},
    \\    {"name":"channel","path":"channel","type":"string"}
    \\  ],
    \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
    \\  "time_fields": [{"name":"created","path":"created_at","type":"timestamp"}],
    \\  "adaptive": {"observe": false},
    \\  "materializations": [
    \\    {"name":"amount_total","op":"sum","group_by":[],"measure":"amount"},
    \\    {"name":"regions","op":"count","group_by":["region"]},
    \\    {"name":"amount_by_region","op":"sum","group_by":["region"],"measure":"amount"},
    \\    {"name":"avg_amount_by_region","op":"avg","group_by":["region"],"measure":"amount"},
    \\    {"name":"min_amount_by_region","op":"min","group_by":["region"],"measure":"amount"},
    \\    {"name":"max_amount_by_region","op":"max","group_by":["region"],"measure":"amount"},
    \\    {"name":"sumsquares_amount_by_region","op":"sumsquares","group_by":["region"],"measure":"amount"},
    \\    {"name":"products","op":"count","group_by":["product"]},
    \\    {"name":"amount_by_product","op":"sum","group_by":["product"],"measure":"amount"},
    \\    {"name":"wide_orders","op":"count","group_by":["tenant","region","product","customer"]},
    \\    {"name":"wide_amount","op":"sum","group_by":["tenant","region","product","customer"],"measure":"amount"},
    \\    {"name":"products_by_tenant","op":"count","group_by":["tenant","product"]},
    \\    {"name":"amount_by_tenant_product","op":"sum","group_by":["tenant","product"],"measure":"amount"},
    \\    {"name":"orders_by_day","op":"count","group_by":["region"],"time":"created","bucket":"day"},
    \\    {"name":"amount_by_day","op":"sum","group_by":["region"],"measure":"amount","time":"created","bucket":"day"}
    \\  ]
    \\}
;

const algebraic_adaptive_count_config =
    \\{
    \\  "version": 1,
    \\  "table": "orders",
    \\  "group_fields": [
    \\    {"name":"kind","path":"kind","type":"string"},
    \\    {"name":"tenant","path":"tenant_id","type":"string"},
    \\    {"name":"region","path":"region","type":"string"},
    \\    {"name":"product","path":"product","type":"string"},
    \\    {"name":"customer","path":"customer_id","type":"integer"},
    \\    {"name":"segment","path":"segment","type":"string"},
    \\    {"name":"store","path":"store_id","type":"string"},
    \\    {"name":"channel","path":"channel","type":"string"}
    \\  ],
    \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
    \\  "time_fields": [{"name":"created","path":"created_at","type":"timestamp"}],
    \\  "adaptive": {"observe": true, "lazy_materialization": true, "min_observations": 1, "max_backfill_rows_per_tick": 100000, "min_estimated_scan_rows_saved": 1},
    \\  "materializations": []
    \\}
;

const algebraic_adaptive_coverage_config =
    \\{
    \\  "version": 1,
    \\  "table": "orders",
    \\  "group_fields": [
    \\    {"name":"kind","path":"kind","type":"string"},
    \\    {"name":"tenant","path":"tenant_id","type":"string"},
    \\    {"name":"region","path":"region","type":"string"},
    \\    {"name":"product","path":"product","type":"string"},
    \\    {"name":"customer","path":"customer_id","type":"integer"},
    \\    {"name":"segment","path":"segment","type":"string"},
    \\    {"name":"store","path":"store_id","type":"string"},
    \\    {"name":"channel","path":"channel","type":"string"}
    \\  ],
    \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
    \\  "time_fields": [{"name":"created","path":"created_at","type":"timestamp"}],
    \\  "adaptive": {"observe": true, "lazy_materialization": true, "min_observations": 1, "max_backfill_rows_per_tick": 100000, "min_estimated_scan_rows_saved": 1},
    \\  "materializations": []
    \\}
;

const algebraic_fallback_count_config =
    \\{
    \\  "version": 1,
    \\  "table": "orders",
    \\  "group_fields": [
    \\    {"name":"region","path":"region","type":"string"}
    \\  ],
    \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
    \\  "adaptive": {"observe": false, "lazy_materialization": false},
    \\  "materializations": []
    \\}
;

const terms_requests = [_]aggregations.SearchAggregationRequest{.{
    .name = "regions",
    .type = "terms",
    .field = "region",
    .size = 16,
    .aggregations = &.{.{
        .name = "amount_by_region",
        .type = "sum",
        .field = "amount",
    }},
}};

const date_requests = [_]aggregations.SearchAggregationRequest{.{
    .name = "orders_by_day",
    .type = "date_histogram",
    .field = "created_at",
    .calendar_interval = "day",
    .aggregations = &.{.{
        .name = "amount_by_day",
        .type = "sum",
        .field = "amount",
    }},
}};

const minmax_requests = [_]aggregations.SearchAggregationRequest{.{
    .name = "regions",
    .type = "terms",
    .field = "region",
    .size = 16,
    .aggregations = &.{
        .{
            .name = "min_amount_by_region",
            .type = "min",
            .field = "amount",
        },
        .{
            .name = "max_amount_by_region",
            .type = "max",
            .field = "amount",
        },
    },
}};

const terms_stats_requests = [_]aggregations.SearchAggregationRequest{.{
    .name = "regions",
    .type = "terms",
    .field = "region",
    .size = 16,
    .aggregations = &.{.{
        .name = "amount_stats_by_region",
        .type = "stats",
        .field = "amount",
    }},
}};

const root_cardinality_requests = [_]aggregations.SearchAggregationRequest{.{
    .name = "customer_cardinality",
    .type = "cardinality",
    .field = "customer_id",
}};

const amount_range_requests = [_]aggregations.SearchAggregationRequest{.{
    .name = "amount_ranges",
    .type = "range",
    .field = "amount",
    .ranges = &.{
        .{ .name = "low", .start = 0, .end = 50 },
        .{ .name = "high", .start = 50, .end = 100 },
    },
    .aggregations = &.{.{
        .name = "customer_cardinality",
        .type = "cardinality",
        .field = "customer_id",
    }},
}};

const amount_histogram_requests = [_]aggregations.SearchAggregationRequest{.{
    .name = "amount_histogram",
    .type = "histogram",
    .field = "amount",
    .interval = 25,
    .aggregations = &.{.{
        .name = "customer_cardinality",
        .type = "cardinality",
        .field = "customer_id",
    }},
}};

const root_sum_requests = [_]aggregations.SearchAggregationRequest{.{
    .name = "amount_total",
    .type = "sum",
    .field = "amount",
}};

const terms_metrics_requests = [_]aggregations.SearchAggregationRequest{.{
    .name = "regions",
    .type = "terms",
    .field = "region",
    .size = 16,
    .aggregations = &.{
        .{
            .name = "amount_by_region",
            .type = "sum",
            .field = "amount",
        },
        .{
            .name = "avg_amount_by_region",
            .type = "avg",
            .field = "amount",
        },
        .{
            .name = "min_amount_by_region",
            .type = "min",
            .field = "amount",
        },
        .{
            .name = "max_amount_by_region",
            .type = "max",
            .field = "amount",
        },
    },
}};

const join_requests = [_]aggregations.SearchAggregationRequest{.{
    .name = "count_by_segment",
    .type = "terms",
    .field = "segment",
    .size = 8,
    .aggregations = &.{.{
        .name = "amount_by_segment",
        .type = "sum",
        .field = "amount",
    }},
}};

const wide_fields = [_][]const u8{ "tenant_id", "region", "product", "customer_id" };
const wide_requests = [_]aggregations.SearchAggregationRequest{.{
    .name = "wide_orders",
    .type = "terms",
    .field = "tenant_id",
    .fields = wide_fields[0..],
    .size = 100_000,
    .aggregations = &.{.{
        .name = "wide_amount",
        .type = "sum",
        .field = "amount",
    }},
}};

const constrained_requests = [_]aggregations.SearchAggregationRequest{.{
    .name = "products_by_tenant",
    .type = "terms",
    .field = "product",
    .size = 64,
    .aggregations = &.{.{
        .name = "amount_by_tenant_product",
        .type = "sum",
        .field = "amount",
    }},
}};

const tenant_zero_constraints = [_]aggregations.FixedConstraint{.{
    .field = "tenant",
    .value = "t000",
}};

const order_kind_constraints = [_]aggregations.FixedConstraint{.{
    .field = "kind",
    .value = "order",
}};

const adaptive_count_requests = [_]aggregations.SearchAggregationRequest{.{
    .name = "regions",
    .type = "terms",
    .field = "region",
    .size = 16,
}};

pub fn main(init: std.process.Init) !void {
    const alloc = std.heap.c_allocator;
    const cfg = try parseArgs(init.minimal.args);

    if (cfg.algebraic_backend == .all) {
        const backends = [_]AlgebraicBackendKind{ .mem, .lmdb, .lsm };
        for (backends) |backend| {
            var next = cfg;
            next.algebraic_backend = backend;
            try runSelectedMode(init.io, alloc, next);
        }
        return;
    }
    try runSelectedMode(init.io, alloc, cfg);
}

fn runSelectedMode(io: std.Io, alloc: std.mem.Allocator, cfg: Config) anyerror!void {
    if (std.mem.eql(u8, cfg.mode, "baseline")) {
        try runBenchmarkCase(io, alloc, cfg, "baseline", .{});
    } else if (std.mem.eql(u8, cfg.mode, "quick")) {
        try runBenchmarkProfile(io, alloc, cfg, "quick");
    } else if (std.mem.eql(u8, cfg.mode, "standard")) {
        try runBenchmarkProfile(io, alloc, cfg, "standard");
    } else if (std.mem.eql(u8, cfg.mode, "large")) {
        try runBenchmarkProfile(io, alloc, cfg, "large");
    } else if (std.mem.eql(u8, cfg.mode, "backend-tuning")) {
        try runBackendTuningCases(io, alloc, cfg);
    } else if (std.mem.eql(u8, cfg.mode, "scale")) {
        try runScaleCases(io, alloc, cfg);
    } else if (std.mem.eql(u8, cfg.mode, "cardinality")) {
        try runCardinalityCases(io, alloc, cfg);
    } else if (std.mem.eql(u8, cfg.mode, "wide")) {
        try runBenchmarkCase(io, alloc, cfg, "wide_composite_keys", .{ .standard_queries = false, .wide_queries = true });
    } else if (std.mem.eql(u8, cfg.mode, "write-amp")) {
        try runWriteAmplificationCases(io, alloc, cfg);
    } else if (std.mem.eql(u8, cfg.mode, "churn")) {
        try runBenchmarkCase(io, alloc, cfg, "update_delete_churn", .{ .standard_queries = false, .churn = true });
    } else if (std.mem.eql(u8, cfg.mode, "cold")) {
        try runBenchmarkCase(io, alloc, cfg, "cold_vs_warm_reads", .{
            .standard_queries = false,
            .standard_join_query = false,
            .cold_warm_queries = true,
            .config_json = algebraic_direct_config,
        });
    } else if (std.mem.eql(u8, cfg.mode, "fanout")) {
        try runFanoutCases(io, alloc, cfg);
    } else if (std.mem.eql(u8, cfg.mode, "constrained")) {
        try runBenchmarkCase(io, alloc, cfg, "constrained_aggregation", .{ .standard_queries = false, .constrained_queries = true });
    } else if (std.mem.eql(u8, cfg.mode, "adaptive")) {
        try runAdaptiveComparisonCases(io, alloc, cfg);
    } else if (std.mem.eql(u8, cfg.mode, "adaptive-coverage")) {
        try runAdaptiveCoverageCases(io, alloc, cfg);
    } else if (std.mem.eql(u8, cfg.mode, "lsm-analytics")) {
        try runLsmAnalyticsCases(io, alloc, cfg);
    } else if (std.mem.eql(u8, cfg.mode, "lsm-analytics-smoke")) {
        try runLsmAnalyticsSmokeCases(io, alloc, cfg);
    } else if (std.mem.eql(u8, cfg.mode, "graph-traversal")) {
        try runGraphTraversalGuardrail(io, alloc, cfg, "graph_traversal");
    } else if (std.mem.eql(u8, cfg.mode, "graph-traversal-smoke")) {
        var next = cfg;
        next.docs = @min(cfg.docs, @as(usize, 100));
        next.repeats = @min(cfg.repeats, @as(usize, 3));
        next.fanout = @min(cfg.fanout, @as(usize, 2));
        try runGraphTraversalGuardrail(io, alloc, next, "graph_traversal_smoke");
    } else if (std.mem.eql(u8, cfg.mode, "all")) {
        try runBenchmarkCase(io, alloc, cfg, "baseline", .{});
        try runBackendTuningCases(io, alloc, cfg);
        try runScaleCases(io, alloc, cfg);
        try runCardinalityCases(io, alloc, cfg);
        try runBenchmarkCase(io, alloc, cfg, "wide_composite_keys", .{ .standard_queries = false, .wide_queries = true });
        try runWriteAmplificationCases(io, alloc, cfg);
        try runBenchmarkCase(io, alloc, cfg, "update_delete_churn", .{ .standard_queries = false, .churn = true });
        try runBenchmarkCase(io, alloc, cfg, "cold_vs_warm_reads", .{
            .standard_queries = false,
            .standard_join_query = false,
            .cold_warm_queries = true,
            .config_json = algebraic_direct_config,
        });
        try runFanoutCases(io, alloc, cfg);
        try runBenchmarkCase(io, alloc, cfg, "constrained_aggregation", .{ .standard_queries = false, .constrained_queries = true });
        try runAdaptiveComparisonCases(io, alloc, cfg);
        try runAdaptiveCoverageCases(io, alloc, cfg);
        try runGraphTraversalGuardrail(io, alloc, cfg, "graph_traversal");
    } else {
        std.debug.print("invalid --mode: {s}\n", .{cfg.mode});
        return error.InvalidArgument;
    }
}

fn runAdaptiveComparisonCases(io: std.Io, alloc: std.mem.Allocator, cfg: Config) !void {
    var next = cfg;
    next.repeats = @min(cfg.repeats, @as(usize, 10));
    try runBenchmarkCase(io, alloc, next, "adaptive_static", .{
        .standard_queries = false,
        .adaptive_compare_queries = true,
        .standard_join_query = false,
        .churn = true,
        .config_json = algebraic_direct_config,
    });
    try runBenchmarkCase(io, alloc, next, "adaptive_fallback", .{
        .standard_queries = false,
        .adaptive_compare_queries = true,
        .standard_join_query = false,
        .churn = true,
        .config_json = algebraic_fallback_count_config,
    });
    try runBenchmarkCase(io, alloc, next, "adaptive_materialized", .{
        .standard_queries = false,
        .adaptive_compare_queries = true,
        .adaptive_warmup = true,
        .standard_join_query = false,
        .churn = true,
        .config_json = algebraic_adaptive_count_config,
    });
}

fn runAdaptiveCoverageCases(io: std.Io, alloc: std.mem.Allocator, cfg: Config) !void {
    var next = cfg;
    next.repeats = @min(cfg.repeats, @as(usize, 10));
    try runBenchmarkCase(io, alloc, next, "adaptive_coverage_static", .{
        .standard_queries = false,
        .adaptive_coverage_queries = true,
        .standard_join_query = false,
        .churn = true,
        .config_json = algebraic_direct_config,
    });
    try runBenchmarkCase(io, alloc, next, "adaptive_coverage_fallback", .{
        .standard_queries = false,
        .adaptive_coverage_queries = true,
        .standard_join_query = false,
        .churn = true,
        .config_json = algebraic_fallback_count_config,
    });
    try runBenchmarkCase(io, alloc, next, "adaptive_coverage_materialized", .{
        .standard_queries = false,
        .adaptive_coverage_queries = true,
        .adaptive_warmup = true,
        .standard_join_query = false,
        .churn = true,
        .config_json = algebraic_adaptive_coverage_config,
    });
}

fn runLsmAnalyticsCases(io: std.Io, alloc: std.mem.Allocator, cfg: Config) !void {
    var next = cfg;
    next.algebraic_backend = .lsm;
    next.algebraic_profile = "lsm_analytics";
    next.repeats = @min(cfg.repeats, @as(usize, 10));
    const direct_options = CaseOptions{
        .standard_join_query = false,
        .config_json = algebraic_direct_config,
    };
    try runScaleCasesWithOptions(io, alloc, next, direct_options);
    try runCardinalityCasesWithOptions(io, alloc, next, .{
        .standard_queries = false,
        .standard_join_query = false,
        .wide_queries = true,
        .config_json = algebraic_direct_config,
    });
    try runBenchmarkCase(io, alloc, next, "lsm_analytics_wide_composite_keys", .{
        .standard_queries = false,
        .standard_join_query = false,
        .wide_queries = true,
        .config_json = algebraic_direct_config,
    });
    try runWriteAmplificationCases(io, alloc, next);
    try runBenchmarkCase(io, alloc, next, "lsm_analytics_update_delete_churn", .{
        .standard_queries = false,
        .standard_join_query = false,
        .churn = true,
        .config_json = algebraic_direct_config,
    });
    var join_next = next;
    join_next.docs = @min(next.docs, @as(usize, 25));
    join_next.batch_size = @min(next.batch_size, @as(usize, 25));
    join_next.customer_cardinality = @min(next.customer_cardinality, @as(usize, 25));
    try runFanoutCases(io, alloc, join_next);
    try runBenchmarkCase(io, alloc, next, "lsm_analytics_constrained_aggregation", .{
        .standard_queries = false,
        .standard_join_query = false,
        .constrained_queries = true,
        .config_json = algebraic_direct_config,
    });
    var adaptive_next = next;
    adaptive_next.docs = @min(next.docs, @as(usize, 25));
    adaptive_next.batch_size = @min(next.batch_size, @as(usize, 25));
    adaptive_next.customer_cardinality = @min(next.customer_cardinality, @as(usize, 25));
    try runAdaptiveCoverageCases(io, alloc, adaptive_next);
}

fn runLsmAnalyticsSmokeCases(io: std.Io, alloc: std.mem.Allocator, cfg: Config) !void {
    var next = cfg;
    next.algebraic_backend = .lsm;
    next.algebraic_profile = "lsm_analytics_smoke";
    next.repeats = @min(cfg.repeats, @as(usize, 3));
    next.region_cardinality = @min(cfg.region_cardinality, @as(usize, 8));
    next.product_cardinality = @min(cfg.product_cardinality, @as(usize, 16));
    next.customer_cardinality = @min(cfg.customer_cardinality, @as(usize, 32));
    next.segment_cardinality = @min(cfg.segment_cardinality, @as(usize, 4));
    next.tenant_cardinality = @min(cfg.tenant_cardinality, @as(usize, 4));
    next.store_cardinality = @min(cfg.store_cardinality, @as(usize, 32));
    next.channel_cardinality = @min(cfg.channel_cardinality, @as(usize, 2));
    next.days = @min(cfg.days, @as(usize, 7));
    try runBenchmarkCase(io, alloc, next, "lsm_analytics_smoke_adaptive", .{
        .standard_queries = false,
        .adaptive_coverage_queries = true,
        .adaptive_warmup = true,
        .standard_join_query = false,
        .churn = true,
        .config_json = algebraic_adaptive_coverage_config,
    });
}

fn runBenchmarkProfile(io: std.Io, alloc: std.mem.Allocator, cfg: Config, profile: []const u8) anyerror!void {
    var next = cfg;
    next.algebraic_profile = profile;
    if (std.mem.eql(u8, profile, "quick")) {
        next.docs = @min(cfg.docs, @as(usize, 1_000));
        next.repeats = @min(cfg.repeats, @as(usize, 3));
        next.batch_size = @min(cfg.batch_size, @as(usize, 250));
        next.customer_cardinality = @min(cfg.customer_cardinality, @as(usize, 256));
        try printBenchmarkProfile(profile, next);
        try runBenchmarkCase(io, alloc, next, "quick_baseline", .{});
        try runBenchmarkCase(io, alloc, next, "quick_constrained", .{ .standard_queries = false, .constrained_queries = true });
        return;
    }
    if (std.mem.eql(u8, profile, "standard")) {
        next.docs = @max(cfg.docs, @as(usize, 20_000));
        next.repeats = @min(cfg.repeats, @as(usize, 10));
        try printBenchmarkProfile(profile, next);
        try runSelectedMode(io, alloc, withMode(next, "all"));
        return;
    }
    if (std.mem.eql(u8, profile, "large")) {
        next.docs = @max(cfg.docs, @as(usize, 500_000));
        next.repeats = @min(@max(cfg.repeats, @as(usize, 10)), @as(usize, 25));
        next.customer_cardinality = @max(cfg.customer_cardinality, @as(usize, 100_000));
        next.product_cardinality = @max(cfg.product_cardinality, @as(usize, 4_096));
        next.churn_ops = @max(cfg.churn_ops, @as(usize, 10_000));
        try printBenchmarkProfile(profile, next);
        try runSelectedMode(io, alloc, withMode(next, "all"));
        return;
    }
    return error.InvalidArgument;
}

fn withMode(cfg: Config, mode: []const u8) Config {
    var next = cfg;
    next.mode = mode;
    return next;
}

fn printBenchmarkProfile(profile: []const u8, cfg: Config) !void {
    std.debug.print(
        "{{\"event\":\"benchmark_profile\",\"profile\":\"{s}\",\"docs\":{d},\"repeats\":{d},\"batch_size\":{d},\"customers\":{d},\"products\":{d},\"churn_ops\":{d}}}\n",
        .{ profile, cfg.docs, cfg.repeats, cfg.batch_size, cfg.customer_cardinality, cfg.product_cardinality, cfg.churn_ops },
    );
}

fn runBenchmarkCase(
    io: std.Io,
    alloc: std.mem.Allocator,
    cfg: Config,
    case_name: []const u8,
    options: CaseOptions,
) !void {
    var dataset = try buildDataset(alloc, cfg);
    defer dataset.deinit(alloc);

    const text_path = try std.fmt.allocPrint(alloc, "/tmp/antfly-algebraic-bench-{d}", .{nowNs()});
    defer alloc.free(text_path);
    defer cleanupTempPath(io, text_path);

    var text_db = try db_mod.DB.open(alloc, text_path, .{});
    defer text_db.close();
    try text_db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });
    const text_build_start = nowNs();
    try loadTextDb(alloc, &text_db, dataset, cfg);
    const text_stats = TextIndexStats{
        .build_ns = elapsedSince(text_build_start),
        .path_bytes = try directorySizeBytes(alloc, io, text_path),
    };

    var algebraic_backend = try openAlgebraicBackend(io, alloc, cfg, cfg.algebraic_backend, null);
    defer algebraic_backend.close(io, alloc);
    const runtime_store = try algebraic_backend.runtimeStore(alloc);
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    var manager = try db_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try db_mod.types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = options.config_json,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", options.config_json);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const build_start = nowNs();
    var bulk_active = false;
    if (cfg.algebraic_bulk_ingest) {
        try store.beginBulkIngestSession();
        bulk_active = true;
    }
    errdefer if (bulk_active) store.abortBulkIngestSession();
    var start: usize = 0;
    const algebraic_apply_options: algebraic_mod.index.ApplyOptions = .{
        .batch_options = if (cfg.algebraic_bulk_ingest) .{ .mode = .bulk_ingest } else .{},
    };
    while (start < dataset.derived_docs.len) : (start += cfg.batch_size) {
        const end = @min(start + cfg.batch_size, dataset.derived_docs.len);
        try manager.algebraic_indexes.items[0].index.applyBatchWithOptions(&store, .{ .documents = dataset.derived_docs[start..end] }, algebraic_apply_options);
    }
    if (cfg.algebraic_bulk_ingest) {
        try store.finishBulkIngestSessionWithOptions(algebraicBulkFinishOptions(cfg));
        bulk_active = false;
    }
    const algebraic_build_ns = elapsedSince(build_start);
    try store.sync(true);
    const algebraic_backend_path_bytes = if (algebraic_backend.path()) |path|
        try directorySizeBytes(alloc, io, path)
    else
        0;

    const result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = dataset.hits,
        .total_hits = @intCast(dataset.hits.len),
    };
    const empty_hits = try alloc.alloc(db_mod.types.SearchHit, 0);
    defer alloc.free(empty_hits);
    const algebraic_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = empty_hits,
        .total_hits = 0,
    };
    const algebraic_ctx = aggregations.Context{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    };

    if (options.adaptive_warmup) {
        const warmup_start = nowNs();
        const changed = try warmAdaptiveComparisonMaterializations(&manager.algebraic_indexes.items[0].index, &store, options.adaptive_coverage_queries);
        const warmup_ns = elapsedSince(warmup_start);
        const counts = try collectAdaptiveWarmupCounts(alloc, &manager.algebraic_indexes.items[0].index, &store);
        printAdaptiveWarmup(case_name, cfg, manager.algebraic_indexes.items[0].index.status(), counts, warmup_ns, changed);
    }

    var sidecar = try collectSidecarStats(alloc, &manager.algebraic_indexes.items[0].index, &store);
    defer sidecar.deinit(alloc);

    printDataset(case_name, cfg, dataset, algebraic_build_ns, sidecar, manager.algebraic_indexes.items[0].index.config().materializations.len, manager.algebraic_indexes.items[0].index.status(), text_stats, algebraic_backend.path(), algebraic_backend_path_bytes, algebraic_backend.lsmWriteStats());
    if (options.standard_queries) {
        try runStandardQueries(alloc, cfg, case_name, &text_db, result, algebraic_result, algebraic_ctx, options.standard_join_query);
    }
    if (options.join_queries) {
        const join_doc = try runScenario(alloc, cfg, case_name, "doc_scan_denormalized", "segment_join_nested_sum", &join_requests, result, .{});
        const join_text = try runTextScenario(alloc, cfg, case_name, &text_db, "full_text_index_denormalized", "segment_join_nested_sum", &join_requests);
        const join_alg = try runScenario(alloc, cfg, case_name, "algebraic_join", "segment_join_nested_sum", &join_requests, algebraic_result, algebraic_ctx);
        printCorrectness(case_name, "segment_join_nested_sum", join_doc, join_text, join_alg, false);
    }
    if (options.wide_queries) {
        const wide_doc = try runScenario(alloc, cfg, case_name, "doc_scan", "wide_composite_terms_sum", &wide_requests, result, .{});
        const wide_text = try runTextScenario(alloc, cfg, case_name, &text_db, "full_text_index", "wide_composite_terms_sum", &wide_requests);
        const wide_alg = try runScenario(alloc, cfg, case_name, "algebraic", "wide_composite_terms_sum", &wide_requests, algebraic_result, algebraic_ctx);
        printCorrectness(case_name, "wide_composite_terms_sum", wide_doc, wide_text, wide_alg, true);
    }
    if (options.constrained_queries) {
        const needle = "\"tenant_id\":\"t000\"";
        const filtered_hits = try shallowFilterHitsByNeedle(alloc, dataset.hits, needle);
        defer if (filtered_hits.len > 0) alloc.free(filtered_hits);
        const filtered_result = db_mod.types.SearchResult{
            .alloc = alloc,
            .hits = filtered_hits,
            .total_hits = @intCast(filtered_hits.len),
        };
        var constrained_ctx = algebraic_ctx;
        constrained_ctx.algebraic_constraints = tenant_zero_constraints[0..];
        const constrained_doc = try runScenario(alloc, cfg, case_name, "doc_scan_filtered", "tenant_product_terms_sum", &constrained_requests, filtered_result, .{});
        const constrained_text = try runTextFilteredScenario(alloc, cfg, case_name, &text_db, "full_text_index_filtered", "tenant_product_terms_sum", &constrained_requests, "tenant_id", "t000");
        const constrained_alg = try runScenario(alloc, cfg, case_name, "algebraic_constrained", "tenant_product_terms_sum", &constrained_requests, algebraic_result, constrained_ctx);
        printCorrectness(case_name, "tenant_product_terms_sum", constrained_doc, constrained_text, constrained_alg, true);
    }
    if (options.adaptive_compare_queries) {
        const adaptive_doc = try runScenario(alloc, cfg, case_name, "doc_scan", "adaptive_terms_count", &adaptive_count_requests, result, .{});
        const adaptive_text = try runTextScenario(alloc, cfg, case_name, &text_db, "full_text_index", "adaptive_terms_count", &adaptive_count_requests);
        const algebraic_input = if (manager.algebraic_indexes.items[0].index.config().materializations.len == 0 and !options.adaptive_warmup) result else algebraic_result;
        const adaptive_alg = try runScenario(alloc, cfg, case_name, "algebraic", "adaptive_terms_count", &adaptive_count_requests, algebraic_input, algebraic_ctx);
        printCorrectness(case_name, "adaptive_terms_count", adaptive_doc, adaptive_text, adaptive_alg, true);
    }
    if (options.adaptive_coverage_queries) {
        try runAdaptiveCoverageQueries(alloc, cfg, case_name, &text_db, result, algebraic_result, algebraic_ctx, &manager.algebraic_indexes.items[0].index, options.adaptive_warmup);
    }
    if (options.cold_warm_queries) {
        try runColdWarmQueries(alloc, cfg, case_name, &text_db, result, algebraic_result, algebraic_ctx);
        if (cfg.algebraic_backend != .mem) {
            try runPersistentAlgebraicColdQuery(io, alloc, cfg, case_name, dataset, options.config_json);
        }
    }
    if (options.churn) {
        try runChurnScenario(alloc, cfg, case_name, &store, &manager, &text_db, dataset);
    }
    printDatasetAlgebraicStatus(case_name, cfg, manager.algebraic_indexes.items[0].index.status(), "final");
}

fn runStandardQueries(
    alloc: std.mem.Allocator,
    cfg: Config,
    case_name: []const u8,
    text_db: *db_mod.DB,
    result: db_mod.types.SearchResult,
    algebraic_result: db_mod.types.SearchResult,
    algebraic_ctx: aggregations.Context,
    include_join: bool,
) !void {
    const terms_doc = try runScenario(alloc, cfg, case_name, "doc_scan", "terms_nested_sum", &terms_requests, result, .{});
    const terms_text = try runTextScenario(alloc, cfg, case_name, text_db, "full_text_index", "terms_nested_sum", &terms_requests);
    const terms_alg = try runScenario(alloc, cfg, case_name, "algebraic", "terms_nested_sum", &terms_requests, algebraic_result, algebraic_ctx);
    printCorrectness(case_name, "terms_nested_sum", terms_doc, terms_text, terms_alg, true);

    const minmax_doc = try runScenario(alloc, cfg, case_name, "doc_scan", "terms_nested_minmax", &minmax_requests, result, .{});
    const minmax_text = try runTextScenario(alloc, cfg, case_name, text_db, "full_text_index", "terms_nested_minmax", &minmax_requests);
    const minmax_alg = try runScenario(alloc, cfg, case_name, "algebraic", "terms_nested_minmax", &minmax_requests, algebraic_result, algebraic_ctx);
    printCorrectness(case_name, "terms_nested_minmax", minmax_doc, minmax_text, minmax_alg, true);

    const stats_doc = try runScenario(alloc, cfg, case_name, "doc_scan", "terms_nested_stats", &terms_stats_requests, result, .{});
    const stats_text = try runTextScenario(alloc, cfg, case_name, text_db, "full_text_index", "terms_nested_stats", &terms_stats_requests);
    const stats_alg = try runScenario(alloc, cfg, case_name, "algebraic", "terms_nested_stats", &terms_stats_requests, algebraic_result, algebraic_ctx);
    printCorrectness(case_name, "terms_nested_stats", stats_doc, stats_text, stats_alg, true);

    const cardinality_doc = try runScenario(alloc, cfg, case_name, "doc_scan", "root_cardinality", &root_cardinality_requests, result, .{});
    const cardinality_text = try runTextScenario(alloc, cfg, case_name, text_db, "full_text_index", "root_cardinality", &root_cardinality_requests);
    var cardinality_ctx = algebraic_ctx;
    cardinality_ctx.algebraic_constraints = order_kind_constraints[0..];
    const cardinality_alg = try runScenario(alloc, cfg, case_name, "algebraic", "root_cardinality", &root_cardinality_requests, algebraic_result, cardinality_ctx);
    printCorrectness(case_name, "root_cardinality", cardinality_doc, cardinality_text, cardinality_alg, true);

    const range_doc = try runScenario(alloc, cfg, case_name, "doc_scan", "range_nested_cardinality", &amount_range_requests, result, .{});
    const range_text = try runTextScenario(alloc, cfg, case_name, text_db, "full_text_index", "range_nested_cardinality", &amount_range_requests);
    const range_alg = try runScenario(alloc, cfg, case_name, "algebraic", "range_nested_cardinality", &amount_range_requests, algebraic_result, algebraic_ctx);
    printCorrectness(case_name, "range_nested_cardinality", range_doc, range_text, range_alg, true);

    const histogram_doc = try runScenario(alloc, cfg, case_name, "doc_scan", "histogram_nested_cardinality", &amount_histogram_requests, result, .{});
    const histogram_text = try runTextScenario(alloc, cfg, case_name, text_db, "full_text_index", "histogram_nested_cardinality", &amount_histogram_requests);
    const histogram_alg = try runScenario(alloc, cfg, case_name, "algebraic", "histogram_nested_cardinality", &amount_histogram_requests, algebraic_result, algebraic_ctx);
    printCorrectness(case_name, "histogram_nested_cardinality", histogram_doc, histogram_text, histogram_alg, true);

    const date_doc = try runScenario(alloc, cfg, case_name, "doc_scan", "date_histogram_nested_sum", &date_requests, result, .{});
    const date_text = try runTextScenario(alloc, cfg, case_name, text_db, "full_text_index", "date_histogram_nested_sum", &date_requests);
    const date_alg = try runScenario(alloc, cfg, case_name, "algebraic", "date_histogram_nested_sum", &date_requests, algebraic_result, algebraic_ctx);
    printCorrectness(case_name, "date_histogram_nested_sum", date_doc, date_text, date_alg, false);

    if (include_join) {
        const join_doc = try runScenario(alloc, cfg, case_name, "doc_scan_denormalized", "segment_join_nested_sum", &join_requests, result, .{});
        const join_text = try runTextScenario(alloc, cfg, case_name, text_db, "full_text_index_denormalized", "segment_join_nested_sum", &join_requests);
        const join_alg = try runScenario(alloc, cfg, case_name, "algebraic_join", "segment_join_nested_sum", &join_requests, algebraic_result, algebraic_ctx);
        printCorrectness(case_name, "segment_join_nested_sum", join_doc, join_text, join_alg, false);
    }
}

fn warmAdaptiveComparisonMaterializations(
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    coverage: bool,
) !u64 {
    const query = algebraic_mod.ir.Query{
        .kind = .terms,
        .aggregation_name = "regions",
        .bucket_field = "region",
    };
    index.recordObservedQueryShapeWithStore(store, query, "bench_adaptive_warmup");
    if (coverage) {
        const coverage_queries = [_]algebraic_mod.ir.Query{
            .{ .kind = .terms, .aggregation_name = "regions", .bucket_field = "region" },
            .{
                .kind = .terms,
                .aggregation_name = "amount_by_region",
                .bucket_field = "region",
                .metric = .{ .name = "amount_by_region", .op = .sum, .field = "amount" },
            },
            .{
                .kind = .terms,
                .aggregation_name = "avg_amount_by_region",
                .bucket_field = "region",
                .metric = .{ .name = "avg_amount_by_region", .op = .avg, .field = "amount" },
            },
            .{
                .kind = .terms,
                .aggregation_name = "min_amount_by_region",
                .bucket_field = "region",
                .metric = .{ .name = "min_amount_by_region", .op = .min, .field = "amount" },
            },
            .{
                .kind = .terms,
                .aggregation_name = "max_amount_by_region",
                .bucket_field = "region",
                .metric = .{ .name = "max_amount_by_region", .op = .max, .field = "amount" },
            },
            .{
                .kind = .date_histogram,
                .aggregation_name = "orders_by_day",
                .time_field = "created",
                .time_bucket = "day",
            },
            .{
                .kind = .date_histogram,
                .aggregation_name = "amount_by_day",
                .time_field = "created",
                .time_bucket = "day",
                .metric = .{ .name = "amount_by_day", .op = .sum, .field = "amount" },
            },
        };
        for (coverage_queries) |coverage_query| index.recordObservedQueryShapeWithStore(store, coverage_query, "bench_adaptive_coverage_warmup");
    }
    var changed = try index.evaluateAdaptiveCandidates(store, 0);
    while (true) {
        const tick_changed = try index.runAdaptiveWork(store, 0);
        if (tick_changed == 0) break;
        changed += tick_changed;
    }
    return changed;
}

fn collectAdaptiveWarmupCounts(
    alloc: std.mem.Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
) !AdaptiveWarmupCounts {
    var counts = AdaptiveWarmupCounts{};

    const candidates = try index.scanPersistedAdaptiveCandidates(store);
    defer {
        for (candidates) |*candidate| candidate.deinit(alloc);
        if (candidates.len > 0) alloc.free(candidates);
    }
    counts.candidate_count = @intCast(candidates.len);

    const progress_rows = try index.scanPersistedAdaptiveProgress(store);
    defer {
        for (progress_rows) |*progress| progress.deinit(alloc);
        if (progress_rows.len > 0) alloc.free(progress_rows);
    }
    counts.progress_count = @intCast(progress_rows.len);
    for (progress_rows) |progress| switch (progress.lifecycle) {
        .ready => counts.ready_count += 1,
        .backfilling => counts.backfilling_count += 1,
        .rebuild_required => counts.rebuild_required_count += 1,
        .stale => counts.stale_count += 1,
        .dematerialize_recommended => counts.dematerialize_recommended_count += 1,
        .observing, .recommended => {},
    };

    const decisions = try index.scanPersistedAdaptiveDecisions(store, 64);
    defer {
        for (decisions) |*decision| decision.deinit(alloc);
        if (decisions.len > 0) alloc.free(decisions);
    }
    counts.decision_history_count = @intCast(decisions.len);
    for (decisions) |decision| {
        if (!std.mem.eql(u8, decision.previous_decision, decision.decision) or decision.score_delta != 0) {
            counts.policy_drift_count += 1;
        }
    }

    return counts;
}

fn runAdaptiveCoverageQueries(
    alloc: std.mem.Allocator,
    cfg: Config,
    case_name: []const u8,
    text_db: *db_mod.DB,
    result: db_mod.types.SearchResult,
    algebraic_result: db_mod.types.SearchResult,
    algebraic_ctx: aggregations.Context,
    index: *const algebraic_mod.index.Index,
    adaptive_warmup: bool,
) !void {
    const algebraic_input = if (index.config().materializations.len == 0 and !adaptive_warmup) result else algebraic_result;

    const root_doc = try runScenario(alloc, cfg, case_name, "doc_scan", "adaptive_root_sum", &root_sum_requests, result, .{});
    const root_text = try runTextScenario(alloc, cfg, case_name, text_db, "full_text_index", "adaptive_root_sum", &root_sum_requests);
    const root_input = if (index.config().materializations.len == 0) result else algebraic_input;
    const root_alg = try runScenario(alloc, cfg, case_name, "algebraic", "adaptive_root_sum", &root_sum_requests, root_input, algebraic_ctx);
    printCorrectness(case_name, "adaptive_root_sum", root_doc, root_text, root_alg, expectedAdaptiveCoverageExact(case_name, "adaptive_root_sum"));
    printAdaptiveCoverageCorrectnessClassification(case_name, "adaptive_root_sum");

    const terms_doc = try runScenario(alloc, cfg, case_name, "doc_scan", "adaptive_terms_metrics", &terms_metrics_requests, result, .{});
    const terms_text = try runTextScenario(alloc, cfg, case_name, text_db, "full_text_index", "adaptive_terms_metrics", &terms_metrics_requests);
    const terms_alg = try runScenario(alloc, cfg, case_name, "algebraic", "adaptive_terms_metrics", &terms_metrics_requests, algebraic_input, algebraic_ctx);
    printCorrectness(case_name, "adaptive_terms_metrics", terms_doc, terms_text, terms_alg, expectedAdaptiveCoverageExact(case_name, "adaptive_terms_metrics"));
    printAdaptiveCoverageCorrectnessClassification(case_name, "adaptive_terms_metrics");

    const date_doc = try runScenario(alloc, cfg, case_name, "doc_scan", "adaptive_date_metrics", &date_requests, result, .{});
    const date_text = try runTextScenario(alloc, cfg, case_name, text_db, "full_text_index", "adaptive_date_metrics", &date_requests);
    const date_alg = try runScenario(alloc, cfg, case_name, "algebraic", "adaptive_date_metrics", &date_requests, algebraic_input, algebraic_ctx);
    printCorrectness(case_name, "adaptive_date_metrics", date_doc, date_text, date_alg, false);

    const needle = "\"tenant_id\":\"t000\"";
    const filtered_hits = try shallowFilterHitsByNeedle(alloc, result.hits, needle);
    defer if (filtered_hits.len > 0) alloc.free(filtered_hits);
    const filtered_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = filtered_hits,
        .total_hits = @intCast(filtered_hits.len),
    };
    var constrained_ctx = algebraic_ctx;
    constrained_ctx.algebraic_constraints = tenant_zero_constraints[0..];
    const constrained_input = if (index.config().materializations.len == 0) filtered_result else algebraic_result;
    const constrained_doc = try runScenario(alloc, cfg, case_name, "doc_scan_filtered", "adaptive_constrained_terms_sum", &constrained_requests, filtered_result, .{});
    const constrained_text = try runTextFilteredScenario(alloc, cfg, case_name, text_db, "full_text_index_filtered", "adaptive_constrained_terms_sum", &constrained_requests, "tenant_id", "t000");
    const constrained_alg = try runScenario(alloc, cfg, case_name, "algebraic_constrained", "adaptive_constrained_terms_sum", &constrained_requests, constrained_input, constrained_ctx);
    printCorrectness(case_name, "adaptive_constrained_terms_sum", constrained_doc, constrained_text, constrained_alg, true);

    const join_doc = try runScenario(alloc, cfg, case_name, "doc_scan_denormalized", "adaptive_join_terms_sum", &join_requests, result, .{});
    const join_text = try runTextScenario(alloc, cfg, case_name, text_db, "full_text_index_denormalized", "adaptive_join_terms_sum", &join_requests);
    const join_input = if (index.config().materializations.len == 0) result else algebraic_result;
    const join_alg = try runScenario(alloc, cfg, case_name, "algebraic_join", "adaptive_join_terms_sum", &join_requests, join_input, algebraic_ctx);
    printCorrectness(case_name, "adaptive_join_terms_sum", join_doc, join_text, join_alg, false);
}

fn expectedAdaptiveCoverageExact(case_name: []const u8, query: []const u8) bool {
    if (std.mem.eql(u8, case_name, "adaptive_coverage_static") and
        (std.mem.eql(u8, query, "adaptive_root_sum") or std.mem.eql(u8, query, "adaptive_terms_metrics")))
    {
        return false;
    }
    return true;
}

fn printAdaptiveCoverageCorrectnessClassification(case_name: []const u8, query: []const u8) void {
    if (expectedAdaptiveCoverageExact(case_name, query)) return;
    std.debug.print(
        "{{\"event\":\"correctness_classification\",\"case\":\"{s}\",\"query\":\"{s}\",\"expected_equal\":false,\"reason\":\"static_declared_materialization_baseline_not_adaptive_exact\"}}\n",
        .{ case_name, query },
    );
}

fn printCorrectness(
    case_name: []const u8,
    query: []const u8,
    doc_scan: QueryStats,
    full_text: QueryStats,
    algebraic: QueryStats,
    expected_equal: bool,
) void {
    const doc_text_match = doc_scan.checksum == full_text.checksum;
    const doc_algebraic_match = doc_scan.checksum == algebraic.checksum;
    const all_match = doc_text_match and doc_algebraic_match;
    std.debug.print(
        "{{\"event\":\"correctness\",\"case\":\"{s}\",\"query\":\"{s}\",\"expected_equal\":{},\"all_match\":{},\"doc_text_match\":{},\"doc_algebraic_match\":{},\"doc_checksum\":{d},\"full_text_checksum\":{d},\"algebraic_checksum\":{d}}}\n",
        .{
            case_name,
            query,
            expected_equal,
            all_match,
            doc_text_match,
            doc_algebraic_match,
            doc_scan.checksum,
            full_text.checksum,
            algebraic.checksum,
        },
    );
}

fn runScaleCases(io: std.Io, alloc: std.mem.Allocator, cfg: Config) !void {
    try runScaleCasesWithOptions(io, alloc, cfg, .{});
}

fn runScaleCasesWithOptions(io: std.Io, alloc: std.mem.Allocator, cfg: Config, options: CaseOptions) !void {
    const points = [_]usize{ 500, 5_000, 50_000, 500_000 };
    var ran_max = false;
    for (points) |point| {
        if (point > cfg.docs) continue;
        var next = cfg;
        next.docs = point;
        next.repeats = @min(cfg.repeats, @as(usize, 10));
        const case_name = try std.fmt.allocPrint(alloc, "scale_{d}", .{point});
        defer alloc.free(case_name);
        try runBenchmarkCase(io, alloc, next, case_name, options);
        if (point == cfg.docs) ran_max = true;
    }
    if (!ran_max) {
        var next = cfg;
        next.repeats = @min(cfg.repeats, @as(usize, 10));
        const case_name = try std.fmt.allocPrint(alloc, "scale_{d}", .{next.docs});
        defer alloc.free(case_name);
        try runBenchmarkCase(io, alloc, next, case_name, options);
    }
}

fn runBackendTuningCases(io: std.Io, alloc: std.mem.Allocator, cfg: Config) !void {
    const cases = [_]struct {
        case_name: []const u8,
        name: []const u8,
        backend: AlgebraicBackendKind,
        bulk: bool,
        compact: bool,
        config_json: []const u8 = algebraic_config,
        standard_queries: bool = true,
        standard_join_query: bool = true,
    }{
        .{ .case_name = "backend_tuning", .name = "normal", .backend = .mem, .bulk = false, .compact = false },
        .{ .case_name = "backend_tuning", .name = "normal", .backend = .lmdb, .bulk = false, .compact = false },
        .{ .case_name = "backend_tuning", .name = "normal", .backend = .lsm, .bulk = false, .compact = false },
        .{ .case_name = "backend_tuning", .name = "bulk_flush", .backend = .lsm, .bulk = true, .compact = false },
        .{ .case_name = "backend_tuning", .name = "bulk_compact", .backend = .lsm, .bulk = true, .compact = true },
        .{ .case_name = "backend_tuning_direct", .name = "direct_normal", .backend = .mem, .bulk = false, .compact = false, .config_json = algebraic_direct_config, .standard_join_query = false },
        .{ .case_name = "backend_tuning_direct", .name = "direct_normal", .backend = .lmdb, .bulk = false, .compact = false, .config_json = algebraic_direct_config, .standard_join_query = false },
        .{ .case_name = "backend_tuning_direct", .name = "direct_normal", .backend = .lsm, .bulk = false, .compact = false, .config_json = algebraic_direct_config, .standard_join_query = false },
        .{ .case_name = "backend_tuning_direct", .name = "direct_bulk_flush", .backend = .lsm, .bulk = true, .compact = false, .config_json = algebraic_direct_config, .standard_join_query = false },
        .{ .case_name = "backend_tuning_direct", .name = "direct_bulk_compact", .backend = .lsm, .bulk = true, .compact = true, .config_json = algebraic_direct_config, .standard_join_query = false },
    };
    for (cases) |case| {
        var next = cfg;
        next.algebraic_backend = case.backend;
        next.algebraic_profile = case.name;
        next.algebraic_bulk_ingest = case.bulk;
        next.algebraic_bulk_compact = case.compact;
        next.algebraic_bulk_flush = true;
        next.repeats = @min(cfg.repeats, @as(usize, 10));
        try runBenchmarkCase(io, alloc, next, case.case_name, .{
            .standard_queries = case.standard_queries,
            .standard_join_query = case.standard_join_query,
            .config_json = case.config_json,
        });
    }
}

fn runCardinalityCases(io: std.Io, alloc: std.mem.Allocator, cfg: Config) !void {
    try runCardinalityCasesWithOptions(io, alloc, cfg, .{ .standard_queries = false, .wide_queries = true });
}

fn runCardinalityCasesWithOptions(io: std.Io, alloc: std.mem.Allocator, cfg: Config, options: CaseOptions) !void {
    const points = [_]usize{ 16, 128, 4_096, 100_000 };
    for (points) |point| {
        var next = cfg;
        next.product_cardinality = point;
        next.repeats = @min(cfg.repeats, @as(usize, 10));
        const case_name = try std.fmt.allocPrint(alloc, "cardinality_products_{d}", .{point});
        defer alloc.free(case_name);
        try runBenchmarkCase(io, alloc, next, case_name, options);
    }
}

fn runFanoutCases(io: std.Io, alloc: std.mem.Allocator, cfg: Config) !void {
    const points = [_]usize{ 1, 4, 16 };
    var ran_configured = false;
    for (points) |point| {
        if (point > cfg.fanout) continue;
        var next = cfg;
        next.fanout = point;
        next.repeats = @min(cfg.repeats, @as(usize, 10));
        const case_name = try std.fmt.allocPrint(alloc, "join_fanout_{d}", .{point});
        defer alloc.free(case_name);
        try runBenchmarkCase(io, alloc, next, case_name, .{ .standard_queries = false, .join_queries = true });
        if (point == cfg.fanout) ran_configured = true;
    }
    if (!ran_configured) {
        var next = cfg;
        next.repeats = @min(cfg.repeats, @as(usize, 10));
        const case_name = try std.fmt.allocPrint(alloc, "join_fanout_{d}", .{next.fanout});
        defer alloc.free(case_name);
        try runBenchmarkCase(io, alloc, next, case_name, .{ .standard_queries = false, .join_queries = true });
    }
}

fn runWriteAmplificationCases(io: std.Io, alloc: std.mem.Allocator, cfg: Config) !void {
    const counts = [_]usize{ 1, 5, 20, 100 };
    for (counts) |count| {
        var next = cfg;
        next.repeats = 1;
        const config_json = try writeAmpConfigAlloc(alloc, count);
        defer alloc.free(config_json);
        const case_name = try std.fmt.allocPrint(alloc, "write_amp_{d}", .{count});
        defer alloc.free(case_name);
        try runBenchmarkCase(io, alloc, next, case_name, .{
            .standard_queries = false,
            .config_json = config_json,
        });
    }
}

fn runGraphTraversalGuardrail(
    io: std.Io,
    alloc: std.mem.Allocator,
    cfg: Config,
    case_name: []const u8,
) !void {
    const path = try std.fmt.allocPrint(alloc, "/tmp/antfly-algebraic-graph-{d}", .{nowNs()});
    defer alloc.free(path);
    defer cleanupTempPath(io, path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    try db.addIndex(.{
        .name = "graph_alg",
        .kind = .graph,
        .config_json = "{\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\"}}}",
    });

    const node_count = @max(cfg.docs, @as(usize, 2));
    var nodes = try alloc.alloc([]u8, node_count);
    var initialized_nodes: usize = 0;
    defer {
        for (nodes[0..initialized_nodes]) |node| alloc.free(node);
        alloc.free(nodes);
    }
    for (nodes, 0..) |*node, i| {
        node.* = try std.fmt.allocPrint(alloc, "n:{d}", .{i});
        initialized_nodes += 1;
    }

    var doc_writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
    defer doc_writes.deinit(alloc);
    try doc_writes.ensureTotalCapacity(alloc, node_count);
    for (nodes) |node| {
        try doc_writes.append(alloc, .{ .key = node, .value = "{\"kind\":\"graph_node\"}" });
    }

    var graph_writes = std.ArrayListUnmanaged(db_mod.types.GraphEdgeWrite).empty;
    defer graph_writes.deinit(alloc);
    const fanout = @max(cfg.fanout, @as(usize, 1));
    for (0..node_count - 1) |i| {
        for (1..fanout + 1) |hop| {
            const target_index = i + hop;
            if (target_index >= node_count) break;
            try graph_writes.append(alloc, .{
                .index_name = "graph_alg",
                .source = nodes[i],
                .target = nodes[target_index],
                .edge_type = "links",
                .weight = 1.0,
            });
        }
    }

    const build_start = nowNs();
    var start: usize = 0;
    while (start < doc_writes.items.len or start < graph_writes.items.len) : (start += cfg.batch_size) {
        const doc_end = @min(start + cfg.batch_size, doc_writes.items.len);
        const graph_end = @min(start + cfg.batch_size, graph_writes.items.len);
        try db.batch(.{
            .writes = if (start < doc_writes.items.len) doc_writes.items[start..doc_end] else &.{},
            .graph_writes = if (start < graph_writes.items.len) graph_writes.items[start..graph_end] else &.{},
            .sync_level = .full_index,
        });
    }
    const build_ns = elapsedSince(build_start);

    const edge_types = [_][]const u8{"links"};
    const target_index = @min(node_count - 1, @as(usize, 16));
    const max_depth: u32 = @intCast(@max(target_index, @as(usize, 1)));

    var checksum: u64 = 0;
    var traverse_total_ns: u64 = 0;
    var shortest_total_ns: u64 = 0;
    var rejected_total_ns: u64 = 0;
    for (0..cfg.repeats) |_| {
        const traverse_start = nowNs();
        var traverse_result = try db.search(alloc, .{
            .graph_queries = &.{
                .{
                    .name = "reachable",
                    .query = .{
                        .query_type = .traverse,
                        .index_name = "graph_alg",
                        .start_nodes = .{ .keys = &.{nodes[0]} },
                        .params = .{
                            .direction = .out,
                            .edge_types = edge_types[0..],
                            .max_depth = max_depth,
                            .max_results = 64,
                        },
                    },
                },
            },
            .include_stored = false,
            .limit = 0,
        });
        traverse_total_ns += elapsedSince(traverse_start);
        if (traverse_result.graph_results.len > 0) checksum +%= traverse_result.graph_results[0].total_hits;
        traverse_result.deinit();

        const shortest_start = nowNs();
        const shortest = try db.findShortestPath(alloc, "graph_alg", nodes[0], nodes[target_index], edge_types[0..], .out, .min_hops, max_depth, 0, 0);
        if (shortest) |path_result| {
            checksum +%= path_result.length;
            paths_mod.freePath(alloc, path_result);
        }
        shortest_total_ns += elapsedSince(shortest_start);

        const rejected_start = nowNs();
        var rejected_result = try db.search(alloc, .{
            .graph_queries = &.{
                .{
                    .name = "reachable_non_dedup",
                    .query = .{
                        .query_type = .traverse,
                        .index_name = "graph_alg",
                        .start_nodes = .{ .keys = &.{nodes[0]} },
                        .params = .{
                            .direction = .out,
                            .edge_types = edge_types[0..],
                            .max_depth = max_depth,
                            .max_results = 64,
                            .deduplicate = false,
                        },
                    },
                },
            },
            .include_stored = false,
            .limit = 0,
        });
        rejected_total_ns += elapsedSince(rejected_start);
        if (rejected_result.graph_results.len > 0) checksum +%= rejected_result.graph_results[0].total_hits;
        rejected_result.deinit();
    }

    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    const graph_stats = findIndexStats(stats, "graph_alg") orelse return error.IndexNotFound;
    const path_bytes = try directorySizeBytes(alloc, io, path);
    std.debug.print(
        "{{\"event\":\"graph_algebraic_traversal\",\"case\":\"{s}\",\"mode\":\"{s}\",\"docs\":{d},\"edges\":{d},\"fanout\":{d},\"repeats\":{d},\"path_bytes\":{d},\"build_ms\":{d:.3},\"traverse_avg_ms\":{d:.3},\"shortest_avg_ms\":{d:.3},\"rejected_avg_ms\":{d:.3},\"checksum\":{d},\"attempted\":{d},\"proven\":{d},\"rejected\":{d},\"fallback\":{d},\"result_nodes\":{d}}}\n",
        .{
            case_name,
            cfg.mode,
            node_count,
            graph_writes.items.len,
            fanout,
            cfg.repeats,
            path_bytes,
            nsToMsFloat(build_ns),
            nsToMsFloat(traverse_total_ns / cfg.repeats),
            nsToMsFloat(shortest_total_ns / cfg.repeats),
            nsToMsFloat(rejected_total_ns / cfg.repeats),
            checksum,
            graph_stats.algebraic_graph_traversal_attempt_count,
            graph_stats.algebraic_graph_traversal_proven_count,
            graph_stats.algebraic_graph_traversal_rejected_count,
            graph_stats.algebraic_graph_traversal_fallback_count,
            graph_stats.algebraic_graph_traversal_result_node_count,
        },
    );
}

fn openAlgebraicBackend(
    io: std.Io,
    alloc: std.mem.Allocator,
    cfg: Config,
    kind: AlgebraicBackendKind,
    existing_path: ?[]const u8,
) !AlgebraicBackend {
    _ = io;
    return switch (kind) {
        .all => unreachable,
        .mem => .{ .mem = .{
            .backend = antfly.mem_backend.Backend.init(alloc, .{}),
        } },
        .lmdb => blk: {
            const path = if (existing_path) |path|
                try alloc.dupe(u8, path)
            else
                try std.fmt.allocPrint(alloc, "/tmp/antfly-algebraic-lmdb-{d}", .{nowNs()});
            errdefer alloc.free(path);
            const path_z = try alloc.dupeZ(u8, path);
            defer alloc.free(path_z);
            const backend = try antfly.lmdb_backend.Backend.open(alloc, path_z.ptr, .{
                .backend = .{ .create_if_missing = true },
                .env = .{ .map_size = algebraicLmdbMapSize() },
            });
            break :blk .{ .lmdb = .{
                .backend = backend,
                .path = path,
            } };
        },
        .lsm => blk: {
            const path = if (existing_path) |path|
                try alloc.dupe(u8, path)
            else
                try std.fmt.allocPrint(alloc, "/tmp/antfly-algebraic-lsm-{d}", .{nowNs()});
            errdefer alloc.free(path);
            const backend = try antfly.lsm_backend.Backend.open(alloc, path, .{
                .backend = .{ .create_if_missing = true },
                .flush_threshold = cfg.lsm_flush_threshold,
                .flush_threshold_bytes = cfg.lsm_flush_threshold_bytes,
                .bulk_ingest_flush_threshold_multiplier = cfg.lsm_bulk_ingest_flush_threshold_multiplier,
                .bulk_ingest_flush_threshold_bytes_multiplier = cfg.lsm_bulk_ingest_flush_threshold_bytes_multiplier,
                .direct_bulk_ingest = cfg.lsm_direct_bulk_ingest,
                .compact_threshold_runs = cfg.lsm_compact_threshold_runs,
                .level_target_runs_base = cfg.lsm_level_target_runs_base,
                .level_target_runs_multiplier = cfg.lsm_level_target_runs_multiplier,
                .level_target_bytes_base = cfg.lsm_level_target_bytes_base,
                .level_target_bytes_multiplier = cfg.lsm_level_target_bytes_multiplier,
                .bloom = .{ .bits_per_key = 10, .min_bits = 64 },
                .io_runtime = .threaded,
            });
            break :blk .{ .lsm = .{
                .backend = backend,
                .path = path,
            } };
        },
    };
}

fn algebraicLmdbMapSize() usize {
    return 8 * 1024 * 1024 * 1024;
}

fn writeAmpConfigAlloc(alloc: std.mem.Allocator, materialization_count: usize) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc,
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"tenant","path":"tenant_id","type":"string"},
        \\    {"name":"region","path":"region","type":"string"},
        \\    {"name":"product","path":"product","type":"string"},
        \\    {"name":"customer","path":"customer_id","type":"integer"},
        \\    {"name":"segment","path":"segment","type":"string"},
        \\    {"name":"store","path":"store_id","type":"string"},
        \\    {"name":"channel","path":"channel","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [{"name":"created","path":"created_at","type":"timestamp"}],
        \\  "materializations": [
    );
    for (0..materialization_count) |i| {
        if (i > 0) try out.appendSlice(alloc, ",\n");
        try appendWriteAmpMaterialization(alloc, &out, i);
    }
    try out.appendSlice(alloc,
        \\
        \\  ]
        \\}
    );
    return try out.toOwnedSlice(alloc);
}

fn appendWriteAmpMaterialization(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    i: usize,
) !void {
    const fields = [_][]const u8{ "tenant", "region", "product", "customer", "segment", "store", "channel" };
    const is_sum = i % 2 == 1;
    const combo = (i / 2) + 1;
    const header = try std.fmt.allocPrint(
        alloc,
        "    {{\"name\":\"m{d}\",\"op\":\"{s}\",\"group_by\":[",
        .{ i, if (is_sum) "sum" else "count" },
    );
    defer alloc.free(header);
    try out.appendSlice(alloc, header);
    var wrote_field = false;
    for (fields, 0..) |field, field_idx| {
        if ((combo & (@as(usize, 1) << @intCast(field_idx))) == 0) continue;
        if (wrote_field) try out.appendSlice(alloc, ",");
        const quoted = try std.fmt.allocPrint(alloc, "\"{s}\"", .{field});
        defer alloc.free(quoted);
        try out.appendSlice(alloc, quoted);
        wrote_field = true;
    }
    if (!wrote_field) try out.appendSlice(alloc, "\"region\"");
    if (is_sum) {
        try out.appendSlice(alloc, "],\"measure\":\"amount\"}");
    } else {
        try out.appendSlice(alloc, "]}");
    }
}

fn parseArgs(args_in: std.process.Args) !Config {
    var cfg = Config{};
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--mode")) {
            cfg.mode = args.next() orelse {
                std.debug.print("missing value for --mode\n", .{});
                return error.InvalidArgument;
            };
        } else if (std.mem.eql(u8, arg, "--algebraic-backend")) {
            cfg.algebraic_backend = try parseAlgebraicBackend(args.next() orelse {
                std.debug.print("missing value for --algebraic-backend\n", .{});
                return error.InvalidArgument;
            });
        } else if (std.mem.eql(u8, arg, "--algebraic-profile")) {
            cfg.algebraic_profile = args.next() orelse {
                std.debug.print("missing value for --algebraic-profile\n", .{});
                return error.InvalidArgument;
            };
        } else if (std.mem.eql(u8, arg, "--algebraic-bulk-ingest")) {
            cfg.algebraic_bulk_ingest = true;
        } else if (std.mem.eql(u8, arg, "--algebraic-bulk-compact")) {
            cfg.algebraic_bulk_compact = true;
        } else if (std.mem.eql(u8, arg, "--algebraic-bulk-no-compact")) {
            cfg.algebraic_bulk_compact = false;
        } else if (std.mem.eql(u8, arg, "--algebraic-bulk-flush")) {
            cfg.algebraic_bulk_flush = true;
        } else if (std.mem.eql(u8, arg, "--algebraic-bulk-no-flush")) {
            cfg.algebraic_bulk_flush = false;
        } else if (std.mem.eql(u8, arg, "--algebraic-bulk-max-deferred-l0-runs")) {
            cfg.algebraic_bulk_max_deferred_l0_runs = try parseNextUsize(&args, "--algebraic-bulk-max-deferred-l0-runs");
        } else if (std.mem.eql(u8, arg, "--algebraic-bulk-max-foreground-compaction-steps")) {
            cfg.algebraic_bulk_max_foreground_compaction_steps = try parseNextUsize(&args, "--algebraic-bulk-max-foreground-compaction-steps");
        } else if (std.mem.eql(u8, arg, "--algebraic-bulk-max-foreground-compaction-input-bytes")) {
            cfg.algebraic_bulk_max_foreground_compaction_input_bytes = try parseNextU64(&args, "--algebraic-bulk-max-foreground-compaction-input-bytes");
        } else if (std.mem.eql(u8, arg, "--algebraic-bulk-max-foreground-compaction-ns")) {
            cfg.algebraic_bulk_max_foreground_compaction_ns = try parseNextU64(&args, "--algebraic-bulk-max-foreground-compaction-ns");
        } else if (std.mem.eql(u8, arg, "--lsm-flush-threshold")) {
            cfg.lsm_flush_threshold = try parseNextUsize(&args, "--lsm-flush-threshold");
        } else if (std.mem.eql(u8, arg, "--lsm-flush-threshold-bytes")) {
            cfg.lsm_flush_threshold_bytes = try parseNextU64(&args, "--lsm-flush-threshold-bytes");
        } else if (std.mem.eql(u8, arg, "--lsm-bulk-ingest-flush-threshold-multiplier")) {
            cfg.lsm_bulk_ingest_flush_threshold_multiplier = try parseNextUsize(&args, "--lsm-bulk-ingest-flush-threshold-multiplier");
        } else if (std.mem.eql(u8, arg, "--lsm-bulk-ingest-flush-threshold-bytes-multiplier")) {
            cfg.lsm_bulk_ingest_flush_threshold_bytes_multiplier = try parseNextUsize(&args, "--lsm-bulk-ingest-flush-threshold-bytes-multiplier");
        } else if (std.mem.eql(u8, arg, "--lsm-direct-bulk-ingest")) {
            cfg.lsm_direct_bulk_ingest = true;
        } else if (std.mem.eql(u8, arg, "--lsm-no-direct-bulk-ingest")) {
            cfg.lsm_direct_bulk_ingest = false;
        } else if (std.mem.eql(u8, arg, "--lsm-compact-threshold-runs")) {
            cfg.lsm_compact_threshold_runs = try parseNextUsize(&args, "--lsm-compact-threshold-runs");
        } else if (std.mem.eql(u8, arg, "--lsm-level-target-runs-base")) {
            cfg.lsm_level_target_runs_base = try parseNextUsize(&args, "--lsm-level-target-runs-base");
        } else if (std.mem.eql(u8, arg, "--lsm-level-target-runs-multiplier")) {
            cfg.lsm_level_target_runs_multiplier = try parseNextUsize(&args, "--lsm-level-target-runs-multiplier");
        } else if (std.mem.eql(u8, arg, "--lsm-level-target-bytes-base")) {
            cfg.lsm_level_target_bytes_base = try parseNextUsize(&args, "--lsm-level-target-bytes-base");
        } else if (std.mem.eql(u8, arg, "--lsm-level-target-bytes-multiplier")) {
            cfg.lsm_level_target_bytes_multiplier = try parseNextUsize(&args, "--lsm-level-target-bytes-multiplier");
        } else if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, "--docs");
        } else if (std.mem.eql(u8, arg, "--repeats")) {
            cfg.repeats = try parseNextUsize(&args, "--repeats");
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, "--batch-size");
        } else if (std.mem.eql(u8, arg, "--regions")) {
            cfg.region_cardinality = try parseNextUsize(&args, "--regions");
        } else if (std.mem.eql(u8, arg, "--products")) {
            cfg.product_cardinality = try parseNextUsize(&args, "--products");
        } else if (std.mem.eql(u8, arg, "--customers")) {
            cfg.customer_cardinality = try parseNextUsize(&args, "--customers");
        } else if (std.mem.eql(u8, arg, "--segments")) {
            cfg.segment_cardinality = try parseNextUsize(&args, "--segments");
        } else if (std.mem.eql(u8, arg, "--tenants")) {
            cfg.tenant_cardinality = try parseNextUsize(&args, "--tenants");
        } else if (std.mem.eql(u8, arg, "--stores")) {
            cfg.store_cardinality = try parseNextUsize(&args, "--stores");
        } else if (std.mem.eql(u8, arg, "--channels")) {
            cfg.channel_cardinality = try parseNextUsize(&args, "--channels");
        } else if (std.mem.eql(u8, arg, "--days")) {
            cfg.days = try parseNextUsize(&args, "--days");
        } else if (std.mem.eql(u8, arg, "--fanout")) {
            cfg.fanout = try parseNextUsize(&args, "--fanout");
        } else if (std.mem.eql(u8, arg, "--churn-ops")) {
            cfg.churn_ops = try parseNextUsize(&args, "--churn-ops");
        } else {
            std.debug.print("invalid argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.repeats == 0 or cfg.batch_size == 0 or cfg.region_cardinality == 0 or
        cfg.product_cardinality == 0 or cfg.customer_cardinality == 0 or cfg.segment_cardinality == 0 or
        cfg.tenant_cardinality == 0 or cfg.store_cardinality == 0 or cfg.channel_cardinality == 0 or
        cfg.days == 0 or cfg.fanout == 0 or
        cfg.lsm_flush_threshold == 0 or cfg.lsm_compact_threshold_runs == 0 or
        cfg.lsm_bulk_ingest_flush_threshold_multiplier == 0 or cfg.lsm_bulk_ingest_flush_threshold_bytes_multiplier == 0 or
        cfg.lsm_level_target_runs_base == 0 or cfg.lsm_level_target_runs_multiplier == 0 or
        cfg.lsm_level_target_bytes_base == 0 or cfg.lsm_level_target_bytes_multiplier == 0)
    {
        return error.InvalidArgument;
    }
    return cfg;
}

fn parseAlgebraicBackend(raw: []const u8) !AlgebraicBackendKind {
    if (std.mem.eql(u8, raw, "all")) return .all;
    if (std.mem.eql(u8, raw, "mem")) return .mem;
    if (std.mem.eql(u8, raw, "lmdb")) return .lmdb;
    if (std.mem.eql(u8, raw, "lsm")) return .lsm;
    std.debug.print("invalid --algebraic-backend: {s}\n", .{raw});
    return error.InvalidArgument;
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

fn algebraicBulkFinishOptions(cfg: Config) antfly.storage_backend_erased.types.BulkIngestFinishOptions {
    return .{
        .compact = cfg.algebraic_bulk_compact,
        .flush = cfg.algebraic_bulk_flush,
        .max_deferred_l0_runs = cfg.algebraic_bulk_max_deferred_l0_runs,
        .max_foreground_compaction_steps = cfg.algebraic_bulk_max_foreground_compaction_steps,
        .max_foreground_compaction_input_bytes = cfg.algebraic_bulk_max_foreground_compaction_input_bytes,
        .max_foreground_compaction_ns = cfg.algebraic_bulk_max_foreground_compaction_ns,
    };
}

fn buildDataset(alloc: std.mem.Allocator, cfg: Config) !Dataset {
    const hits = try alloc.alloc(db_mod.types.SearchHit, cfg.docs);
    errdefer alloc.free(hits);
    const customer_docs = cfg.customer_cardinality * cfg.fanout;
    const derived_docs = try alloc.alloc(db_mod.derived_types.DerivedDocument, cfg.docs + customer_docs);
    errdefer alloc.free(derived_docs);
    var initialized: usize = 0;
    var initialized_derived: usize = 0;
    errdefer {
        for (hits[0..initialized]) |hit| {
            alloc.free(hit.id);
            if (hit.stored_data) |stored| alloc.free(stored);
        }
        if (initialized_derived > cfg.docs) {
            for (derived_docs[cfg.docs..initialized_derived]) |doc| {
                alloc.free(doc.key);
                if (doc.cleaned_value) |value| alloc.free(value);
            }
        }
    }

    var doc_json_bytes: usize = 0;
    var derived_json_bytes: usize = 0;
    for (0..cfg.docs) |doc_idx| {
        const id = try std.fmt.allocPrint(alloc, "order:{d:0>8}", .{doc_idx});
        errdefer alloc.free(id);
        const body = try encodeOrderJsonAlloc(alloc, cfg, doc_idx);
        errdefer alloc.free(body);
        doc_json_bytes += body.len;
        derived_json_bytes += body.len;
        hits[doc_idx] = .{
            .id = id,
            .stored_data = body,
        };
        derived_docs[doc_idx] = .{
            .key = id,
            .action = .upsert,
            .cleaned_value = body,
        };
        initialized += 1;
        initialized_derived += 1;
    }
    for (0..cfg.customer_cardinality) |customer_idx| {
        for (0..cfg.fanout) |fanout_idx| {
            const derived_idx = cfg.docs + customer_idx * cfg.fanout + fanout_idx;
            const id = try std.fmt.allocPrint(alloc, "customer:{d:0>8}:{d:0>3}", .{ customer_idx, fanout_idx });
            errdefer alloc.free(id);
            const body = try encodeCustomerJsonAlloc(alloc, cfg, customer_idx, fanout_idx);
            errdefer alloc.free(body);
            derived_json_bytes += body.len;
            derived_docs[derived_idx] = .{
                .key = id,
                .action = .upsert,
                .cleaned_value = body,
            };
            initialized_derived += 1;
        }
    }
    return .{
        .hits = hits,
        .derived_docs = derived_docs,
        .doc_json_bytes = doc_json_bytes,
        .derived_json_bytes = derived_json_bytes,
    };
}

fn encodeOrderJsonAlloc(alloc: std.mem.Allocator, cfg: Config, doc_idx: usize) ![]u8 {
    const tenant_idx = doc_idx % cfg.tenant_cardinality;
    const region_idx = doc_idx % cfg.region_cardinality;
    const product_idx = (doc_idx * 17) % cfg.product_cardinality;
    const customer_idx = (doc_idx * 8191) % cfg.customer_cardinality;
    const segment_idx = customer_idx % cfg.segment_cardinality;
    const store_idx = (doc_idx * 101) % cfg.store_cardinality;
    const channel_idx = (doc_idx * 7) % cfg.channel_cardinality;
    const day_idx = (doc_idx * 13) % cfg.days;
    const amount: f64 = @as(f64, @floatFromInt((doc_idx * 37) % 10_000)) / 100.0 + 1.0;
    return try std.fmt.allocPrint(
        alloc,
        "{{\"kind\":\"order\",\"tenant_id\":\"t{d:0>3}\",\"region\":\"r{d:0>3}\",\"product\":\"p{d:0>5}\",\"customer_id\":{d},\"segment\":\"s{d:0>2}\",\"store_id\":\"st{d:0>5}\",\"channel\":\"ch{d:0>2}\",\"created_at\":\"2026-05-{d:0>2}T{d:0>2}:00:00Z\",\"amount\":{d}}}",
        .{
            tenant_idx,
            region_idx,
            product_idx,
            customer_idx,
            segment_idx,
            store_idx,
            channel_idx,
            (day_idx % 28) + 1,
            doc_idx % 24,
            amount,
        },
    );
}

fn encodeCustomerJsonAlloc(alloc: std.mem.Allocator, cfg: Config, customer_idx: usize, fanout_idx: usize) ![]u8 {
    const tenant_idx = customer_idx % cfg.tenant_cardinality;
    const segment_idx = (customer_idx + fanout_idx) % cfg.segment_cardinality;
    const store_idx = (customer_idx * 101 + fanout_idx) % cfg.store_cardinality;
    const channel_idx = (customer_idx * 7 + fanout_idx) % cfg.channel_cardinality;
    return try std.fmt.allocPrint(
        alloc,
        "{{\"kind\":\"customer\",\"tenant_id\":\"t{d:0>3}\",\"customer_id\":{d},\"segment\":\"s{d:0>2}\",\"store_id\":\"st{d:0>5}\",\"channel\":\"ch{d:0>2}\"}}",
        .{ tenant_idx, customer_idx, segment_idx, store_idx, channel_idx },
    );
}

fn loadTextDb(alloc: std.mem.Allocator, db: *db_mod.DB, dataset: Dataset, cfg: Config) !void {
    var start: usize = 0;
    while (start < dataset.hits.len) : (start += cfg.batch_size) {
        const end = @min(start + cfg.batch_size, dataset.hits.len);
        const writes = try alloc.alloc(db_mod.types.BatchWrite, end - start);
        defer alloc.free(writes);
        for (dataset.hits[start..end], 0..) |hit, i| {
            writes[i] = .{
                .key = hit.id,
                .value = hit.stored_data orelse return error.InvalidArgument,
            };
        }
        try db.batch(.{
            .writes = writes,
            .sync_level = .full_index,
        });
    }
    try db.runUntilIdle();
}

fn runScenario(
    alloc: std.mem.Allocator,
    cfg: Config,
    case_name: []const u8,
    engine: []const u8,
    query: []const u8,
    requests: []const aggregations.SearchAggregationRequest,
    result: db_mod.types.SearchResult,
    ctx: aggregations.Context,
) !QueryStats {
    var stats = QueryStats{};
    for (0..cfg.repeats) |_| {
        const start = nowNs();
        const rows = try aggregations.computeSearchAggregations(alloc, requests, result, ctx);
        const elapsed = elapsedSince(start);
        stats.total_ns += elapsed;
        stats.min_ns = @min(stats.min_ns, elapsed);
        stats.max_ns = @max(stats.max_ns, elapsed);
        stats.checksum +%= checksumResults(rows);
        aggregations.deinitResults(alloc, rows);
    }
    std.debug.print(
        "{{\"event\":\"query\",\"case\":\"{s}\",\"algebraic_backend\":\"{s}\",\"algebraic_profile\":\"{s}\",\"engine\":\"{s}\",\"query\":\"{s}\",\"docs\":{d},\"repeats\":{d},\"total_ms\":{d:.3},\"avg_ms\":{d:.3},\"min_ms\":{d:.3},\"max_ms\":{d:.3},\"checksum\":{d}}}\n",
        .{
            case_name,
            cfg.algebraic_backend.label(),
            cfg.algebraic_profile,
            engine,
            query,
            cfg.docs,
            cfg.repeats,
            nsToMsFloat(stats.total_ns),
            nsToMsFloat(stats.total_ns / cfg.repeats),
            nsToMsFloat(stats.min_ns),
            nsToMsFloat(stats.max_ns),
            stats.checksum,
        },
    );
    return stats;
}

fn runTextScenario(
    alloc: std.mem.Allocator,
    cfg: Config,
    case_name: []const u8,
    db: *db_mod.DB,
    engine: []const u8,
    query: []const u8,
    requests: []const aggregations.SearchAggregationRequest,
) !QueryStats {
    var stats = QueryStats{};
    for (0..cfg.repeats) |_| {
        const start = nowNs();
        var result = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .match_all = {} },
            .limit = u32Limit(cfg.docs),
            .include_stored = true,
        });
        defer result.deinit();
        const rows = try aggregations.computeSearchAggregations(alloc, requests, result, .{});
        const elapsed = elapsedSince(start);
        stats.total_ns += elapsed;
        stats.min_ns = @min(stats.min_ns, elapsed);
        stats.max_ns = @max(stats.max_ns, elapsed);
        stats.checksum +%= checksumResults(rows);
        aggregations.deinitResults(alloc, rows);
    }
    std.debug.print(
        "{{\"event\":\"query\",\"case\":\"{s}\",\"algebraic_backend\":\"{s}\",\"algebraic_profile\":\"{s}\",\"engine\":\"{s}\",\"query\":\"{s}\",\"docs\":{d},\"repeats\":{d},\"total_ms\":{d:.3},\"avg_ms\":{d:.3},\"min_ms\":{d:.3},\"max_ms\":{d:.3},\"checksum\":{d}}}\n",
        .{
            case_name,
            cfg.algebraic_backend.label(),
            cfg.algebraic_profile,
            engine,
            query,
            cfg.docs,
            cfg.repeats,
            nsToMsFloat(stats.total_ns),
            nsToMsFloat(stats.total_ns / cfg.repeats),
            nsToMsFloat(stats.min_ns),
            nsToMsFloat(stats.max_ns),
            stats.checksum,
        },
    );
    return stats;
}

fn runTextFilteredScenario(
    alloc: std.mem.Allocator,
    cfg: Config,
    case_name: []const u8,
    db: *db_mod.DB,
    engine: []const u8,
    query: []const u8,
    requests: []const aggregations.SearchAggregationRequest,
    filter_field: []const u8,
    filter_term: []const u8,
) !QueryStats {
    var stats = QueryStats{};
    for (0..cfg.repeats) |_| {
        const start = nowNs();
        var result = try db.search(alloc, .{
            .index_name = "ft_v1",
            .query = .{ .term = .{ .field = filter_field, .term = filter_term } },
            .limit = u32Limit(cfg.docs),
            .include_stored = true,
        });
        defer result.deinit();
        const rows = try aggregations.computeSearchAggregations(alloc, requests, result, .{});
        const elapsed = elapsedSince(start);
        stats.total_ns += elapsed;
        stats.min_ns = @min(stats.min_ns, elapsed);
        stats.max_ns = @max(stats.max_ns, elapsed);
        stats.checksum +%= checksumResults(rows);
        aggregations.deinitResults(alloc, rows);
    }
    std.debug.print(
        "{{\"event\":\"query\",\"case\":\"{s}\",\"algebraic_backend\":\"{s}\",\"algebraic_profile\":\"{s}\",\"engine\":\"{s}\",\"query\":\"{s}\",\"docs\":{d},\"repeats\":{d},\"total_ms\":{d:.3},\"avg_ms\":{d:.3},\"min_ms\":{d:.3},\"max_ms\":{d:.3},\"checksum\":{d}}}\n",
        .{
            case_name,
            cfg.algebraic_backend.label(),
            cfg.algebraic_profile,
            engine,
            query,
            cfg.docs,
            cfg.repeats,
            nsToMsFloat(stats.total_ns),
            nsToMsFloat(stats.total_ns / cfg.repeats),
            nsToMsFloat(stats.min_ns),
            nsToMsFloat(stats.max_ns),
            stats.checksum,
        },
    );
    return stats;
}

fn shallowFilterHitsByNeedle(
    alloc: std.mem.Allocator,
    hits: []const db_mod.types.SearchHit,
    needle: []const u8,
) ![]db_mod.types.SearchHit {
    var count: usize = 0;
    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        if (std.mem.indexOf(u8, stored, needle) != null) count += 1;
    }
    const filtered = try alloc.alloc(db_mod.types.SearchHit, count);
    var out: usize = 0;
    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        if (std.mem.indexOf(u8, stored, needle) == null) continue;
        filtered[out] = hit;
        out += 1;
    }
    return filtered;
}

fn runColdWarmQueries(
    alloc: std.mem.Allocator,
    cfg: Config,
    case_name: []const u8,
    text_db: *db_mod.DB,
    result: db_mod.types.SearchResult,
    algebraic_result: db_mod.types.SearchResult,
    algebraic_ctx: aggregations.Context,
) !void {
    var cold_cfg = cfg;
    cold_cfg.repeats = 1;
    const cold_doc = try runScenario(alloc, cold_cfg, case_name, "doc_scan_cold_first", "terms_nested_sum", &terms_requests, result, .{});
    const cold_text = try runTextScenario(alloc, cold_cfg, case_name, text_db, "full_text_index_cold_first", "terms_nested_sum", &terms_requests);
    const cold_alg = try runScenario(alloc, cold_cfg, case_name, "algebraic_cold_first", "terms_nested_sum", &terms_requests, algebraic_result, algebraic_ctx);
    printCorrectness(case_name, "terms_nested_sum", cold_doc, cold_text, cold_alg, true);
    _ = try runScenario(alloc, cfg, case_name, "doc_scan_warm", "terms_nested_sum", &terms_requests, result, .{});
    _ = try runTextScenario(alloc, cfg, case_name, text_db, "full_text_index_warm", "terms_nested_sum", &terms_requests);
    _ = try runScenario(alloc, cfg, case_name, "algebraic_warm", "terms_nested_sum", &terms_requests, algebraic_result, algebraic_ctx);
}

fn runPersistentAlgebraicColdQuery(
    io: std.Io,
    alloc: std.mem.Allocator,
    cfg: Config,
    case_name: []const u8,
    dataset: Dataset,
    config_json: []const u8,
) !void {
    const path = try std.fmt.allocPrint(alloc, "/tmp/antfly-algebraic-cold-{s}-{d}", .{ cfg.algebraic_backend.label(), nowNs() });
    defer alloc.free(path);
    defer cleanupTempPath(io, path);

    {
        var backend = try openAlgebraicBackend(io, alloc, cfg, cfg.algebraic_backend, path);
        defer backend.closeKeepPath(alloc);
        const runtime_store = try backend.runtimeStore(alloc);
        var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
        defer store.close();
        var manager = try openAlgebraicManager(alloc, config_json);
        defer manager.deinit();
        var start: usize = 0;
        const algebraic_apply_options: algebraic_mod.index.ApplyOptions = .{
            .batch_options = if (cfg.algebraic_bulk_ingest) .{ .mode = .bulk_ingest } else .{},
        };
        while (start < dataset.derived_docs.len) : (start += cfg.batch_size) {
            const end = @min(start + cfg.batch_size, dataset.derived_docs.len);
            try manager.algebraic_indexes.items[0].index.applyBatchWithOptions(&store, .{ .documents = dataset.derived_docs[start..end] }, algebraic_apply_options);
        }
        try store.sync(true);
    }

    var reopened_backend = try openAlgebraicBackend(io, alloc, cfg, cfg.algebraic_backend, path);
    defer reopened_backend.close(io, alloc);
    const reopened_runtime = try reopened_backend.runtimeStore(alloc);
    var reopened_store = try docstore_mod.DocStore.openRuntime(alloc, reopened_runtime);
    defer reopened_store.close();
    var reopened_manager = try openAlgebraicManager(alloc, config_json);
    defer reopened_manager.deinit();
    const empty_hits = try alloc.alloc(db_mod.types.SearchHit, 0);
    defer alloc.free(empty_hits);
    const algebraic_result = db_mod.types.SearchResult{
        .alloc = alloc,
        .hits = empty_hits,
        .total_hits = 0,
    };
    const algebraic_ctx = aggregations.Context{
        .index_manager = &reopened_manager,
        .doc_store = &reopened_store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    };
    var cold_cfg = cfg;
    cold_cfg.repeats = 1;
    _ = try runScenario(alloc, cold_cfg, case_name, "algebraic_persistent_reopen_cold", "terms_nested_sum", &terms_requests, algebraic_result, algebraic_ctx);
    _ = try runScenario(alloc, cfg, case_name, "algebraic_persistent_reopen_warm", "terms_nested_sum", &terms_requests, algebraic_result, algebraic_ctx);
}

fn openAlgebraicManager(alloc: std.mem.Allocator, config_json: []const u8) !db_mod.IndexManager {
    var manager = try db_mod.IndexManager.init(alloc, ".");
    errdefer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try db_mod.types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = config_json,
    });
    errdefer {
        var tmp = config;
        tmp.deinit(alloc);
    }
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", config_json);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });
    return manager;
}

fn runChurnScenario(
    alloc: std.mem.Allocator,
    cfg: Config,
    case_name: []const u8,
    store: *docstore_mod.DocStore,
    manager: *db_mod.IndexManager,
    text_db: *db_mod.DB,
    dataset: Dataset,
) !void {
    var algebraic_total_ns: u64 = 0;
    var text_total_ns: u64 = 0;
    var sidecar_before = try collectSidecarStats(alloc, &manager.algebraic_indexes.items[0].index, store);
    defer sidecar_before.deinit(alloc);
    const start_status = manager.algebraic_indexes.items[0].index.status();
    var op_start: usize = 0;
    while (op_start < cfg.churn_ops) : (op_start += cfg.batch_size) {
        const op_end = @min(op_start + cfg.batch_size, cfg.churn_ops);
        const batch_len = op_end - op_start;
        const docs = try alloc.alloc(db_mod.derived_types.DerivedDocument, batch_len);
        defer alloc.free(docs);
        var writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
        defer writes.deinit(alloc);
        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer deletes.deinit(alloc);
        var deleted_keys = std.ArrayListUnmanaged([]const u8).empty;
        defer deleted_keys.deinit(alloc);
        var owned_values = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_values.items) |value| alloc.free(value);
            owned_values.deinit(alloc);
        }
        for (op_start..op_end, 0..) |op_idx, i| {
            const doc_idx = op_idx % dataset.hits.len;
            const key = dataset.hits[doc_idx].id;
            if (op_idx % 4 == 3) {
                docs[i] = .{ .key = key, .action = .delete };
                try deletes.append(alloc, key);
                try deleted_keys.append(alloc, key);
            } else {
                const value = try encodeOrderJsonAlloc(alloc, cfg, cfg.docs + op_idx);
                try owned_values.append(alloc, value);
                docs[i] = .{ .key = key, .action = .upsert, .cleaned_value = value };
                try writes.append(alloc, .{ .key = key, .value = value });
            }
        }
        const algebraic_start = nowNs();
        try manager.algebraic_indexes.items[0].index.applyBatchWithOptions(
            store,
            .{
                .documents = docs,
                .deleted_keys = deleted_keys.items,
            },
            .{ .batch_options = if (cfg.algebraic_bulk_ingest) .{ .mode = .bulk_ingest } else .{} },
        );
        algebraic_total_ns += elapsedSince(algebraic_start);
        const text_start = nowNs();
        try text_db.batch(.{
            .writes = writes.items,
            .deletes = deletes.items,
            .sync_level = .full_index,
        });
        text_total_ns += elapsedSince(text_start);
    }
    try text_db.runUntilIdle();
    var sidecar = try collectSidecarStats(alloc, &manager.algebraic_indexes.items[0].index, store);
    defer sidecar.deinit(alloc);
    const end_status = manager.algebraic_indexes.items[0].index.status();
    std.debug.print(
        "{{\"event\":\"churn\",\"case\":\"{s}\",\"docs\":{d},\"ops\":{d},\"batch_size\":{d},\"algebraic_bulk_ingest\":{},\"algebraic_update_ms\":{d:.3},\"full_text_update_ms\":{d:.3},\"algebraic_sidecar_entries\":{d},\"algebraic_sidecar_bytes_estimate\":{d},\"algebraic_adaptive_maintenance_plan_build_count\":{d},\"algebraic_adaptive_maintenance_cached_spec_count\":{d},\"algebraic_adaptive_maintenance_disabled_count\":{d}}}\n",
        .{
            case_name,
            cfg.docs,
            cfg.churn_ops,
            cfg.batch_size,
            cfg.algebraic_bulk_ingest,
            nsToMsFloat(algebraic_total_ns),
            nsToMsFloat(text_total_ns),
            sidecar.entries,
            sidecar.key_bytes + sidecar.value_bytes,
            end_status.adaptive_maintenance_plan_build_count - start_status.adaptive_maintenance_plan_build_count,
            end_status.adaptive_maintenance_cached_spec_count - start_status.adaptive_maintenance_cached_spec_count,
            end_status.adaptive_maintenance_disabled_count - start_status.adaptive_maintenance_disabled_count,
        },
    );
    printChurnRowFamilyBreakdown(case_name, cfg, sidecar_before, sidecar, nsToMsFloat(algebraic_total_ns));
}

fn u32Limit(value: usize) u32 {
    return std.math.cast(u32, value) orelse std.math.maxInt(u32);
}

fn checksumResults(results: []const aggregations.SearchAggregationResult) u64 {
    var hash = std.hash.Wyhash.init(0);
    for (results) |result| checksumResult(&hash, result);
    return hash.final();
}

fn checksumResult(hash: *std.hash.Wyhash, result: aggregations.SearchAggregationResult) void {
    hash.update(result.name);
    hash.update(result.type);
    hash.update(result.field);
    if (result.value_json) |value| hash.update(value);
    for (result.buckets) |bucket| {
        hash.update(bucket.key_json);
        var buf: [32]u8 = undefined;
        const count_text = std.fmt.bufPrint(&buf, "{d}", .{bucket.count}) catch unreachable;
        hash.update(count_text);
        for (bucket.aggregations) |child| checksumResult(hash, child);
    }
}

fn collectSidecarStats(
    alloc: std.mem.Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
) !SidecarStats {
    _ = index;
    var stats = SidecarStats{};
    const entries = try store.scanPrefix(alloc, algebraic_namespace_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, entries);
    for (entries) |entry| {
        stats.entries += 1;
        stats.key_bytes += entry.key.len;
        stats.value_bytes += entry.value.len;
        const kind = sidecarKind(entry.key) catch "unknown";
        const kind_stats = try ensureSidecarKindStats(alloc, &stats, kind);
        kind_stats.entries += 1;
        kind_stats.key_bytes += entry.key.len;
        kind_stats.value_bytes += entry.value.len;
    }
    return stats;
}

fn printDataset(
    case_name: []const u8,
    cfg: Config,
    dataset: Dataset,
    algebraic_build_ns: u64,
    sidecar: SidecarStats,
    materialization_count: usize,
    status: algebraic_mod.index.Status,
    text_stats: TextIndexStats,
    algebraic_backend_path: ?[]const u8,
    algebraic_backend_path_bytes: u64,
    lsm_write_stats: antfly.lsm_backend.Backend.WriteStats,
) void {
    const support_entries = sidecarKindEntries(sidecar, "minmax");
    const support_bytes = sidecarKindBytes(sidecar, "minmax");
    const symbol_entries = sidecarKindEntries(sidecar, "sym");
    const symbol_bytes = sidecarKindBytes(sidecar, "sym");
    const minmax_cache_entries = sidecarKindEntries(sidecar, "minmax_cache");
    const minmax_cache_bytes = sidecarKindBytes(sidecar, "minmax_cache");
    std.debug.print(
        "{{\"event\":\"dataset\",\"case\":\"{s}\",\"mode\":\"{s}\",\"algebraic_backend\":\"{s}\",\"algebraic_profile\":\"{s}\",\"algebraic_bulk_ingest\":{},\"algebraic_bulk_compact\":{},\"algebraic_bulk_flush\":{},\"algebraic_backend_path\":\"{s}\",\"algebraic_backend_path_bytes\":{d},\"docs\":{d},\"derived_docs\":{d},\"fanout\":{d},\"materialization_count\":{d},\"doc_json_bytes\":{d},\"derived_json_bytes\":{d},\"full_text_build_ms\":{d:.3},\"full_text_db_path_bytes\":{d},\"algebraic_build_ms\":{d:.3},\"algebraic_sidecar_entries\":{d},\"algebraic_sidecar_key_bytes\":{d},\"algebraic_sidecar_value_bytes\":{d},\"algebraic_sidecar_bytes_estimate\":{d},\"algebraic_support_entries\":{d},\"algebraic_support_bytes\":{d},\"algebraic_symbol_entries\":{d},\"algebraic_symbol_bytes\":{d},\"algebraic_minmax_cache_entries\":{d},\"algebraic_minmax_cache_bytes\":{d}",
        .{
            case_name,
            cfg.mode,
            cfg.algebraic_backend.label(),
            cfg.algebraic_profile,
            cfg.algebraic_bulk_ingest,
            cfg.algebraic_bulk_compact,
            cfg.algebraic_bulk_flush,
            algebraic_backend_path orelse "",
            algebraic_backend_path_bytes,
            cfg.docs,
            dataset.derived_docs.len,
            cfg.fanout,
            materialization_count,
            dataset.doc_json_bytes,
            dataset.derived_json_bytes,
            nsToMsFloat(text_stats.build_ns),
            text_stats.path_bytes,
            nsToMsFloat(algebraic_build_ns),
            sidecar.entries,
            sidecar.key_bytes,
            sidecar.value_bytes,
            sidecar.key_bytes + sidecar.value_bytes,
            support_entries,
            support_bytes,
            symbol_entries,
            symbol_bytes,
            minmax_cache_entries,
            minmax_cache_bytes,
        },
    );
    std.debug.print(
        ",\"algebraic_lsm_flushes\":{d},\"algebraic_lsm_flush_output_runs\":{d},\"algebraic_lsm_sorted_ingest_runs\":{d},\"algebraic_lsm_sorted_ingest_bytes\":{d},\"algebraic_lsm_write_pressure_compactions\":{d}}}\n",
        .{
            lsm_write_stats.flushes,
            lsm_write_stats.flush_output_runs,
            lsm_write_stats.sorted_ingest_runs,
            lsm_write_stats.sorted_ingest_bytes,
            lsm_write_stats.write_pressure_compactions,
        },
    );
    printDatasetAlgebraicStatus(case_name, cfg, status, "initial");
    std.debug.print(
        "{{\"event\":\"dataset_shape\",\"case\":\"{s}\",\"algebraic_backend\":\"{s}\",\"algebraic_profile\":\"{s}\",\"regions\":{d},\"products\":{d},\"customers\":{d},\"segments\":{d},\"tenants\":{d},\"stores\":{d},\"channels\":{d},\"days\":{d}}}\n",
        .{
            case_name,
            cfg.algebraic_backend.label(),
            cfg.algebraic_profile,
            cfg.region_cardinality,
            cfg.product_cardinality,
            cfg.customer_cardinality,
            cfg.segment_cardinality,
            cfg.tenant_cardinality,
            cfg.store_cardinality,
            cfg.channel_cardinality,
            cfg.days,
        },
    );
    std.debug.print(
        "{{\"event\":\"dataset_lsm_config\",\"case\":\"{s}\",\"algebraic_backend\":\"{s}\",\"algebraic_profile\":\"{s}\",\"lsm_flush_threshold\":{d},\"lsm_flush_threshold_bytes\":{d},\"lsm_bulk_ingest_flush_threshold_multiplier\":{d},\"lsm_bulk_ingest_flush_threshold_bytes_multiplier\":{d},\"lsm_direct_bulk_ingest\":{},\"lsm_compact_threshold_runs\":{d},\"lsm_level_target_runs_base\":{d},\"lsm_level_target_runs_multiplier\":{d},\"lsm_level_target_bytes_base\":{d},\"lsm_level_target_bytes_multiplier\":{d},\"algebraic_bulk_max_deferred_l0_runs\":{d},\"algebraic_bulk_max_foreground_compaction_steps\":{d},\"algebraic_bulk_max_foreground_compaction_input_bytes\":{d},\"algebraic_bulk_max_foreground_compaction_ns\":{d}}}\n",
        .{
            case_name,
            cfg.algebraic_backend.label(),
            cfg.algebraic_profile,
            cfg.lsm_flush_threshold,
            cfg.lsm_flush_threshold_bytes,
            cfg.lsm_bulk_ingest_flush_threshold_multiplier,
            cfg.lsm_bulk_ingest_flush_threshold_bytes_multiplier,
            cfg.lsm_direct_bulk_ingest,
            cfg.lsm_compact_threshold_runs,
            cfg.lsm_level_target_runs_base,
            cfg.lsm_level_target_runs_multiplier,
            cfg.lsm_level_target_bytes_base,
            cfg.lsm_level_target_bytes_multiplier,
            cfg.algebraic_bulk_max_deferred_l0_runs orelse 0,
            cfg.algebraic_bulk_max_foreground_compaction_steps,
            cfg.algebraic_bulk_max_foreground_compaction_input_bytes orelse 0,
            cfg.algebraic_bulk_max_foreground_compaction_ns orelse 0,
        },
    );
    for (sidecar.kinds) |kind| {
        std.debug.print(
            "{{\"event\":\"sidecar_kind\",\"case\":\"{s}\",\"kind\":\"{s}\",\"entries\":{d},\"key_bytes\":{d},\"value_bytes\":{d},\"bytes_estimate\":{d}}}\n",
            .{
                case_name,
                kind.kind,
                kind.entries,
                kind.key_bytes,
                kind.value_bytes,
                kind.key_bytes + kind.value_bytes,
            },
        );
    }
}

fn printAdaptiveWarmup(
    case_name: []const u8,
    cfg: Config,
    status: algebraic_mod.index.Status,
    counts: AdaptiveWarmupCounts,
    warmup_ns: u64,
    changed: u64,
) void {
    std.debug.print(
        "{{\"event\":\"adaptive_warmup\",\"case\":\"{s}\",\"algebraic_backend\":\"{s}\",\"algebraic_profile\":\"{s}\",\"adaptive_warmup_ms\":{d:.3},\"adaptive_backfill_ticks\":{d},\"adaptive_ready_count\":{d},\"adaptive_candidate_count\":{d},\"adaptive_progress_count\":{d},\"adaptive_backfilling_count\":{d},\"adaptive_rebuild_required_count\":{d},\"adaptive_stale_count\":{d},\"adaptive_cleanup_recommended_count\":{d},\"adaptive_decision_history_count\":{d},\"adaptive_policy_drift_count\":{d},\"adaptive_recommendation_count\":{d},\"observed_query_shape_count\":{d}}}\n",
        .{
            case_name,
            cfg.algebraic_backend.label(),
            cfg.algebraic_profile,
            nsToMsFloat(warmup_ns),
            changed,
            counts.ready_count,
            counts.candidate_count,
            counts.progress_count,
            counts.backfilling_count,
            counts.rebuild_required_count,
            counts.stale_count,
            counts.dematerialize_recommended_count,
            counts.decision_history_count,
            counts.policy_drift_count,
            status.recommendation_count,
            status.observed_query_shape_count,
        },
    );
}

fn printDatasetAlgebraicStatus(
    case_name: []const u8,
    cfg: Config,
    status: algebraic_mod.index.Status,
    phase: []const u8,
) void {
    std.debug.print(
        "{{\"event\":\"dataset_algebraic_status\",\"phase\":\"{s}\",\"case\":\"{s}\",\"algebraic_backend\":\"{s}\",\"algebraic_profile\":\"{s}\"",
        .{
            phase,
            case_name,
            cfg.algebraic_backend.label(),
            cfg.algebraic_profile,
        },
    );
    std.debug.print(
        ",\"algebraic_join_facts_scanned\":{d},\"algebraic_join_facts_matched\":{d},\"algebraic_join_facts_pruned\":{d},\"algebraic_accumulator_flush_count\":{d},\"algebraic_symbol_cache_hits\":{d},\"algebraic_symbol_cache_misses\":{d},\"algebraic_minmax_cache_hits\":{d},\"algebraic_minmax_cache_misses\":{d},\"algebraic_minmax_support_scans\":{d}",
        .{
            status.join_facts_scanned,
            status.join_facts_matched,
            status.join_facts_pruned,
            status.accumulator_flush_count,
            status.symbol_cache_hits,
            status.symbol_cache_misses,
            status.minmax_cache_hits,
            status.minmax_cache_misses,
            status.minmax_support_scans,
        },
    );
    std.debug.print(
        ",\"algebraic_planner_selected\":{d},\"algebraic_planner_fallback_count\":{d},\"algebraic_planner_last_decision\":\"{s}\",\"algebraic_planner_last_fallback_reason\":\"{s}\",\"algebraic_planner_last_estimated_scan_rows\":{d},\"algebraic_planner_last_estimated_result_buckets\":{d},\"algebraic_planner_lifecycle_ready\":{},\"algebraic_planner_lifecycle_blocking_reason\":\"{s}\"",
        .{
            status.planner_algebraic_selected,
            status.planner_fallback_count,
            status.planner_last_decision orelse "",
            status.planner_last_fallback_reason orelse "",
            status.planner_last_estimated_scan_rows orelse 0,
            status.planner_last_estimated_result_buckets orelse 0,
            status.planner_lifecycle_ready,
            status.planner_lifecycle_blocking_reason orelse "",
        },
    );
    std.debug.print(
        ",\"algebraic_dictionary_registry_claimed_count\":{d},\"algebraic_dictionary_registry_already_owned_count\":{d},\"algebraic_dictionary_registry_owned_by_other_count\":{d},\"algebraic_dictionary_registry_ready_hit_count\":{d},\"algebraic_dictionary_registry_ready_miss_count\":{d},\"algebraic_path_dictionary_fst_rebuild_count\":{d}",
        .{
            status.dictionary_registry_claimed_count,
            status.dictionary_registry_already_owned_count,
            status.dictionary_registry_owned_by_other_count,
            status.dictionary_registry_ready_hit_count,
            status.dictionary_registry_ready_miss_count,
            status.path_dictionary_fst_rebuild_count,
        },
    );
    std.debug.print(
        ",\"algebraic_distributed_partial_validation_proven_count\":{d},\"algebraic_distributed_partial_validation_rejected_count\":{d},\"algebraic_distributed_partial_rows_exported_count\":{d}",
        .{
            status.distributed_partial_validation_proven_count,
            status.distributed_partial_validation_rejected_count,
            status.distributed_partial_rows_exported_count,
        },
    );
    std.debug.print(
        ",\"algebraic_vector_filter_attempt_count\":{d},\"algebraic_vector_filter_resolved_count\":{d},\"algebraic_vector_filter_unsupported_count\":{d},\"algebraic_vector_filter_fail_closed_count\":{d},\"algebraic_vector_filter_include_doc_id_count\":{d},\"algebraic_vector_filter_exclude_doc_id_count\":{d}",
        .{
            status.vector_filter_attempt_count,
            status.vector_filter_resolved_count,
            status.vector_filter_unsupported_count,
            status.vector_filter_fail_closed_count,
            status.vector_filter_include_doc_id_count,
            status.vector_filter_exclude_doc_id_count,
        },
    );
    std.debug.print(
        ",\"algebraic_observed_query_shape_count\":{d},\"algebraic_recommendation_count\":{d},\"algebraic_last_observed_query_shape\":\"{s}\",\"algebraic_last_recommended_shape\":\"{s}\",\"algebraic_doc_fact_write_count\":{d},\"algebraic_doc_fact_delete_count\":{d},\"algebraic_adaptive_maintenance_plan_build_count\":{d},\"algebraic_adaptive_maintenance_cached_spec_count\":{d},\"algebraic_adaptive_maintenance_disabled_count\":{d}}}\n",
        .{
            status.observed_query_shape_count,
            status.recommendation_count,
            status.last_observed_query_shape orelse "",
            status.last_recommended_materialization orelse "",
            status.doc_fact_write_count,
            status.doc_fact_delete_count,
            status.adaptive_maintenance_plan_build_count,
            status.adaptive_maintenance_cached_spec_count,
            status.adaptive_maintenance_disabled_count,
        },
    );
}

fn sidecarKindEntries(sidecar: SidecarStats, kind: []const u8) usize {
    for (sidecar.kinds) |item| {
        if (std.mem.eql(u8, item.kind, kind)) return item.entries;
    }
    return 0;
}

fn findIndexStats(stats: db_mod.types.DBStats, index_name: []const u8) ?db_mod.types.DBIndexStats {
    for (stats.indexes) |item| {
        if (std.mem.eql(u8, item.name, index_name)) return item;
    }
    return null;
}

fn sidecarKindBytes(sidecar: SidecarStats, kind: []const u8) usize {
    for (sidecar.kinds) |item| {
        if (std.mem.eql(u8, item.kind, kind)) return item.key_bytes + item.value_bytes;
    }
    return 0;
}

fn sidecarRowFamilyStats(sidecar: SidecarStats, family: []const u8) RowFamilyStats {
    var out = RowFamilyStats{};
    for (sidecar.kinds) |item| {
        const item_family = sidecarKindRowFamily(item.kind);
        if (!std.mem.eql(u8, item_family, family)) continue;
        out.entries += item.entries;
        out.key_bytes += item.key_bytes;
        out.value_bytes += item.value_bytes;
    }
    return out;
}

fn sidecarKindRowFamily(kind: []const u8) []const u8 {
    if (std.mem.eql(u8, kind, "materialized_expr") or std.mem.eql(u8, kind, "tensor")) return "materialized_expr";
    if (std.mem.eql(u8, kind, "docfact") or std.mem.eql(u8, kind, "docfact_scalar") or std.mem.eql(u8, kind, "docfact_field")) return "docfact";
    if (std.mem.eql(u8, kind, "pathfact")) return "pathfact";
    if (std.mem.eql(u8, kind, "path_lookup") or std.mem.eql(u8, kind, "promoted_path_lookup")) return "path_lookup";
    if (std.mem.eql(u8, kind, "path_profile") or std.mem.eql(u8, kind, "path_profile_history")) return "path_profile";
    if (std.mem.eql(u8, kind, "jf")) return "joinfact";
    if (std.mem.eql(u8, kind, "docjf")) return "docjf";
    if (std.mem.eql(u8, kind, "minmax") or std.mem.eql(u8, kind, "minmax_cache")) return "minmax";
    if (std.mem.eql(u8, kind, "sym")) return "sym";
    return "other";
}

fn printChurnRowFamilyBreakdown(case_name: []const u8, cfg: Config, before: SidecarStats, after: SidecarStats, algebraic_update_ms: f64) void {
    for (churn_row_families) |family| {
        const before_stats = sidecarRowFamilyStats(before, family);
        const after_stats = sidecarRowFamilyStats(after, family);
        const before_bytes = before_stats.key_bytes + before_stats.value_bytes;
        const after_bytes = after_stats.key_bytes + after_stats.value_bytes;
        std.debug.print(
            "{{\"event\":\"churn_row_family\",\"case\":\"{s}\",\"family\":\"{s}\",\"docs\":{d},\"ops\":{d},\"batch_size\":{d},\"algebraic_bulk_ingest\":{},\"algebraic_update_ms\":{d:.3},\"entries_before\":{d},\"entries_after\":{d},\"entries_delta\":{d},\"bytes_before\":{d},\"bytes_after\":{d},\"bytes_delta\":{d}}}\n",
            .{
                case_name,
                family,
                cfg.docs,
                cfg.churn_ops,
                cfg.batch_size,
                cfg.algebraic_bulk_ingest,
                algebraic_update_ms,
                before_stats.entries,
                after_stats.entries,
                signedDelta(after_stats.entries, before_stats.entries),
                before_bytes,
                after_bytes,
                signedDelta(after_bytes, before_bytes),
            },
        );
    }
}

fn signedDelta(after: usize, before: usize) i64 {
    if (after >= before) return @intCast(after - before);
    return -@as(i64, @intCast(before - after));
}

fn ensureSidecarKindStats(alloc: std.mem.Allocator, stats: *SidecarStats, kind: []const u8) !*SidecarKindStats {
    for (stats.kinds) |*item| {
        if (std.mem.eql(u8, item.kind, kind)) return item;
    }
    const next = if (stats.kinds.len == 0)
        try alloc.alloc(SidecarKindStats, 1)
    else
        try alloc.realloc(stats.kinds, stats.kinds.len + 1);
    stats.kinds = next;
    stats.kinds[stats.kinds.len - 1] = .{ .kind = try alloc.dupe(u8, kind) };
    return &stats.kinds[stats.kinds.len - 1];
}

fn sidecarKind(key: []const u8) ![]const u8 {
    const index_name = try algebraic_mod.token.componentAt(key, algebraic_namespace_prefix.len);
    const version = try algebraic_mod.token.componentAt(key, index_name.next);
    const kind = try algebraic_mod.token.componentAt(key, version.next);
    return kind.payload;
}

fn directorySizeBytes(alloc: std.mem.Allocator, io: std.Io, path: []const u8) !u64 {
    var dir = std.Io.Dir.openDirAbsolute(io, path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    defer dir.close(io);
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    var total: u64 = 0;
    while (try walker.next(io)) |entry| {
        if (entry.kind != .file) continue;
        const stat = dir.statFile(io, entry.path, .{}) catch continue;
        total +|= stat.size;
    }
    return total;
}

fn cleanupTempPath(io: std.Io, path: []const u8) void {
    const prefix = "/tmp/";
    if (!std.mem.startsWith(u8, path, prefix)) return;
    var tmp = std.Io.Dir.openDirAbsolute(io, "/tmp", .{}) catch return;
    defer tmp.close(io);
    tmp.deleteTree(io, path[prefix.len..]) catch {};
}

fn nowNs() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    return nowNs() - start_ns;
}

fn nsToMsFloat(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, @floatFromInt(std.time.ns_per_ms));
}
