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
const types = @import("types.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const derived_types = @import("derived/derived_types.zig");
const docstore_mod = @import("../docstore.zig");
const algebraic_mod = @import("algebraic/mod.zig");
const analysis_mod = @import("../../search/analysis.zig");
const search_agg_mod = @import("../../search/aggregation.zig");
const search_mod = @import("../../search/search.zig");
const distributed_stats_mod = @import("../../search/distributed_stats.zig");
const geo_mod = @import("../../search/geo.zig");
const regex_mod = @import("../../search/regex.zig");

pub const NumericRangeRequest = struct {
    name: []const u8 = "",
    start: ?f64 = null,
    end: ?f64 = null,
};

pub const DateRangeRequest = struct {
    name: []const u8 = "",
    start: ?[]const u8 = null,
    end: ?[]const u8 = null,
};

pub const DistanceRangeRequest = struct {
    name: []const u8 = "",
    from: ?f64 = null,
    to: ?f64 = null,
};

pub const SearchAggregationRequest = struct {
    name: []const u8,
    type: []const u8,
    field: []const u8,
    fields: []const []const u8 = &.{},
    size: i64 = 0,
    interval: f64 = 0,
    calendar_interval: []const u8 = "",
    fixed_interval: []const u8 = "",
    min_doc_count: i64 = 0,
    significance_algorithm: []const u8 = "",
    background_query: ?BackgroundQuery = null,
    bucket_path: []const u8 = "",
    sort_order: []const u8 = "",
    from: i64 = 0,
    window: i64 = 0,
    gap_policy: []const u8 = "",
    term_prefix: []const u8 = "",
    term_pattern: []const u8 = "",
    ranges: []const NumericRangeRequest = &.{},
    date_ranges: []const DateRangeRequest = &.{},
    distance_ranges: []const DistanceRangeRequest = &.{},
    center_lat: f64 = 0,
    center_lon: f64 = 0,
    distance_unit: []const u8 = "",
    geohash_precision: u8 = 0,
    algebraic_join: ?algebraic_mod.ir.JoinRef = null,
    aggregations: []const SearchAggregationRequest = &.{},
};

pub const BackgroundQuery = union(enum) {
    match_all: void,
    match: struct {
        field: []const u8,
        text: []const u8,
    },
    term: struct {
        field: []const u8,
        term: []const u8,
    },
};

pub const SearchAggregationBucket = struct {
    key_json: []const u8,
    count: i64,
    score: ?f64 = null,
    bg_count: ?i64 = null,
    aggregations: []SearchAggregationResult = &.{},

    pub fn deinit(self: *SearchAggregationBucket, alloc: Allocator) void {
        alloc.free(self.key_json);
        for (self.aggregations) |*agg| agg.deinit(alloc);
        if (self.aggregations.len > 0) alloc.free(self.aggregations);
        self.* = undefined;
    }
};

pub const SearchAggregationResult = struct {
    name: []const u8,
    field: []const u8,
    type: []const u8,
    value_json: ?[]const u8 = null,
    metadata_json: ?[]const u8 = null,
    buckets: []SearchAggregationBucket = &.{},

    pub fn deinit(self: *SearchAggregationResult, alloc: Allocator) void {
        if (self.value_json) |value_json| alloc.free(value_json);
        if (self.metadata_json) |metadata_json| alloc.free(metadata_json);
        for (self.buckets) |*bucket| bucket.deinit(alloc);
        if (self.buckets.len > 0) alloc.free(self.buckets);
        self.* = undefined;
    }
};

pub const DistributedBackgroundTextStats = struct {
    aggregation_name: []const u8,
    field: []const u8,
    background_doc_count: u32 = 0,
    term_doc_freqs: []const distributed_stats_mod.TermDocFreq = &.{},

    pub fn deinit(self: *DistributedBackgroundTextStats, alloc: Allocator) void {
        alloc.free(self.aggregation_name);
        alloc.free(self.field);
        for (self.term_doc_freqs) |item| alloc.free(item.term);
        if (self.term_doc_freqs.len > 0) alloc.free(self.term_doc_freqs);
        self.* = undefined;
    }
};

pub fn deinitDistributedBackgroundTextStats(
    alloc: Allocator,
    items: []const DistributedBackgroundTextStats,
) void {
    for (items) |item| {
        alloc.free(item.aggregation_name);
        alloc.free(item.field);
        for (item.term_doc_freqs) |term_doc_freq| alloc.free(term_doc_freq.term);
        if (item.term_doc_freqs.len > 0) alloc.free(item.term_doc_freqs);
    }
    if (items.len > 0) alloc.free(items);
}

pub fn deinitResults(alloc: Allocator, results: []SearchAggregationResult) void {
    for (results) |*result| result.deinit(alloc);
    if (results.len > 0) alloc.free(results);
}

pub const Context = struct {
    index_manager: ?*index_manager_mod.IndexManager = null,
    doc_store: ?*docstore_mod.DocStore = null,
    full_text_index_name: ?[]const u8 = null,
    algebraic_index_name: ?[]const u8 = null,
    algebraic_scope: AlgebraicScope = .disabled,
    algebraic_available: bool = false,
    algebraic_constraints: []const FixedConstraint = &.{},
    distributed_text_stats: []const distributed_stats_mod.TextFieldStats = &.{},
    distributed_background_text_stats: []const DistributedBackgroundTextStats = &.{},
};

pub const AlgebraicScope = enum {
    disabled,
    root,
};

pub const FixedConstraint = algebraic_mod.ir.Constraint;

const TermsBucketAccum = struct {
    key: []u8,
    count: i64,
    child_folds: []AlgebraicMetricFold,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.key);
        for (self.child_folds) |*fold| fold.deinit(alloc);
        if (self.child_folds.len > 0) alloc.free(self.child_folds);
        self.* = undefined;
    }
};

const CompositeTermsCandidate = struct {
    key_json: []u8,
    bucket: *TermsBucketAccum,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.key_json);
        self.* = undefined;
    }
};

const TermsCardinalityBucketAccum = struct {
    key: []u8,
    count: i64,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.key);
        self.* = undefined;
    }
};

const DateBucketAccum = struct {
    bucket_start: []u8,
    count: i64,
    child_folds: []AlgebraicMetricFold,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.bucket_start);
        for (self.child_folds) |*fold| fold.deinit(alloc);
        if (self.child_folds.len > 0) alloc.free(self.child_folds);
        self.* = undefined;
    }
};

const HistogramBucketAccum = struct {
    bucket_index: i64,
    count: i64,
    child_folds: []AlgebraicMetricFold,

    fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.child_folds) |*fold| fold.deinit(alloc);
        if (self.child_folds.len > 0) alloc.free(self.child_folds);
        self.* = undefined;
    }
};

pub fn computeSearchAggregations(
    alloc: Allocator,
    requests: []const SearchAggregationRequest,
    result: types.SearchResult,
    ctx: Context,
) anyerror![]SearchAggregationResult {
    return try computeSearchAggregationsAtDepth(alloc, requests, result, ctx, true);
}

fn computeSearchAggregationsAtDepth(
    alloc: Allocator,
    requests: []const SearchAggregationRequest,
    result: types.SearchResult,
    ctx: Context,
    allow_algebraic: bool,
) anyerror![]SearchAggregationResult {
    const split = try splitAggregationRequests(alloc, requests);
    defer split.deinit(alloc);

    var primary = try alloc.alloc(SearchAggregationResult, split.primary.len);
    var primary_filled: usize = 0;
    errdefer {
        for (primary[0..primary_filled]) |*agg| agg.deinit(alloc);
        alloc.free(primary);
    }
    for (split.primary, 0..) |request, i| {
        primary[i] = try computeSingleAggregation(alloc, request, result.hits, ctx, allow_algebraic);
        primary_filled = i + 1;
    }

    if (split.pipeline.len == 0) return primary;

    const pipeline = try computeRootPipelineAggregations(alloc, split.pipeline, primary);
    errdefer {
        for (pipeline) |*agg| agg.deinit(alloc);
        if (pipeline.len > 0) alloc.free(pipeline);
    }

    var out = try alloc.alloc(SearchAggregationResult, primary.len + pipeline.len);
    errdefer alloc.free(out);
    for (primary, 0..) |agg, i| out[i] = agg;
    for (pipeline, 0..) |agg, i| out[primary.len + i] = agg;
    alloc.free(primary);
    if (pipeline.len > 0) alloc.free(pipeline);
    return out;
}

fn computeSingleAggregation(
    alloc: Allocator,
    request: SearchAggregationRequest,
    hits: []const types.SearchHit,
    ctx: Context,
    allow_algebraic: bool,
) anyerror!SearchAggregationResult {
    if (allow_algebraic) {
        if (try computeAlgebraicAggregation(alloc, request, ctx)) |aggregation| return aggregation;
    }
    if (std.mem.eql(u8, request.type, "count")) {
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .value_json = try std.fmt.allocPrint(alloc, "{d}", .{hits.len}),
        };
    }
    if (std.mem.eql(u8, request.type, "sum")) return try computeNumericMetricAggregation(alloc, request, hits, .sum);
    if (std.mem.eql(u8, request.type, "min")) return try computeNumericMetricAggregation(alloc, request, hits, .min);
    if (std.mem.eql(u8, request.type, "max")) return try computeNumericMetricAggregation(alloc, request, hits, .max);
    if (std.mem.eql(u8, request.type, "avg")) return try computeNumericMetricAggregation(alloc, request, hits, .avg);
    if (std.mem.eql(u8, request.type, "stats")) return try computeNumericMetricAggregation(alloc, request, hits, .stats);
    if (std.mem.eql(u8, request.type, "sumsquares")) return try computeNumericMetricAggregation(alloc, request, hits, .sumsquares);
    if (std.mem.eql(u8, request.type, "cardinality")) return try computeCardinalityAggregation(alloc, request, hits);
    if (std.mem.eql(u8, request.type, "terms")) return try computeTermsAggregation(alloc, request, hits, ctx);
    if (std.mem.eql(u8, request.type, "significant_terms")) return try computeSignificantTermsAggregation(alloc, request, hits, ctx);
    if (std.mem.eql(u8, request.type, "histogram")) return try computeHistogramAggregation(alloc, request, hits, ctx);
    if (std.mem.eql(u8, request.type, "date_histogram")) return try computeDateHistogramAggregation(alloc, request, hits, ctx);
    if (std.mem.eql(u8, request.type, "geohash_grid")) return try computeGeohashGridAggregation(alloc, request, hits, ctx);
    if (std.mem.eql(u8, request.type, "range") or std.mem.eql(u8, request.type, "date_range") or std.mem.eql(u8, request.type, "geo_distance")) return try computeRangeAggregation(alloc, request, hits, ctx);
    if (isPipelineAggregation(request.type)) return error.UnsupportedAggregation;
    return error.UnsupportedAggregation;
}

fn computeAlgebraicAggregation(
    alloc: Allocator,
    request: SearchAggregationRequest,
    ctx: Context,
) !?SearchAggregationResult {
    if (ctx.algebraic_scope != .root) return null;
    if (!ctx.algebraic_available) return null;
    const manager = ctx.index_manager orelse return null;
    const store = ctx.doc_store orelse return null;
    const entry = resolveAlgebraicIndex(manager, ctx.algebraic_index_name) orelse return null;
    const index = &entry.index;
    if (index.hasErrors()) {
        index.recordPlannerFallback("index_errors", null, null);
        return null;
    }
    if (!index.plannerLifecycleReady()) {
        index.recordPlannerFallback("schema_lifecycle_not_ready", null, null);
        return null;
    }

    if (std.mem.eql(u8, request.type, "stats")) {
        const maybe_result = computeAlgebraicStatsAggregation(alloc, index, store, request, ctx.algebraic_constraints) catch |err| switch (err) {
            error.AlgebraicPlannerScanTooLarge => {
                index.recordPlannerFallback("stats_rollup_too_many_rows", null, null);
                return null;
            },
            else => return err,
        };
        if (maybe_result) |result| {
            index.recordPlannerSelected(null, null);
            return result;
        }
        index.recordPlannerFallback("stats_unsupported", null, null);
        return null;
    }

    if (std.mem.eql(u8, request.type, "cardinality")) {
        const count = (try index.exactCardinalityForFieldAlloc(store, request.field, ctx.algebraic_constraints)) orelse {
            index.recordPlannerFallback("cardinality_unsupported", null, null);
            return null;
        };
        index.recordPlannerSelected(null, count);
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .value_json = try std.fmt.allocPrint(alloc, "{{\"value\":{d}}}", .{count}),
        };
    }

    if (isAlgebraicMetricType(request.type)) {
        const op = algebraic_mod.algebra.Op.parse(request.type) orelse return null;
        const query = algebraic_mod.ir.Query{
            .kind = .metric,
            .aggregation_name = request.name,
            .constraints = ctx.algebraic_constraints,
            .metric = .{ .name = request.name, .op = op, .field = request.field },
            .join = request.algebraic_join,
        };
        const plan_result = algebraic_mod.planner.planMetricQuery(index, query);
        const plan = plan_result.metric orelse {
            if (plan_result.derived_join_fold) |derived| {
                const raw = try algebraicDerivedJoinMetricRawAlloc(index, store, derived);
                defer if (raw) |bytes| index.alloc.free(bytes);
                index.recordPlannerSelected(null, null);
                return .{
                    .name = request.name,
                    .field = request.field,
                    .type = request.type,
                    .value_json = try algebraicMetricValueJsonAlloc(alloc, op, raw),
                };
            }
            if (try adaptiveMetricRawForQueryAlloc(alloc, index, store, query)) |adaptive_raw| {
                var owned_raw = adaptive_raw;
                defer owned_raw.deinit(index.alloc);
                index.recordPlannerSelected(1, null);
                return .{
                    .name = request.name,
                    .field = request.field,
                    .type = request.type,
                    .value_json = try algebraicMetricValueJsonAlloc(alloc, op, owned_raw.raw),
                };
            }
            index.recordPlannerFallback("metric_no_materialization", plan_result.estimated_scan_rows, plan_result.estimated_result_buckets);
            index.recordObservedQueryShapeWithStore(store, query, "metric_no_materialization");
            return null;
        };
        if (try algebraicPlanExceedsScanBudget(index, store, plan, ctx.algebraic_constraints)) {
            index.recordPlannerFallback("metric_rollup_too_many_rows", try algebraicPlanEstimatedRows(index, store, plan, ctx.algebraic_constraints), null);
            index.recordObservedQueryShapeWithStore(store, query, "metric_rollup_too_many_rows");
            return null;
        }
        const raw = try algebraicMetricRawForPlanAlloc(alloc, index, store, plan, ctx.algebraic_constraints);
        defer if (raw) |bytes| alloc.free(bytes);
        index.recordPlannerSelected(try algebraicPlanEstimatedRows(index, store, plan, ctx.algebraic_constraints), null);
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .value_json = try algebraicMetricValueJsonAlloc(alloc, plan.op, raw),
        };
    }

    if (std.mem.eql(u8, request.type, "terms")) {
        const maybe_result = computeAlgebraicTermsAggregation(alloc, index, store, request, ctx.algebraic_constraints) catch |err| switch (err) {
            error.UnsupportedAggregation => {
                try recordAlgebraicBucketObservation(alloc, index, store, request, ctx.algebraic_constraints, "terms_unsupported");
                index.recordPlannerFallback("terms_unsupported", null, null);
                return null;
            },
            error.AlgebraicPlannerScanTooLarge => {
                try recordAlgebraicBucketObservation(alloc, index, store, request, ctx.algebraic_constraints, "terms_too_many_rows");
                index.recordPlannerFallback("terms_too_many_rows", null, null);
                return null;
            },
            error.AlgebraicResultBucketLimit => {
                try recordAlgebraicBucketObservation(alloc, index, store, request, ctx.algebraic_constraints, "terms_too_many_buckets");
                index.recordPlannerFallback("terms_too_many_buckets", null, null);
                return null;
            },
            else => return err,
        };
        if (maybe_result) |result| {
            index.recordPlannerSelected(null, result.buckets.len);
            return result;
        }
        try recordAlgebraicBucketObservation(alloc, index, store, request, ctx.algebraic_constraints, "terms_unsupported");
        index.recordPlannerFallback("terms_unsupported", null, null);
        return null;
    }

    if (std.mem.eql(u8, request.type, "date_histogram")) {
        const maybe_result = computeAlgebraicDateHistogramAggregation(alloc, index, store, request, ctx.algebraic_constraints) catch |err| switch (err) {
            error.AlgebraicPlannerScanTooLarge => {
                try recordAlgebraicBucketObservation(alloc, index, store, request, ctx.algebraic_constraints, "date_histogram_too_many_rows");
                index.recordPlannerFallback("date_histogram_too_many_rows", null, null);
                return null;
            },
            error.AlgebraicResultBucketLimit => {
                try recordAlgebraicBucketObservation(alloc, index, store, request, ctx.algebraic_constraints, "date_histogram_too_many_buckets");
                index.recordPlannerFallback("date_histogram_too_many_buckets", null, null);
                return null;
            },
            else => return err,
        };
        if (maybe_result) |result| {
            index.recordPlannerSelected(null, result.buckets.len);
            return result;
        }
        try recordAlgebraicBucketObservation(alloc, index, store, request, ctx.algebraic_constraints, "date_histogram_unsupported");
        index.recordPlannerFallback("date_histogram_unsupported", null, null);
        return null;
    }

    if (std.mem.eql(u8, request.type, "histogram")) {
        const maybe_result = computeAlgebraicHistogramAggregation(alloc, index, store, request, ctx.algebraic_constraints) catch |err| switch (err) {
            error.AlgebraicResultBucketLimit => {
                index.recordPlannerFallback("histogram_too_many_buckets", null, null);
                return null;
            },
            else => return err,
        };
        if (maybe_result) |result| {
            index.recordPlannerSelected(null, result.buckets.len);
            return result;
        }
        index.recordPlannerFallback("histogram_unsupported", null, null);
        return null;
    }

    if (std.mem.eql(u8, request.type, "range") or std.mem.eql(u8, request.type, "date_range")) {
        if (try computeAlgebraicRangeAggregation(alloc, index, store, request, ctx.algebraic_constraints)) |result| {
            index.recordPlannerSelected(null, result.buckets.len);
            return result;
        }
        index.recordPlannerFallback("range_unsupported", null, null);
        return null;
    }

    index.recordPlannerFallback("unsupported_type", null, null);
    return null;
}

fn resolveAlgebraicIndex(
    manager: *index_manager_mod.IndexManager,
    preferred_name: ?[]const u8,
) ?*index_manager_mod.IndexManager.AlgebraicIndex {
    if (preferred_name) |name| {
        if (manager.algebraicIndex(name)) |entry| return entry;
        return null;
    }
    return manager.algebraicIndex(null);
}

fn isAlgebraicMetricType(agg_type: []const u8) bool {
    return std.mem.eql(u8, agg_type, "count") or
        std.mem.eql(u8, agg_type, "sum") or
        std.mem.eql(u8, agg_type, "sumsquares") or
        std.mem.eql(u8, agg_type, "avg") or
        std.mem.eql(u8, agg_type, "min") or
        std.mem.eql(u8, agg_type, "max");
}

fn isAlgebraicFoldMetricType(agg_type: []const u8) bool {
    return isAlgebraicMetricType(agg_type) or std.mem.eql(u8, agg_type, "stats");
}

fn isAlgebraicDocIdMetricType(agg_type: []const u8) bool {
    return isAlgebraicFoldMetricType(agg_type) or std.mem.eql(u8, agg_type, "cardinality");
}

const adaptive_tensor_fragments = [_]algebraic_mod.ir.TensorFragment{ .slice, .reduce, .merge };
const adaptive_scalar_output_dims = [_]algebraic_mod.ir.Dimension{.scalar};
const adaptive_bucket_scalar_output_dims = [_]algebraic_mod.ir.Dimension{ .bucket, .scalar };
const adaptive_time_bucket_scalar_output_dims = [_]algebraic_mod.ir.Dimension{ .time, .bucket, .scalar };
const adaptive_count_laws = [_]algebraic_mod.law.Id{.count};
const adaptive_sum_laws = [_]algebraic_mod.law.Id{.sum};
const adaptive_sumsquares_laws = [_]algebraic_mod.law.Id{.sumsquares};
const adaptive_avg_laws = [_]algebraic_mod.law.Id{.avg};
const adaptive_min_laws = [_]algebraic_mod.law.Id{.min};
const adaptive_max_laws = [_]algebraic_mod.law.Id{.max};

fn adaptiveTensorLawIds(op: algebraic_mod.algebra.Op) []const algebraic_mod.law.Id {
    return switch (op) {
        .count => &adaptive_count_laws,
        .sum => &adaptive_sum_laws,
        .sumsquares => &adaptive_sumsquares_laws,
        .avg => &adaptive_avg_laws,
        .min => &adaptive_min_laws,
        .max => &adaptive_max_laws,
    };
}

fn adaptiveTensorOutputDims(kind: algebraic_mod.ir.QueryKind, has_bucket_axis: bool, has_time_axis: bool) []const algebraic_mod.ir.Dimension {
    if (kind == .date_histogram or has_time_axis) return &adaptive_time_bucket_scalar_output_dims;
    if (kind == .terms or has_bucket_axis) return &adaptive_bucket_scalar_output_dims;
    return &adaptive_scalar_output_dims;
}

fn proveAdaptiveTensorRead(
    materialization_id: []const u8,
    op: algebraic_mod.algebra.Op,
    output_dims: []const algebraic_mod.ir.Dimension,
) bool {
    const path = algebraic_mod.ir.PhysicalAccessPath{
        .owner = materialization_id,
        .layout = .materialized_tensor,
        .fragments = &adaptive_tensor_fragments,
        .output_dims = output_dims,
        .law_ids = adaptiveTensorLawIds(op),
    };
    return algebraic_mod.ir.accessPathCanSatisfy(path, .{
        .fragment = .reduce,
        .layout = .materialized_tensor,
        .output_dims = output_dims,
        .owner = materialization_id,
        .law_id = algebraic_mod.law.fromOp(op),
    }).safe();
}

const AlgebraicNumericRangeFilter = struct {
    numeric_range: Range = .{},

    const Range = struct {
        field: []const u8 = "",
        min: ?f64 = null,
        max: ?f64 = null,
        inclusive_min: bool = true,
        inclusive_max: bool = false,
    };
};

const AlgebraicDateRangeFilter = struct {
    date_range: Range = .{},

    const Range = struct {
        field: []const u8 = "",
        start_ns: ?u64 = null,
        end_ns: ?u64 = null,
        inclusive_start: bool = true,
        inclusive_end: bool = false,
    };
};

fn algebraicNumericRangeFilterJsonAlloc(alloc: Allocator, field: []const u8, range_spec: NumericRangeRequest) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, AlgebraicNumericRangeFilter{
        .numeric_range = .{
            .field = field,
            .min = range_spec.start,
            .max = range_spec.end,
        },
    }, .{ .emit_null_optional_fields = false });
}

fn algebraicDateRangeFilterJsonAlloc(alloc: Allocator, field: []const u8, range_spec: DateRangeRequest) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, AlgebraicDateRangeFilter{
        .date_range = .{
            .field = field,
            .start_ns = if (range_spec.start) |start| try parseRfc3339ToNs(start) else null,
            .end_ns = if (range_spec.end) |end| try parseRfc3339ToNs(end) else null,
        },
    }, .{ .emit_null_optional_fields = false });
}

fn algebraicCandidateCountWithConstraints(
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    doc_ids: []const []const u8,
    constraints: []const FixedConstraint,
) !i64 {
    if (constraints.len == 0) return @intCast(doc_ids.len);
    const constraint_ids = try index.docIdsForConstraintsAlloc(store, constraints);
    defer index.freeDocIds(constraint_ids);
    var count: i64 = 0;
    for (doc_ids) |doc_id| {
        if (aggregationContainsDocId(constraint_ids, doc_id)) count += 1;
    }
    return count;
}

fn aggregationContainsDocId(doc_ids: []const []const u8, needle: []const u8) bool {
    for (doc_ids) |doc_id| {
        if (std.mem.eql(u8, doc_id, needle)) return true;
    }
    return false;
}

fn algebraicConstrainedDocIdsAlloc(
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    doc_ids: []const []const u8,
    constraints: []const FixedConstraint,
) !?[][]u8 {
    if (constraints.len == 0) return null;
    const constraint_ids = try index.docIdsForConstraintsAlloc(store, constraints);
    defer index.freeDocIds(constraint_ids);
    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |doc_id| index.alloc.free(doc_id);
        out.deinit(index.alloc);
    }
    for (doc_ids) |doc_id| {
        if (!aggregationContainsDocId(constraint_ids, doc_id)) continue;
        try out.append(index.alloc, try index.alloc.dupe(u8, doc_id));
    }
    return try out.toOwnedSlice(index.alloc);
}

fn algebraicDocIdMetricResultsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    requests: []const SearchAggregationRequest,
    doc_ids: []const []const u8,
) ![]SearchAggregationResult {
    var out = try alloc.alloc(SearchAggregationResult, requests.len);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |*result| result.deinit(alloc);
        alloc.free(out);
    }
    for (requests, 0..) |request, i| {
        if (std.mem.eql(u8, request.type, "cardinality")) {
            const value = (try index.exactCardinalityForDocIdsAlloc(store, request.field, doc_ids)) orelse return error.UnsupportedAggregation;
            out[i] = .{
                .name = request.name,
                .field = request.field,
                .type = request.type,
                .value_json = try std.fmt.allocPrint(alloc, "{{\"value\":{d}}}", .{value}),
            };
            filled = i + 1;
            continue;
        }
        if (std.mem.eql(u8, request.type, "stats")) {
            const resolved = index.resolveMeasureField(request.field) orelse return error.UnsupportedAggregation;
            const avg_raw = try index.rawMetricForResolvedDocIdsAlloc(store, .avg, resolved, doc_ids);
            defer if (avg_raw) |bytes| index.alloc.free(bytes);
            const min_raw = try index.rawMetricForResolvedDocIdsAlloc(store, .min, resolved, doc_ids);
            defer if (min_raw) |bytes| index.alloc.free(bytes);
            const max_raw = try index.rawMetricForResolvedDocIdsAlloc(store, .max, resolved, doc_ids);
            defer if (max_raw) |bytes| index.alloc.free(bytes);
            const sum_squares_raw = try index.rawMetricForResolvedDocIdsAlloc(store, .sumsquares, resolved, doc_ids);
            defer if (sum_squares_raw) |bytes| index.alloc.free(bytes);
            out[i] = .{
                .name = request.name,
                .field = request.field,
                .type = request.type,
                .value_json = (try algebraicStatsValueJsonAlloc(alloc, avg_raw, min_raw, max_raw, sum_squares_raw)) orelse return error.UnsupportedAggregation,
            };
            filled = i + 1;
            continue;
        }
        const op = algebraic_mod.algebra.Op.parse(request.type) orelse return error.UnsupportedAggregation;
        const raw = if (op == .count)
            try index.rawMetricForDocIdsAlloc(store, op, request.field, doc_ids)
        else blk: {
            const resolved = index.resolveMeasureField(request.field) orelse return error.UnsupportedAggregation;
            break :blk try index.rawMetricForResolvedDocIdsAlloc(store, op, resolved, doc_ids);
        };
        defer if (raw) |bytes| index.alloc.free(bytes);
        out[i] = .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .value_json = try algebraicMetricValueJsonAlloc(alloc, op, raw),
        };
        filled = i + 1;
    }
    return out;
}

fn exceedsAlgebraicResultBucketLimit(index: *const algebraic_mod.index.Index, bucket_count: usize) bool {
    const limit = index.config().max_result_buckets orelse return false;
    return bucket_count > limit;
}

fn algebraicPlanExceedsScanBudget(
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    plan: algebraic_mod.planner.MetricPlan,
    constraints: []const FixedConstraint,
) !bool {
    if (plan.materialization.group_by.len <= constraints.len) return false;
    return try algebraicMaterializationExceedsScanBudget(index, store, plan.materialization.name);
}

fn algebraicPlanEstimatedRows(
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    plan: algebraic_mod.planner.MetricPlan,
    constraints: []const FixedConstraint,
) !?usize {
    if (plan.materialization.group_by.len <= constraints.len) return 1;
    const limit = index.config().max_planner_scan_rows orelse 4096;
    const capped_limit = if (limit == std.math.maxInt(usize)) limit else limit + 1;
    return try index.countMaterializedExpressionRowsUpTo(store, plan.materialization.name, capped_limit);
}

fn algebraicMaterializationExceedsScanBudget(
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    materialization: []const u8,
) !bool {
    const limit = index.config().max_planner_scan_rows orelse return false;
    const capped_limit = if (limit == std.math.maxInt(usize)) limit else limit + 1;
    const rows = try index.countMaterializedExpressionRowsUpTo(store, materialization, capped_limit);
    return rows > limit;
}

fn recordAlgebraicBucketObservation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
    reason: []const u8,
) !void {
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    const child_metrics = algebraicChildMetricsAlloc(alloc, child_aggs.primary) catch try alloc.alloc(algebraic_mod.ir.Metric, 0);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    const bucket_name = if (std.mem.eql(u8, request.type, "date_histogram")) algebraicBucketName(request) else null;
    const time_field_name = if (std.mem.eql(u8, request.type, "date_histogram"))
        if (index.fieldConfig(request.field, .time)) |field| field.name else request.field
    else
        null;
    index.recordObservedQueryShapeWithStore(store, .{
        .kind = if (std.mem.eql(u8, request.type, "date_histogram")) .date_histogram else .terms,
        .aggregation_name = request.name,
        .bucket_field = if (std.mem.eql(u8, request.type, "terms")) request.field else null,
        .time_field = time_field_name,
        .time_bucket = bucket_name,
        .constraints = constraints,
        .child_metrics = child_metrics,
        .join = request.algebraic_join,
    }, reason);
    for (child_metrics) |child_metric| {
        index.recordObservedQueryShapeWithStore(store, .{
            .kind = if (std.mem.eql(u8, request.type, "date_histogram")) .date_histogram else .terms,
            .aggregation_name = child_metric.name,
            .bucket_field = if (std.mem.eql(u8, request.type, "terms")) request.field else null,
            .time_field = time_field_name,
            .time_bucket = bucket_name,
            .constraints = constraints,
            .metric = child_metric,
            .join = request.algebraic_join,
        }, reason);
    }
}

fn algebraicMetricValueJsonAlloc(alloc: Allocator, op: algebraic_mod.algebra.Op, raw: ?[]const u8) ![]u8 {
    switch (op) {
        .count => {
            const count = if (raw) |bytes| try algebraic_mod.algebra.parseI64(bytes) else 0;
            return try std.fmt.allocPrint(alloc, "{d}", .{count});
        },
        .sum, .sumsquares => {
            const sum = if (raw) |bytes| try algebraic_mod.algebra.parseF64(bytes) else 0;
            return try std.fmt.allocPrint(alloc, "{d}", .{sum});
        },
        .min, .max => {
            if (raw) |bytes| {
                const value = try algebraic_mod.algebra.parseF64(bytes);
                return try std.fmt.allocPrint(alloc, "{d}", .{value});
            }
            return try alloc.dupe(u8, "null");
        },
        .avg => {
            const avg = if (raw) |bytes| try algebraic_mod.algebra.parseAvg(bytes) else algebraic_mod.algebra.AvgState{};
            if (avg.count == 0) return try alloc.dupe(u8, "{\"count\":0,\"sum\":0,\"avg\":0}");
            return try std.fmt.allocPrint(alloc, "{{\"count\":{d},\"sum\":{d},\"avg\":{d}}}", .{
                avg.count,
                avg.sum,
                avg.sum / @as(f64, @floatFromInt(avg.count)),
            });
        },
    }
}

fn algebraicStatsValueJsonAlloc(
    alloc: Allocator,
    avg_raw: ?[]const u8,
    min_raw: ?[]const u8,
    max_raw: ?[]const u8,
    sum_squares_raw: ?[]const u8,
) !?[]u8 {
    const avg_state = if (avg_raw) |raw| try algebraic_mod.algebra.parseAvg(raw) else algebraic_mod.algebra.AvgState{};
    if (avg_state.count == 0) {
        return try alloc.dupe(u8, "{\"count\":0,\"sum\":0,\"avg\":0,\"min\":null,\"max\":null,\"sum_squares\":0,\"variance\":0,\"std_dev\":0}");
    }

    const min_value = if (min_raw) |raw| try algebraic_mod.algebra.parseF64(raw) else return null;
    const max_value = if (max_raw) |raw| try algebraic_mod.algebra.parseF64(raw) else return null;
    const sum_squares = if (sum_squares_raw) |raw| try algebraic_mod.algebra.parseF64(raw) else return null;
    const avg_value = avg_state.sum / @as(f64, @floatFromInt(avg_state.count));
    const variance = (sum_squares / @as(f64, @floatFromInt(avg_state.count))) - (avg_value * avg_value);
    const non_negative_variance = if (variance < 0) 0 else variance;
    return try std.fmt.allocPrint(
        alloc,
        "{{\"count\":{d},\"sum\":{d},\"avg\":{d},\"min\":{d},\"max\":{d},\"sum_squares\":{d},\"variance\":{d},\"std_dev\":{d}}}",
        .{ avg_state.count, avg_state.sum, avg_value, min_value, max_value, sum_squares, non_negative_variance, @sqrt(non_negative_variance) },
    );
}

const AlgebraicMetricRead = struct {
    raw: ?[]u8 = null,

    fn deinit(self: *AlgebraicMetricRead, alloc: Allocator) void {
        if (self.raw) |bytes| alloc.free(bytes);
        self.* = undefined;
    }
};

fn algebraicMetricReadForQueryAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    query: algebraic_mod.ir.Query,
) !?AlgebraicMetricRead {
    const plan_result = algebraic_mod.planner.planMetricQuery(index, query);
    const plan = plan_result.metric orelse {
        if (plan_result.derived_join_fold) |derived| {
            const raw = try algebraicDerivedJoinMetricRawAlloc(index, store, derived);
            defer if (raw) |bytes| index.alloc.free(bytes);
            return .{ .raw = if (raw) |bytes| try alloc.dupe(u8, bytes) else null };
        }
        if (try adaptiveMetricRawForQueryAlloc(alloc, index, store, query)) |adaptive_raw| {
            var owned_raw = adaptive_raw;
            defer owned_raw.deinit(index.alloc);
            return .{ .raw = if (owned_raw.raw) |bytes| try alloc.dupe(u8, bytes) else null };
        }
        return null;
    };
    if (try algebraicPlanExceedsScanBudget(index, store, plan, query.constraints)) return error.AlgebraicPlannerScanTooLarge;
    return .{ .raw = try algebraicMetricRawForPlanAlloc(alloc, index, store, plan, query.constraints) };
}

fn algebraicStatsMetricQuery(
    request: SearchAggregationRequest,
    op: algebraic_mod.algebra.Op,
    constraints: []const FixedConstraint,
) algebraic_mod.ir.Query {
    return .{
        .kind = .metric,
        .aggregation_name = request.name,
        .constraints = constraints,
        .metric = .{ .name = request.name, .op = op, .field = request.field },
        .join = request.algebraic_join,
    };
}

fn computeAlgebraicStatsAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?SearchAggregationResult {
    if (request.field.len == 0) return null;

    var avg_read = (try algebraicMetricReadForQueryAlloc(alloc, index, store, algebraicStatsMetricQuery(request, .avg, constraints))) orelse return null;
    defer avg_read.deinit(alloc);
    var min_read = (try algebraicMetricReadForQueryAlloc(alloc, index, store, algebraicStatsMetricQuery(request, .min, constraints))) orelse return null;
    defer min_read.deinit(alloc);
    var max_read = (try algebraicMetricReadForQueryAlloc(alloc, index, store, algebraicStatsMetricQuery(request, .max, constraints))) orelse return null;
    defer max_read.deinit(alloc);
    var sum_squares_read = (try algebraicMetricReadForQueryAlloc(alloc, index, store, algebraicStatsMetricQuery(request, .sumsquares, constraints))) orelse return null;
    defer sum_squares_read.deinit(alloc);

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .value_json = (try algebraicStatsValueJsonAlloc(alloc, avg_read.raw, min_read.raw, max_read.raw, sum_squares_read.raw)) orelse return null,
    };
}

fn planAlgebraicMetricForConstraints(
    index: *const algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
    time_field: ?[]const u8,
    bucket: ?[]const u8,
) ?algebraic_mod.planner.MetricPlan {
    const op = algebraic_mod.algebra.Op.parse(request.type) orelse return null;
    const query = algebraic_mod.ir.Query{
        .kind = .metric,
        .aggregation_name = request.name,
        .time_field = time_field,
        .time_bucket = bucket,
        .constraints = constraints,
        .metric = .{ .name = request.name, .op = op, .field = request.field },
        .join = request.algebraic_join,
    };
    return algebraic_mod.planner.planMetricQuery(index, query).metric;
}

fn planAlgebraicBucketCount(
    index: *const algebraic_mod.index.Index,
    aggregation_name: []const u8,
    constraints: []const FixedConstraint,
    bucket_group_field: ?[]const u8,
    time_field: ?[]const u8,
    bucket: ?[]const u8,
) ?algebraic_mod.planner.MetricPlan {
    const query = algebraic_mod.ir.Query{
        .kind = if (time_field != null) .date_histogram else .terms,
        .aggregation_name = aggregation_name,
        .bucket_field = bucket_group_field,
        .time_field = time_field,
        .time_bucket = bucket,
        .constraints = constraints,
        .join = null,
    };
    return algebraic_mod.planner.planBucketCountQuery(index, query).count_metric;
}

fn planAlgebraicMetricForGroupLayout(
    index: *const algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    group_layout: []const []const u8,
    time_field: ?[]const u8,
    bucket: ?[]const u8,
) ?algebraic_mod.planner.MetricPlan {
    const op = algebraic_mod.algebra.Op.parse(request.type) orelse return null;
    return algebraic_mod.planner.planMetricForGroupLayout(
        index,
        .{ .name = request.name, .op = op, .field = request.field },
        group_layout,
        time_field,
        bucket,
        null,
    );
}

fn algebraicChildMetricsAlloc(alloc: Allocator, requests: []const SearchAggregationRequest) ![]algebraic_mod.ir.Metric {
    var metric_count: usize = 0;
    for (requests) |request| {
        metric_count += algebraicChildMetricComponentCount(request);
    }

    const metrics = try alloc.alloc(algebraic_mod.ir.Metric, metric_count);
    errdefer if (metrics.len > 0) alloc.free(metrics);
    var filled: usize = 0;
    for (requests) |request| {
        if (std.mem.eql(u8, request.type, "stats")) {
            metrics[filled] = .{ .name = request.name, .op = .avg, .field = request.field };
            metrics[filled + 1] = .{ .name = request.name, .op = .min, .field = request.field };
            metrics[filled + 2] = .{ .name = request.name, .op = .max, .field = request.field };
            metrics[filled + 3] = .{ .name = request.name, .op = .sumsquares, .field = request.field };
            filled += 4;
            continue;
        }
        const op = algebraic_mod.algebra.Op.parse(request.type) orelse return error.UnsupportedAggregation;
        metrics[filled] = .{ .name = request.name, .op = op, .field = request.field };
        filled += 1;
    }
    return metrics;
}

fn algebraicChildMetricComponentCount(request: SearchAggregationRequest) usize {
    if (std.mem.eql(u8, request.type, "stats")) return 4;
    return if (algebraic_mod.algebra.Op.parse(request.type) != null) 1 else 0;
}

fn materializationMetricMatches(
    index: *const algebraic_mod.index.Index,
    mat: algebraic_mod.index.MaterializationConfig,
    op: algebraic_mod.algebra.Op,
    measure_field: []const u8,
    time_field: ?[]const u8,
    bucket: ?[]const u8,
) bool {
    const mat_op = algebraic_mod.algebra.Op.parse(mat.op) orelse return false;
    if (mat_op != op) return false;
    if (op == .count) {
        if (mat.measure != null) return false;
    } else {
        const mat_measure = mat.measure orelse return false;
        const field = index.fieldConfig(measure_field, .measure) orelse return false;
        if (!std.mem.eql(u8, mat_measure, field.name)) return false;
    }
    if (time_field) |query_time| {
        const mat_time = mat.time orelse return false;
        const field = index.fieldConfig(query_time, .time) orelse return false;
        if (!std.mem.eql(u8, mat_time, field.name)) return false;
    } else if (mat.time != null) {
        return false;
    }
    if (bucket) |query_bucket| {
        const mat_bucket = mat.bucket orelse return false;
        if (!std.mem.eql(u8, mat_bucket, query_bucket)) return false;
    } else if (mat.bucket != null) {
        return false;
    }
    return true;
}

fn materializationGroupMatchesConstraints(
    index: *const algebraic_mod.index.Index,
    mat: algebraic_mod.index.MaterializationConfig,
    constraints: []const FixedConstraint,
    bucket_group_field: ?[]const u8,
) bool {
    const expected_len = constraints.len + @intFromBool(bucket_group_field != null);
    if (mat.group_by.len != expected_len) return false;
    for (constraints) |constraint| {
        const field = index.fieldConfig(constraint.field, .group) orelse return false;
        if (fieldPosition(mat.group_by, field.name) == null) return false;
    }
    if (bucket_group_field) |bucket_field| {
        if (fieldPosition(mat.group_by, bucket_field) == null) return false;
    }
    return true;
}

fn materializationGroupCoversConstraints(
    index: *const algebraic_mod.index.Index,
    mat: algebraic_mod.index.MaterializationConfig,
    constraints: []const FixedConstraint,
    bucket_group_field: ?[]const u8,
) bool {
    if (mat.group_by.len < constraints.len + @intFromBool(bucket_group_field != null)) return false;
    for (constraints) |constraint| {
        const field = index.fieldConfig(constraint.field, .group) orelse return false;
        if (fieldPosition(mat.group_by, field.name) == null) return false;
    }
    if (bucket_group_field) |bucket_field| {
        if (fieldPosition(mat.group_by, bucket_field) == null) return false;
    }
    return true;
}

fn sameGroupLayout(lhs: []const []const u8, rhs: []const []const u8) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn algebraicMetricRawForPlanAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    plan: algebraic_mod.planner.MetricPlan,
    constraints: []const FixedConstraint,
) !?[]u8 {
    if (plan.materialization.group_by.len == constraints.len) {
        const group_key = try groupKeyForMaterializationConstraintsAlloc(alloc, index, plan.materialization, constraints);
        defer alloc.free(group_key);
        const raw = try index.rawValueAlloc(store, plan.materialization.name, group_key);
        defer if (raw) |bytes| index.alloc.free(bytes);
        return if (raw) |bytes| try alloc.dupe(u8, bytes) else null;
    }

    const entries = try index.scanMaterializedExpressionEntriesForMaterialization(store, plan.materialization.name);
    defer {
        for (entries) |*entry| entry.deinit(index.alloc);
        if (entries.len > 0) index.alloc.free(entries);
    }

    var fold = AlgebraicMetricFold.init(plan.op);
    defer fold.deinit(alloc);

    for (entries) |entry| {
        const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, entry.group_key) catch continue;
        defer {
            for (decoded) |item| alloc.free(item);
            alloc.free(decoded);
        }
        if (!decodedGroupMatchesConstraints(index, plan.materialization, decoded, constraints)) continue;
        fold.addRaw(alloc, entry.value) catch continue;
    }
    return try fold.rawAlloc(alloc);
}

fn algebraicDerivedJoinMetricRawAlloc(
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: algebraic_mod.index.DerivedJoinFoldRequest,
) !?[]u8 {
    const semantic_id = request.measure orelse request.join.name;
    const entries = (try scanDerivedJoinFoldEntriesWithProgramAlloc(index.alloc, index, store, request, semantic_id)) orelse return null;
    defer {
        for (entries) |*entry| entry.deinit(index.alloc);
        if (entries.len > 0) index.alloc.free(entries);
    }
    var fold = AlgebraicMetricFold.init(request.op);
    defer fold.deinit(index.alloc);
    for (entries) |entry| {
        fold.addRaw(index.alloc, entry.value) catch continue;
    }
    return try fold.rawAlloc(index.alloc);
}

fn scanDerivedJoinFoldEntriesWithProgramAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: algebraic_mod.index.DerivedJoinFoldRequest,
    semantic_id: []const u8,
) !?[]algebraic_mod.index.FoldEntry {
    var tensor_program = (try algebraic_mod.planner.planDerivedJoinFoldTensorProgramAlloc(alloc, request, semantic_id)) orelse return null;
    defer tensor_program.deinit(alloc);
    return try index.scanDerivedJoinFoldEntriesWithTensorProgram(
        store,
        request,
        tensor_program.access_paths,
        tensor_program.asProgram(),
        tensor_program.output,
    );
}

fn adaptiveMetricRawForQueryAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    query: algebraic_mod.ir.Query,
) !?algebraic_mod.index.AdaptiveRawResult {
    const recommendation = try algebraic_mod.adaptive.recommendationAlloc(alloc, query);
    defer alloc.free(recommendation);
    const fields = try adaptiveRecommendationGroupFieldsAlloc(alloc, index, query);
    defer if (fields.len > 0) alloc.free(fields);
    const group_key = try groupKeyForConstraintFieldsAlloc(alloc, index, fields, query.constraints);
    defer alloc.free(group_key);
    const metric = query.metric orelse return null;
    if (try index.adaptiveMaterializedExpressionRawValueForRecommendationAlloc(store, recommendation, group_key, null)) |expr_raw| {
        return expr_raw;
    }
    const materialization_id = (try index.readyAdaptiveMaterializationIdAlloc(store, recommendation)) orelse return null;
    defer index.alloc.free(materialization_id);
    if (!proveAdaptiveTensorRead(
        materialization_id,
        metric.op,
        adaptiveTensorOutputDims(query.kind, query.bucket_field != null or query.constraints.len > 0, query.time_field != null),
    )) return null;
    return .{ .raw = try index.adaptiveTensorRawValueAlloc(store, materialization_id, group_key, null) };
}

fn adaptiveRecommendationGroupFieldsAlloc(
    alloc: Allocator,
    index: *const algebraic_mod.index.Index,
    query: algebraic_mod.ir.Query,
) ![][]const u8 {
    const extra = @intFromBool(query.bucket_field != null);
    const fields = try alloc.alloc([]const u8, query.constraints.len + extra);
    var pos: usize = 0;
    if (query.bucket_field) |bucket_field| {
        const field = index.fieldConfig(bucket_field, .group) orelse return error.UnsupportedAggregation;
        fields[pos] = field.name;
        pos += 1;
    }
    for (query.constraints) |constraint| {
        const field = index.fieldConfig(constraint.field, .group) orelse return error.UnsupportedAggregation;
        fields[pos] = field.name;
        pos += 1;
    }
    return fields;
}

const AlgebraicMetricFold = struct {
    op: algebraic_mod.algebra.Op,
    raw_value: ?[]u8 = null,
    found: bool = false,

    fn init(op: algebraic_mod.algebra.Op) AlgebraicMetricFold {
        return .{ .op = op };
    }

    fn deinit(self: *AlgebraicMetricFold, alloc: Allocator) void {
        if (self.raw_value) |bytes| alloc.free(bytes);
        self.* = undefined;
    }

    fn addRaw(self: *AlgebraicMetricFold, alloc: Allocator, raw: []const u8) !void {
        const law_id = algebraic_mod.law.fromOp(self.op);
        const next = try algebraic_mod.tensor.mergeOneSlotValuesAlloc(alloc, law_id, self.raw_value, raw);
        if (self.raw_value) |old| alloc.free(old);
        self.raw_value = next;
        self.found = true;
    }

    fn rawAlloc(self: AlgebraicMetricFold, alloc: Allocator) !?[]u8 {
        if (!self.found) return null;
        return if (self.raw_value) |bytes| try alloc.dupe(u8, bytes) else null;
    }

    fn valueJsonAlloc(self: AlgebraicMetricFold, alloc: Allocator) ![]u8 {
        if (!self.found) return try algebraicMetricValueJsonAlloc(alloc, self.op, null);
        return try algebraicMetricValueJsonAlloc(alloc, self.op, self.raw_value);
    }
};

fn fieldPosition(fields: []const []const u8, field_name: []const u8) ?usize {
    for (fields, 0..) |field, i| {
        if (std.mem.eql(u8, field, field_name)) return i;
    }
    return null;
}

fn compositeTermsBucketKeyAlloc(
    alloc: Allocator,
    index: *const algebraic_mod.index.Index,
    group_layout: []const []const u8,
    bucket_fields: []const []const u8,
    decoded: []const []const u8,
) !?[]u8 {
    const values = try alloc.alloc([]const u8, bucket_fields.len);
    defer alloc.free(values);
    for (bucket_fields, 0..) |bucket_field_name, i| {
        const field = index.fieldConfig(bucket_field_name, .group) orelse return null;
        const pos = fieldPosition(group_layout, field.name) orelse return null;
        if (pos >= decoded.len) return null;
        values[i] = decoded[pos];
    }
    return try algebraic_mod.token.canonicalTupleAlloc(alloc, values);
}

fn compositeTermsKeyJsonAlloc(
    alloc: Allocator,
    index: *const algebraic_mod.index.Index,
    bucket_key: []const u8,
) ![]u8 {
    const decoded = try algebraic_mod.token.decodeTupleAlloc(alloc, bucket_key);
    defer {
        for (decoded) |item| alloc.free(item);
        alloc.free(decoded);
    }
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '[');
    for (decoded, 0..) |component, i| {
        if (i > 0) try out.append(alloc, ',');
        const component_json = try index.scalarTokenJsonAlloc(alloc, component);
        defer alloc.free(component_json);
        try out.appendSlice(alloc, component_json);
    }
    try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

fn optionalStringEql(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    if (lhs) |left| {
        const right = rhs orelse return false;
        return std.mem.eql(u8, left, right);
    }
    return rhs == null;
}

fn constraintValueForField(
    index: *const algebraic_mod.index.Index,
    constraints: []const FixedConstraint,
    field_name: []const u8,
) ?[]const u8 {
    for (constraints) |constraint| {
        const field = index.fieldConfig(constraint.field, .group) orelse continue;
        if (std.mem.eql(u8, field.name, field_name)) return constraint.value;
    }
    return null;
}

fn groupKeyForMaterializationConstraintsAlloc(
    alloc: Allocator,
    index: *const algebraic_mod.index.Index,
    mat: algebraic_mod.index.MaterializationConfig,
    constraints: []const FixedConstraint,
) ![]u8 {
    const values = try alloc.alloc([]const u8, mat.group_by.len);
    defer alloc.free(values);
    var owned = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned.items) |item| alloc.free(item);
        owned.deinit(alloc);
    }
    for (mat.group_by, 0..) |field_name, i| {
        const raw = constraintValueForField(index, constraints, field_name) orelse return error.UnsupportedAggregation;
        const encoded = try index.constraintTokenAlloc(alloc, field_name, raw);
        try owned.append(alloc, encoded);
        values[i] = encoded;
    }
    return try algebraic_mod.token.canonicalTupleAlloc(alloc, values);
}

fn groupKeyForConstraintFieldsAlloc(
    alloc: Allocator,
    index: *const algebraic_mod.index.Index,
    fields: []const []const u8,
    constraints: []const FixedConstraint,
) ![]u8 {
    const values = try alloc.alloc([]const u8, fields.len);
    defer alloc.free(values);
    var owned = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (owned.items) |item| alloc.free(item);
        owned.deinit(alloc);
    }
    for (fields, 0..) |field_name, i| {
        const raw = constraintValueForField(index, constraints, field_name) orelse return error.UnsupportedAggregation;
        const encoded = try index.constraintTokenAlloc(alloc, field_name, raw);
        try owned.append(alloc, encoded);
        values[i] = encoded;
    }
    return try algebraic_mod.token.canonicalTupleAlloc(alloc, values);
}

fn decodedGroupMatchesConstraints(
    index: *const algebraic_mod.index.Index,
    mat: algebraic_mod.index.MaterializationConfig,
    decoded: []const []const u8,
    constraints: []const FixedConstraint,
) bool {
    if (decoded.len != mat.group_by.len) return false;
    for (constraints) |constraint| {
        const field = index.fieldConfig(constraint.field, .group) orelse return false;
        const pos = fieldPosition(mat.group_by, field.name) orelse return false;
        const encoded = index.constraintTokenAlloc(index.alloc, field.name, constraint.value) catch return false;
        defer index.alloc.free(encoded);
        if (!std.mem.eql(u8, decoded[pos], encoded)) return false;
    }
    return true;
}

fn decodedAdaptiveGroupMatchesConstraints(
    index: *const algebraic_mod.index.Index,
    decoded: []const []const u8,
    constraints: []const FixedConstraint,
    offset: usize,
) bool {
    if (decoded.len < offset + constraints.len) return false;
    for (constraints, 0..) |constraint, i| {
        const field = index.fieldConfig(constraint.field, .group) orelse return false;
        const encoded = index.constraintTokenAlloc(index.alloc, field.name, constraint.value) catch return false;
        defer index.alloc.free(encoded);
        if (!std.mem.eql(u8, decoded[offset + i], encoded)) return false;
    }
    return true;
}

fn computeAlgebraicTermsAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?SearchAggregationResult {
    if (request.fields.len > 1) {
        return try computeAlgebraicCompositeTermsAggregation(alloc, index, store, request, constraints);
    }
    if (request.field.len == 0 or request.background_query != null) return null;
    const bucket_field = index.fieldConfig(request.field, .group) orelse {
        if (std.mem.startsWith(u8, request.field, "/")) {
            const path_child_aggs = try splitAggregationRequests(alloc, request.aggregations);
            defer path_child_aggs.deinit(alloc);
            if (termsChildAggsAllCardinality(path_child_aggs.primary)) {
                return try computeAlgebraicPathFactTermsCardinalityChildrenAggregation(alloc, index, store, request, constraints, path_child_aggs.primary);
            }
            return try computeAlgebraicPathFactTermsAggregation(alloc, index, store, request, constraints);
        }
        return null;
    };
    if (constraintValueForField(index, constraints, bucket_field.name) != null) return null;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    if (termsChildAggsAllCardinality(child_aggs.primary)) {
        return try computeAlgebraicTermsCardinalityChildrenAggregation(alloc, index, store, request, bucket_field.name, constraints, child_aggs.primary, child_aggs.pipeline);
    }
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    const query = algebraic_mod.ir.Query{
        .kind = .terms,
        .aggregation_name = request.name,
        .bucket_field = bucket_field.name,
        .constraints = constraints,
        .child_metrics = child_metrics,
        .join = request.algebraic_join,
    };
    var plan_result = try algebraic_mod.planner.planBucketQueryAlloc(alloc, index, query);
    defer plan_result.deinit(alloc);
    const count_plan = plan_result.count_metric orelse {
        if (plan_result.derived_join_fold) |derived| {
            return try computeDerivedJoinTermsAggregation(
                alloc,
                index,
                store,
                request,
                child_aggs.primary,
                derived,
                plan_result.derived_child_join_folds,
            );
        }
        return try computeAdaptiveTermsAggregation(alloc, index, store, request, bucket_field.name, constraints, child_aggs.primary, child_aggs.pipeline);
    };
    const child_plans = plan_result.child_metrics;
    const bucket_position = fieldPosition(count_plan.materialization.group_by, bucket_field.name) orelse return null;
    if (try algebraicMaterializationExceedsScanBudget(index, store, count_plan.materialization.name)) return error.AlgebraicPlannerScanTooLarge;

    var buckets_accum = std.ArrayListUnmanaged(TermsBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    var tensor_program = (try algebraic_mod.planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, query)) orelse return null;
    defer tensor_program.deinit(alloc);
    const row_names = try materializedExpressionNamesFromTensorProgramOutputsAlloc(alloc, tensor_program);
    defer if (row_names.len > 0) alloc.free(row_names);
    const rows_opt = try index.scanMaterializedExpressionRows(store, row_names);
    if (rows_opt) |rows| {
        defer {
            for (rows) |*row| row.deinit(index.alloc);
            if (rows.len > 0) index.alloc.free(rows);
        }
        for (rows) |row| {
            if (row.values.len < 1 + child_plans.len) return error.InvalidAlgebraicTensorRow;
            const count_raw = row.values[0] orelse continue;
            const count = algebraic_mod.algebra.parseI64(count_raw) catch continue;
            const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, row.group_key) catch continue;
            defer {
                for (decoded) |item| alloc.free(item);
                alloc.free(decoded);
            }
            if (decoded.len != count_plan.materialization.group_by.len) continue;
            if (!decodedGroupMatchesConstraints(index, count_plan.materialization, decoded, constraints)) continue;
            const key = decoded[bucket_position];
            const key_text = index.scalarTokenTextAlloc(alloc, key) catch continue;
            defer alloc.free(key_text);
            if (request.term_prefix.len > 0 and !std.mem.startsWith(u8, key_text, request.term_prefix)) continue;
            if (request.term_pattern.len > 0 and !(try regexMatches(alloc, request.term_pattern, key_text))) continue;

            const bucket_accum = try ensureTermsBucketAccum(alloc, &buckets_accum, key, child_plans);
            bucket_accum.count += count;
            for (child_plans, 0..) |_, child_idx| {
                if (row.values[child_idx + 1]) |bytes| try bucket_accum.child_folds[child_idx].addRaw(alloc, bytes);
            }
        }
    } else {
        const entries = try index.scanMaterializedExpressionEntriesForMaterialization(store, count_plan.materialization.name);
        defer {
            for (entries) |*entry| entry.deinit(index.alloc);
            if (entries.len > 0) index.alloc.free(entries);
        }
        for (entries) |entry| {
            const count = algebraic_mod.algebra.parseI64(entry.value) catch continue;
            const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, entry.group_key) catch continue;
            defer {
                for (decoded) |item| alloc.free(item);
                alloc.free(decoded);
            }
            if (decoded.len != count_plan.materialization.group_by.len) continue;
            if (!decodedGroupMatchesConstraints(index, count_plan.materialization, decoded, constraints)) continue;
            const key = decoded[bucket_position];
            const key_text = index.scalarTokenTextAlloc(alloc, key) catch continue;
            defer alloc.free(key_text);
            if (request.term_prefix.len > 0 and !std.mem.startsWith(u8, key_text, request.term_prefix)) continue;
            if (request.term_pattern.len > 0 and !(try regexMatches(alloc, request.term_pattern, key_text))) continue;

            const bucket_accum = try ensureTermsBucketAccum(alloc, &buckets_accum, key, child_plans);
            bucket_accum.count += count;
            for (child_plans, 0..) |child_plan, child_idx| {
                const raw = try index.rawValueAlloc(store, child_plan.materialization.name, entry.group_key);
                defer if (raw) |bytes| index.alloc.free(bytes);
                if (raw) |bytes| try bucket_accum.child_folds[child_idx].addRaw(alloc, bytes);
            }
        }
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(TermsBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }

    std.mem.sort(TermsBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: TermsBucketAccum, rhs: TermsBucketAccum) bool {
            if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            return lhs.count > rhs.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < candidates.len) @intCast(request.size) else candidates.len;
    var buckets = try alloc.alloc(SearchAggregationBucket, limit);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (candidates[0..limit], 0..) |candidate, idx| {
        const key_json = try index.scalarTokenJsonAlloc(alloc, candidate.key);
        buckets[idx] = .{
            .key_json = key_json,
            .count = candidate.count,
            .aggregations = try algebraicNestedMetricFoldResults(alloc, child_aggs.primary, candidate.child_folds),
        };
        buckets_filled = idx + 1;
    }
    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeAlgebraicCompositeTermsAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?SearchAggregationResult {
    if (request.background_query != null or request.fields.len < 2) return null;
    if (request.term_prefix.len > 0 or request.term_pattern.len > 0) return null;
    for (request.fields) |field_name| {
        const field = index.fieldConfig(field_name, .group) orelse return null;
        if (constraintValueForField(index, constraints, field.name) != null) return null;
    }
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    const query = algebraic_mod.ir.Query{
        .kind = .terms,
        .aggregation_name = request.name,
        .bucket_fields = request.fields,
        .constraints = constraints,
        .child_metrics = child_metrics,
        .join = request.algebraic_join,
    };
    var plan_result = try algebraic_mod.planner.planBucketQueryAlloc(alloc, index, query);
    defer plan_result.deinit(alloc);
    const count_plan = plan_result.count_metric orelse return null;
    const child_plans = plan_result.child_metrics;
    if (try algebraicMaterializationExceedsScanBudget(index, store, count_plan.materialization.name)) return error.AlgebraicPlannerScanTooLarge;

    var buckets_accum = std.ArrayListUnmanaged(TermsBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    var tensor_program = (try algebraic_mod.planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, query)) orelse return null;
    defer tensor_program.deinit(alloc);
    const row_names = try materializedExpressionNamesFromTensorProgramOutputsAlloc(alloc, tensor_program);
    defer if (row_names.len > 0) alloc.free(row_names);
    const rows_opt = try index.scanMaterializedExpressionRows(store, row_names);
    if (rows_opt) |rows| {
        defer {
            for (rows) |*row| row.deinit(index.alloc);
            if (rows.len > 0) index.alloc.free(rows);
        }
        for (rows) |row| {
            if (row.values.len < 1 + child_plans.len) return error.InvalidAlgebraicTensorRow;
            const count_raw = row.values[0] orelse continue;
            const count = algebraic_mod.algebra.parseI64(count_raw) catch continue;
            const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, row.group_key) catch continue;
            defer {
                for (decoded) |item| alloc.free(item);
                alloc.free(decoded);
            }
            if (decoded.len != count_plan.materialization.group_by.len) continue;
            if (!decodedGroupMatchesConstraints(index, count_plan.materialization, decoded, constraints)) continue;
            const bucket_key = (try compositeTermsBucketKeyAlloc(alloc, index, count_plan.materialization.group_by, request.fields, decoded)) orelse continue;
            defer alloc.free(bucket_key);
            const bucket_accum = try ensureTermsBucketAccum(alloc, &buckets_accum, bucket_key, child_plans);
            bucket_accum.count += count;
            for (child_plans, 0..) |_, child_idx| {
                if (row.values[child_idx + 1]) |bytes| try bucket_accum.child_folds[child_idx].addRaw(alloc, bytes);
            }
        }
    } else {
        const entries = try index.scanMaterializedExpressionEntriesForMaterialization(store, count_plan.materialization.name);
        defer {
            for (entries) |*entry| entry.deinit(index.alloc);
            if (entries.len > 0) index.alloc.free(entries);
        }
        for (entries) |entry| {
            const count = algebraic_mod.algebra.parseI64(entry.value) catch continue;
            const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, entry.group_key) catch continue;
            defer {
                for (decoded) |item| alloc.free(item);
                alloc.free(decoded);
            }
            if (decoded.len != count_plan.materialization.group_by.len) continue;
            if (!decodedGroupMatchesConstraints(index, count_plan.materialization, decoded, constraints)) continue;
            const bucket_key = (try compositeTermsBucketKeyAlloc(alloc, index, count_plan.materialization.group_by, request.fields, decoded)) orelse continue;
            defer alloc.free(bucket_key);
            const bucket_accum = try ensureTermsBucketAccum(alloc, &buckets_accum, bucket_key, child_plans);
            bucket_accum.count += count;
            for (child_plans, 0..) |child_plan, child_idx| {
                const raw = try index.rawValueAlloc(store, child_plan.materialization.name, entry.group_key);
                defer if (raw) |bytes| index.alloc.free(bytes);
                if (raw) |bytes| try bucket_accum.child_folds[child_idx].addRaw(alloc, bytes);
            }
        }
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;

    var candidates = try alloc.alloc(CompositeTermsCandidate, kept_count);
    var candidates_filled: usize = 0;
    var candidates_freed = false;
    errdefer {
        if (!candidates_freed) {
            for (candidates[0..candidates_filled]) |*candidate| candidate.deinit(alloc);
            if (candidates.len > 0) alloc.free(candidates);
        }
    }
    for (buckets_accum.items) |*bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[candidates_filled] = .{
            .key_json = try compositeTermsKeyJsonAlloc(alloc, index, bucket.key),
            .bucket = bucket,
        };
        candidates_filled += 1;
    }

    std.mem.sort(CompositeTermsCandidate, candidates, {}, struct {
        fn lessThan(_: void, lhs: CompositeTermsCandidate, rhs: CompositeTermsCandidate) bool {
            if (lhs.bucket.count == rhs.bucket.count) return std.mem.order(u8, lhs.key_json, rhs.key_json) == .lt;
            return lhs.bucket.count > rhs.bucket.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < candidates.len) @intCast(request.size) else candidates.len;
    var buckets = try alloc.alloc(SearchAggregationBucket, limit);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (candidates[0..limit], 0..) |*candidate, idx| {
        var nested = try algebraicNestedMetricFoldResults(alloc, child_aggs.primary, candidate.bucket.child_folds);
        errdefer if (nested.len > 0) deinitResults(alloc, nested);
        buckets[idx] = .{
            .key_json = candidate.key_json,
            .count = candidate.bucket.count,
            .aggregations = nested,
        };
        buckets_filled = idx + 1;
        candidate.key_json = candidate.key_json[0..0];
        nested = nested[0..0];
    }
    for (candidates[limit..]) |*candidate| candidate.deinit(alloc);
    if (candidates.len > 0) alloc.free(candidates);
    candidates_freed = true;
    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeAlgebraicPathFactTermsAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?SearchAggregationResult {
    for (constraints) |constraint| {
        if (!std.mem.startsWith(u8, constraint.field, "/")) return null;
    }

    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    const child_ops = try childOpsForMetricsAlloc(alloc, child_metrics);
    defer if (child_ops.len > 0) alloc.free(child_ops);
    const child_measure_kinds = pathFactChildMetricMeasureKindsAlloc(alloc, index, store, child_metrics) catch |err| switch (err) {
        error.UnsupportedAggregation => return null,
        else => return err,
    };
    defer if (child_measure_kinds.len > 0) alloc.free(child_measure_kinds);

    var stats = (try index.pathProfileStatsAlloc(store, request.field)) orelse return null;
    defer stats.deinit(index.alloc);

    var kinds = std.ArrayListUnmanaged(algebraic_mod.pathfact.Kind).empty;
    defer kinds.deinit(alloc);
    if (stats.profile.string_count > 0) try kinds.append(alloc, .string);
    if (stats.profile.number_count > 0) try kinds.append(alloc, .number);
    if (stats.profile.bool_count > 0) try kinds.append(alloc, .bool);
    if (stats.profile.null_count > 0) try kinds.append(alloc, .null);
    if (stats.profile.object_count > 0) try kinds.append(alloc, .object);
    if (stats.profile.array_count > 0 and stats.profile.scalarKindCount() == 0) try kinds.append(alloc, .array);
    if (kinds.items.len == 0) return null;

    var buckets_accum = std.ArrayListUnmanaged(TermsBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    for (kinds.items) |kind| {
        const fold = algebraic_mod.index.PathFactBucketFoldRequest{
            .kind = .terms,
            .op = .count,
            .bucket_path = request.field,
            .bucket_kind = kind,
            .constraints = constraints,
        };
        var plan = (try algebraic_mod.planner.planPathFactBucketFoldTensorProgramAlloc(alloc, index, fold, request.name)) orelse return null;
        defer plan.deinit(alloc);
        const entries = (try index.scanPathFactBucketFoldEntriesWithTensorProgram(store, fold, plan.access_paths, plan.asProgram(), plan.output)) orelse return null;
        defer {
            for (entries) |*entry| entry.deinit(index.alloc);
            if (entries.len > 0) index.alloc.free(entries);
        }
        for (entries) |entry| {
            const count = algebraic_mod.algebra.parseI64(entry.value) catch continue;
            var decoded = decodePathFactTermsAxisAlloc(alloc, entry.group_key) catch continue;
            defer decoded.deinit(alloc);
            if (request.term_prefix.len > 0 and (decoded.kind != .string or !std.mem.startsWith(u8, decoded.value, request.term_prefix))) continue;
            if (request.term_pattern.len > 0 and (decoded.kind != .string or !(try regexMatches(alloc, request.term_pattern, decoded.value)))) continue;
            const bucket_accum = try ensureTermsBucketAccumForOps(alloc, &buckets_accum, entry.group_key, child_ops);
            bucket_accum.count += count;
        }
    }

    for (child_metrics, 0..) |metric, child_idx| {
        const measure_path = metric.field;
        for (kinds.items) |kind| {
            const fold = algebraic_mod.index.PathFactBucketFoldRequest{
                .kind = .terms,
                .op = metric.op,
                .bucket_path = request.field,
                .bucket_kind = kind,
                .measure_path = measure_path,
                .measure_kind = child_measure_kinds[child_idx],
                .constraints = constraints,
            };
            var plan = (try algebraic_mod.planner.planPathFactBucketFoldTensorProgramAlloc(alloc, index, fold, metric.name)) orelse return null;
            defer plan.deinit(alloc);
            const entries = (try index.scanPathFactBucketFoldEntriesWithTensorProgram(store, fold, plan.access_paths, plan.asProgram(), plan.output)) orelse return null;
            defer {
                for (entries) |*entry| entry.deinit(index.alloc);
                if (entries.len > 0) index.alloc.free(entries);
            }
            for (entries) |entry| {
                const bucket_accum = findTermsBucketAccum(&buckets_accum, entry.group_key) orelse continue;
                try bucket_accum.child_folds[child_idx].addRaw(alloc, entry.value);
            }
        }
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(TermsBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }

    std.mem.sort(TermsBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: TermsBucketAccum, rhs: TermsBucketAccum) bool {
            if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            return lhs.count > rhs.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < candidates.len) @intCast(request.size) else candidates.len;
    var buckets = try alloc.alloc(SearchAggregationBucket, limit);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (candidates[0..limit], 0..) |candidate, idx| {
        var decoded = try decodePathFactTermsAxisAlloc(alloc, candidate.key);
        defer decoded.deinit(alloc);
        buckets[idx] = .{
            .key_json = try pathFactTermKeyJsonAlloc(alloc, decoded.kind, decoded.value),
            .count = candidate.count,
            .aggregations = try algebraicNestedMetricFoldResults(alloc, child_aggs.primary, candidate.child_folds),
        };
        buckets_filled = idx + 1;
    }
    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn termsChildAggsAllCardinality(requests: []const SearchAggregationRequest) bool {
    if (requests.len == 0) return false;
    for (requests) |request| {
        if (!std.mem.eql(u8, request.type, "cardinality") or request.field.len == 0) return false;
    }
    return true;
}

fn computeAlgebraicPathFactTermsCardinalityChildrenAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
    child_requests: []const SearchAggregationRequest,
) !?SearchAggregationResult {
    const children = try alloc.alloc(algebraic_mod.index.CardinalityChildRequest, child_requests.len);
    defer if (children.len > 0) alloc.free(children);
    for (child_requests, 0..) |child, i| {
        children[i] = .{
            .name = child.name,
            .field = child.field,
        };
    }

    const partials = (try index.scanDistributedTermsCardinalityPartials(store, request.name, request.field, children, constraints)) orelse return null;
    defer algebraic_mod.distributed.freePartials(index.alloc, partials);

    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    return try algebraicTermsAggregationFromDistributedPartialsAlloc(alloc, index, request, constraints, merged);
}

fn computeAlgebraicTermsCardinalityChildrenAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    bucket_field: []const u8,
    constraints: []const FixedConstraint,
    child_requests: []const SearchAggregationRequest,
    pipeline_aggs: []const SearchAggregationRequest,
) !?SearchAggregationResult {
    const entries = try index.scalarDocFactEntriesForFieldAlloc(store, .group, bucket_field);
    defer {
        for (entries) |*entry| entry.deinit(index.alloc);
        if (entries.len > 0) index.alloc.free(entries);
    }
    const constraint_ids = if (constraints.len > 0) try index.docIdsForConstraintsAlloc(store, constraints) else null;
    defer if (constraint_ids) |ids| index.freeDocIds(ids);

    var buckets_accum = std.ArrayListUnmanaged(TermsCardinalityBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    for (entries) |entry| {
        if (constraint_ids) |ids| {
            if (!aggregationContainsDocId(ids, entry.doc_id)) continue;
        }
        const key_text = index.scalarTokenTextAlloc(alloc, entry.scalar) catch continue;
        defer alloc.free(key_text);
        if (request.term_prefix.len > 0 and !std.mem.startsWith(u8, key_text, request.term_prefix)) continue;
        if (request.term_pattern.len > 0 and !(try regexMatches(alloc, request.term_pattern, key_text))) continue;
        const bucket = try ensureTermsCardinalityBucketAccum(alloc, &buckets_accum, entry.scalar);
        bucket.count += 1;
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(TermsCardinalityBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }
    std.mem.sort(TermsCardinalityBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: TermsCardinalityBucketAccum, rhs: TermsCardinalityBucketAccum) bool {
            if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            return lhs.count > rhs.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < candidates.len) @intCast(request.size) else candidates.len;
    var buckets = try alloc.alloc(SearchAggregationBucket, limit);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (candidates[0..limit], 0..) |candidate, idx| {
        const key_text = try index.scalarTokenTextAlloc(alloc, candidate.key);
        defer alloc.free(key_text);
        const child_constraints = try alloc.alloc(FixedConstraint, constraints.len + 1);
        defer alloc.free(child_constraints);
        @memcpy(child_constraints[0..constraints.len], constraints);
        child_constraints[constraints.len] = .{ .field = bucket_field, .value = key_text };

        const child_results = try alloc.alloc(SearchAggregationResult, child_requests.len);
        var child_filled: usize = 0;
        errdefer {
            for (child_results[0..child_filled]) |*child| child.deinit(alloc);
            alloc.free(child_results);
        }
        for (child_requests, 0..) |child_request, child_idx| {
            const value = (try index.exactCardinalityForFieldAlloc(store, child_request.field, child_constraints)) orelse return null;
            child_results[child_idx] = .{
                .name = child_request.name,
                .field = child_request.field,
                .type = child_request.type,
                .value_json = try std.fmt.allocPrint(alloc, "{{\"value\":{d}}}", .{value}),
            };
            child_filled = child_idx + 1;
        }

        buckets[idx] = .{
            .key_json = try index.scalarTokenJsonAlloc(alloc, candidate.key),
            .count = candidate.count,
            .aggregations = child_results,
        };
        buckets_filled = idx + 1;
    }
    try applyPipelineAggregations(alloc, pipeline_aggs, &buckets);

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn pathFactChildMetricMeasureKindsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    metrics: []const algebraic_mod.ir.Metric,
) ![]algebraic_mod.pathfact.Kind {
    const kinds = try alloc.alloc(algebraic_mod.pathfact.Kind, metrics.len);
    errdefer if (kinds.len > 0) alloc.free(kinds);
    for (metrics, 0..) |metric, i| {
        if (metric.op == .count) return error.UnsupportedAggregation;
        const field = metric.field;
        if (!std.mem.startsWith(u8, field, "/")) return error.UnsupportedAggregation;
        var stats = (try index.pathProfileStatsAlloc(store, field)) orelse return error.UnsupportedAggregation;
        defer stats.deinit(index.alloc);
        if (stats.profile.number_count > 0 and
            stats.profile.string_count == 0 and
            stats.profile.bool_count == 0 and
            stats.profile.null_count == 0 and
            stats.profile.object_count == 0 and
            stats.profile.array_count == 0)
        {
            kinds[i] = .number;
            continue;
        }
        if (stats.profile.string_count > 0 and
            stats.profile.string_numeric_parse_success_count == stats.profile.string_count and
            stats.profile.string_numeric_parse_failure_count == 0 and
            stats.profile.number_count == 0 and
            stats.profile.bool_count == 0 and
            stats.profile.null_count == 0 and
            stats.profile.object_count == 0 and
            stats.profile.array_count == 0)
        {
            kinds[i] = .string;
            continue;
        }
        return error.UnsupportedAggregation;
    }
    return kinds;
}

fn computeDerivedJoinTermsAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    child_requests: []const SearchAggregationRequest,
    derived: algebraic_mod.index.DerivedJoinFoldRequest,
    child_derived: []const algebraic_mod.index.DerivedJoinFoldRequest,
) !?SearchAggregationResult {
    if (derived.op != .count or derived.group_by.len != 1 or derived.measure != null) return null;
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_requests);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    if (child_metrics.len != child_derived.len) return null;
    const child_ops = try alloc.alloc(algebraic_mod.algebra.Op, child_derived.len);
    defer if (child_ops.len > 0) alloc.free(child_ops);
    for (child_derived, 0..) |child, i| {
        if (child.group_by.len != 1 or !std.mem.eql(u8, child.group_by[0], derived.group_by[0])) return null;
        child_ops[i] = child.op;
    }
    const entries = (try scanDerivedJoinFoldEntriesWithProgramAlloc(alloc, index, store, derived, request.name)) orelse return null;
    defer {
        for (entries) |*entry| entry.deinit(index.alloc);
        if (entries.len > 0) index.alloc.free(entries);
    }

    var buckets_accum = std.ArrayListUnmanaged(TermsBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    for (entries) |entry| {
        const count = algebraic_mod.algebra.parseI64(entry.value) catch continue;
        const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, entry.group_key) catch continue;
        defer {
            for (decoded) |item| alloc.free(item);
            alloc.free(decoded);
        }
        if (decoded.len != 1) continue;
        const key = decoded[0];
        const key_text = index.scalarTokenTextAlloc(alloc, key) catch continue;
        defer alloc.free(key_text);
        if (request.term_prefix.len > 0 and !std.mem.startsWith(u8, key_text, request.term_prefix)) continue;
        if (request.term_pattern.len > 0 and !(try regexMatches(alloc, request.term_pattern, key_text))) continue;

        const bucket_accum = try ensureTermsBucketAccumForOps(alloc, &buckets_accum, key, child_ops);
        bucket_accum.count += count;
    }
    for (child_derived, 0..) |child, child_idx| {
        const child_entries = (try scanDerivedJoinFoldEntriesWithProgramAlloc(alloc, index, store, child, child_metrics[child_idx].name)) orelse continue;
        defer {
            for (child_entries) |*entry| entry.deinit(index.alloc);
            if (child_entries.len > 0) index.alloc.free(child_entries);
        }
        for (child_entries) |entry| {
            const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, entry.group_key) catch continue;
            defer {
                for (decoded) |item| alloc.free(item);
                alloc.free(decoded);
            }
            if (decoded.len != 1) continue;
            const bucket_accum = findTermsBucketAccum(&buckets_accum, decoded[0]) orelse continue;
            try bucket_accum.child_folds[child_idx].addRaw(alloc, entry.value);
        }
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(TermsBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }

    std.mem.sort(TermsBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: TermsBucketAccum, rhs: TermsBucketAccum) bool {
            if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            return lhs.count > rhs.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < candidates.len) @intCast(request.size) else candidates.len;
    var buckets = try alloc.alloc(SearchAggregationBucket, limit);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (candidates[0..limit], 0..) |candidate, idx| {
        buckets[idx] = .{
            .key_json = try index.scalarTokenJsonAlloc(alloc, candidate.key),
            .count = candidate.count,
            .aggregations = try algebraicNestedMetricFoldResults(alloc, child_requests, candidate.child_folds),
        };
        buckets_filled = idx + 1;
    }

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

pub fn algebraicTermsAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    if (request.field.len == 0 or request.background_query != null) return null;
    const bucket_field = index.fieldConfig(request.field, .group) orelse {
        if (std.mem.startsWith(u8, request.field, "/")) {
            const path_child_aggs = try splitAggregationRequests(alloc, request.aggregations);
            defer path_child_aggs.deinit(alloc);
            if (termsChildAggsAllCardinality(path_child_aggs.primary)) {
                return try algebraicPathFactTermsCardinalityAggregationFromDistributedPartialsAlloc(alloc, index, request, path_child_aggs.primary, path_child_aggs.pipeline, merged);
            }
            return try algebraicPathFactTermsAggregationFromDistributedPartialsAlloc(alloc, index, request, merged);
        }
        return null;
    };
    if (constraintValueForField(index, constraints, bucket_field.name) != null) return null;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    if (termsChildAggsAllCardinality(child_aggs.primary)) {
        return try algebraicTermsCardinalityAggregationFromDistributedPartialsAlloc(alloc, index, request, bucket_field.name, child_aggs.primary, child_aggs.pipeline, merged);
    }
    for (child_aggs.primary) |child| {
        if (!isAlgebraicDocIdMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    var plan_result = try algebraic_mod.planner.planBucketQueryAlloc(alloc, index, .{
        .kind = .terms,
        .aggregation_name = request.name,
        .bucket_field = bucket_field.name,
        .constraints = constraints,
        .child_metrics = child_metrics,
    });
    defer plan_result.deinit(alloc);
    const count_plan = plan_result.count_metric orelse return null;
    const child_plans = plan_result.child_metrics;
    const bucket_position = fieldPosition(count_plan.materialization.group_by, bucket_field.name) orelse return null;

    var buckets_accum = std.ArrayListUnmanaged(TermsBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    for (merged.rows) |partial| {
        if (!std.mem.eql(u8, partial.metric, count_plan.materialization.name)) continue;
        const count = algebraic_mod.algebra.parseI64(partial.value) catch continue;
        const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, partial.canonical_axis) catch continue;
        defer {
            for (decoded) |item| alloc.free(item);
            alloc.free(decoded);
        }
        if (decoded.len != count_plan.materialization.group_by.len) continue;
        if (!decodedGroupMatchesConstraints(index, count_plan.materialization, decoded, constraints)) continue;
        const key = decoded[bucket_position];
        const key_text = index.scalarTokenTextAlloc(alloc, key) catch continue;
        defer alloc.free(key_text);
        if (request.term_prefix.len > 0 and !std.mem.startsWith(u8, key_text, request.term_prefix)) continue;
        if (request.term_pattern.len > 0 and !(try regexMatches(alloc, request.term_pattern, key_text))) continue;

        const bucket_accum = try ensureTermsBucketAccum(alloc, &buckets_accum, key, child_plans);
        bucket_accum.count += count;
        for (child_plans, 0..) |child_plan, child_idx| {
            const raw = distributedPartialValue(merged.rows, partial.canonical_axis, child_plan.materialization.name) orelse continue;
            try bucket_accum.child_folds[child_idx].addRaw(alloc, raw);
        }
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(TermsBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }

    std.mem.sort(TermsBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: TermsBucketAccum, rhs: TermsBucketAccum) bool {
            if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            return lhs.count > rhs.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < candidates.len) @intCast(request.size) else candidates.len;
    var buckets = try alloc.alloc(SearchAggregationBucket, limit);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (candidates[0..limit], 0..) |candidate, idx| {
        buckets[idx] = .{
            .key_json = try index.scalarTokenJsonAlloc(alloc, candidate.key),
            .count = candidate.count,
            .aggregations = try algebraicNestedMetricFoldResults(alloc, child_aggs.primary, candidate.child_folds),
        };
        buckets_filled = idx + 1;
    }

    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

pub fn algebraicTermsCardinalityAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    bucket_field: []const u8,
    child_requests: []const SearchAggregationRequest,
    pipeline_aggs: []const SearchAggregationRequest,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    if (!termsChildAggsAllCardinality(child_requests)) return null;

    var buckets_accum = std.ArrayListUnmanaged(TermsCardinalityBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    for (merged.rows) |partial| {
        if (!std.mem.eql(u8, partial.metric, request.name)) continue;
        if (partial.law_id != .count) continue;
        const count = algebraic_mod.algebra.parseI64(partial.value) catch continue;
        const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, partial.canonical_axis) catch continue;
        defer {
            for (decoded) |item| alloc.free(item);
            alloc.free(decoded);
        }
        if (decoded.len != 1) continue;
        const key_text = index.scalarTokenTextAlloc(alloc, decoded[0]) catch continue;
        defer alloc.free(key_text);
        if (request.term_prefix.len > 0 and !std.mem.startsWith(u8, key_text, request.term_prefix)) continue;
        if (request.term_pattern.len > 0 and !(try regexMatches(alloc, request.term_pattern, key_text))) continue;

        const bucket = try ensureTermsCardinalityBucketAccum(alloc, &buckets_accum, decoded[0]);
        bucket.count += count;
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(TermsCardinalityBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }
    std.mem.sort(TermsCardinalityBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: TermsCardinalityBucketAccum, rhs: TermsCardinalityBucketAccum) bool {
            if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            return lhs.count > rhs.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < candidates.len) @intCast(request.size) else candidates.len;
    var buckets = try alloc.alloc(SearchAggregationBucket, limit);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (candidates[0..limit], 0..) |candidate, idx| {
        const axis = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{candidate.key});
        defer alloc.free(axis);
        const child_results = try alloc.alloc(SearchAggregationResult, child_requests.len);
        var child_filled: usize = 0;
        errdefer {
            for (child_results[0..child_filled]) |*child| child.deinit(alloc);
            alloc.free(child_results);
        }
        for (child_requests, 0..) |child_request, child_idx| {
            var cardinality: u64 = 0;
            for (merged.rows) |row| {
                if (row.law_id != .count) continue;
                if (!std.mem.eql(u8, row.canonical_axis, axis)) continue;
                if (!(try distributedCardinalityMetricMatches(alloc, row.metric, child_request.name))) continue;
                cardinality += 1;
            }
            child_results[child_idx] = .{
                .name = child_request.name,
                .field = child_request.field,
                .type = child_request.type,
                .value_json = try std.fmt.allocPrint(alloc, "{{\"value\":{d}}}", .{cardinality}),
            };
            child_filled = child_idx + 1;
        }
        buckets[idx] = .{
            .key_json = try index.scalarTokenJsonAlloc(alloc, candidate.key),
            .count = candidate.count,
            .aggregations = child_results,
        };
        buckets_filled = idx + 1;
    }

    try applyPipelineAggregations(alloc, pipeline_aggs, &buckets);
    return .{
        .name = request.name,
        .field = bucket_field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn algebraicPathFactTermsCardinalityAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    child_requests: []const SearchAggregationRequest,
    pipeline_aggs: []const SearchAggregationRequest,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    if (!termsChildAggsAllCardinality(child_requests)) return null;

    var buckets_accum = std.ArrayListUnmanaged(TermsCardinalityBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    for (merged.rows) |partial| {
        if (!std.mem.eql(u8, partial.metric, request.name)) continue;
        if (partial.law_id != .count) continue;
        const count = algebraic_mod.algebra.parseI64(partial.value) catch continue;
        var decoded = decodePathFactTermsAxisAlloc(alloc, partial.canonical_axis) catch continue;
        defer decoded.deinit(alloc);
        if (request.term_prefix.len > 0 and (decoded.kind != .string or !std.mem.startsWith(u8, decoded.value, request.term_prefix))) continue;
        if (request.term_pattern.len > 0 and (decoded.kind != .string or !(try regexMatches(alloc, request.term_pattern, decoded.value)))) continue;

        const bucket = try ensureTermsCardinalityBucketAccum(alloc, &buckets_accum, partial.canonical_axis);
        bucket.count += count;
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(TermsCardinalityBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }
    std.mem.sort(TermsCardinalityBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: TermsCardinalityBucketAccum, rhs: TermsCardinalityBucketAccum) bool {
            if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            return lhs.count > rhs.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < candidates.len) @intCast(request.size) else candidates.len;
    var buckets = try alloc.alloc(SearchAggregationBucket, limit);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (candidates[0..limit], 0..) |candidate, idx| {
        var decoded = try decodePathFactTermsAxisAlloc(alloc, candidate.key);
        defer decoded.deinit(alloc);
        const child_results = try alloc.alloc(SearchAggregationResult, child_requests.len);
        var child_filled: usize = 0;
        errdefer {
            for (child_results[0..child_filled]) |*child| child.deinit(alloc);
            alloc.free(child_results);
        }
        for (child_requests, 0..) |child_request, child_idx| {
            var cardinality: u64 = 0;
            for (merged.rows) |row| {
                if (row.law_id != .count) continue;
                if (!std.mem.eql(u8, row.canonical_axis, candidate.key)) continue;
                if (!(try distributedCardinalityMetricMatches(alloc, row.metric, child_request.name))) continue;
                cardinality += 1;
            }
            child_results[child_idx] = .{
                .name = child_request.name,
                .field = child_request.field,
                .type = child_request.type,
                .value_json = try std.fmt.allocPrint(alloc, "{{\"value\":{d}}}", .{cardinality}),
            };
            child_filled = child_idx + 1;
        }
        buckets[idx] = .{
            .key_json = try pathFactTermKeyJsonAlloc(alloc, decoded.kind, decoded.value),
            .count = candidate.count,
            .aggregations = child_results,
        };
        buckets_filled = idx + 1;
    }

    try applyPipelineAggregations(alloc, pipeline_aggs, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn algebraicPathFactTermsAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    const child_ops = try childOpsForMetricsAlloc(alloc, child_metrics);
    defer if (child_ops.len > 0) alloc.free(child_ops);

    var buckets_accum = std.ArrayListUnmanaged(TermsBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    for (merged.rows) |partial| {
        if (!std.mem.eql(u8, partial.metric, request.name)) continue;
        if (partial.law_id != .count) continue;
        const count = algebraic_mod.algebra.parseI64(partial.value) catch continue;
        var decoded = decodePathFactTermsAxisAlloc(alloc, partial.canonical_axis) catch continue;
        defer decoded.deinit(alloc);
        if (request.term_prefix.len > 0 and (decoded.kind != .string or !std.mem.startsWith(u8, decoded.value, request.term_prefix))) continue;
        if (request.term_pattern.len > 0 and (decoded.kind != .string or !(try regexMatches(alloc, request.term_pattern, decoded.value)))) continue;

        const bucket_accum = try ensureTermsBucketAccumForOps(alloc, &buckets_accum, partial.canonical_axis, child_ops);
        bucket_accum.count += count;
        for (child_metrics, 0..) |metric, child_idx| {
            const raw = distributedPartialValueByLaw(merged.rows, partial.canonical_axis, metric.name, algebraic_mod.law.fromOp(metric.op)) orelse continue;
            try bucket_accum.child_folds[child_idx].addRaw(alloc, raw);
        }
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(TermsBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }

    std.mem.sort(TermsBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: TermsBucketAccum, rhs: TermsBucketAccum) bool {
            if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            return lhs.count > rhs.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < candidates.len) @intCast(request.size) else candidates.len;
    var buckets = try alloc.alloc(SearchAggregationBucket, limit);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (candidates[0..limit], 0..) |candidate, idx| {
        var decoded = try decodePathFactTermsAxisAlloc(alloc, candidate.key);
        defer decoded.deinit(alloc);
        buckets[idx] = .{
            .key_json = try pathFactTermKeyJsonAlloc(alloc, decoded.kind, decoded.value),
            .count = candidate.count,
            .aggregations = try algebraicNestedMetricFoldResults(alloc, child_aggs.primary, candidate.child_folds),
        };
        buckets_filled = idx + 1;
    }

    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

const OwnedPathFactTermsAxis = struct {
    kind: algebraic_mod.pathfact.Kind,
    value: []u8,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.value);
        self.* = undefined;
    }
};

fn decodePathFactTermsAxisAlloc(alloc: Allocator, axis: []const u8) !OwnedPathFactTermsAxis {
    const parts = try algebraic_mod.token.decodeTupleAlloc(alloc, axis);
    defer {
        for (parts) |part| alloc.free(part);
        alloc.free(parts);
    }
    if (parts.len != 2) return error.InvalidAlgebraicTensorExpr;
    const kind = std.meta.stringToEnum(algebraic_mod.pathfact.Kind, parts[0]) orelse return error.InvalidAlgebraicTensorExpr;
    return .{
        .kind = kind,
        .value = try alloc.dupe(u8, parts[1]),
    };
}

fn pathFactTermKeyJsonAlloc(alloc: Allocator, kind: algebraic_mod.pathfact.Kind, value: []const u8) ![]u8 {
    return switch (kind) {
        .bool => if (std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "false"))
            try alloc.dupe(u8, value)
        else
            error.InvalidAlgebraicTensorExpr,
        .number => blk: {
            _ = std.fmt.parseFloat(f64, value) catch return error.InvalidAlgebraicTensorExpr;
            break :blk try alloc.dupe(u8, value);
        },
        .string => try std.json.Stringify.valueAlloc(alloc, value, .{}),
        .null => if (value.len == 0) try alloc.dupe(u8, "null") else error.InvalidAlgebraicTensorExpr,
        .object => if (value.len == 0) try alloc.dupe(u8, "{\"kind\":\"object\",\"structural\":true}") else error.InvalidAlgebraicTensorExpr,
        .array => if (value.len == 0) try alloc.dupe(u8, "{\"kind\":\"array\",\"structural\":true}") else error.InvalidAlgebraicTensorExpr,
    };
}

pub fn algebraicDistributedTermsMaterializationsAlloc(
    alloc: Allocator,
    index: *const algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?[][]const u8 {
    if (request.field.len == 0 or request.background_query != null) return null;
    const bucket_field = index.fieldConfig(request.field, .group) orelse return null;
    if (constraintValueForField(index, constraints, bucket_field.name) != null) return null;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    var plan_result = try algebraic_mod.planner.planBucketQueryAlloc(alloc, index, .{
        .kind = .terms,
        .aggregation_name = request.name,
        .bucket_field = bucket_field.name,
        .constraints = constraints,
        .child_metrics = child_metrics,
    });
    defer plan_result.deinit(alloc);
    const count_plan = plan_result.count_metric orelse return null;
    return try materializedExpressionNamesOwnedAlloc(alloc, count_plan, plan_result.child_metrics);
}

pub fn algebraicMetricAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    const op = algebraic_mod.algebra.Op.parse(request.type) orelse return null;
    const query = algebraic_mod.ir.Query{
        .kind = .metric,
        .aggregation_name = request.name,
        .constraints = constraints,
        .metric = .{ .name = request.name, .op = op, .field = request.field },
    };
    const plan = algebraic_mod.planner.planMetricQuery(index, query).metric orelse return null;

    const raw = try algebraicDistributedMetricRawForPlanAlloc(alloc, index, plan, constraints, merged);
    defer if (raw) |bytes| alloc.free(bytes);

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .value_json = try algebraicMetricValueJsonAlloc(alloc, plan.op, raw),
    };
}

pub fn algebraicCardinalityAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    request: SearchAggregationRequest,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    if (!std.mem.eql(u8, request.type, "cardinality") or request.field.len == 0) return null;
    var count: u64 = 0;
    for (merged.rows) |row| {
        if (row.law_id != .count) continue;
        if (!(try distributedCardinalityMetricMatches(alloc, row.metric, request.name))) continue;
        count += 1;
    }
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .value_json = try std.fmt.allocPrint(alloc, "{{\"value\":{d}}}", .{count}),
    };
}

fn distributedCardinalityMetricMatches(
    alloc: Allocator,
    metric: []const u8,
    aggregation_name: []const u8,
) !bool {
    const parts = algebraic_mod.token.decodeTupleAlloc(alloc, metric) catch return false;
    defer {
        for (parts) |part| alloc.free(part);
        if (parts.len > 0) alloc.free(parts);
    }
    return parts.len == 3 and
        std.mem.eql(u8, parts[0], "cardinality:v1") and
        std.mem.eql(u8, parts[1], aggregation_name);
}

pub fn algebraicDistributedMetricMaterializationsAlloc(
    alloc: Allocator,
    index: *const algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?[][]const u8 {
    const op = algebraic_mod.algebra.Op.parse(request.type) orelse return null;
    if (request.algebraic_join != null) return null;
    const query = algebraic_mod.ir.Query{
        .kind = .metric,
        .aggregation_name = request.name,
        .constraints = constraints,
        .metric = .{ .name = request.name, .op = op, .field = request.field },
    };
    const plan = algebraic_mod.planner.planMetricQuery(index, query).metric orelse return null;
    return try materializedExpressionNamesOwnedAlloc(alloc, plan, &.{});
}

fn algebraicDistributedMetricRawForPlanAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    plan: algebraic_mod.planner.MetricPlan,
    constraints: []const FixedConstraint,
    merged: algebraic_mod.distributed.MergeSet,
) !?[]u8 {
    if (plan.materialization.group_by.len == constraints.len) {
        const group_key = try groupKeyForMaterializationConstraintsAlloc(alloc, index, plan.materialization, constraints);
        defer alloc.free(group_key);
        const value = distributedPartialValue(merged.rows, group_key, plan.materialization.name) orelse return null;
        return try alloc.dupe(u8, value);
    }

    var fold = AlgebraicMetricFold.init(plan.op);
    defer fold.deinit(alloc);
    for (merged.rows) |partial| {
        if (!std.mem.eql(u8, partial.metric, plan.materialization.name)) continue;
        const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, partial.canonical_axis) catch continue;
        defer {
            for (decoded) |item| alloc.free(item);
            alloc.free(decoded);
        }
        if (!decodedGroupMatchesConstraints(index, plan.materialization, decoded, constraints)) continue;
        try fold.addRaw(alloc, partial.value);
    }
    return try fold.rawAlloc(alloc);
}

fn algebraicStatsMetricPlan(
    index: *const algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    op: algebraic_mod.algebra.Op,
    constraints: []const FixedConstraint,
) ?algebraic_mod.planner.MetricPlan {
    return algebraic_mod.planner.planMetricQuery(index, algebraicStatsMetricQuery(request, op, constraints)).metric;
}

fn appendUniqueOwnedString(
    alloc: Allocator,
    names: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    for (names.items) |existing| {
        if (std.mem.eql(u8, existing, value)) return;
    }
    try names.append(alloc, try alloc.dupe(u8, value));
}

pub fn algebraicDistributedStatsMaterializationsAlloc(
    alloc: Allocator,
    index: *const algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?[][]const u8 {
    if (!std.mem.eql(u8, request.type, "stats")) return null;
    if (request.field.len == 0 or request.algebraic_join != null) return null;

    var names = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (names.items) |name| alloc.free(@constCast(name));
        names.deinit(alloc);
    }

    const ops = [_]algebraic_mod.algebra.Op{ .avg, .min, .max, .sumsquares };
    for (ops) |op| {
        const plan = algebraicStatsMetricPlan(index, request, op, constraints) orelse return null;
        const plan_names = try materializedExpressionNamesOwnedAlloc(alloc, plan, &.{});
        defer freeAlgebraicDistributedMaterializations(alloc, plan_names);
        for (plan_names) |name| try appendUniqueOwnedString(alloc, &names, name);
    }
    return try names.toOwnedSlice(alloc);
}

pub fn algebraicStatsAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    if (!std.mem.eql(u8, request.type, "stats")) return null;
    if (request.field.len == 0 or request.algebraic_join != null) return null;

    const avg_plan = algebraicStatsMetricPlan(index, request, .avg, constraints) orelse return null;
    const min_plan = algebraicStatsMetricPlan(index, request, .min, constraints) orelse return null;
    const max_plan = algebraicStatsMetricPlan(index, request, .max, constraints) orelse return null;
    const sum_squares_plan = algebraicStatsMetricPlan(index, request, .sumsquares, constraints) orelse return null;

    const avg_raw = try algebraicDistributedMetricRawForPlanAlloc(alloc, index, avg_plan, constraints, merged);
    defer if (avg_raw) |bytes| alloc.free(bytes);
    const min_raw = try algebraicDistributedMetricRawForPlanAlloc(alloc, index, min_plan, constraints, merged);
    defer if (min_raw) |bytes| alloc.free(bytes);
    const max_raw = try algebraicDistributedMetricRawForPlanAlloc(alloc, index, max_plan, constraints, merged);
    defer if (max_raw) |bytes| alloc.free(bytes);
    const sum_squares_raw = try algebraicDistributedMetricRawForPlanAlloc(alloc, index, sum_squares_plan, constraints, merged);
    defer if (sum_squares_raw) |bytes| alloc.free(bytes);

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .value_json = (try algebraicStatsValueJsonAlloc(alloc, avg_raw, min_raw, max_raw, sum_squares_raw)) orelse return null,
    };
}

pub fn algebraicDateHistogramAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    if (request.field.len == 0) return null;
    const bucket_name = algebraicBucketName(request) orelse return null;
    const interval = parseDateInterval(request) catch return null;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    if (termsChildAggsAllCardinality(child_aggs.primary)) {
        return try algebraicHistogramCardinalityAggregationFromDistributedPartialsAlloc(alloc, index, request, true, child_aggs.primary, child_aggs.pipeline, merged);
    }
    const time_field = index.fieldConfig(request.field, .time) orelse return null;
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    var plan_result = try algebraic_mod.planner.planBucketQueryAlloc(alloc, index, .{
        .kind = .date_histogram,
        .aggregation_name = request.name,
        .time_field = time_field.name,
        .time_bucket = bucket_name,
        .constraints = constraints,
        .child_metrics = child_metrics,
    });
    defer plan_result.deinit(alloc);
    const count_plan = plan_result.count_metric orelse return null;
    const child_plans = plan_result.child_metrics;

    var buckets_accum = std.ArrayListUnmanaged(DateBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    for (merged.rows) |partial| {
        if (!std.mem.eql(u8, partial.metric, count_plan.materialization.name)) continue;
        const count = algebraic_mod.algebra.parseI64(partial.value) catch continue;
        const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, partial.canonical_axis) catch continue;
        defer {
            for (decoded) |item| alloc.free(item);
            alloc.free(decoded);
        }
        if (decoded.len != 2) continue;
        _ = (try parseRfc3339ToNs(decoded[0])) orelse continue;
        const inner_group = algebraic_mod.token.decodeTupleAlloc(alloc, decoded[1]) catch continue;
        defer {
            for (inner_group) |item| alloc.free(item);
            alloc.free(inner_group);
        }
        if (inner_group.len != count_plan.materialization.group_by.len) continue;
        if (!decodedGroupMatchesConstraints(index, count_plan.materialization, inner_group, constraints)) continue;

        const bucket_accum = try ensureDateBucketAccum(alloc, &buckets_accum, decoded[0], child_plans);
        bucket_accum.count += count;
        for (child_plans, 0..) |child_plan, child_idx| {
            const raw = distributedPartialValue(merged.rows, partial.canonical_axis, child_plan.materialization.name) orelse continue;
            try bucket_accum.child_folds[child_idx].addRaw(alloc, raw);
        }
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(DateBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }

    std.mem.sort(DateBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: DateBucketAccum, rhs: DateBucketAccum) bool {
            return std.mem.order(u8, lhs.bucket_start, rhs.bucket_start) == .lt;
        }
    }.lessThan);

    const bucket_keys = if (request.min_doc_count == 0 and candidates.len > 0) blk: {
        const first_ns = (try parseRfc3339ToNs(candidates[0].bucket_start)) orelse return null;
        const last_ns = (try parseRfc3339ToNs(candidates[candidates.len - 1].bucket_start)) orelse return null;
        break :blk try fillDateHistogramBucketKeys(alloc, first_ns, last_ns, interval);
    } else blk: {
        const keys = try alloc.alloc(u64, candidates.len);
        errdefer alloc.free(keys);
        for (candidates, 0..) |candidate, i| {
            keys[i] = (try parseRfc3339ToNs(candidate.bucket_start)) orelse return null;
        }
        break :blk keys;
    };
    defer if (bucket_keys.len > 0) alloc.free(bucket_keys);

    var buckets = try alloc.alloc(SearchAggregationBucket, bucket_keys.len);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (bucket_keys, 0..) |bucket_key, idx| {
        const formatted = try formatRfc3339Bucket(alloc, bucket_key);
        defer alloc.free(formatted);
        var candidate_opt: ?DateBucketAccum = null;
        for (candidates) |candidate| {
            if (std.mem.eql(u8, candidate.bucket_start, formatted)) {
                candidate_opt = candidate;
                break;
            }
        }
        const nested = if (candidate_opt) |candidate|
            try algebraicNestedMetricFoldResults(alloc, child_aggs.primary, candidate.child_folds)
        else
            try alloc.alloc(SearchAggregationResult, 0);
        buckets[idx] = .{
            .key_json = try std.json.Stringify.valueAlloc(alloc, formatted, .{}),
            .count = if (candidate_opt) |candidate| candidate.count else 0,
            .aggregations = nested,
        };
        buckets_filled = idx + 1;
    }

    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

pub fn algebraicDistributedDateHistogramMaterializationsAlloc(
    alloc: Allocator,
    index: *const algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?[][]const u8 {
    if (request.field.len == 0) return null;
    const time_field = index.fieldConfig(request.field, .time) orelse return null;
    const bucket_name = algebraicBucketName(request) orelse return null;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    var plan_result = try algebraic_mod.planner.planBucketQueryAlloc(alloc, index, .{
        .kind = .date_histogram,
        .aggregation_name = request.name,
        .time_field = time_field.name,
        .time_bucket = bucket_name,
        .constraints = constraints,
        .child_metrics = child_metrics,
    });
    defer plan_result.deinit(alloc);
    const count_plan = plan_result.count_metric orelse return null;
    return try materializedExpressionNamesOwnedAlloc(alloc, count_plan, plan_result.child_metrics);
}

pub fn algebraicDateRangeAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    {
        const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
        defer child_aggs.deinit(alloc);
        if (termsChildAggsAllCardinality(child_aggs.primary)) {
            return try algebraicRangeCardinalityAggregationFromDistributedPartialsAlloc(alloc, index, request, false, child_aggs.primary, child_aggs.pipeline, merged);
        }
    }
    if (try algebraicDistributedDateRangePlanAlloc(alloc, index, request, constraints)) |plan_result_value| {
        var plan_result = plan_result_value;
        defer plan_result.deinit(alloc);
        const count_plan = plan_result.count_metric orelse return null;
        const child_plans = plan_result.child_metrics;
        const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
        defer child_aggs.deinit(alloc);
        const child_ops = try childOpsForPlansAlloc(alloc, child_plans);
        defer if (child_ops.len > 0) alloc.free(child_ops);

        var buckets_accum = std.ArrayListUnmanaged(HistogramBucketAccum).empty;
        defer {
            for (buckets_accum.items) |*item| item.deinit(alloc);
            buckets_accum.deinit(alloc);
        }

        for (merged.rows) |partial| {
            if (!std.mem.eql(u8, partial.metric, count_plan.materialization.name)) continue;
            const count = algebraic_mod.algebra.parseI64(partial.value) catch continue;
            const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, partial.canonical_axis) catch continue;
            defer {
                for (decoded) |item| alloc.free(item);
                alloc.free(decoded);
            }
            if (decoded.len != 2) continue;
            const bucket_ns = (try parseRfc3339ToNs(decoded[0])) orelse continue;
            const inner_group = algebraic_mod.token.decodeTupleAlloc(alloc, decoded[1]) catch continue;
            defer {
                for (inner_group) |item| alloc.free(item);
                alloc.free(inner_group);
            }
            if (inner_group.len != count_plan.materialization.group_by.len) continue;
            if (!decodedGroupMatchesConstraints(index, count_plan.materialization, inner_group, constraints)) continue;

            for (request.date_ranges, 0..) |range_spec, range_idx| {
                const start_ns = if (range_spec.start) |start| (try parseRfc3339ToNs(start)) orelse continue else null;
                const end_ns = if (range_spec.end) |end| (try parseRfc3339ToNs(end)) orelse continue else null;
                if (!matchesDateRangeValue(bucket_ns, start_ns, end_ns)) continue;
                const bucket_accum = try ensureHistogramBucketAccumForOps(alloc, &buckets_accum, @intCast(range_idx), child_ops);
                bucket_accum.count += count;
                for (child_plans, 0..) |child_plan, child_idx| {
                    const raw = distributedPartialValue(merged.rows, partial.canonical_axis, child_plan.materialization.name) orelse continue;
                    try bucket_accum.child_folds[child_idx].addRaw(alloc, raw);
                }
            }
        }

        if (exceedsAlgebraicResultBucketLimit(index, request.date_ranges.len)) return error.AlgebraicResultBucketLimit;
        var buckets = try alloc.alloc(SearchAggregationBucket, request.date_ranges.len);
        var buckets_filled: usize = 0;
        errdefer {
            for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
            alloc.free(buckets);
        }
        for (request.date_ranges, 0..) |range_spec, idx| {
            const candidate_opt = findHistogramBucketInSlice(buckets_accum.items, @intCast(idx));
            buckets[idx] = .{
                .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
                .count = if (candidate_opt) |candidate| candidate.count else 0,
                .aggregations = if (candidate_opt) |candidate|
                    try algebraicNestedMetricFoldResults(alloc, child_aggs.primary, candidate.child_folds)
                else
                    try emptyMetricResults(alloc, child_aggs.primary),
            };
            buckets_filled = idx + 1;
        }

        try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .buckets = buckets,
        };
    }

    return try algebraicDocFactDateRangeAggregationFromDistributedPartialsAlloc(alloc, index, request, merged);
}

pub fn algebraicDistributedDateRangeMaterializationsAlloc(
    alloc: Allocator,
    index: *const algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?[][]const u8 {
    var plan_result = (try algebraicDistributedDateRangePlanAlloc(alloc, index, request, constraints)) orelse return null;
    defer plan_result.deinit(alloc);
    const count_plan = plan_result.count_metric orelse return null;
    return try materializedExpressionNamesOwnedAlloc(alloc, count_plan, plan_result.child_metrics);
}

fn algebraicDistributedDateRangePlanAlloc(
    alloc: Allocator,
    index: *const algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?algebraic_mod.planner.PlanResult {
    if (request.field.len == 0 or request.date_ranges.len == 0 or request.ranges.len > 0 or request.distance_ranges.len > 0) return null;
    const time_field = index.fieldConfig(request.field, .time) orelse return null;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);

    for (index.config().materializations) |mat| {
        const op = algebraic_mod.algebra.Op.parse(mat.op) orelse continue;
        if (op != .count or mat.time == null or mat.bucket == null) continue;
        if (!std.mem.eql(u8, mat.time.?, time_field.name)) continue;
        const interval = dateIntervalFromBucketName(mat.bucket.?) orelse continue;
        if (!dateRangesAlignToInterval(request.date_ranges, interval)) continue;
        var plan_result = try algebraic_mod.planner.planBucketQueryAlloc(alloc, index, .{
            .kind = .date_histogram,
            .aggregation_name = request.name,
            .time_field = time_field.name,
            .time_bucket = mat.bucket.?,
            .constraints = constraints,
            .child_metrics = child_metrics,
        });
        if (plan_result.count_metric == null) {
            plan_result.deinit(alloc);
            continue;
        }
        return plan_result;
    }
    return null;
}

fn dateIntervalFromBucketName(bucket: []const u8) ?search_agg_mod.DateInterval {
    if (std.mem.eql(u8, bucket, "minute") or std.mem.eql(u8, bucket, "1m")) return .minute;
    if (std.mem.eql(u8, bucket, "hour") or std.mem.eql(u8, bucket, "1h") or std.mem.eql(u8, bucket, "60m")) return .hour;
    if (std.mem.eql(u8, bucket, "day") or std.mem.eql(u8, bucket, "1d") or std.mem.eql(u8, bucket, "24h")) return .day;
    if (std.mem.eql(u8, bucket, "week") or std.mem.eql(u8, bucket, "1w")) return .week;
    if (std.mem.eql(u8, bucket, "month") or std.mem.eql(u8, bucket, "1M")) return .month;
    if (std.mem.eql(u8, bucket, "year") or std.mem.eql(u8, bucket, "1y")) return .year;
    return null;
}

fn dateRangesAlignToInterval(ranges: []const DateRangeRequest, interval: search_agg_mod.DateInterval) bool {
    for (ranges) |range| {
        if (range.start) |start| {
            const ns = (parseRfc3339ToNs(start) catch return false) orelse return false;
            if (search_agg_mod.truncateToInterval(ns, interval) != ns) return false;
        }
        if (range.end) |end| {
            const ns = (parseRfc3339ToNs(end) catch return false) orelse return false;
            if (search_agg_mod.truncateToInterval(ns, interval) != ns) return false;
        }
    }
    return true;
}

pub fn algebraicHistogramAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    if (request.field.len == 0 or request.interval <= 0) return null;
    {
        const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
        defer child_aggs.deinit(alloc);
        if (termsChildAggsAllCardinality(child_aggs.primary)) {
            return try algebraicHistogramCardinalityAggregationFromDistributedPartialsAlloc(alloc, index, request, false, child_aggs.primary, child_aggs.pipeline, merged);
        }
    }
    const bucket_field = index.fieldConfig(request.field, .group) orelse {
        if (index.fieldConfig(request.field, .measure) != null) {
            return try algebraicDocFactHistogramAggregationFromDistributedPartialsAlloc(alloc, index, request, merged);
        }
        if (std.mem.startsWith(u8, request.field, "/")) {
            return try algebraicDocFactHistogramAggregationFromDistributedPartialsAlloc(alloc, index, request, merged);
        }
        return null;
    };
    const bucket_kind = algebraic_mod.value.kindFromFieldType(bucket_field.type);
    if (bucket_kind != .number and bucket_kind != .integer) return null;
    if (constraintValueForField(index, constraints, bucket_field.name) != null) return null;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    var plan_result = try algebraic_mod.planner.planBucketQueryAlloc(alloc, index, .{
        .kind = .terms,
        .aggregation_name = request.name,
        .bucket_field = bucket_field.name,
        .constraints = constraints,
        .child_metrics = child_metrics,
    });
    defer plan_result.deinit(alloc);
    const count_plan = plan_result.count_metric orelse return null;
    const child_plans = plan_result.child_metrics;
    const bucket_position = fieldPosition(count_plan.materialization.group_by, bucket_field.name) orelse return null;
    const child_ops = try childOpsForPlansAlloc(alloc, child_plans);
    defer if (child_ops.len > 0) alloc.free(child_ops);

    var buckets_accum = std.ArrayListUnmanaged(HistogramBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    for (merged.rows) |partial| {
        if (!std.mem.eql(u8, partial.metric, count_plan.materialization.name)) continue;
        const count = algebraic_mod.algebra.parseI64(partial.value) catch continue;
        const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, partial.canonical_axis) catch continue;
        defer {
            for (decoded) |item| alloc.free(item);
            alloc.free(decoded);
        }
        if (decoded.len != count_plan.materialization.group_by.len) continue;
        if (!decodedGroupMatchesConstraints(index, count_plan.materialization, decoded, constraints)) continue;
        const key_text = index.scalarTokenTextAlloc(alloc, decoded[bucket_position]) catch continue;
        defer alloc.free(key_text);
        const numeric = std.fmt.parseFloat(f64, key_text) catch continue;
        const bucket_index: i64 = @intFromFloat(@floor(numeric / request.interval));
        const bucket_accum = try ensureHistogramBucketAccumForOps(alloc, &buckets_accum, bucket_index, child_ops);
        bucket_accum.count += count;
        for (child_plans, 0..) |child_plan, child_idx| {
            const raw = distributedPartialValue(merged.rows, partial.canonical_axis, child_plan.materialization.name) orelse continue;
            try bucket_accum.child_folds[child_idx].addRaw(alloc, raw);
        }
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(HistogramBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }

    std.mem.sort(HistogramBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: HistogramBucketAccum, rhs: HistogramBucketAccum) bool {
            return lhs.bucket_index < rhs.bucket_index;
        }
    }.lessThan);

    const keys = if (request.min_doc_count == 0 and candidates.len > 0)
        try fillHistogramBucketKeys(alloc, candidates[0].bucket_index, candidates[candidates.len - 1].bucket_index)
    else blk: {
        const owned = try alloc.alloc(i64, candidates.len);
        errdefer alloc.free(owned);
        for (candidates, 0..) |candidate, i| owned[i] = candidate.bucket_index;
        break :blk owned;
    };
    defer if (keys.len > 0) alloc.free(keys);
    if (exceedsAlgebraicResultBucketLimit(index, keys.len)) return error.AlgebraicResultBucketLimit;

    var buckets = try alloc.alloc(SearchAggregationBucket, keys.len);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (keys, 0..) |bucket_index, i| {
        const candidate_opt = findHistogramBucketInSlice(candidates, bucket_index);
        const nested = if (candidate_opt) |candidate|
            try algebraicNestedMetricFoldResults(alloc, child_aggs.primary, candidate.child_folds)
        else
            try emptyMetricResults(alloc, child_aggs.primary);
        buckets[i] = .{
            .key_json = try std.fmt.allocPrint(alloc, "{d}", .{@as(f64, @floatFromInt(bucket_index)) * request.interval}),
            .count = if (candidate_opt) |candidate| candidate.count else 0,
            .aggregations = nested,
        };
        buckets_filled = i + 1;
    }

    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn algebraicDocFactHistogramAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    const child_ops = try childOpsForMetricsAlloc(alloc, child_metrics);
    defer if (child_ops.len > 0) alloc.free(child_ops);

    var buckets_accum = std.ArrayListUnmanaged(HistogramBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    for (merged.rows) |partial| {
        if (!std.mem.eql(u8, partial.metric, request.name)) continue;
        if (partial.law_id != .count) continue;
        const count = algebraic_mod.algebra.parseI64(partial.value) catch continue;
        const bucket_index = std.fmt.parseInt(i64, partial.canonical_axis, 10) catch continue;
        const bucket_accum = try ensureHistogramBucketAccumForOps(alloc, &buckets_accum, bucket_index, child_ops);
        bucket_accum.count += count;
    }
    for (child_metrics, 0..) |metric, child_idx| {
        const law_id = algebraic_mod.law.fromOp(metric.op);
        for (merged.rows) |partial| {
            if (!std.mem.eql(u8, partial.metric, metric.name)) continue;
            if (partial.law_id != law_id) continue;
            const bucket_index = std.fmt.parseInt(i64, partial.canonical_axis, 10) catch continue;
            const bucket_accum = try ensureHistogramBucketAccumForOps(alloc, &buckets_accum, bucket_index, child_ops);
            try bucket_accum.child_folds[child_idx].addRaw(alloc, partial.value);
        }
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(HistogramBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }

    std.mem.sort(HistogramBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: HistogramBucketAccum, rhs: HistogramBucketAccum) bool {
            return lhs.bucket_index < rhs.bucket_index;
        }
    }.lessThan);

    const keys = if (request.min_doc_count == 0 and candidates.len > 0)
        try fillHistogramBucketKeys(alloc, candidates[0].bucket_index, candidates[candidates.len - 1].bucket_index)
    else blk: {
        const owned = try alloc.alloc(i64, candidates.len);
        errdefer alloc.free(owned);
        for (candidates, 0..) |candidate, i| owned[i] = candidate.bucket_index;
        break :blk owned;
    };
    defer if (keys.len > 0) alloc.free(keys);
    if (exceedsAlgebraicResultBucketLimit(index, keys.len)) return error.AlgebraicResultBucketLimit;

    var buckets = try alloc.alloc(SearchAggregationBucket, keys.len);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (keys, 0..) |bucket_index, i| {
        const candidate_opt = findHistogramBucketInSlice(candidates, bucket_index);
        buckets[i] = .{
            .key_json = try std.fmt.allocPrint(alloc, "{d}", .{@as(f64, @floatFromInt(bucket_index)) * request.interval}),
            .count = if (candidate_opt) |candidate| candidate.count else 0,
            .aggregations = if (candidate_opt) |candidate|
                try algebraicNestedMetricFoldResults(alloc, child_aggs.primary, candidate.child_folds)
            else
                try emptyMetricResults(alloc, child_aggs.primary),
        };
        buckets_filled = i + 1;
    }

    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

pub fn algebraicDistributedHistogramMaterializationsAlloc(
    alloc: Allocator,
    index: *const algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?[][]const u8 {
    if (request.field.len == 0 or request.interval <= 0) return null;
    const bucket_field = index.fieldConfig(request.field, .group) orelse return null;
    const bucket_kind = algebraic_mod.value.kindFromFieldType(bucket_field.type);
    if (bucket_kind != .number and bucket_kind != .integer) return null;
    if (constraintValueForField(index, constraints, bucket_field.name) != null) return null;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    var plan_result = try algebraic_mod.planner.planBucketQueryAlloc(alloc, index, .{
        .kind = .terms,
        .aggregation_name = request.name,
        .bucket_field = bucket_field.name,
        .constraints = constraints,
        .child_metrics = child_metrics,
    });
    defer plan_result.deinit(alloc);
    const count_plan = plan_result.count_metric orelse return null;
    return try materializedExpressionNamesOwnedAlloc(alloc, count_plan, plan_result.child_metrics);
}

pub fn algebraicRangeAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    if (request.field.len == 0 or request.ranges.len == 0 or request.date_ranges.len > 0 or request.distance_ranges.len > 0) return null;
    {
        const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
        defer child_aggs.deinit(alloc);
        if (termsChildAggsAllCardinality(child_aggs.primary)) {
            return try algebraicRangeCardinalityAggregationFromDistributedPartialsAlloc(alloc, index, request, true, child_aggs.primary, child_aggs.pipeline, merged);
        }
    }
    const bucket_field = index.fieldConfig(request.field, .group) orelse {
        if (index.fieldConfig(request.field, .measure) != null) {
            return try algebraicDocFactRangeAggregationFromDistributedPartialsAlloc(alloc, index, request, merged);
        }
        if (std.mem.startsWith(u8, request.field, "/")) {
            return try algebraicDocFactRangeAggregationFromDistributedPartialsAlloc(alloc, index, request, merged);
        }
        return null;
    };
    const bucket_kind = algebraic_mod.value.kindFromFieldType(bucket_field.type);
    if (bucket_kind != .number and bucket_kind != .integer) return null;
    if (constraintValueForField(index, constraints, bucket_field.name) != null) return null;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    var plan_result = try algebraic_mod.planner.planBucketQueryAlloc(alloc, index, .{
        .kind = .terms,
        .aggregation_name = request.name,
        .bucket_field = bucket_field.name,
        .constraints = constraints,
        .child_metrics = child_metrics,
    });
    defer plan_result.deinit(alloc);
    const count_plan = plan_result.count_metric orelse return null;
    const child_plans = plan_result.child_metrics;
    const bucket_position = fieldPosition(count_plan.materialization.group_by, bucket_field.name) orelse return null;
    const child_ops = try childOpsForPlansAlloc(alloc, child_plans);
    defer if (child_ops.len > 0) alloc.free(child_ops);

    var buckets_accum = std.ArrayListUnmanaged(HistogramBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    for (merged.rows) |partial| {
        if (!std.mem.eql(u8, partial.metric, count_plan.materialization.name)) continue;
        const count = algebraic_mod.algebra.parseI64(partial.value) catch continue;
        const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, partial.canonical_axis) catch continue;
        defer {
            for (decoded) |item| alloc.free(item);
            alloc.free(decoded);
        }
        if (decoded.len != count_plan.materialization.group_by.len) continue;
        if (!decodedGroupMatchesConstraints(index, count_plan.materialization, decoded, constraints)) continue;
        const key_text = index.scalarTokenTextAlloc(alloc, decoded[bucket_position]) catch continue;
        defer alloc.free(key_text);
        const numeric = std.fmt.parseFloat(f64, key_text) catch continue;
        for (request.ranges, 0..) |range_spec, range_idx| {
            if (!matchesNumericRangeValue(numeric, range_spec)) continue;
            const bucket_accum = try ensureHistogramBucketAccumForOps(alloc, &buckets_accum, @intCast(range_idx), child_ops);
            bucket_accum.count += count;
            for (child_plans, 0..) |child_plan, child_idx| {
                const raw = distributedPartialValue(merged.rows, partial.canonical_axis, child_plan.materialization.name) orelse continue;
                try bucket_accum.child_folds[child_idx].addRaw(alloc, raw);
            }
        }
    }

    if (exceedsAlgebraicResultBucketLimit(index, request.ranges.len)) return error.AlgebraicResultBucketLimit;
    var buckets = try alloc.alloc(SearchAggregationBucket, request.ranges.len);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (request.ranges, 0..) |range_spec, idx| {
        const candidate_opt = findHistogramBucketInSlice(buckets_accum.items, @intCast(idx));
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
            .count = if (candidate_opt) |candidate| candidate.count else 0,
            .aggregations = if (candidate_opt) |candidate|
                try algebraicNestedMetricFoldResults(alloc, child_aggs.primary, candidate.child_folds)
            else
                try emptyMetricResults(alloc, child_aggs.primary),
        };
        buckets_filled = idx + 1;
    }

    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn algebraicDocFactRangeAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    const child_ops = try childOpsForMetricsAlloc(alloc, child_metrics);
    defer if (child_ops.len > 0) alloc.free(child_ops);
    if (exceedsAlgebraicResultBucketLimit(index, request.ranges.len)) return error.AlgebraicResultBucketLimit;

    var buckets = try alloc.alloc(SearchAggregationBucket, request.ranges.len);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (request.ranges, 0..) |range_spec, idx| {
        const axis = try numericRangeAxisAlloc(alloc, range_spec);
        defer alloc.free(axis);
        const raw = distributedPartialValue(merged.rows, axis, request.name);
        const count = if (raw) |value| algebraic_mod.algebra.parseI64(value) catch 0 else 0;
        const child_folds = try alloc.alloc(AlgebraicMetricFold, child_ops.len);
        errdefer alloc.free(child_folds);
        for (child_ops, 0..) |op, child_idx| {
            child_folds[child_idx] = AlgebraicMetricFold.init(op);
            const metric = child_metrics[child_idx];
            const child_raw = distributedPartialValueByLaw(merged.rows, axis, metric.name, algebraic_mod.law.fromOp(metric.op)) orelse continue;
            try child_folds[child_idx].addRaw(alloc, child_raw);
        }
        defer {
            for (child_folds) |*fold| fold.deinit(alloc);
            if (child_folds.len > 0) alloc.free(child_folds);
        }
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
            .count = count,
            .aggregations = try algebraicNestedMetricFoldResults(alloc, child_aggs.primary, child_folds),
        };
        buckets_filled = idx + 1;
    }

    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn numericRangeAxisAlloc(alloc: Allocator, range_spec: NumericRangeRequest) ![]u8 {
    const start_text = if (range_spec.start) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else null;
    defer if (start_text) |bytes| alloc.free(bytes);
    const end_text = if (range_spec.end) |value| try std.fmt.allocPrint(alloc, "{d}", .{value}) else null;
    defer if (end_text) |bytes| alloc.free(bytes);
    return try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{ start_text orelse "", end_text orelse "" });
}

fn algebraicDocFactDateRangeAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    if (request.field.len == 0 or request.date_ranges.len == 0 or request.ranges.len > 0 or request.distance_ranges.len > 0) return null;
    if (index.fieldConfig(request.field, .time) == null and !std.mem.startsWith(u8, request.field, "/")) return null;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    const child_ops = try childOpsForMetricsAlloc(alloc, child_metrics);
    defer if (child_ops.len > 0) alloc.free(child_ops);
    if (exceedsAlgebraicResultBucketLimit(index, request.date_ranges.len)) return error.AlgebraicResultBucketLimit;

    var buckets = try alloc.alloc(SearchAggregationBucket, request.date_ranges.len);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (request.date_ranges, 0..) |range_spec, idx| {
        const axis = try dateRangeAxisAlloc(alloc, range_spec);
        defer alloc.free(axis);
        const raw = distributedPartialValue(merged.rows, axis, request.name);
        const count = if (raw) |value| algebraic_mod.algebra.parseI64(value) catch 0 else 0;
        const child_folds = try alloc.alloc(AlgebraicMetricFold, child_ops.len);
        errdefer alloc.free(child_folds);
        for (child_ops, 0..) |op, child_idx| {
            child_folds[child_idx] = AlgebraicMetricFold.init(op);
            const metric = child_metrics[child_idx];
            const child_raw = distributedPartialValueByLaw(merged.rows, axis, metric.name, algebraic_mod.law.fromOp(metric.op)) orelse continue;
            try child_folds[child_idx].addRaw(alloc, child_raw);
        }
        defer {
            for (child_folds) |*fold| fold.deinit(alloc);
            if (child_folds.len > 0) alloc.free(child_folds);
        }
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
            .count = count,
            .aggregations = try algebraicNestedMetricFoldResults(alloc, child_aggs.primary, child_folds),
        };
        buckets_filled = idx + 1;
    }

    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn dateRangeAxisAlloc(alloc: Allocator, range_spec: DateRangeRequest) ![]u8 {
    return try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{ range_spec.start orelse "", range_spec.end orelse "" });
}

fn algebraicRangeCardinalityAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    numeric: bool,
    child_requests: []const SearchAggregationRequest,
    pipeline_aggs: []const SearchAggregationRequest,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    _ = index;
    if (!termsChildAggsAllCardinality(child_requests)) return null;
    const bucket_count = if (numeric) request.ranges.len else request.date_ranges.len;
    if (bucket_count == 0) return null;
    var buckets = try alloc.alloc(SearchAggregationBucket, bucket_count);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (0..bucket_count) |idx| {
        const axis = if (numeric)
            try numericRangeAxisAlloc(alloc, request.ranges[idx])
        else
            try dateRangeAxisAlloc(alloc, request.date_ranges[idx]);
        defer alloc.free(axis);
        const canonical_axis = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{axis});
        defer alloc.free(canonical_axis);
        const raw = distributedPartialValue(merged.rows, canonical_axis, request.name);
        const count = if (raw) |value| algebraic_mod.algebra.parseI64(value) catch 0 else 0;
        const child_results = try alloc.alloc(SearchAggregationResult, child_requests.len);
        var child_filled: usize = 0;
        errdefer {
            for (child_results[0..child_filled]) |*child| child.deinit(alloc);
            alloc.free(child_results);
        }
        for (child_requests, 0..) |child_request, child_idx| {
            var cardinality: u64 = 0;
            for (merged.rows) |row| {
                if (row.law_id != .count) continue;
                if (!std.mem.eql(u8, row.canonical_axis, canonical_axis)) continue;
                if (!(try distributedCardinalityMetricMatches(alloc, row.metric, child_request.name))) continue;
                cardinality += 1;
            }
            child_results[child_idx] = .{
                .name = child_request.name,
                .field = child_request.field,
                .type = child_request.type,
                .value_json = try std.fmt.allocPrint(alloc, "{{\"value\":{d}}}", .{cardinality}),
            };
            child_filled = child_idx + 1;
        }
        const range_name = if (numeric) request.ranges[idx].name else request.date_ranges[idx].name;
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_name}),
            .count = count,
            .aggregations = child_results,
        };
        buckets_filled = idx + 1;
    }
    try applyPipelineAggregations(alloc, pipeline_aggs, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn algebraicHistogramCardinalityAggregationFromDistributedPartialsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    date: bool,
    child_requests: []const SearchAggregationRequest,
    pipeline_aggs: []const SearchAggregationRequest,
    merged: algebraic_mod.distributed.MergeSet,
) !?SearchAggregationResult {
    if (!termsChildAggsAllCardinality(child_requests)) return null;
    const date_interval = if (date) parseDateInterval(request) catch return null else undefined;

    var buckets_accum = std.ArrayListUnmanaged(HistogramCardinalityBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    for (merged.rows) |partial| {
        if (!std.mem.eql(u8, partial.metric, request.name)) continue;
        if (partial.law_id != .count) continue;
        const count = algebraic_mod.algebra.parseI64(partial.value) catch continue;
        const axis = try distributedSingleAxisAlloc(alloc, partial.canonical_axis);
        defer alloc.free(axis);
        if (date) {
            _ = (try parseRfc3339ToNs(axis)) orelse continue;
        } else {
            _ = std.fmt.parseInt(i64, axis, 10) catch continue;
        }
        const bucket = try ensureHistogramCardinalityBucketAccum(alloc, &buckets_accum, axis);
        bucket.count += count;
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(HistogramCardinalityBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }

    if (date) {
        std.mem.sort(HistogramCardinalityBucketAccum, candidates, {}, struct {
            fn lessThan(_: void, lhs: HistogramCardinalityBucketAccum, rhs: HistogramCardinalityBucketAccum) bool {
                return std.mem.order(u8, lhs.axis, rhs.axis) == .lt;
            }
        }.lessThan);
    } else {
        std.mem.sort(HistogramCardinalityBucketAccum, candidates, {}, struct {
            fn lessThan(_: void, lhs: HistogramCardinalityBucketAccum, rhs: HistogramCardinalityBucketAccum) bool {
                const left = std.fmt.parseInt(i64, lhs.axis, 10) catch 0;
                const right = std.fmt.parseInt(i64, rhs.axis, 10) catch 0;
                return left < right;
            }
        }.lessThan);
    }

    const numeric_keys = if (!date and request.min_doc_count == 0 and candidates.len > 0) blk: {
        const first = std.fmt.parseInt(i64, candidates[0].axis, 10) catch return null;
        const last = std.fmt.parseInt(i64, candidates[candidates.len - 1].axis, 10) catch return null;
        break :blk try fillHistogramBucketKeys(alloc, first, last);
    } else try alloc.alloc(i64, 0);
    defer if (numeric_keys.len > 0) alloc.free(numeric_keys);
    const date_keys = if (date and request.min_doc_count == 0 and candidates.len > 0) blk: {
        const first_ns = (try parseRfc3339ToNs(candidates[0].axis)) orelse return null;
        const last_ns = (try parseRfc3339ToNs(candidates[candidates.len - 1].axis)) orelse return null;
        break :blk try fillDateHistogramBucketKeys(alloc, first_ns, last_ns, date_interval);
    } else try alloc.alloc(u64, 0);
    defer if (date_keys.len > 0) alloc.free(date_keys);

    const bucket_count = if (date)
        (if (date_keys.len > 0) date_keys.len else candidates.len)
    else
        (if (numeric_keys.len > 0) numeric_keys.len else candidates.len);
    if (exceedsAlgebraicResultBucketLimit(index, bucket_count)) return error.AlgebraicResultBucketLimit;

    var buckets = try alloc.alloc(SearchAggregationBucket, bucket_count);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (0..bucket_count) |i| {
        const axis = if (date) blk: {
            if (date_keys.len > 0) break :blk try formatRfc3339Bucket(alloc, date_keys[i]);
            break :blk try alloc.dupe(u8, candidates[i].axis);
        } else blk: {
            if (numeric_keys.len > 0) break :blk try std.fmt.allocPrint(alloc, "{d}", .{numeric_keys[i]});
            break :blk try alloc.dupe(u8, candidates[i].axis);
        };
        defer alloc.free(axis);
        const candidate_opt = findHistogramCardinalityBucket(candidates, axis);
        const child_results = try alloc.alloc(SearchAggregationResult, child_requests.len);
        var child_filled: usize = 0;
        errdefer {
            for (child_results[0..child_filled]) |*child| child.deinit(alloc);
            alloc.free(child_results);
        }
        const canonical_axis = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{axis});
        defer alloc.free(canonical_axis);
        for (child_requests, 0..) |child_request, child_idx| {
            var cardinality: u64 = 0;
            for (merged.rows) |row| {
                if (row.law_id != .count) continue;
                if (!std.mem.eql(u8, row.canonical_axis, canonical_axis)) continue;
                if (!(try distributedCardinalityMetricMatches(alloc, row.metric, child_request.name))) continue;
                cardinality += 1;
            }
            child_results[child_idx] = .{
                .name = child_request.name,
                .field = child_request.field,
                .type = child_request.type,
                .value_json = try std.fmt.allocPrint(alloc, "{{\"value\":{d}}}", .{cardinality}),
            };
            child_filled = child_idx + 1;
        }
        buckets[i] = .{
            .key_json = if (date)
                try std.json.Stringify.valueAlloc(alloc, axis, .{})
            else blk: {
                const bucket_index = std.fmt.parseInt(i64, axis, 10) catch 0;
                break :blk try std.fmt.allocPrint(alloc, "{d}", .{@as(f64, @floatFromInt(bucket_index)) * request.interval});
            },
            .count = if (candidate_opt) |candidate| candidate.count else 0,
            .aggregations = child_results,
        };
        buckets_filled = i + 1;
    }
    try applyPipelineAggregations(alloc, pipeline_aggs, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

const HistogramCardinalityBucketAccum = struct {
    axis: []u8,
    count: i64 = 0,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.axis);
        self.* = undefined;
    }
};

fn ensureHistogramCardinalityBucketAccum(
    alloc: Allocator,
    buckets_accum: *std.ArrayListUnmanaged(HistogramCardinalityBucketAccum),
    axis: []const u8,
) !*HistogramCardinalityBucketAccum {
    for (buckets_accum.items) |*bucket| {
        if (std.mem.eql(u8, bucket.axis, axis)) return bucket;
    }
    try buckets_accum.append(alloc, .{ .axis = try alloc.dupe(u8, axis) });
    return &buckets_accum.items[buckets_accum.items.len - 1];
}

fn findHistogramCardinalityBucket(
    candidates: []const HistogramCardinalityBucketAccum,
    axis: []const u8,
) ?HistogramCardinalityBucketAccum {
    for (candidates) |candidate| {
        if (std.mem.eql(u8, candidate.axis, axis)) return candidate;
    }
    return null;
}

fn distributedSingleAxisAlloc(alloc: Allocator, canonical_axis: []const u8) ![]u8 {
    const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, canonical_axis) catch return error.UnsupportedAggregation;
    defer {
        for (decoded) |item| alloc.free(item);
        if (decoded.len > 0) alloc.free(decoded);
    }
    if (decoded.len != 1) return error.UnsupportedAggregation;
    return try alloc.dupe(u8, decoded[0]);
}

pub fn algebraicDistributedRangeMaterializationsAlloc(
    alloc: Allocator,
    index: *const algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?[][]const u8 {
    if (request.field.len == 0 or request.ranges.len == 0 or request.date_ranges.len > 0 or request.distance_ranges.len > 0) return null;
    const bucket_field = index.fieldConfig(request.field, .group) orelse return null;
    const bucket_kind = algebraic_mod.value.kindFromFieldType(bucket_field.type);
    if (bucket_kind != .number and bucket_kind != .integer) return null;
    if (constraintValueForField(index, constraints, bucket_field.name) != null) return null;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    var plan_result = try algebraic_mod.planner.planBucketQueryAlloc(alloc, index, .{
        .kind = .terms,
        .aggregation_name = request.name,
        .bucket_field = bucket_field.name,
        .constraints = constraints,
        .child_metrics = child_metrics,
    });
    defer plan_result.deinit(alloc);
    const count_plan = plan_result.count_metric orelse return null;
    return try materializedExpressionNamesOwnedAlloc(alloc, count_plan, plan_result.child_metrics);
}

fn childOpsForPlansAlloc(
    alloc: Allocator,
    child_plans: []const algebraic_mod.planner.MetricPlan,
) ![]algebraic_mod.algebra.Op {
    const ops = try alloc.alloc(algebraic_mod.algebra.Op, child_plans.len);
    errdefer if (ops.len > 0) alloc.free(ops);
    for (child_plans, 0..) |child_plan, i| ops[i] = child_plan.op;
    return ops;
}

fn childOpsForMetricsAlloc(
    alloc: Allocator,
    child_metrics: []const algebraic_mod.ir.Metric,
) ![]algebraic_mod.algebra.Op {
    const ops = try alloc.alloc(algebraic_mod.algebra.Op, child_metrics.len);
    errdefer if (ops.len > 0) alloc.free(ops);
    for (child_metrics, 0..) |metric, i| ops[i] = metric.op;
    return ops;
}

const AdaptiveTermsParentCandidate = struct {
    bucket_key: []u8,
    group_key: []u8,
    count: i64,

    fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.bucket_key);
        alloc.free(self.group_key);
        self.* = undefined;
    }
};

const AdaptiveTermsParentCandidates = struct {
    items: []AdaptiveTermsParentCandidate,

    fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.items) |*item| item.deinit(alloc);
        if (self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }
};

fn adaptiveTermsParentCandidatesAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    entries: []const algebraic_mod.index.FoldEntry,
    constraints: []const FixedConstraint,
    request: SearchAggregationRequest,
) !AdaptiveTermsParentCandidates {
    var candidates = std.ArrayListUnmanaged(AdaptiveTermsParentCandidate).empty;
    errdefer {
        for (candidates.items) |*item| item.deinit(alloc);
        candidates.deinit(alloc);
    }
    const bucket_position: usize = 0;
    for (entries) |entry| {
        const count = algebraic_mod.algebra.parseI64(entry.value) catch continue;
        const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, entry.group_key) catch continue;
        defer {
            for (decoded) |item| alloc.free(item);
            alloc.free(decoded);
        }
        if (decoded.len != constraints.len + 1) continue;
        if (!decodedAdaptiveGroupMatchesConstraints(index, decoded, constraints, 1)) continue;
        const key = decoded[bucket_position];
        const key_text = index.scalarTokenTextAlloc(alloc, key) catch continue;
        defer alloc.free(key_text);
        if (request.term_prefix.len > 0 and !std.mem.startsWith(u8, key_text, request.term_prefix)) continue;
        if (request.term_pattern.len > 0 and !(try regexMatches(alloc, request.term_pattern, key_text))) continue;
        var bucket_key = try alloc.dupe(u8, key);
        errdefer if (bucket_key.len > 0) alloc.free(bucket_key);
        var group_key = try alloc.dupe(u8, entry.group_key);
        errdefer if (group_key.len > 0) alloc.free(group_key);
        try candidates.append(alloc, .{
            .bucket_key = bucket_key,
            .group_key = group_key,
            .count = count,
        });
        bucket_key = &.{};
        group_key = &.{};
    }
    return .{
        .items = try candidates.toOwnedSlice(alloc),
    };
}

fn computeAdaptiveTermsAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    bucket_field_name: []const u8,
    constraints: []const FixedConstraint,
    child_requests: []const SearchAggregationRequest,
    pipeline_aggs: []const SearchAggregationRequest,
) !?SearchAggregationResult {
    const query = algebraic_mod.ir.Query{
        .kind = .terms,
        .aggregation_name = request.name,
        .bucket_field = bucket_field_name,
        .constraints = constraints,
    };
    const recommendation = try algebraic_mod.adaptive.recommendationAlloc(alloc, query);
    defer alloc.free(recommendation);
    const count_materialization_id = (try index.readyAdaptiveMaterializationIdAlloc(store, recommendation)) orelse return null;
    defer index.alloc.free(count_materialization_id);
    if (!proveAdaptiveTensorRead(count_materialization_id, .count, adaptiveTensorOutputDims(.terms, true, false))) return null;
    const entries = (try index.scanAdaptiveTensorEntriesForRecommendation(store, recommendation)) orelse return null;
    defer {
        for (entries) |*entry| entry.deinit(index.alloc);
        if (entries.len > 0) index.alloc.free(entries);
    }
    var parent_candidates = try adaptiveTermsParentCandidatesAlloc(alloc, index, entries, constraints, request);
    defer parent_candidates.deinit(alloc);
    const child_adaptive = (try adaptiveChildMetricPlansAlloc(alloc, index, store, .terms, bucket_field_name, null, null, constraints, child_requests)) orelse return null;
    defer child_adaptive.deinit(alloc, index.alloc);
    const scan_budget = index.config().max_planner_scan_rows orelse 4096;
    const estimated_child_scan_rows = try estimateAdaptiveChildScanRowsUpTo(index, store, child_adaptive, scan_budget +| 1);
    const child_read_plan = adaptiveChildReadPlan(adaptiveChildReadCosts(index, parent_candidates.items.len, child_adaptive.ops.len, estimated_child_scan_rows));
    var child_entries: ?AdaptiveChildEntrySets = null;
    switch (child_read_plan) {
        .indexed_scan => {
            child_entries = (try adaptiveChildEntrySetsAlloc(alloc, index, store, child_adaptive)) orelse return null;
        },
        .point_lookup => {},
    }
    defer if (child_entries) |sets| sets.deinit(alloc, index.alloc);

    var buckets_accum = std.ArrayListUnmanaged(TermsBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }
    for (parent_candidates.items) |candidate| {
        const bucket_accum = try ensureAdaptiveTermsBucketAccum(alloc, &buckets_accum, candidate.bucket_key, child_adaptive.ops);
        bucket_accum.count += candidate.count;
        for (child_adaptive.ops, 0..) |_, child_idx| {
            if (child_entries) |sets| {
                if (sets.valueFor(child_idx, candidate.group_key)) |bytes| {
                    try bucket_accum.child_folds[child_idx].addRaw(alloc, bytes);
                }
            } else if (try adaptiveChildRawPointLookupAlloc(index, store, child_adaptive, child_idx, candidate.group_key, null)) |raw_result| {
                var owned_raw = raw_result;
                defer owned_raw.deinit(index.alloc);
                if (owned_raw.raw) |bytes| {
                    try bucket_accum.child_folds[child_idx].addRaw(alloc, bytes);
                }
            }
        }
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(TermsBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }
    std.mem.sort(TermsBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: TermsBucketAccum, rhs: TermsBucketAccum) bool {
            if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            return lhs.count > rhs.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < candidates.len) @intCast(request.size) else candidates.len;
    var buckets = try alloc.alloc(SearchAggregationBucket, limit);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (candidates[0..limit], 0..) |candidate, idx| {
        buckets[idx] = .{
            .key_json = try index.scalarTokenJsonAlloc(alloc, candidate.key),
            .count = candidate.count,
            .aggregations = try algebraicNestedMetricFoldResults(alloc, child_requests, candidate.child_folds),
        };
        buckets_filled = idx + 1;
    }
    try applyPipelineAggregations(alloc, pipeline_aggs, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn distributedPartialValue(rows: []const algebraic_mod.distributed.Merged, canonical_axis: []const u8, metric: []const u8) ?[]const u8 {
    for (rows) |row| {
        if (std.mem.eql(u8, row.metric, metric) and std.mem.eql(u8, row.canonical_axis, canonical_axis)) return row.value;
    }
    return null;
}

fn distributedPartialValueByLaw(rows: []const algebraic_mod.distributed.Merged, canonical_axis: []const u8, metric: []const u8, law_id: algebraic_mod.law.Id) ?[]const u8 {
    for (rows) |row| {
        if (row.law_id == law_id and std.mem.eql(u8, row.metric, metric) and std.mem.eql(u8, row.canonical_axis, canonical_axis)) return row.value;
    }
    return null;
}

fn materializedExpressionNamesAlloc(
    alloc: Allocator,
    count_plan: algebraic_mod.planner.MetricPlan,
    child_plans: []const algebraic_mod.planner.MetricPlan,
) ![][]const u8 {
    const names = try alloc.alloc([]const u8, child_plans.len + 1);
    names[0] = count_plan.materialization.name;
    for (child_plans, 0..) |child_plan, i| names[i + 1] = child_plan.materialization.name;
    return names;
}

fn materializedExpressionNamesFromTensorProgramOutputsAlloc(
    alloc: Allocator,
    program_plan: algebraic_mod.planner.TensorProgramQueryPlan,
) ![][]const u8 {
    const proof = try algebraic_mod.ir.tensorProgramProof(alloc, program_plan.access_paths, program_plan.asProgram());
    if (!proof.safe()) return error.UnsupportedAggregation;
    const single_output = [_]algebraic_mod.ir.TensorProgramRef{program_plan.output};
    const output_refs = if (program_plan.outputs.len > 0) program_plan.outputs else single_output[0..];
    const names = try alloc.alloc([]const u8, output_refs.len);
    errdefer if (names.len > 0) alloc.free(names);
    for (output_refs, 0..) |output_ref, i| {
        const step_idx = switch (output_ref) {
            .step => |idx| idx,
            .input => return error.UnsupportedAggregation,
        };
        if (step_idx >= program_plan.steps.len) return error.UnsupportedAggregation;
        const expr = program_plan.steps[step_idx].expr;
        if (expr.layout == null or expr.layout.? != .materialized_tensor) return error.UnsupportedAggregation;
        if (expr.fragment != .reduce and expr.fragment != .join) return error.UnsupportedAggregation;
        if (expr.law_id == null) return error.UnsupportedAggregation;
        names[i] = expr.semantic_id orelse expr.owner orelse return error.UnsupportedAggregation;
    }
    return names;
}

fn materializedExpressionNamesOwnedAlloc(
    alloc: Allocator,
    count_plan: algebraic_mod.planner.MetricPlan,
    child_plans: []const algebraic_mod.planner.MetricPlan,
) ![][]const u8 {
    const names = try alloc.alloc([]const u8, child_plans.len + 1);
    var initialized: usize = 0;
    errdefer {
        for (names[0..initialized]) |name| alloc.free(@constCast(name));
        alloc.free(names);
    }
    names[0] = try alloc.dupe(u8, count_plan.materialization.name);
    initialized = 1;
    for (child_plans, 0..) |child_plan, i| {
        names[i + 1] = try alloc.dupe(u8, child_plan.materialization.name);
        initialized = i + 2;
    }
    return names;
}

pub fn freeAlgebraicDistributedMaterializations(alloc: Allocator, names: []const []const u8) void {
    for (names) |name| alloc.free(@constCast(name));
    if (names.len > 0) alloc.free(@constCast(names));
}

fn ensureTermsBucketAccum(
    alloc: Allocator,
    buckets_accum: *std.ArrayListUnmanaged(TermsBucketAccum),
    key: []const u8,
    child_plans: []const algebraic_mod.planner.MetricPlan,
) !*TermsBucketAccum {
    for (buckets_accum.items) |*item| {
        if (std.mem.eql(u8, item.key, key)) return item;
    }
    const child_folds = try alloc.alloc(AlgebraicMetricFold, child_plans.len);
    errdefer alloc.free(child_folds);
    for (child_plans, 0..) |child_plan, i| child_folds[i] = AlgebraicMetricFold.init(child_plan.op);
    try buckets_accum.append(alloc, .{
        .key = try alloc.dupe(u8, key),
        .count = 0,
        .child_folds = child_folds,
    });
    return &buckets_accum.items[buckets_accum.items.len - 1];
}

fn ensureTermsCardinalityBucketAccum(
    alloc: Allocator,
    buckets_accum: *std.ArrayListUnmanaged(TermsCardinalityBucketAccum),
    key: []const u8,
) !*TermsCardinalityBucketAccum {
    for (buckets_accum.items) |*item| {
        if (std.mem.eql(u8, item.key, key)) return item;
    }
    try buckets_accum.append(alloc, .{
        .key = try alloc.dupe(u8, key),
        .count = 0,
    });
    return &buckets_accum.items[buckets_accum.items.len - 1];
}

fn findTermsBucketAccum(
    buckets_accum: *std.ArrayListUnmanaged(TermsBucketAccum),
    key: []const u8,
) ?*TermsBucketAccum {
    for (buckets_accum.items) |*item| {
        if (std.mem.eql(u8, item.key, key)) return item;
    }
    return null;
}

fn ensureTermsBucketAccumForOps(
    alloc: Allocator,
    buckets_accum: *std.ArrayListUnmanaged(TermsBucketAccum),
    key: []const u8,
    child_ops: []const algebraic_mod.algebra.Op,
) !*TermsBucketAccum {
    if (findTermsBucketAccum(buckets_accum, key)) |item| return item;
    const child_folds = try alloc.alloc(AlgebraicMetricFold, child_ops.len);
    errdefer alloc.free(child_folds);
    for (child_ops, 0..) |op, i| child_folds[i] = AlgebraicMetricFold.init(op);
    try buckets_accum.append(alloc, .{
        .key = try alloc.dupe(u8, key),
        .count = 0,
        .child_folds = child_folds,
    });
    return &buckets_accum.items[buckets_accum.items.len - 1];
}

fn ensureDateBucketAccum(
    alloc: Allocator,
    buckets_accum: *std.ArrayListUnmanaged(DateBucketAccum),
    bucket_start: []const u8,
    child_plans: []const algebraic_mod.planner.MetricPlan,
) !*DateBucketAccum {
    for (buckets_accum.items) |*item| {
        if (std.mem.eql(u8, item.bucket_start, bucket_start)) return item;
    }
    const child_folds = try alloc.alloc(AlgebraicMetricFold, child_plans.len);
    errdefer alloc.free(child_folds);
    for (child_plans, 0..) |child_plan, i| child_folds[i] = AlgebraicMetricFold.init(child_plan.op);
    try buckets_accum.append(alloc, .{
        .bucket_start = try alloc.dupe(u8, bucket_start),
        .count = 0,
        .child_folds = child_folds,
    });
    return &buckets_accum.items[buckets_accum.items.len - 1];
}

fn findDateBucketAccum(
    buckets_accum: *std.ArrayListUnmanaged(DateBucketAccum),
    bucket_start: []const u8,
) ?*DateBucketAccum {
    for (buckets_accum.items) |*item| {
        if (std.mem.eql(u8, item.bucket_start, bucket_start)) return item;
    }
    return null;
}

fn findHistogramBucketAccum(
    buckets_accum: *std.ArrayListUnmanaged(HistogramBucketAccum),
    bucket_index: i64,
) ?*HistogramBucketAccum {
    for (buckets_accum.items) |*item| {
        if (item.bucket_index == bucket_index) return item;
    }
    return null;
}

fn ensureHistogramBucketAccumForOps(
    alloc: Allocator,
    buckets_accum: *std.ArrayListUnmanaged(HistogramBucketAccum),
    bucket_index: i64,
    child_ops: []const algebraic_mod.algebra.Op,
) !*HistogramBucketAccum {
    if (findHistogramBucketAccum(buckets_accum, bucket_index)) |item| return item;
    const child_folds = try alloc.alloc(AlgebraicMetricFold, child_ops.len);
    errdefer alloc.free(child_folds);
    for (child_ops, 0..) |op, i| child_folds[i] = AlgebraicMetricFold.init(op);
    try buckets_accum.append(alloc, .{
        .bucket_index = bucket_index,
        .count = 0,
        .child_folds = child_folds,
    });
    return &buckets_accum.items[buckets_accum.items.len - 1];
}

fn ensureDateBucketAccumForOps(
    alloc: Allocator,
    buckets_accum: *std.ArrayListUnmanaged(DateBucketAccum),
    bucket_start: []const u8,
    child_ops: []const algebraic_mod.algebra.Op,
) !*DateBucketAccum {
    if (findDateBucketAccum(buckets_accum, bucket_start)) |item| return item;
    const child_folds = try alloc.alloc(AlgebraicMetricFold, child_ops.len);
    errdefer alloc.free(child_folds);
    for (child_ops, 0..) |op, i| child_folds[i] = AlgebraicMetricFold.init(op);
    try buckets_accum.append(alloc, .{
        .bucket_start = try alloc.dupe(u8, bucket_start),
        .count = 0,
        .child_folds = child_folds,
    });
    return &buckets_accum.items[buckets_accum.items.len - 1];
}

const AdaptiveChildMetricPlans = struct {
    materialization_ids: [][]u8,
    recommendations: [][]u8,
    ops: []algebraic_mod.algebra.Op,

    fn deinit(self: AdaptiveChildMetricPlans, alloc: Allocator, index_alloc: Allocator) void {
        for (self.materialization_ids) |materialization_id| index_alloc.free(materialization_id);
        for (self.recommendations) |recommendation| alloc.free(recommendation);
        if (self.materialization_ids.len > 0) alloc.free(self.materialization_ids);
        if (self.recommendations.len > 0) alloc.free(self.recommendations);
        if (self.ops.len > 0) alloc.free(self.ops);
    }
};

const AdaptiveChildReadCosts = struct {
    parent_candidate_count: usize,
    child_metric_count: usize,
    estimated_point_lookup_rows: usize,
    estimated_child_scan_rows: usize,
    scan_budget: usize,
};

const AdaptiveChildReadPlan = enum {
    point_lookup,
    indexed_scan,
};

fn adaptiveChildReadCosts(
    index: *const algebraic_mod.index.Index,
    parent_candidate_count: usize,
    child_metric_count: usize,
    estimated_child_scan_rows: usize,
) AdaptiveChildReadCosts {
    return .{
        .parent_candidate_count = parent_candidate_count,
        .child_metric_count = child_metric_count,
        .estimated_point_lookup_rows = saturatingMul(parent_candidate_count, child_metric_count),
        .estimated_child_scan_rows = estimated_child_scan_rows,
        .scan_budget = index.config().max_planner_scan_rows orelse 4096,
    };
}

fn estimateAdaptiveChildScanRowsUpTo(
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    plans: AdaptiveChildMetricPlans,
    limit: usize,
) !usize {
    var total: usize = 0;
    for (plans.recommendations) |recommendation| {
        const remaining = limit -| total;
        if (remaining == 0) return limit;
        const child_rows = (try index.countAdaptiveTensorRowsForRecommendationUpTo(store, recommendation, remaining)) orelse return limit;
        total = saturatingAdd(total, child_rows);
        if (total >= limit) return limit;
    }
    return total;
}

fn adaptiveChildReadPlan(
    costs: AdaptiveChildReadCosts,
) AdaptiveChildReadPlan {
    if (costs.child_metric_count == 0) return .point_lookup;
    if (costs.parent_candidate_count == 0) return .point_lookup;
    if (costs.estimated_point_lookup_rows <= costs.estimated_child_scan_rows) return .point_lookup;
    if (costs.estimated_child_scan_rows > costs.scan_budget) return .point_lookup;
    return .indexed_scan;
}

test "adaptive child read planner avoids scans over budget" {
    try std.testing.expectEqual(AdaptiveChildReadPlan.point_lookup, adaptiveChildReadPlan(.{
        .parent_candidate_count = 10,
        .child_metric_count = 2,
        .estimated_point_lookup_rows = 20,
        .estimated_child_scan_rows = 101,
        .scan_budget = 100,
    }));
    try std.testing.expectEqual(AdaptiveChildReadPlan.indexed_scan, adaptiveChildReadPlan(.{
        .parent_candidate_count = 10,
        .child_metric_count = 2,
        .estimated_point_lookup_rows = 20,
        .estimated_child_scan_rows = 19,
        .scan_budget = 100,
    }));
}

fn saturatingMul(lhs: usize, rhs: usize) usize {
    return std.math.mul(usize, lhs, rhs) catch std.math.maxInt(usize);
}

fn saturatingAdd(lhs: usize, rhs: usize) usize {
    return std.math.add(usize, lhs, rhs) catch std.math.maxInt(usize);
}

fn adaptiveChildRawPointLookupAlloc(
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    plans: AdaptiveChildMetricPlans,
    child_idx: usize,
    group_key: []const u8,
    bucket: ?[]const u8,
) !?algebraic_mod.index.AdaptiveRawResult {
    if (child_idx >= plans.materialization_ids.len or child_idx >= plans.recommendations.len) return null;
    if (try index.adaptiveMaterializedExpressionRawValueForRecommendationAlloc(
        store,
        plans.recommendations[child_idx],
        group_key,
        bucket,
    )) |expr_raw| {
        return expr_raw;
    }
    return .{
        .raw = try index.adaptiveTensorRawValueAlloc(
            store,
            plans.materialization_ids[child_idx],
            group_key,
            bucket,
        ),
    };
}

const AdaptiveChildEntrySets = struct {
    entries: [][]algebraic_mod.index.FoldEntry,
    maps: []std.StringHashMapUnmanaged([]const u8),

    fn deinit(self: AdaptiveChildEntrySets, alloc: Allocator, index_alloc: Allocator) void {
        for (self.maps) |*map| map.deinit(alloc);
        if (self.maps.len > 0) alloc.free(self.maps);
        for (self.entries) |entries| {
            for (entries) |*entry| entry.deinit(index_alloc);
            if (entries.len > 0) index_alloc.free(entries);
        }
        if (self.entries.len > 0) alloc.free(self.entries);
    }

    fn valueFor(self: AdaptiveChildEntrySets, child_idx: usize, group_key: []const u8) ?[]const u8 {
        if (child_idx >= self.entries.len) return null;
        return self.maps[child_idx].get(group_key);
    }
};

fn adaptiveChildEntrySetsAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    plans: AdaptiveChildMetricPlans,
) !?AdaptiveChildEntrySets {
    const entries = try alloc.alloc([]algebraic_mod.index.FoldEntry, plans.recommendations.len);
    var filled: usize = 0;
    errdefer {
        for (entries[0..filled]) |child_entries| {
            for (child_entries) |*entry| entry.deinit(index.alloc);
            if (child_entries.len > 0) index.alloc.free(child_entries);
        }
        if (entries.len > 0) alloc.free(entries);
    }
    const maps = try alloc.alloc(std.StringHashMapUnmanaged([]const u8), plans.recommendations.len);
    var maps_filled: usize = 0;
    for (maps) |*map| map.* = .empty;
    errdefer {
        for (maps[0..maps_filled]) |*map| map.deinit(alloc);
        if (maps.len > 0) alloc.free(maps);
    }
    for (plans.recommendations, 0..) |recommendation, i| {
        entries[i] = (try index.scanAdaptiveTensorEntriesForRecommendation(store, recommendation)) orelse {
            for (maps[0..maps_filled]) |*map| map.deinit(alloc);
            if (maps.len > 0) alloc.free(maps);
            for (entries[0..filled]) |child_entries| {
                for (child_entries) |*entry| entry.deinit(index.alloc);
                if (child_entries.len > 0) index.alloc.free(child_entries);
            }
            if (entries.len > 0) alloc.free(entries);
            return null;
        };
        filled = i + 1;
        maps_filled = i + 1;
        for (entries[i]) |entry| {
            try maps[i].put(alloc, entry.group_key, entry.value);
        }
    }
    return .{ .entries = entries, .maps = maps };
}

fn adaptiveChildMetricPlansAlloc(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    kind: algebraic_mod.ir.QueryKind,
    bucket_field: ?[]const u8,
    time_field: ?[]const u8,
    time_bucket: ?[]const u8,
    constraints: []const FixedConstraint,
    child_requests: []const SearchAggregationRequest,
) !?AdaptiveChildMetricPlans {
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_requests);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    const materialization_ids = try alloc.alloc([]u8, child_metrics.len);
    var filled: usize = 0;
    errdefer {
        for (materialization_ids[0..filled]) |materialization_id| index.alloc.free(materialization_id);
        if (materialization_ids.len > 0) alloc.free(materialization_ids);
    }
    const recommendations = try alloc.alloc([]u8, child_metrics.len);
    var recommendations_filled: usize = 0;
    errdefer {
        for (recommendations[0..recommendations_filled]) |recommendation| alloc.free(recommendation);
        if (recommendations.len > 0) alloc.free(recommendations);
    }
    const ops = try alloc.alloc(algebraic_mod.algebra.Op, child_metrics.len);
    errdefer if (ops.len > 0) alloc.free(ops);

    for (child_metrics, 0..) |child, i| {
        const op = child.op;
        ops[i] = op;
        const recommendation = try algebraic_mod.adaptive.recommendationAlloc(alloc, .{
            .kind = kind,
            .aggregation_name = child.name,
            .bucket_field = bucket_field,
            .time_field = time_field,
            .time_bucket = time_bucket,
            .constraints = constraints,
            .metric = .{ .name = child.name, .op = op, .field = child.field },
        });
        recommendations[i] = recommendation;
        recommendations_filled = i + 1;
        materialization_ids[i] = (try index.readyAdaptiveMaterializationIdAlloc(store, recommendation)) orelse {
            for (recommendations[0..recommendations_filled]) |owned_recommendation| alloc.free(owned_recommendation);
            for (materialization_ids[0..filled]) |materialization_id| index.alloc.free(materialization_id);
            if (materialization_ids.len > 0) alloc.free(materialization_ids);
            if (recommendations.len > 0) alloc.free(recommendations);
            if (ops.len > 0) alloc.free(ops);
            return null;
        };
        if (!proveAdaptiveTensorRead(
            materialization_ids[i],
            op,
            adaptiveTensorOutputDims(kind, bucket_field != null or constraints.len > 0, time_field != null),
        )) {
            index.alloc.free(materialization_ids[i]);
            for (recommendations[0..recommendations_filled]) |owned_recommendation| alloc.free(owned_recommendation);
            for (materialization_ids[0..filled]) |materialization_id| index.alloc.free(materialization_id);
            if (materialization_ids.len > 0) alloc.free(materialization_ids);
            if (recommendations.len > 0) alloc.free(recommendations);
            if (ops.len > 0) alloc.free(ops);
            return null;
        }
        filled = i + 1;
    }
    return .{ .materialization_ids = materialization_ids, .recommendations = recommendations, .ops = ops };
}

fn ensureAdaptiveTermsBucketAccum(
    alloc: Allocator,
    buckets_accum: *std.ArrayListUnmanaged(TermsBucketAccum),
    key: []const u8,
    child_ops: []const algebraic_mod.algebra.Op,
) !*TermsBucketAccum {
    for (buckets_accum.items) |*item| {
        if (std.mem.eql(u8, item.key, key)) return item;
    }
    const child_folds = try alloc.alloc(AlgebraicMetricFold, child_ops.len);
    errdefer alloc.free(child_folds);
    for (child_ops, 0..) |op, i| child_folds[i] = AlgebraicMetricFold.init(op);
    try buckets_accum.append(alloc, .{
        .key = try alloc.dupe(u8, key),
        .count = 0,
        .child_folds = child_folds,
    });
    return &buckets_accum.items[buckets_accum.items.len - 1];
}

fn ensureAdaptiveDateBucketAccum(
    alloc: Allocator,
    buckets_accum: *std.ArrayListUnmanaged(DateBucketAccum),
    bucket_start: []const u8,
    child_ops: []const algebraic_mod.algebra.Op,
) !*DateBucketAccum {
    for (buckets_accum.items) |*item| {
        if (std.mem.eql(u8, item.bucket_start, bucket_start)) return item;
    }
    const child_folds = try alloc.alloc(AlgebraicMetricFold, child_ops.len);
    errdefer alloc.free(child_folds);
    for (child_ops, 0..) |op, i| child_folds[i] = AlgebraicMetricFold.init(op);
    try buckets_accum.append(alloc, .{
        .bucket_start = try alloc.dupe(u8, bucket_start),
        .count = 0,
        .child_folds = child_folds,
    });
    return &buckets_accum.items[buckets_accum.items.len - 1];
}

fn computeDerivedJoinHistogramAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    child_requests: []const SearchAggregationRequest,
    derived: algebraic_mod.index.DerivedJoinFoldRequest,
    child_derived: []const algebraic_mod.index.DerivedJoinFoldRequest,
) !?SearchAggregationResult {
    if (derived.op != .count or derived.group_by.len != 0 or derived.measure != null) return null;
    if (derived.histogram_field == null or derived.histogram_role == null or derived.histogram_interval <= 0) return null;
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_requests);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    if (child_metrics.len != child_derived.len) return null;
    const child_ops = try alloc.alloc(algebraic_mod.algebra.Op, child_derived.len);
    defer if (child_ops.len > 0) alloc.free(child_ops);
    for (child_derived, 0..) |child, i| {
        if (child.group_by.len != 0) return null;
        if (!optionalStringEql(child.histogram_field, derived.histogram_field) or child.histogram_role != derived.histogram_role or child.histogram_interval != derived.histogram_interval) return null;
        child_ops[i] = child.op;
    }

    const entries = (try scanDerivedJoinFoldEntriesWithProgramAlloc(alloc, index, store, derived, request.name)) orelse return null;
    defer {
        for (entries) |*entry| entry.deinit(index.alloc);
        if (entries.len > 0) index.alloc.free(entries);
    }

    var buckets_accum = std.ArrayListUnmanaged(HistogramBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    for (entries) |entry| {
        const count = algebraic_mod.algebra.parseI64(entry.value) catch continue;
        const bucket_index = std.fmt.parseInt(i64, entry.group_key, 10) catch continue;
        const bucket_accum = try ensureHistogramBucketAccumForOps(alloc, &buckets_accum, bucket_index, child_ops);
        bucket_accum.count += count;
    }
    for (child_derived, 0..) |child, child_idx| {
        const child_entries = (try scanDerivedJoinFoldEntriesWithProgramAlloc(alloc, index, store, child, child_metrics[child_idx].name)) orelse continue;
        defer {
            for (child_entries) |*entry| entry.deinit(index.alloc);
            if (child_entries.len > 0) index.alloc.free(child_entries);
        }
        for (child_entries) |entry| {
            const bucket_index = std.fmt.parseInt(i64, entry.group_key, 10) catch continue;
            const bucket_accum = findHistogramBucketAccum(&buckets_accum, bucket_index) orelse continue;
            try bucket_accum.child_folds[child_idx].addRaw(alloc, entry.value);
        }
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(HistogramBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }

    std.mem.sort(HistogramBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: HistogramBucketAccum, rhs: HistogramBucketAccum) bool {
            return lhs.bucket_index < rhs.bucket_index;
        }
    }.lessThan);

    const keys = if (request.min_doc_count == 0 and candidates.len > 0)
        try fillHistogramBucketKeys(alloc, candidates[0].bucket_index, candidates[candidates.len - 1].bucket_index)
    else blk: {
        const owned = try alloc.alloc(i64, candidates.len);
        errdefer alloc.free(owned);
        for (candidates, 0..) |candidate, i| owned[i] = candidate.bucket_index;
        break :blk owned;
    };
    defer if (keys.len > 0) alloc.free(keys);
    if (exceedsAlgebraicResultBucketLimit(index, keys.len)) return error.AlgebraicResultBucketLimit;

    var buckets = try alloc.alloc(SearchAggregationBucket, keys.len);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (keys, 0..) |bucket_index, i| {
        const candidate_opt = findHistogramBucketInSlice(candidates, bucket_index);
        const nested = if (candidate_opt) |candidate|
            try algebraicNestedMetricFoldResults(alloc, child_requests, candidate.child_folds)
        else
            try emptyMetricResults(alloc, child_requests);
        buckets[i] = .{
            .key_json = try std.fmt.allocPrint(alloc, "{d}", .{@as(f64, @floatFromInt(bucket_index)) * request.interval}),
            .count = if (candidate_opt) |candidate| candidate.count else 0,
            .aggregations = nested,
        };
        buckets_filled = i + 1;
    }
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn findHistogramBucketInSlice(candidates: []const HistogramBucketAccum, bucket_index: i64) ?HistogramBucketAccum {
    for (candidates) |candidate| {
        if (candidate.bucket_index == bucket_index) return candidate;
    }
    return null;
}

fn computeAlgebraicHistogramAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?SearchAggregationResult {
    if (request.field.len == 0 or request.interval <= 0) return null;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    if (termsChildAggsAllCardinality(child_aggs.primary)) {
        const children = try alloc.alloc(algebraic_mod.index.CardinalityChildRequest, child_aggs.primary.len);
        defer if (children.len > 0) alloc.free(children);
        for (child_aggs.primary, 0..) |child, i| {
            children[i] = .{
                .name = child.name,
                .field = child.field,
            };
        }
        const partials = (try index.scanDistributedHistogramCardinalityPartials(
            store,
            request.name,
            request.field,
            .numeric,
            request.interval,
            "",
            children,
            constraints,
        )) orelse return null;
        defer algebraic_mod.distributed.freePartials(index.alloc, partials);
        var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
        defer merged.deinit(alloc);
        return try algebraicHistogramCardinalityAggregationFromDistributedPartialsAlloc(alloc, index, request, false, child_aggs.primary, child_aggs.pipeline, merged);
    }
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    if (request.algebraic_join != null) {
        const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        var plan = try algebraic_mod.planner.planBucketQueryAlloc(alloc, index, .{
            .kind = .histogram,
            .aggregation_name = request.name,
            .bucket_field = request.field,
            .bucket_interval = request.interval,
            .constraints = constraints,
            .child_metrics = child_metrics,
            .join = request.algebraic_join,
        });
        defer plan.deinit(alloc);
        if (plan.derived_join_fold) |derived| {
            return try computeDerivedJoinHistogramAggregation(
                alloc,
                index,
                store,
                request,
                child_aggs.primary,
                derived,
                plan.derived_child_join_folds,
            );
        }
        return null;
    }
    const measure_field = index.fieldConfig(request.field, .measure);
    const group_field = index.fieldConfig(request.field, .group);
    if (measure_field != null and group_field != null) return null;
    const field = measure_field orelse group_field orelse return null;
    const role: algebraic_mod.fact.Role = if (measure_field != null) .measure else .group;
    const kind = algebraic_mod.value.kindFromFieldType(field.type);
    if (kind != .number and kind != .integer) return null;

    const entries = try index.scalarDocFactEntriesForFieldAlloc(store, role, field.name);
    defer {
        for (entries) |*entry| entry.deinit(index.alloc);
        if (entries.len > 0) index.alloc.free(entries);
    }

    var grouped = std.AutoHashMap(i64, std.ArrayListUnmanaged([]const u8)).init(alloc);
    defer {
        var it_grouped = grouped.iterator();
        while (it_grouped.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }
    for (entries) |entry| {
        const scalar = algebraic_mod.value.parseScalarAlloc(alloc, entry.scalar) catch continue;
        defer algebraic_mod.value.deinitScalar(alloc, scalar);
        if (scalar.kind != .number and scalar.kind != .integer) continue;
        const numeric = std.fmt.parseFloat(f64, scalar.canonical) catch continue;
        const bucket_index: i64 = @intFromFloat(@floor(numeric / request.interval));
        const grouped_entry = try grouped.getOrPut(bucket_index);
        if (!grouped_entry.found_existing) grouped_entry.value_ptr.* = .empty;
        try grouped_entry.value_ptr.append(alloc, entry.doc_id);
    }

    var candidates = std.ArrayListUnmanaged(HistogramBucketAccum).empty;
    defer {
        for (candidates.items) |*candidate| candidate.deinit(alloc);
        candidates.deinit(alloc);
    }
    var grouped_it = grouped.iterator();
    while (grouped_it.next()) |entry| {
        const constrained_ids = try algebraicConstrainedDocIdsAlloc(index, store, entry.value_ptr.items, constraints);
        defer if (constrained_ids) |ids| index.freeDocIds(ids);
        const effective_ids: []const []const u8 = if (constrained_ids) |ids| ids else entry.value_ptr.items;
        const count: i64 = @intCast(effective_ids.len);
        if (request.min_doc_count > 0 and count < request.min_doc_count) continue;
        if (count == 0) continue;

        const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
        defer if (child_metrics.len > 0) alloc.free(child_metrics);
        const child_folds = try alloc.alloc(AlgebraicMetricFold, child_metrics.len);
        errdefer alloc.free(child_folds);
        for (child_metrics, 0..) |child, i| {
            child_folds[i] = AlgebraicMetricFold.init(child.op);
            const raw = if (child.op == .count)
                try index.rawMetricForDocIdsAlloc(store, child.op, child.field, effective_ids)
            else blk: {
                const resolved = index.resolveMeasureField(child.field) orelse return error.UnsupportedAggregation;
                break :blk try index.rawMetricForResolvedDocIdsAlloc(store, child.op, resolved, effective_ids);
            };
            defer if (raw) |bytes| index.alloc.free(bytes);
            if (raw) |bytes| try child_folds[i].addRaw(alloc, bytes);
        }
        try candidates.append(alloc, .{
            .bucket_index = entry.key_ptr.*,
            .count = count,
            .child_folds = child_folds,
        });
    }
    if (exceedsAlgebraicResultBucketLimit(index, candidates.items.len)) return error.AlgebraicResultBucketLimit;

    std.mem.sort(HistogramBucketAccum, candidates.items, {}, struct {
        fn lessThan(_: void, lhs: HistogramBucketAccum, rhs: HistogramBucketAccum) bool {
            return lhs.bucket_index < rhs.bucket_index;
        }
    }.lessThan);

    const keys = if (request.min_doc_count == 0 and candidates.items.len > 0)
        try fillHistogramBucketKeys(alloc, candidates.items[0].bucket_index, candidates.items[candidates.items.len - 1].bucket_index)
    else blk: {
        const owned = try alloc.alloc(i64, candidates.items.len);
        errdefer alloc.free(owned);
        for (candidates.items, 0..) |candidate, i| owned[i] = candidate.bucket_index;
        break :blk owned;
    };
    defer if (keys.len > 0) alloc.free(keys);
    if (exceedsAlgebraicResultBucketLimit(index, keys.len)) return error.AlgebraicResultBucketLimit;

    var buckets = try alloc.alloc(SearchAggregationBucket, keys.len);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (keys, 0..) |bucket_index, i| {
        var candidate_opt: ?*HistogramBucketAccum = null;
        for (candidates.items) |*candidate| {
            if (candidate.bucket_index == bucket_index) {
                candidate_opt = candidate;
                break;
            }
        }
        const nested = if (candidate_opt) |candidate|
            try algebraicNestedMetricFoldResults(alloc, child_aggs.primary, candidate.child_folds)
        else
            try emptyMetricResults(alloc, child_aggs.primary);
        buckets[i] = .{
            .key_json = try std.fmt.allocPrint(alloc, "{d}", .{@as(f64, @floatFromInt(bucket_index)) * request.interval}),
            .count = if (candidate_opt) |candidate| candidate.count else 0,
            .aggregations = nested,
        };
        buckets_filled = i + 1;
    }
    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeAlgebraicRangeAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?SearchAggregationResult {
    if (request.field.len == 0) return null;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicDocIdMetricType(child.type)) return null;
    }
    if (request.distance_ranges.len > 0) return null;

    const has_numeric = request.ranges.len > 0;
    const has_date = request.date_ranges.len > 0;
    if ((@intFromBool(has_numeric) + @intFromBool(has_date)) != 1) return null;

    const bucket_count = if (has_numeric) request.ranges.len else request.date_ranges.len;
    if (exceedsAlgebraicResultBucketLimit(index, bucket_count)) return error.AlgebraicResultBucketLimit;

    if (request.algebraic_join != null) {
        return try computeDerivedJoinRangeAggregation(alloc, index, store, request, child_aggs.primary, child_aggs.pipeline, constraints, has_numeric);
    }

    var buckets = try alloc.alloc(SearchAggregationBucket, bucket_count);
    var filled: usize = 0;
    errdefer {
        for (buckets[0..filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }

    if (has_numeric) {
        for (request.ranges, 0..) |range_spec, idx| {
            const filter_json = try algebraicNumericRangeFilterJsonAlloc(alloc, request.field, range_spec);
            defer alloc.free(filter_json);
            const doc_ids = (try index.docIdsForFilterJsonAlloc(store, filter_json)) orelse {
                for (buckets[0..filled]) |*bucket| bucket.deinit(alloc);
                alloc.free(buckets);
                return null;
            };
            defer index.freeDocIds(doc_ids);
            const constrained_ids = try algebraicConstrainedDocIdsAlloc(index, store, doc_ids, constraints);
            defer if (constrained_ids) |ids| index.freeDocIds(ids);
            const effective_ids: []const []const u8 = if (constrained_ids) |ids| ids else doc_ids;
            buckets[idx] = .{
                .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
                .count = @intCast(effective_ids.len),
                .aggregations = try algebraicDocIdMetricResultsAlloc(alloc, index, store, child_aggs.primary, effective_ids),
            };
            filled = idx + 1;
        }
    } else {
        for (request.date_ranges, 0..) |range_spec, idx| {
            const filter_json = try algebraicDateRangeFilterJsonAlloc(alloc, request.field, range_spec);
            defer alloc.free(filter_json);
            const doc_ids = (try index.docIdsForFilterJsonAlloc(store, filter_json)) orelse {
                for (buckets[0..filled]) |*bucket| bucket.deinit(alloc);
                alloc.free(buckets);
                return null;
            };
            defer index.freeDocIds(doc_ids);
            const constrained_ids = try algebraicConstrainedDocIdsAlloc(index, store, doc_ids, constraints);
            defer if (constrained_ids) |ids| index.freeDocIds(ids);
            const effective_ids: []const []const u8 = if (constrained_ids) |ids| ids else doc_ids;
            buckets[idx] = .{
                .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
                .count = @intCast(effective_ids.len),
                .aggregations = try algebraicDocIdMetricResultsAlloc(alloc, index, store, child_aggs.primary, effective_ids),
            };
            filled = idx + 1;
        }
    }

    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

const DerivedJoinRangeField = struct {
    name: []const u8,
    role: algebraic_mod.fact.Role,
    kind: algebraic_mod.index.DerivedJoinRangeKind,
};

fn computeDerivedJoinRangeAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    child_requests: []const SearchAggregationRequest,
    pipeline_requests: []const SearchAggregationRequest,
    constraints: []const FixedConstraint,
    numeric: bool,
) !?SearchAggregationResult {
    const range_field = derivedJoinRangeField(index, request.field, if (numeric) .numeric else .date) orelse return null;
    const bucket_count = if (numeric) request.ranges.len else request.date_ranges.len;
    var buckets = try alloc.alloc(SearchAggregationBucket, bucket_count);
    var filled: usize = 0;
    errdefer {
        for (buckets[0..filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }

    if (numeric) {
        for (request.ranges, 0..) |range_spec, idx| {
            const start_text: ?[]u8 = if (range_spec.start) |start| try std.fmt.allocPrint(alloc, "{d}", .{start}) else null;
            defer if (start_text) |text| alloc.free(text);
            const end_text: ?[]u8 = if (range_spec.end) |end| try std.fmt.allocPrint(alloc, "{d}", .{end}) else null;
            defer if (end_text) |text| alloc.free(text);
            buckets[idx] = try computeDerivedJoinRangeBucket(
                alloc,
                index,
                store,
                request,
                child_requests,
                constraints,
                range_field,
                range_spec.name,
                start_text,
                end_text,
            ) orelse return null;
            filled = idx + 1;
        }
    } else {
        for (request.date_ranges, 0..) |range_spec, idx| {
            buckets[idx] = try computeDerivedJoinRangeBucket(
                alloc,
                index,
                store,
                request,
                child_requests,
                constraints,
                range_field,
                range_spec.name,
                range_spec.start,
                range_spec.end,
            ) orelse return null;
            filled = idx + 1;
        }
    }

    try applyPipelineAggregations(alloc, pipeline_requests, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeDerivedJoinRangeBucket(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    child_requests: []const SearchAggregationRequest,
    constraints: []const FixedConstraint,
    range_field: DerivedJoinRangeField,
    range_name: []const u8,
    range_start: ?[]const u8,
    range_end: ?[]const u8,
) !?SearchAggregationBucket {
    const count_fold = derivedJoinRangeFoldRequest(index, request, constraints, range_field, .{ .name = request.name, .op = .count }, range_start, range_end) orelse return null;
    const count_raw = try algebraicDerivedJoinMetricRawAlloc(index, store, count_fold);
    defer if (count_raw) |raw| index.alloc.free(raw);
    const count = if (count_raw) |raw| try algebraic_mod.algebra.parseI64(raw) else 0;

    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_requests);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    const child_folds = try alloc.alloc(AlgebraicMetricFold, child_metrics.len);
    var initialized: usize = 0;
    defer {
        for (child_folds[0..initialized]) |*fold| fold.deinit(alloc);
        if (child_folds.len > 0) alloc.free(child_folds);
    }
    for (child_metrics, 0..) |child, child_idx| {
        child_folds[child_idx] = AlgebraicMetricFold.init(child.op);
        initialized = child_idx + 1;
        const child_fold_request = derivedJoinRangeFoldRequest(
            index,
            request,
            constraints,
            range_field,
            child,
            range_start,
            range_end,
        ) orelse return null;
        const child_raw = try algebraicDerivedJoinMetricRawAlloc(index, store, child_fold_request);
        defer if (child_raw) |raw| index.alloc.free(raw);
        if (child_raw) |raw| try child_folds[child_idx].addRaw(alloc, raw);
    }
    return .{
        .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_name}),
        .count = count,
        .aggregations = try algebraicNestedMetricFoldResults(alloc, child_requests, child_folds),
    };
}

fn derivedJoinRangeFoldRequest(
    index: *const algebraic_mod.index.Index,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
    range_field: DerivedJoinRangeField,
    metric: algebraic_mod.ir.Metric,
    range_start: ?[]const u8,
    range_end: ?[]const u8,
) ?algebraic_mod.index.DerivedJoinFoldRequest {
    const join_ref = request.algebraic_join orelse return null;
    const join_cfg = algebraicJoinConfigByName(index, join_ref.name) orelse return null;
    const law_id = algebraic_mod.law.fromOp(metric.op);
    if (!algebraic_mod.join.queryRewriteProof(join_cfg, join_ref, .{
        .kind = .derived_distributive_fold,
        .law_id = law_id,
        .bounded_fanout = join_cfg.max_fanout != null,
    }).safe()) return null;
    const measure = switch (metric.op) {
        .count => null,
        .sum, .sumsquares, .min, .max, .avg => blk: {
            const field = index.fieldConfig(metric.field, .measure) orelse return null;
            break :blk field.name;
        },
    };
    return .{
        .join = join_ref,
        .op = metric.op,
        .range_field = range_field.name,
        .range_role = range_field.role,
        .range_kind = range_field.kind,
        .range_start = range_start,
        .range_end = range_end,
        .measure = measure,
        .constraints = constraints,
    };
}

fn algebraicJoinConfigByName(index: *const algebraic_mod.index.Index, name: []const u8) ?algebraic_mod.index.JoinConfig {
    for (index.config().joins) |join_cfg| {
        if (std.mem.eql(u8, join_cfg.name, name)) return join_cfg;
    }
    return null;
}

fn derivedJoinRangeField(
    index: *const algebraic_mod.index.Index,
    field_name: []const u8,
    kind: algebraic_mod.index.DerivedJoinRangeKind,
) ?DerivedJoinRangeField {
    var found: ?DerivedJoinRangeField = null;
    switch (kind) {
        .numeric => {
            if (index.fieldConfig(field_name, .measure)) |field| {
                const field_kind = algebraic_mod.value.kindFromFieldType(field.type);
                if (field_kind == .number or field_kind == .integer) found = .{ .name = field.name, .role = .measure, .kind = kind };
            }
            if (index.fieldConfig(field_name, .group)) |field| {
                const field_kind = algebraic_mod.value.kindFromFieldType(field.type);
                if (field_kind == .number or field_kind == .integer) {
                    if (found != null) return null;
                    found = .{ .name = field.name, .role = .group, .kind = kind };
                }
            }
        },
        .date => {
            if (index.fieldConfig(field_name, .time)) |field| {
                const field_kind = algebraic_mod.value.kindFromFieldType(field.type);
                if (field_kind == .datetime) found = .{ .name = field.name, .role = .time, .kind = kind };
            }
            if (index.fieldConfig(field_name, .group)) |field| {
                const field_kind = algebraic_mod.value.kindFromFieldType(field.type);
                if (field_kind == .datetime) {
                    if (found != null) return null;
                    found = .{ .name = field.name, .role = .group, .kind = kind };
                }
            }
        },
    }
    return found;
}

fn computeAlgebraicDateHistogramAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    constraints: []const FixedConstraint,
) !?SearchAggregationResult {
    if (request.field.len == 0) return null;
    const time_field = index.fieldConfig(request.field, .time) orelse return null;
    const bucket_name = algebraicBucketName(request) orelse return null;
    const interval = parseDateInterval(request) catch return null;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    for (child_aggs.primary) |child| {
        if (!isAlgebraicFoldMetricType(child.type)) return null;
    }
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_aggs.primary);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    const query = algebraic_mod.ir.Query{
        .kind = .date_histogram,
        .aggregation_name = request.name,
        .time_field = time_field.name,
        .time_bucket = bucket_name,
        .constraints = constraints,
        .child_metrics = child_metrics,
        .join = request.algebraic_join,
    };
    var plan_result = try algebraic_mod.planner.planBucketQueryAlloc(alloc, index, query);
    defer plan_result.deinit(alloc);
    const count_plan = plan_result.count_metric orelse {
        if (plan_result.derived_join_fold) |derived| {
            return try computeDerivedJoinDateHistogramAggregation(
                alloc,
                index,
                store,
                request,
                interval,
                child_aggs.primary,
                derived,
                plan_result.derived_child_join_folds,
            );
        }
        return try computeAdaptiveDateHistogramAggregation(alloc, index, store, request, time_field.name, bucket_name, interval, constraints, child_aggs.primary, child_aggs.pipeline);
    };
    const child_plans = plan_result.child_metrics;
    if (try algebraicMaterializationExceedsScanBudget(index, store, count_plan.materialization.name)) return error.AlgebraicPlannerScanTooLarge;

    var buckets_accum = std.ArrayListUnmanaged(DateBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }
    var tensor_program = (try algebraic_mod.planner.planBucketQueryMultiOutputTensorProgramAlloc(alloc, index, query)) orelse return null;
    defer tensor_program.deinit(alloc);
    const row_names = try materializedExpressionNamesFromTensorProgramOutputsAlloc(alloc, tensor_program);
    defer if (row_names.len > 0) alloc.free(row_names);
    const rows_opt = try index.scanMaterializedExpressionRows(store, row_names);
    if (rows_opt) |rows| {
        defer {
            for (rows) |*row| row.deinit(index.alloc);
            if (rows.len > 0) index.alloc.free(rows);
        }
        for (rows) |row| {
            const count_raw = row.values[0] orelse continue;
            const count = algebraic_mod.algebra.parseI64(count_raw) catch continue;
            const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, row.group_key) catch continue;
            defer {
                for (decoded) |item| alloc.free(item);
                alloc.free(decoded);
            }
            if (decoded.len != 2) continue;
            _ = (try parseRfc3339ToNs(decoded[0])) orelse continue;
            const inner_group = algebraic_mod.token.decodeTupleAlloc(alloc, decoded[1]) catch continue;
            defer {
                for (inner_group) |item| alloc.free(item);
                alloc.free(inner_group);
            }
            if (inner_group.len != count_plan.materialization.group_by.len) continue;
            if (!decodedGroupMatchesConstraints(index, count_plan.materialization, inner_group, constraints)) continue;

            const bucket_accum = try ensureDateBucketAccum(alloc, &buckets_accum, decoded[0], child_plans);
            bucket_accum.count += count;
            for (child_plans, 0..) |_, child_idx| {
                if (row.values[child_idx + 1]) |bytes| try bucket_accum.child_folds[child_idx].addRaw(alloc, bytes);
            }
        }
    } else {
        const entries = try index.scanMaterializedExpressionEntriesForMaterialization(store, count_plan.materialization.name);
        defer {
            for (entries) |*entry| entry.deinit(index.alloc);
            if (entries.len > 0) index.alloc.free(entries);
        }
        for (entries) |entry| {
            const count = algebraic_mod.algebra.parseI64(entry.value) catch continue;
            const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, entry.group_key) catch continue;
            defer {
                for (decoded) |item| alloc.free(item);
                alloc.free(decoded);
            }
            if (decoded.len != 2) continue;
            _ = (try parseRfc3339ToNs(decoded[0])) orelse continue;
            const inner_group = algebraic_mod.token.decodeTupleAlloc(alloc, decoded[1]) catch continue;
            defer {
                for (inner_group) |item| alloc.free(item);
                alloc.free(inner_group);
            }
            if (inner_group.len != count_plan.materialization.group_by.len) continue;
            if (!decodedGroupMatchesConstraints(index, count_plan.materialization, inner_group, constraints)) continue;

            const bucket_accum = try ensureDateBucketAccum(alloc, &buckets_accum, decoded[0], child_plans);
            bucket_accum.count += count;
            for (child_plans, 0..) |child_plan, child_idx| {
                const raw = try index.rawValueAlloc(store, child_plan.materialization.name, entry.group_key);
                defer if (raw) |bytes| index.alloc.free(bytes);
                if (raw) |bytes| try bucket_accum.child_folds[child_idx].addRaw(alloc, bytes);
            }
        }
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(DateBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }

    std.mem.sort(DateBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: DateBucketAccum, rhs: DateBucketAccum) bool {
            return std.mem.order(u8, lhs.bucket_start, rhs.bucket_start) == .lt;
        }
    }.lessThan);

    const bucket_keys = if (request.min_doc_count == 0 and candidates.len > 0) blk: {
        const first_ns = (try parseRfc3339ToNs(candidates[0].bucket_start)) orelse return null;
        const last_ns = (try parseRfc3339ToNs(candidates[candidates.len - 1].bucket_start)) orelse return null;
        break :blk try fillDateHistogramBucketKeys(alloc, first_ns, last_ns, interval);
    } else blk: {
        const keys = try alloc.alloc(u64, candidates.len);
        errdefer alloc.free(keys);
        for (candidates, 0..) |candidate, i| {
            keys[i] = (try parseRfc3339ToNs(candidate.bucket_start)) orelse return null;
        }
        break :blk keys;
    };
    defer if (bucket_keys.len > 0) alloc.free(bucket_keys);

    var buckets = try alloc.alloc(SearchAggregationBucket, bucket_keys.len);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (bucket_keys, 0..) |bucket_key, idx| {
        const formatted = try formatRfc3339Bucket(alloc, bucket_key);
        defer alloc.free(formatted);
        var candidate_opt: ?DateBucketAccum = null;
        for (candidates) |candidate| {
            if (std.mem.eql(u8, candidate.bucket_start, formatted)) {
                candidate_opt = candidate;
                break;
            }
        }
        const nested = if (candidate_opt) |candidate|
            try algebraicNestedMetricFoldResults(alloc, child_aggs.primary, candidate.child_folds)
        else
            try alloc.alloc(SearchAggregationResult, 0);
        buckets[idx] = .{
            .key_json = try std.json.Stringify.valueAlloc(alloc, formatted, .{}),
            .count = if (candidate_opt) |candidate| candidate.count else 0,
            .aggregations = nested,
        };
        buckets_filled = idx + 1;
    }
    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeDerivedJoinDateHistogramAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    interval: search_agg_mod.DateInterval,
    child_requests: []const SearchAggregationRequest,
    derived: algebraic_mod.index.DerivedJoinFoldRequest,
    child_derived: []const algebraic_mod.index.DerivedJoinFoldRequest,
) !?SearchAggregationResult {
    if (derived.op != .count or derived.group_by.len != 0 or derived.measure != null) return null;
    if (derived.time_field == null or derived.time_bucket == null) return null;
    const child_metrics = try algebraicChildMetricsAlloc(alloc, child_requests);
    defer if (child_metrics.len > 0) alloc.free(child_metrics);
    if (child_metrics.len != child_derived.len) return null;
    const child_ops = try alloc.alloc(algebraic_mod.algebra.Op, child_derived.len);
    defer if (child_ops.len > 0) alloc.free(child_ops);
    for (child_derived, 0..) |child, i| {
        if (child.group_by.len != 0) return null;
        if (!optionalStringEql(child.time_field, derived.time_field) or !optionalStringEql(child.time_bucket, derived.time_bucket)) return null;
        child_ops[i] = child.op;
    }

    const entries = (try scanDerivedJoinFoldEntriesWithProgramAlloc(alloc, index, store, derived, request.name)) orelse return null;
    defer {
        for (entries) |*entry| entry.deinit(index.alloc);
        if (entries.len > 0) index.alloc.free(entries);
    }

    var buckets_accum = std.ArrayListUnmanaged(DateBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }

    for (entries) |entry| {
        const count = algebraic_mod.algebra.parseI64(entry.value) catch continue;
        const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, entry.group_key) catch continue;
        defer {
            for (decoded) |item| alloc.free(item);
            alloc.free(decoded);
        }
        if (decoded.len != 2) continue;
        _ = (try parseRfc3339ToNs(decoded[0])) orelse continue;
        const inner_group = algebraic_mod.token.decodeTupleAlloc(alloc, decoded[1]) catch continue;
        defer {
            for (inner_group) |item| alloc.free(item);
            alloc.free(inner_group);
        }
        if (inner_group.len != 0) continue;
        const bucket_accum = try ensureDateBucketAccumForOps(alloc, &buckets_accum, decoded[0], child_ops);
        bucket_accum.count += count;
    }
    for (child_derived, 0..) |child, child_idx| {
        const child_entries = (try scanDerivedJoinFoldEntriesWithProgramAlloc(alloc, index, store, child, child_metrics[child_idx].name)) orelse continue;
        defer {
            for (child_entries) |*entry| entry.deinit(index.alloc);
            if (child_entries.len > 0) index.alloc.free(child_entries);
        }
        for (child_entries) |entry| {
            const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, entry.group_key) catch continue;
            defer {
                for (decoded) |item| alloc.free(item);
                alloc.free(decoded);
            }
            if (decoded.len != 2) continue;
            const bucket_accum = findDateBucketAccum(&buckets_accum, decoded[0]) orelse continue;
            try bucket_accum.child_folds[child_idx].addRaw(alloc, entry.value);
        }
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(DateBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }
    std.mem.sort(DateBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: DateBucketAccum, rhs: DateBucketAccum) bool {
            return std.mem.order(u8, lhs.bucket_start, rhs.bucket_start) == .lt;
        }
    }.lessThan);

    const bucket_keys = if (request.min_doc_count == 0 and candidates.len > 0) blk: {
        const first_ns = (try parseRfc3339ToNs(candidates[0].bucket_start)) orelse return null;
        const last_ns = (try parseRfc3339ToNs(candidates[candidates.len - 1].bucket_start)) orelse return null;
        break :blk try fillDateHistogramBucketKeys(alloc, first_ns, last_ns, interval);
    } else blk: {
        const keys = try alloc.alloc(u64, candidates.len);
        errdefer alloc.free(keys);
        for (candidates, 0..) |candidate, i| {
            keys[i] = (try parseRfc3339ToNs(candidate.bucket_start)) orelse return null;
        }
        break :blk keys;
    };
    defer if (bucket_keys.len > 0) alloc.free(bucket_keys);

    var buckets = try alloc.alloc(SearchAggregationBucket, bucket_keys.len);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (bucket_keys, 0..) |bucket_key, idx| {
        const formatted = try formatRfc3339Bucket(alloc, bucket_key);
        defer alloc.free(formatted);
        var candidate_opt: ?DateBucketAccum = null;
        for (candidates) |candidate| {
            if (std.mem.eql(u8, candidate.bucket_start, formatted)) {
                candidate_opt = candidate;
                break;
            }
        }
        const nested = if (candidate_opt) |candidate|
            try algebraicNestedMetricFoldResults(alloc, child_requests, candidate.child_folds)
        else
            try alloc.alloc(SearchAggregationResult, 0);
        buckets[idx] = .{
            .key_json = try std.json.Stringify.valueAlloc(alloc, formatted, .{}),
            .count = if (candidate_opt) |candidate| candidate.count else 0,
            .aggregations = nested,
        };
        buckets_filled = idx + 1;
    }
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeAdaptiveDateHistogramAggregation(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    request: SearchAggregationRequest,
    time_field_name: []const u8,
    bucket_name: []const u8,
    interval: search_agg_mod.DateInterval,
    constraints: []const FixedConstraint,
    child_requests: []const SearchAggregationRequest,
    pipeline_aggs: []const SearchAggregationRequest,
) !?SearchAggregationResult {
    const query = algebraic_mod.ir.Query{
        .kind = .date_histogram,
        .aggregation_name = request.name,
        .time_field = time_field_name,
        .time_bucket = bucket_name,
        .constraints = constraints,
    };
    const recommendation = try algebraic_mod.adaptive.recommendationAlloc(alloc, query);
    defer alloc.free(recommendation);
    const count_materialization_id = (try index.readyAdaptiveMaterializationIdAlloc(store, recommendation)) orelse return null;
    defer index.alloc.free(count_materialization_id);
    if (!proveAdaptiveTensorRead(count_materialization_id, .count, adaptiveTensorOutputDims(.date_histogram, false, true))) return null;
    const entries = (try index.scanAdaptiveTensorEntriesForRecommendation(store, recommendation)) orelse return null;
    defer {
        for (entries) |*entry| entry.deinit(index.alloc);
        if (entries.len > 0) index.alloc.free(entries);
    }
    const child_adaptive = (try adaptiveChildMetricPlansAlloc(alloc, index, store, .date_histogram, null, time_field_name, bucket_name, constraints, child_requests)) orelse return null;
    defer child_adaptive.deinit(alloc, index.alloc);

    var buckets_accum = std.ArrayListUnmanaged(DateBucketAccum).empty;
    defer {
        for (buckets_accum.items) |*item| item.deinit(alloc);
        buckets_accum.deinit(alloc);
    }
    for (entries) |entry| {
        const count = algebraic_mod.algebra.parseI64(entry.value) catch continue;
        const decoded = algebraic_mod.token.decodeTupleAlloc(alloc, entry.group_key) catch continue;
        defer {
            for (decoded) |item| alloc.free(item);
            alloc.free(decoded);
        }
        if (decoded.len != 2) continue;
        _ = (try parseRfc3339ToNs(decoded[0])) orelse continue;
        const inner_group = algebraic_mod.token.decodeTupleAlloc(alloc, decoded[1]) catch continue;
        defer {
            for (inner_group) |item| alloc.free(item);
            alloc.free(inner_group);
        }
        if (inner_group.len != constraints.len) continue;
        if (!decodedAdaptiveGroupMatchesConstraints(index, inner_group, constraints, 0)) continue;
        const bucket_accum = try ensureAdaptiveDateBucketAccum(alloc, &buckets_accum, decoded[0], child_adaptive.ops);
        bucket_accum.count += count;
        for (child_adaptive.materialization_ids, 0..) |materialization_id, child_idx| {
            var raw_result = if (try index.adaptiveMaterializedExpressionRawValueForRecommendationAlloc(
                store,
                child_adaptive.recommendations[child_idx],
                decoded[1],
                decoded[0],
            )) |expr_raw|
                expr_raw
            else
                algebraic_mod.index.AdaptiveRawResult{ .raw = try index.adaptiveTensorRawValueAlloc(store, materialization_id, decoded[1], decoded[0]) };
            defer raw_result.deinit(index.alloc);
            if (raw_result.raw) |bytes| try bucket_accum.child_folds[child_idx].addRaw(alloc, bytes);
        }
    }

    var kept_count: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        kept_count += 1;
    }
    if (exceedsAlgebraicResultBucketLimit(index, kept_count)) return error.AlgebraicResultBucketLimit;
    var candidates = try alloc.alloc(DateBucketAccum, kept_count);
    defer if (candidates.len > 0) alloc.free(candidates);
    var kept_idx: usize = 0;
    for (buckets_accum.items) |bucket| {
        if (request.min_doc_count > 0 and bucket.count < request.min_doc_count) continue;
        candidates[kept_idx] = bucket;
        kept_idx += 1;
    }
    std.mem.sort(DateBucketAccum, candidates, {}, struct {
        fn lessThan(_: void, lhs: DateBucketAccum, rhs: DateBucketAccum) bool {
            return std.mem.order(u8, lhs.bucket_start, rhs.bucket_start) == .lt;
        }
    }.lessThan);

    const bucket_keys = if (request.min_doc_count == 0 and candidates.len > 0) blk: {
        const first_ns = (try parseRfc3339ToNs(candidates[0].bucket_start)) orelse return null;
        const last_ns = (try parseRfc3339ToNs(candidates[candidates.len - 1].bucket_start)) orelse return null;
        break :blk try fillDateHistogramBucketKeys(alloc, first_ns, last_ns, interval);
    } else blk: {
        const keys = try alloc.alloc(u64, candidates.len);
        errdefer alloc.free(keys);
        for (candidates, 0..) |candidate, i| {
            keys[i] = (try parseRfc3339ToNs(candidate.bucket_start)) orelse return null;
        }
        break :blk keys;
    };
    defer if (bucket_keys.len > 0) alloc.free(bucket_keys);

    var buckets = try alloc.alloc(SearchAggregationBucket, bucket_keys.len);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (bucket_keys, 0..) |bucket_key, idx| {
        const formatted = try formatRfc3339Bucket(alloc, bucket_key);
        defer alloc.free(formatted);
        var candidate_opt: ?DateBucketAccum = null;
        for (candidates) |candidate| {
            if (std.mem.eql(u8, candidate.bucket_start, formatted)) {
                candidate_opt = candidate;
                break;
            }
        }
        buckets[idx] = .{
            .key_json = try std.json.Stringify.valueAlloc(alloc, formatted, .{}),
            .count = if (candidate_opt) |candidate| candidate.count else 0,
            .aggregations = if (candidate_opt) |candidate|
                try algebraicNestedMetricFoldResults(alloc, child_requests, candidate.child_folds)
            else
                try emptyMetricResults(alloc, child_requests),
        };
        buckets_filled = idx + 1;
    }
    try applyPipelineAggregations(alloc, pipeline_aggs, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

pub fn algebraicBucketName(request: SearchAggregationRequest) ?[]const u8 {
    const interval = if (request.calendar_interval.len > 0)
        request.calendar_interval
    else if (request.fixed_interval.len > 0)
        request.fixed_interval
    else
        return null;
    if (std.mem.eql(u8, interval, "hour") or std.mem.eql(u8, interval, "1h") or std.mem.eql(u8, interval, "60m")) return "hour";
    if (std.mem.eql(u8, interval, "day") or std.mem.eql(u8, interval, "1d") or std.mem.eql(u8, interval, "24h")) return "day";
    if (std.mem.eql(u8, interval, "month") or std.mem.eql(u8, interval, "1M")) return "month";
    return null;
}

fn algebraicNestedMetricResults(
    alloc: Allocator,
    index: *algebraic_mod.index.Index,
    store: *docstore_mod.DocStore,
    requests: []const SearchAggregationRequest,
    plans: []const algebraic_mod.planner.MetricPlan,
    group_key: []const u8,
) ![]SearchAggregationResult {
    var out = try alloc.alloc(SearchAggregationResult, requests.len);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |*result| result.deinit(alloc);
        alloc.free(out);
    }
    for (requests, plans, 0..) |request, plan, i| {
        const raw = try index.rawValueAlloc(store, plan.materialization.name, group_key);
        defer if (raw) |bytes| index.alloc.free(bytes);
        out[i] = .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .value_json = try algebraicMetricValueJsonAlloc(alloc, plan.op, raw),
        };
        filled = i + 1;
    }
    return out;
}

fn algebraicNestedMetricFoldResults(
    alloc: Allocator,
    requests: []const SearchAggregationRequest,
    folds: []const AlgebraicMetricFold,
) ![]SearchAggregationResult {
    var out = try alloc.alloc(SearchAggregationResult, requests.len);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |*result| result.deinit(alloc);
        alloc.free(out);
    }
    var fold_idx: usize = 0;
    for (requests, 0..) |request, i| {
        if (std.mem.eql(u8, request.type, "stats")) {
            if (fold_idx + 4 > folds.len) return error.UnsupportedAggregation;
            const avg_raw = try folds[fold_idx].rawAlloc(alloc);
            defer if (avg_raw) |bytes| alloc.free(bytes);
            const min_raw = try folds[fold_idx + 1].rawAlloc(alloc);
            defer if (min_raw) |bytes| alloc.free(bytes);
            const max_raw = try folds[fold_idx + 2].rawAlloc(alloc);
            defer if (max_raw) |bytes| alloc.free(bytes);
            const sum_squares_raw = try folds[fold_idx + 3].rawAlloc(alloc);
            defer if (sum_squares_raw) |bytes| alloc.free(bytes);
            out[i] = .{
                .name = request.name,
                .field = request.field,
                .type = request.type,
                .value_json = (try algebraicStatsValueJsonAlloc(alloc, avg_raw, min_raw, max_raw, sum_squares_raw)) orelse return error.UnsupportedAggregation,
            };
            fold_idx += 4;
        } else {
            if (fold_idx >= folds.len) return error.UnsupportedAggregation;
            out[i] = .{
                .name = request.name,
                .field = request.field,
                .type = request.type,
                .value_json = try folds[fold_idx].valueJsonAlloc(alloc),
            };
            fold_idx += 1;
        }
        filled = i + 1;
    }
    return out;
}

fn emptyMetricResults(
    alloc: Allocator,
    requests: []const SearchAggregationRequest,
) ![]SearchAggregationResult {
    var out = try alloc.alloc(SearchAggregationResult, requests.len);
    var filled: usize = 0;
    errdefer {
        for (out[0..filled]) |*result| result.deinit(alloc);
        alloc.free(out);
    }
    for (requests, 0..) |request, i| {
        if (std.mem.eql(u8, request.type, "stats")) {
            out[i] = .{
                .name = request.name,
                .field = request.field,
                .type = request.type,
                .value_json = (try algebraicStatsValueJsonAlloc(alloc, null, null, null, null)).?,
            };
        } else {
            const op = algebraic_mod.algebra.Op.parse(request.type) orelse return error.UnsupportedAggregation;
            out[i] = .{
                .name = request.name,
                .field = request.field,
                .type = request.type,
                .value_json = try algebraicMetricValueJsonAlloc(alloc, op, null),
            };
        }
        filled = i + 1;
    }
    return out;
}

const NumericMetricKind = enum { sum, min, max, avg, stats, sumsquares };

fn computeNumericMetricAggregation(
    alloc: Allocator,
    request: SearchAggregationRequest,
    hits: []const types.SearchHit,
    kind: NumericMetricKind,
) !SearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;

    var sum: f64 = 0;
    var sum_squares: f64 = 0;
    var count: i64 = 0;
    var min_value: f64 = std.math.inf(f64);
    var max_value: f64 = -std.math.inf(f64);

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        accumulateNumericJsonValue(value, &sum, &sum_squares, &count, &min_value, &max_value);
    }

    const value_json = switch (kind) {
        .sum => try std.fmt.allocPrint(alloc, "{d}", .{sum}),
        .min => if (count == 0) try alloc.dupe(u8, "null") else try std.fmt.allocPrint(alloc, "{d}", .{min_value}),
        .max => if (count == 0) try alloc.dupe(u8, "null") else try std.fmt.allocPrint(alloc, "{d}", .{max_value}),
        .avg => if (count == 0)
            try alloc.dupe(u8, "{\"count\":0,\"sum\":0,\"avg\":0}")
        else
            try std.fmt.allocPrint(alloc, "{{\"count\":{d},\"sum\":{d},\"avg\":{d}}}", .{ count, sum, sum / @as(f64, @floatFromInt(count)) }),
        .sumsquares => try std.fmt.allocPrint(alloc, "{d}", .{sum_squares}),
        .stats => blk: {
            if (count == 0) break :blk try alloc.dupe(u8, "{\"count\":0,\"sum\":0,\"avg\":0,\"min\":null,\"max\":null,\"sum_squares\":0,\"variance\":0,\"std_dev\":0}");
            const avg = sum / @as(f64, @floatFromInt(count));
            const variance = (sum_squares / @as(f64, @floatFromInt(count))) - (avg * avg);
            const non_negative_variance = if (variance < 0) 0 else variance;
            break :blk try std.fmt.allocPrint(
                alloc,
                "{{\"count\":{d},\"sum\":{d},\"avg\":{d},\"min\":{d},\"max\":{d},\"sum_squares\":{d},\"variance\":{d},\"std_dev\":{d}}}",
                .{ count, sum, avg, min_value, max_value, sum_squares, non_negative_variance, @sqrt(non_negative_variance) },
            );
        },
    };
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .value_json = value_json,
    };
}

fn computeCardinalityAggregation(
    alloc: Allocator,
    request: SearchAggregationRequest,
    hits: []const types.SearchHit,
) !SearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;

    var seen = std.StringHashMap(void).init(alloc);
    defer {
        var it = seen.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        seen.deinit();
    }

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        try collectCardinalityValues(alloc, &seen, value);
    }

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .value_json = try std.fmt.allocPrint(alloc, "{{\"value\":{d}}}", .{seen.count()}),
    };
}

fn computeTermsAggregation(
    alloc: Allocator,
    request: SearchAggregationRequest,
    hits: []const types.SearchHit,
    ctx: Context,
) anyerror!SearchAggregationResult {
    if (request.fields.len > 1) return try computeCompositeTermsAggregation(alloc, request, hits, ctx);
    if (request.field.len == 0) return error.InvalidAggregation;

    var counts = std.StringHashMap(i64).init(alloc);
    defer {
        var it = counts.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        counts.deinit();
    }
    var grouped = std.StringHashMap(std.ArrayListUnmanaged(types.SearchHit)).init(alloc);
    defer {
        var it = grouped.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        try appendTermAggregationValues(alloc, &counts, &grouped, hit, value);
    }

    var entries = std.ArrayList(struct { key: []const u8, count: i64 }).empty;
    defer entries.deinit(alloc);
    var it = counts.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const count = entry.value_ptr.*;
        if (request.term_prefix.len > 0 and !std.mem.startsWith(u8, key, request.term_prefix)) continue;
        if (request.term_pattern.len > 0 and !(try regexMatches(alloc, request.term_pattern, key))) continue;
        if (request.min_doc_count > 0 and count < request.min_doc_count) continue;
        try entries.append(alloc, .{ .key = key, .count = count });
    }
    std.mem.sort(@TypeOf(entries.items[0]), entries.items, {}, struct {
        fn lessThan(_: void, lhs: @TypeOf(entries.items[0]), rhs: @TypeOf(entries.items[0])) bool {
            if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            return lhs.count > rhs.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < entries.items.len) @intCast(request.size) else entries.items.len;
    var buckets = try alloc.alloc(SearchAggregationBucket, limit);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (entries.items[0..limit], 0..) |entry, idx| {
        const grouped_hits = grouped.get(entry.key).?.items;
        const nested = blk: {
            if (child_aggs.primary.len == 0) break :blk try alloc.alloc(SearchAggregationResult, 0);
            break :blk try computeSearchAggregationsAtDepth(alloc, child_aggs.primary, .{
                .alloc = alloc,
                .hits = grouped_hits,
                .total_hits = @intCast(grouped_hits.len),
            }, ctx, false);
        };
        buckets[idx] = .{
            .key_json = try std.json.Stringify.valueAlloc(alloc, entry.key, .{}),
            .count = entry.count,
            .aggregations = nested,
        };
        buckets_filled = idx + 1;
    }
    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

const CompositeTermComponents = struct {
    items: [][]u8,

    fn deinit(self: *CompositeTermComponents, alloc: Allocator) void {
        for (self.items) |item| alloc.free(item);
        if (self.items.len > 0) alloc.free(self.items);
        self.* = undefined;
    }
};

fn computeCompositeTermsAggregation(
    alloc: Allocator,
    request: SearchAggregationRequest,
    hits: []const types.SearchHit,
    ctx: Context,
) anyerror!SearchAggregationResult {
    if (request.fields.len < 2) return error.InvalidAggregation;
    if (request.term_prefix.len > 0 or request.term_pattern.len > 0) return error.UnsupportedAggregation;

    var counts = std.StringHashMap(i64).init(alloc);
    defer {
        var it = counts.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        counts.deinit();
    }
    var grouped = std.StringHashMap(std.ArrayListUnmanaged(types.SearchHit)).init(alloc);
    defer {
        var it = grouped.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();

        var components = try alloc.alloc(CompositeTermComponents, request.fields.len);
        var components_filled: usize = 0;
        defer {
            for (components[0..components_filled]) |*component| component.deinit(alloc);
            if (components.len > 0) alloc.free(components);
        }
        var missing_field = false;
        for (request.fields, 0..) |field, i| {
            const value = extractValueAtPath(parsed.value, field) orelse {
                missing_field = true;
                break;
            };
            components[i] = try compositeTermComponentsAlloc(alloc, value);
            components_filled = i + 1;
            if (components[i].items.len == 0) {
                missing_field = true;
                break;
            }
        }
        if (missing_field) continue;
        try appendCompositeTermAggregationValues(alloc, &counts, &grouped, hit, components);
    }

    var entries = std.ArrayList(struct { key: []const u8, count: i64 }).empty;
    defer entries.deinit(alloc);
    var it = counts.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const count = entry.value_ptr.*;
        if (request.min_doc_count > 0 and count < request.min_doc_count) continue;
        try entries.append(alloc, .{ .key = key, .count = count });
    }
    std.mem.sort(@TypeOf(entries.items[0]), entries.items, {}, struct {
        fn lessThan(_: void, lhs: @TypeOf(entries.items[0]), rhs: @TypeOf(entries.items[0])) bool {
            if (lhs.count == rhs.count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
            return lhs.count > rhs.count;
        }
    }.lessThan);

    const limit: usize = if (request.size > 0 and @as(usize, @intCast(request.size)) < entries.items.len) @intCast(request.size) else entries.items.len;
    var buckets = try alloc.alloc(SearchAggregationBucket, limit);
    var buckets_filled: usize = 0;
    errdefer {
        for (buckets[0..buckets_filled]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (entries.items[0..limit], 0..) |entry, idx| {
        const grouped_hits = grouped.get(entry.key).?.items;
        const nested = blk: {
            if (child_aggs.primary.len == 0) break :blk try alloc.alloc(SearchAggregationResult, 0);
            break :blk try computeSearchAggregationsAtDepth(alloc, child_aggs.primary, .{
                .alloc = alloc,
                .hits = grouped_hits,
                .total_hits = @intCast(grouped_hits.len),
            }, ctx, false);
        };
        errdefer deinitResults(alloc, nested);
        buckets[idx] = .{
            .key_json = try alloc.dupe(u8, entry.key),
            .count = entry.count,
            .aggregations = nested,
        };
        buckets_filled = idx + 1;
    }
    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = if (request.field.len > 0) request.field else request.fields[0],
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeSignificantTermsAggregation(
    alloc: Allocator,
    request: SearchAggregationRequest,
    hits: []const types.SearchHit,
    ctx: Context,
) anyerror!SearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;
    const manager = ctx.index_manager;
    const text_index = if (manager) |mgr| mgr.textIndex(ctx.full_text_index_name) else null;
    const snapshot = if (text_index) |index| index.snapshot() else null;
    const distributed_stats = findDistributedTextStats(ctx.distributed_text_stats, request.field);
    const distributed_background_stats = findDistributedBackgroundTextStats(ctx.distributed_background_text_stats, request.name, request.field);
    if (text_index == null and distributed_stats == null and distributed_background_stats == null) return error.UnsupportedAggregation;

    var foreground = std.StringHashMap(i64).init(alloc);
    defer {
        var it = foreground.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        foreground.deinit();
    }
    var grouped = std.StringHashMap(std.ArrayListUnmanaged(types.SearchHit)).init(alloc);
    defer {
        var it = grouped.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }

    const min_doc_count: i64 = if (request.min_doc_count > 0) request.min_doc_count else 1;
    const size: usize = @intCast(if (request.size > 0) request.size else 10);
    const foreground_doc_count: i64 = @intCast(hits.len);
    var background_counts = std.StringHashMap(i64).init(alloc);
    defer {
        var it = background_counts.keyIterator();
        while (it.next()) |key| alloc.free(key.*);
        background_counts.deinit();
    }
    var background_doc_count: i64 = if (distributed_stats) |stats|
        @intCast(stats.global_doc_count)
    else if (snapshot) |snap|
        @intCast(snap.global_doc_count)
    else
        0;

    if (request.background_query) |background_query| {
        if (distributed_background_stats) |stats| {
            background_doc_count = @intCast(stats.background_doc_count);
            for (stats.term_doc_freqs) |term| {
                const bg_entry = try background_counts.getOrPut(term.term);
                if (bg_entry.found_existing) {
                    bg_entry.value_ptr.* = @intCast(term.doc_freq);
                } else {
                    bg_entry.key_ptr.* = try alloc.dupe(u8, term.term);
                    bg_entry.value_ptr.* = @intCast(term.doc_freq);
                }
            }
        } else {
            const snap = snapshot orelse return error.UnsupportedAggregation;
            var background_result = try executeBackgroundQuery(alloc, snap, background_query);
            defer background_result.deinit();
            background_doc_count = @intCast(background_result.total_hits);
            for (background_result.hits) |hit| {
                const stored = hit.stored_data orelse continue;
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
                defer parsed.deinit();
                const value = extractValueAtPath(parsed.value, request.field) orelse continue;

                var seen_terms = std.StringHashMap(void).init(alloc);
                defer {
                    var it = seen_terms.keyIterator();
                    while (it.next()) |key| alloc.free(key.*);
                    seen_terms.deinit();
                }
                try collectSignificantTermsFromValue(alloc, value, &seen_terms);

                var seen_it = seen_terms.keyIterator();
                while (seen_it.next()) |term_ptr| {
                    const term = term_ptr.*;
                    if (request.term_prefix.len > 0 and !std.mem.startsWith(u8, term, request.term_prefix)) continue;
                    if (request.term_pattern.len > 0 and !(try regexMatches(alloc, request.term_pattern, term))) continue;
                    const bg_entry = try background_counts.getOrPut(term);
                    if (bg_entry.found_existing) {
                        bg_entry.value_ptr.* += 1;
                    } else {
                        bg_entry.key_ptr.* = try alloc.dupe(u8, term);
                        bg_entry.value_ptr.* = 1;
                    }
                }
            }
        }
    }

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;

        var seen_terms = std.StringHashMap(void).init(alloc);
        defer {
            var it = seen_terms.keyIterator();
            while (it.next()) |key| alloc.free(key.*);
            seen_terms.deinit();
        }
        try collectSignificantTermsFromValue(alloc, value, &seen_terms);

        var it = seen_terms.keyIterator();
        while (it.next()) |term_ptr| {
            const term = term_ptr.*;
            if (request.term_prefix.len > 0 and !std.mem.startsWith(u8, term, request.term_prefix)) continue;
            if (request.term_pattern.len > 0 and !(try regexMatches(alloc, request.term_pattern, term))) continue;

            const count_entry = try foreground.getOrPut(term);
            if (count_entry.found_existing) {
                count_entry.value_ptr.* += 1;
            } else {
                count_entry.key_ptr.* = try alloc.dupe(u8, term);
                count_entry.value_ptr.* = 1;
            }

            const group_entry = try grouped.getOrPut(count_entry.key_ptr.*);
            if (!group_entry.found_existing) group_entry.value_ptr.* = .empty;
            try group_entry.value_ptr.append(alloc, hit);
        }
    }

    const ScoredTerm = struct {
        key: []const u8,
        fg_count: i64,
        bg_count: i64,
        score: f64,
    };
    var scored = std.ArrayList(ScoredTerm).empty;
    defer scored.deinit(alloc);

    var it = foreground.iterator();
    while (it.next()) |entry| {
        const term = entry.key_ptr.*;
        const fg_count = entry.value_ptr.*;
        if (fg_count < min_doc_count) continue;

        const bg_count: i64 = if (request.background_query != null)
            background_counts.get(term) orelse 0
        else blk: {
            var whole_bg_count: i64 = if (distributed_stats) |stats|
                @intCast(stats.termDocFreq(term) orelse 0)
            else blk2: {
                const snap = snapshot orelse return error.UnsupportedAggregation;
                break :blk2 @intCast(try snap.termDocFreq(alloc, request.field, term));
            };
            if (whole_bg_count == 0) whole_bg_count = fg_count;
            break :blk whole_bg_count;
        };

        const score = calculateSignificanceScore(
            if (request.significance_algorithm.len > 0) request.significance_algorithm else "jlh",
            fg_count,
            foreground_doc_count,
            bg_count,
            background_doc_count,
        );
        try scored.append(alloc, .{
            .key = term,
            .fg_count = fg_count,
            .bg_count = bg_count,
            .score = score,
        });
    }

    std.mem.sort(ScoredTerm, scored.items, {}, struct {
        fn lessThan(_: void, lhs: ScoredTerm, rhs: ScoredTerm) bool {
            if (lhs.score == rhs.score) {
                if (lhs.fg_count == rhs.fg_count) return std.mem.order(u8, lhs.key, rhs.key) == .lt;
                return lhs.fg_count > rhs.fg_count;
            }
            return lhs.score > rhs.score;
        }
    }.lessThan);

    const limit = @min(size, scored.items.len);
    var buckets = try alloc.alloc(SearchAggregationBucket, limit);
    errdefer {
        for (buckets) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (scored.items[0..limit], 0..) |entry, idx| {
        const grouped_hits = grouped.get(entry.key).?.items;
        const nested = blk: {
            if (request.aggregations.len == 0) break :blk try alloc.alloc(SearchAggregationResult, 0);
            break :blk try computeSearchAggregationsAtDepth(alloc, request.aggregations, .{
                .alloc = alloc,
                .hits = grouped_hits,
                .total_hits = @intCast(grouped_hits.len),
            }, ctx, false);
        };
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{entry.key}),
            .count = entry.fg_count,
            .score = entry.score,
            .bg_count = entry.bg_count,
            .aggregations = nested,
        };
    }

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .metadata_json = try std.fmt.allocPrint(
            alloc,
            "{{\"algorithm\":\"{s}\",\"fg_doc_count\":{d},\"bg_doc_count\":{d},\"unique_terms\":{d},\"significant_terms\":{d}}}",
            .{
                if (request.significance_algorithm.len > 0) request.significance_algorithm else "jlh",
                foreground_doc_count,
                background_doc_count,
                foreground.count(),
                limit,
            },
        ),
        .buckets = buckets,
    };
}

fn findDistributedTextStats(
    items: []const distributed_stats_mod.TextFieldStats,
    field: []const u8,
) ?distributed_stats_mod.TextFieldStats {
    for (items) |item| {
        if (std.mem.eql(u8, item.field, field)) return item;
    }
    return null;
}

fn findDistributedBackgroundTextStats(
    items: []const DistributedBackgroundTextStats,
    aggregation_name: []const u8,
    field: []const u8,
) ?DistributedBackgroundTextStats {
    for (items) |item| {
        if (std.mem.eql(u8, item.aggregation_name, aggregation_name) and std.mem.eql(u8, item.field, field)) return item;
    }
    return null;
}

fn executeBackgroundQuery(
    alloc: Allocator,
    snapshot: *const @import("../../index.zig").IndexSnapshot,
    query: BackgroundQuery,
) !search_mod.SearchResult {
    const request: search_mod.SearchRequest = .{
        .query = switch (query) {
            .match_all => .{ .match_all = {} },
            .match => |match| .{ .match = .{
                .field = match.field,
                .text = match.text,
            } },
            .term => |term| .{ .term = .{
                .field = term.field,
                .term = term.term,
            } },
        },
        .k = snapshot.global_doc_count,
        .include_stored = true,
    };
    return search_mod.execute(alloc, snapshot, request);
}

fn computeHistogramAggregation(
    alloc: Allocator,
    request: SearchAggregationRequest,
    hits: []const types.SearchHit,
    ctx: Context,
) anyerror!SearchAggregationResult {
    if (request.field.len == 0 or request.interval <= 0) return error.InvalidAggregation;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);

    var bucket_counts = std.AutoHashMap(i64, i64).init(alloc);
    defer bucket_counts.deinit();
    var grouped = std.AutoHashMap(i64, std.ArrayListUnmanaged(types.SearchHit)).init(alloc);
    defer {
        var it_grouped = grouped.iterator();
        while (it_grouped.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, stored, .{}) catch continue;
        defer parsed.deinit();
        const value = extractValueAtPath(parsed.value, request.field) orelse continue;
        if (jsonValueToF64(value)) |numeric| {
            const bucket_index = @as(i64, @intFromFloat(@floor(numeric / request.interval)));
            const entry = try bucket_counts.getOrPut(bucket_index);
            if (entry.found_existing) entry.value_ptr.* += 1 else entry.value_ptr.* = 1;
            const grouped_entry = try grouped.getOrPut(bucket_index);
            if (!grouped_entry.found_existing) grouped_entry.value_ptr.* = .empty;
            try grouped_entry.value_ptr.append(alloc, hit);
        }
    }

    var present_keys = try alloc.alloc(i64, bucket_counts.count());
    defer if (present_keys.len > 0) alloc.free(present_keys);
    var iter = bucket_counts.iterator();
    var present_count: usize = 0;
    while (iter.next()) |entry| {
        if (request.min_doc_count > 0 and entry.value_ptr.* < request.min_doc_count) continue;
        present_keys[present_count] = entry.key_ptr.*;
        present_count += 1;
    }
    std.mem.sort(i64, present_keys[0..present_count], {}, struct {
        fn lessThan(_: void, lhs: i64, rhs: i64) bool {
            return lhs < rhs;
        }
    }.lessThan);

    const keys = if (request.min_doc_count == 0 and present_count > 0)
        try fillHistogramBucketKeys(alloc, present_keys[0], present_keys[present_count - 1])
    else
        try alloc.dupe(i64, present_keys[0..present_count]);
    defer if (keys.len > 0) alloc.free(keys);

    var buckets = try alloc.alloc(SearchAggregationBucket, keys.len);
    errdefer {
        for (buckets[0..keys.len]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (keys, 0..) |bucket_index, i| {
        const nested = blk: {
            if (child_aggs.primary.len == 0) break :blk try alloc.alloc(SearchAggregationResult, 0);
            if (grouped.get(bucket_index)) |list| {
                break :blk try computeSearchAggregationsAtDepth(alloc, child_aggs.primary, .{
                    .alloc = alloc,
                    .hits = list.items,
                    .total_hits = @intCast(list.items.len),
                }, ctx, false);
            }
            break :blk try alloc.alloc(SearchAggregationResult, 0);
        };
        buckets[i] = .{
            .key_json = try std.fmt.allocPrint(alloc, "{d}", .{@as(f64, @floatFromInt(bucket_index)) * request.interval}),
            .count = bucket_counts.get(bucket_index) orelse 0,
            .aggregations = nested,
        };
    }
    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeDateHistogramAggregation(
    alloc: Allocator,
    request: SearchAggregationRequest,
    hits: []const types.SearchHit,
    ctx: Context,
) anyerror!SearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    const interval = try parseDateInterval(request);
    var agg = search_agg_mod.DateHistogramAgg.init(alloc, interval);
    defer agg.deinit();
    var grouped = std.AutoHashMap(u64, std.ArrayListUnmanaged(types.SearchHit)).init(alloc);
    defer {
        var it_grouped = grouped.iterator();
        while (it_grouped.next()) |entry| entry.value_ptr.deinit(alloc);
        grouped.deinit();
    }

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        const value = extractTimestampFieldFromStoredJson(alloc, stored, request.field) catch null;
        if (value) |ns| {
            try agg.collect(ns);
            const bucket_key = search_agg_mod.truncateToInterval(ns, interval);
            const entry = try grouped.getOrPut(bucket_key);
            if (!entry.found_existing) entry.value_ptr.* = .empty;
            try entry.value_ptr.append(alloc, hit);
        }
    }

    const present_keys = try agg.sortedKeys(alloc);
    defer if (present_keys.len > 0) alloc.free(present_keys);

    var kept: usize = 0;
    for (present_keys) |key| {
        const count = agg.getCount(key);
        if (request.min_doc_count > 0 and count < @as(u64, @intCast(request.min_doc_count))) continue;
        kept += 1;
    }

    const keys = if (request.min_doc_count == 0 and kept > 0)
        try fillDateHistogramBucketKeys(alloc, present_keys[0], present_keys[present_keys.len - 1], interval)
    else blk: {
        var filtered = try alloc.alloc(u64, kept);
        var idx: usize = 0;
        for (present_keys) |key| {
            const count = agg.getCount(key);
            if (request.min_doc_count > 0 and count < @as(u64, @intCast(request.min_doc_count))) continue;
            filtered[idx] = key;
            idx += 1;
        }
        break :blk filtered;
    };
    defer if (keys.len > 0) alloc.free(keys);

    var buckets = try alloc.alloc(SearchAggregationBucket, keys.len);
    errdefer {
        for (buckets[0..keys.len]) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (keys, 0..) |key, idx| {
        const formatted = try formatRfc3339Bucket(alloc, key);
        defer alloc.free(formatted);
        const nested = blk: {
            if (child_aggs.primary.len == 0) break :blk try alloc.alloc(SearchAggregationResult, 0);
            if (grouped.get(key)) |list| {
                break :blk try computeSearchAggregationsAtDepth(alloc, child_aggs.primary, .{
                    .alloc = alloc,
                    .hits = list.items,
                    .total_hits = @intCast(list.items.len),
                }, ctx, false);
            }
            break :blk try alloc.alloc(SearchAggregationResult, 0);
        };
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{formatted}),
            .count = @intCast(agg.getCount(key)),
            .aggregations = nested,
        };
    }
    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);

    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeRangeAggregation(
    alloc: Allocator,
    request: SearchAggregationRequest,
    hits: []const types.SearchHit,
    ctx: Context,
) anyerror!SearchAggregationResult {
    if (request.field.len == 0) return error.InvalidAggregation;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);
    const has_numeric = request.ranges.len > 0;
    const has_date = request.date_ranges.len > 0;
    const has_distance = request.distance_ranges.len > 0;
    if ((@intFromBool(has_numeric) + @intFromBool(has_date) + @intFromBool(has_distance)) != 1) return error.InvalidAggregation;

    if (has_numeric) {
        var buckets = try alloc.alloc(SearchAggregationBucket, request.ranges.len);
        errdefer {
            for (buckets) |*bucket| bucket.deinit(alloc);
            alloc.free(buckets);
        }
        for (request.ranges, 0..) |range_spec, idx| {
            var count: i64 = 0;
            var matched = std.ArrayListUnmanaged(types.SearchHit).empty;
            defer matched.deinit(alloc);
            for (hits) |hit| {
                const stored = hit.stored_data orelse continue;
                const value = extractNumericFieldFromStoredJson(alloc, stored, request.field) catch null;
                if (value) |numeric| {
                    if (matchesNumericRangeValue(numeric, range_spec)) {
                        count += 1;
                        try matched.append(alloc, hit);
                    }
                }
            }
            const nested = if (child_aggs.primary.len > 0) try computeSearchAggregationsAtDepth(alloc, child_aggs.primary, .{
                .alloc = alloc,
                .hits = matched.items,
                .total_hits = @intCast(matched.items.len),
            }, ctx, false) else try alloc.alloc(SearchAggregationResult, 0);
            buckets[idx] = .{
                .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
                .count = count,
                .aggregations = nested,
            };
        }
        try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .buckets = buckets,
        };
    }

    if (has_date) {
        var buckets = try alloc.alloc(SearchAggregationBucket, request.date_ranges.len);
        errdefer {
            for (buckets) |*bucket| bucket.deinit(alloc);
            alloc.free(buckets);
        }
        for (request.date_ranges, 0..) |range_spec, idx| {
            var count: i64 = 0;
            var matched = std.ArrayListUnmanaged(types.SearchHit).empty;
            defer matched.deinit(alloc);
            const start_ns = if (range_spec.start) |start| try parseRfc3339ToNs(start) else null;
            const end_ns = if (range_spec.end) |end| try parseRfc3339ToNs(end) else null;
            for (hits) |hit| {
                const stored = hit.stored_data orelse continue;
                const value = extractTimestampFieldFromStoredJson(alloc, stored, request.field) catch null;
                if (value) |timestamp| {
                    if (matchesDateRangeValue(timestamp, start_ns, end_ns)) {
                        count += 1;
                        try matched.append(alloc, hit);
                    }
                }
            }
            const nested = if (child_aggs.primary.len > 0) try computeSearchAggregationsAtDepth(alloc, child_aggs.primary, .{
                .alloc = alloc,
                .hits = matched.items,
                .total_hits = @intCast(matched.items.len),
            }, ctx, false) else try alloc.alloc(SearchAggregationResult, 0);
            buckets[idx] = .{
                .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
                .count = count,
                .aggregations = nested,
            };
        }
        try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
        return .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .buckets = buckets,
        };
    }

    var bands = try alloc.alloc(search_agg_mod.GeoDistanceRange, request.distance_ranges.len);
    defer alloc.free(bands);
    for (request.distance_ranges, 0..) |range_spec, idx| {
        bands[idx] = .{
            .from = if (range_spec.from) |from| try distanceToMeters(from, request.distance_unit) else null,
            .to = if (range_spec.to) |to| try distanceToMeters(to, request.distance_unit) else null,
        };
    }

    var agg = try search_agg_mod.GeoDistanceAgg.init(alloc, .{
        .lat = request.center_lat,
        .lon = request.center_lon,
    }, bands);
    defer agg.deinit();

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        const point = extractGeoPointFieldFromStoredJson(alloc, stored, request.field) catch null;
        if (point) |geo_point| {
            agg.collect(geo_point);
        }
    }

    var buckets = try alloc.alloc(SearchAggregationBucket, request.distance_ranges.len);
    errdefer {
        for (buckets) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (request.distance_ranges, 0..) |range_spec, idx| {
        var matched = std.ArrayListUnmanaged(types.SearchHit).empty;
        defer matched.deinit(alloc);
        const from_meters = if (range_spec.from) |from| try distanceToMeters(from, request.distance_unit) else null;
        const to_meters = if (range_spec.to) |to| try distanceToMeters(to, request.distance_unit) else null;
        for (hits) |hit| {
            const stored = hit.stored_data orelse continue;
            const point = extractGeoPointFieldFromStoredJson(alloc, stored, request.field) catch null;
            if (point) |geo_point| {
                const dist = geo_mod.haversineDistance(.{ .lat = request.center_lat, .lon = request.center_lon }, geo_point);
                if (matchesGeoDistanceValue(dist, from_meters, to_meters)) try matched.append(alloc, hit);
            }
        }
        const nested = if (child_aggs.primary.len > 0) try computeSearchAggregationsAtDepth(alloc, child_aggs.primary, .{
            .alloc = alloc,
            .hits = matched.items,
            .total_hits = @intCast(matched.items.len),
        }, ctx, false) else try alloc.alloc(SearchAggregationResult, 0);
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{range_spec.name}),
            .count = @intCast(agg.bands[idx].count),
            .aggregations = nested,
        };
    }
    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

fn computeGeohashGridAggregation(
    alloc: Allocator,
    request: SearchAggregationRequest,
    hits: []const types.SearchHit,
    ctx: Context,
) anyerror!SearchAggregationResult {
    if (request.field.len == 0 or request.geohash_precision == 0) return error.InvalidAggregation;
    const child_aggs = try splitAggregationRequests(alloc, request.aggregations);
    defer child_aggs.deinit(alloc);

    var agg = search_agg_mod.GeohashGridAgg.init(alloc, request.geohash_precision);
    defer agg.deinit();
    var grouped = std.StringHashMap(std.ArrayListUnmanaged(types.SearchHit)).init(alloc);
    defer {
        var it = grouped.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinit(alloc);
        }
        grouped.deinit();
    }

    for (hits) |hit| {
        const stored = hit.stored_data orelse continue;
        const point = extractGeoPointFieldFromStoredJson(alloc, stored, request.field) catch null;
        if (point) |geo_point| {
            try agg.collect(geo_point);
            const hash = geo_mod.encode(geo_point, request.geohash_precision);
            const key = hash[0..request.geohash_precision];
            const gop = try grouped.getOrPut(key);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, key);
                gop.value_ptr.* = .empty;
            }
            try gop.value_ptr.append(alloc, hit);
        }
    }

    const limit_u32: u32 = if (request.size > 0) @intCast(request.size) else 100;
    const entries = try agg.topK(alloc, limit_u32);
    defer alloc.free(entries);

    var buckets = try alloc.alloc(SearchAggregationBucket, entries.len);
    errdefer {
        for (buckets) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets);
    }
    for (entries, 0..) |entry, idx| {
        const nested = blk: {
            if (child_aggs.primary.len == 0) break :blk try alloc.alloc(SearchAggregationResult, 0);
            const grouped_hits = grouped.get(entry.geohash) orelse break :blk try alloc.alloc(SearchAggregationResult, 0);
            break :blk try computeSearchAggregationsAtDepth(alloc, child_aggs.primary, .{
                .alloc = alloc,
                .hits = grouped_hits.items,
                .total_hits = @intCast(grouped_hits.items.len),
            }, ctx, false);
        };
        buckets[idx] = .{
            .key_json = try std.fmt.allocPrint(alloc, "\"{s}\"", .{entry.geohash}),
            .count = @intCast(entry.count),
            .aggregations = nested,
        };
    }
    try applyPipelineAggregations(alloc, child_aggs.pipeline, &buckets);
    return .{
        .name = request.name,
        .field = request.field,
        .type = request.type,
        .buckets = buckets,
    };
}

const SplitAggregationRequests = struct {
    primary: []SearchAggregationRequest,
    pipeline: []SearchAggregationRequest,

    fn deinit(self: SplitAggregationRequests, alloc: Allocator) void {
        if (self.primary.len > 0) alloc.free(self.primary);
        if (self.pipeline.len > 0) alloc.free(self.pipeline);
    }
};

fn splitAggregationRequests(alloc: Allocator, requests: []const SearchAggregationRequest) !SplitAggregationRequests {
    var primary_count: usize = 0;
    var pipeline_count: usize = 0;
    for (requests) |request| {
        if (isPipelineAggregation(request.type))
            pipeline_count += 1
        else
            primary_count += 1;
    }
    const primary = try alloc.alloc(SearchAggregationRequest, primary_count);
    errdefer if (primary.len > 0) alloc.free(primary);
    const pipeline = try alloc.alloc(SearchAggregationRequest, pipeline_count);
    errdefer if (pipeline.len > 0) alloc.free(pipeline);
    var primary_idx: usize = 0;
    var pipeline_idx: usize = 0;
    for (requests) |request| {
        if (isPipelineAggregation(request.type)) {
            pipeline[pipeline_idx] = request;
            pipeline_idx += 1;
        } else {
            primary[primary_idx] = request;
            primary_idx += 1;
        }
    }
    return .{ .primary = primary, .pipeline = pipeline };
}

pub fn isPipelineAggregation(agg_type: []const u8) bool {
    return std.mem.eql(u8, agg_type, "bucket_sort") or
        std.mem.eql(u8, agg_type, "moving_avg") or
        std.mem.eql(u8, agg_type, "cumulative_sum") or
        std.mem.eql(u8, agg_type, "derivative") or
        std.mem.eql(u8, agg_type, "sum_bucket") or
        std.mem.eql(u8, agg_type, "avg_bucket") or
        std.mem.eql(u8, agg_type, "min_bucket") or
        std.mem.eql(u8, agg_type, "max_bucket") or
        std.mem.eql(u8, agg_type, "stats_bucket") or
        std.mem.eql(u8, agg_type, "extended_stats_bucket") or
        std.mem.eql(u8, agg_type, "percentiles_bucket");
}

pub fn computeRootPipelineAggregations(
    alloc: Allocator,
    pipeline_requests: []const SearchAggregationRequest,
    primary_results: []const SearchAggregationResult,
) ![]SearchAggregationResult {
    var out = try alloc.alloc(SearchAggregationResult, pipeline_requests.len);
    var out_filled: usize = 0;
    errdefer {
        for (out[0..out_filled]) |*agg| agg.deinit(alloc);
        alloc.free(out);
    }
    for (pipeline_requests, 0..) |request, i| {
        if (request.aggregations.len > 0) return error.UnsupportedAggregation;
        if (request.bucket_path.len == 0) return error.InvalidAggregation;

        if (std.mem.eql(u8, request.type, "sum_bucket") or
            std.mem.eql(u8, request.type, "avg_bucket") or
            std.mem.eql(u8, request.type, "min_bucket") or
            std.mem.eql(u8, request.type, "max_bucket") or
            std.mem.eql(u8, request.type, "stats_bucket") or
            std.mem.eql(u8, request.type, "extended_stats_bucket") or
            std.mem.eql(u8, request.type, "percentiles_bucket"))
        {
            const path_sep = std.mem.indexOfScalar(u8, request.bucket_path, '>') orelse return error.InvalidAggregation;
            const agg_name = request.bucket_path[0..path_sep];
            const bucket_metric_path = request.bucket_path[path_sep + 1 ..];
            if (agg_name.len == 0 or bucket_metric_path.len == 0) return error.InvalidAggregation;
            const source = findAggregationByName(primary_results, agg_name) orelse return error.InvalidAggregation;
            if (source.buckets.len == 0) return error.InvalidAggregation;

            var sum: f64 = 0;
            var sum_squares: f64 = 0;
            var min_value = std.math.inf(f64);
            var max_value = -std.math.inf(f64);
            var values = std.ArrayListUnmanaged(f64).empty;
            defer values.deinit(alloc);
            for (source.buckets) |bucket| {
                const value = try resolvePipelineBucketValue(alloc, bucket, bucket_metric_path);
                try values.append(alloc, value);
                sum += value;
                sum_squares += value * value;
                if (value < min_value) min_value = value;
                if (value > max_value) max_value = value;
            }
            const avg = sum / @as(f64, @floatFromInt(source.buckets.len));
            const value_json = if (std.mem.eql(u8, request.type, "sum_bucket"))
                try std.fmt.allocPrint(alloc, "{d}", .{sum})
            else if (std.mem.eql(u8, request.type, "avg_bucket"))
                try std.fmt.allocPrint(alloc, "{d}", .{avg})
            else if (std.mem.eql(u8, request.type, "min_bucket"))
                try std.fmt.allocPrint(alloc, "{d}", .{min_value})
            else if (std.mem.eql(u8, request.type, "max_bucket"))
                try std.fmt.allocPrint(alloc, "{d}", .{max_value})
            else if (std.mem.eql(u8, request.type, "extended_stats_bucket")) blk: {
                const variance = if (values.items.len == 0)
                    0
                else blk_var: {
                    const mean = avg;
                    var total_variance: f64 = 0;
                    for (values.items) |value| {
                        const diff = value - mean;
                        total_variance += diff * diff;
                    }
                    break :blk_var total_variance / @as(f64, @floatFromInt(values.items.len));
                };
                const std_dev = @sqrt(if (variance < 0) 0 else variance);
                break :blk try std.fmt.allocPrint(
                    alloc,
                    "{{\"count\":{d},\"sum\":{d},\"avg\":{d},\"min\":{d},\"max\":{d},\"sum_of_squares\":{d},\"variance\":{d},\"std_deviation\":{d}}}",
                    .{ values.items.len, sum, avg, min_value, max_value, sum_squares, variance, std_dev },
                );
            } else if (std.mem.eql(u8, request.type, "percentiles_bucket"))
                try buildPercentilesBucketValueJson(alloc, values.items)
            else
                try std.fmt.allocPrint(
                    alloc,
                    "{{\"count\":{d},\"sum\":{d},\"avg\":{d},\"min\":{d},\"max\":{d}}}",
                    .{ source.buckets.len, sum, avg, min_value, max_value },
                );
            out[i] = .{
                .name = request.name,
                .field = request.field,
                .type = request.type,
                .value_json = value_json,
            };
            out_filled = i + 1;
        } else {
            return error.UnsupportedAggregation;
        }
    }
    return out;
}

fn buildPercentilesBucketValueJson(alloc: Allocator, values: []const f64) ![]u8 {
    if (values.len == 0) return try alloc.dupe(u8, "{}");

    const percentiles = [_]f64{ 1, 5, 25, 50, 75, 95, 99 };
    const sorted = try alloc.dupe(f64, values);
    defer alloc.free(sorted);
    std.mem.sort(f64, sorted, {}, struct {
        fn lessThan(_: void, lhs: f64, rhs: f64) bool {
            return lhs < rhs;
        }
    }.lessThan);

    var list = std.ArrayListUnmanaged(u8).empty;
    errdefer list.deinit(alloc);
    try list.append(alloc, '{');
    for (percentiles, 0..) |p, i| {
        if (i > 0) try list.append(alloc, ',');
        const value = percentileValue(sorted, p);
        const entry = try std.fmt.allocPrint(alloc, "\"{d}\":{d}", .{ p, value });
        defer alloc.free(entry);
        try list.appendSlice(alloc, entry);
    }
    try list.append(alloc, '}');
    return try list.toOwnedSlice(alloc);
}

fn percentileValue(sorted: []const f64, percentile: f64) f64 {
    if (sorted.len == 0) return 0;
    if (sorted.len == 1) return sorted[0];

    const rank = (percentile / 100.0) * @as(f64, @floatFromInt(sorted.len - 1));
    const lower_index: usize = @intFromFloat(@floor(rank));
    const upper_index: usize = @intFromFloat(@ceil(rank));
    if (lower_index == upper_index) return sorted[lower_index];

    const lower = sorted[lower_index];
    const upper = sorted[upper_index];
    const fraction = rank - @as(f64, @floatFromInt(lower_index));
    return lower + ((upper - lower) * fraction);
}

fn findAggregationByName(results: []const SearchAggregationResult, name: []const u8) ?SearchAggregationResult {
    for (results) |result| {
        if (std.mem.eql(u8, result.name, name)) return result;
    }
    return null;
}

fn applyPipelineAggregations(
    alloc: Allocator,
    pipeline_requests: []const SearchAggregationRequest,
    buckets: *[]SearchAggregationBucket,
) !void {
    for (pipeline_requests) |request| {
        if (request.aggregations.len > 0) return error.UnsupportedAggregation;
        if (request.bucket_path.len == 0) return error.InvalidAggregation;

        if (std.mem.eql(u8, request.type, "bucket_sort")) {
            try applyBucketSort(alloc, request, buckets);
        } else if (std.mem.eql(u8, request.type, "moving_avg")) {
            try applyMovingAverage(alloc, request, buckets);
        } else if (std.mem.eql(u8, request.type, "cumulative_sum")) {
            var running: f64 = 0;
            for (buckets.*) |*bucket| {
                const value = try resolvePipelineBucketValue(alloc, bucket.*, request.bucket_path);
                running += value;
                try appendBucketAggregation(alloc, bucket, .{
                    .name = request.name,
                    .field = request.field,
                    .type = request.type,
                    .value_json = try std.fmt.allocPrint(alloc, "{d}", .{running}),
                });
            }
        } else if (std.mem.eql(u8, request.type, "derivative")) {
            var previous: ?f64 = null;
            for (buckets.*) |*bucket| {
                const value = try resolvePipelineBucketValue(alloc, bucket.*, request.bucket_path);
                const derivative_json = if (previous) |prev|
                    try std.fmt.allocPrint(alloc, "{d}", .{value - prev})
                else
                    try alloc.dupe(u8, "null");
                previous = value;
                try appendBucketAggregation(alloc, bucket, .{
                    .name = request.name,
                    .field = request.field,
                    .type = request.type,
                    .value_json = derivative_json,
                });
            }
        } else {
            return error.UnsupportedAggregation;
        }
    }
}

fn applyMovingAverage(
    alloc: Allocator,
    request: SearchAggregationRequest,
    buckets_ptr: *[]SearchAggregationBucket,
) !void {
    if (request.window <= 0) return error.InvalidAggregation;
    const insert_zeros = if (request.gap_policy.len == 0 or std.mem.eql(u8, request.gap_policy, "skip"))
        false
    else if (std.mem.eql(u8, request.gap_policy, "insert_zeros"))
        true
    else
        return error.InvalidAggregation;

    const window: usize = @intCast(request.window);
    var values = try alloc.alloc(?f64, buckets_ptr.*.len);
    defer alloc.free(values);
    var found_any_value = false;
    for (buckets_ptr.*, 0..) |bucket, i| {
        values[i] = resolvePipelineBucketValueOptional(alloc, bucket, request.bucket_path) catch |err| switch (err) {
            error.InvalidAggregation => null,
            else => return err,
        };
        if (values[i] != null) found_any_value = true;
    }
    if (!found_any_value) return error.InvalidAggregation;

    for (buckets_ptr.*, 0..) |*bucket, idx| {
        const start = if (idx + 1 > window) idx + 1 - window else 0;
        var sum: f64 = 0;
        var count: usize = 0;
        for (values[start .. idx + 1]) |maybe_value| {
            if (maybe_value) |value| {
                sum += value;
                count += 1;
            } else if (insert_zeros) {
                count += 1;
            }
        }
        const value_json = if (count == 0)
            try alloc.dupe(u8, "null")
        else
            try std.fmt.allocPrint(alloc, "{d}", .{sum / @as(f64, @floatFromInt(count))});
        try appendBucketAggregation(alloc, bucket, .{
            .name = request.name,
            .field = request.field,
            .type = request.type,
            .value_json = value_json,
        });
    }
}

fn applyBucketSort(
    alloc: Allocator,
    request: SearchAggregationRequest,
    buckets_ptr: *[]SearchAggregationBucket,
) !void {
    if (request.bucket_path.len == 0) return error.InvalidAggregation;
    if (request.from < 0) return error.InvalidAggregation;

    const descending = if (request.sort_order.len == 0)
        false
    else if (std.mem.eql(u8, request.sort_order, "asc"))
        false
    else if (std.mem.eql(u8, request.sort_order, "desc"))
        true
    else
        return error.InvalidAggregation;

    var sort_values = try alloc.alloc(f64, buckets_ptr.*.len);
    defer alloc.free(sort_values);
    for (buckets_ptr.*, 0..) |bucket, i| {
        sort_values[i] = try resolvePipelineBucketValue(alloc, bucket, request.bucket_path);
    }

    const SortContext = struct {
        descending: bool,
        sort_values: []const f64,
        buckets: []const SearchAggregationBucket,

        fn lessThan(ctx: @This(), lhs: usize, rhs: usize) bool {
            const lhs_value = ctx.sort_values[lhs];
            const rhs_value = ctx.sort_values[rhs];
            if (lhs_value == rhs_value) {
                return std.mem.order(u8, ctx.buckets[lhs].key_json, ctx.buckets[rhs].key_json) == .lt;
            }
            return if (ctx.descending) lhs_value > rhs_value else lhs_value < rhs_value;
        }
    };

    const permutation = try alloc.alloc(usize, buckets_ptr.*.len);
    defer alloc.free(permutation);
    for (permutation, 0..) |*entry, i| entry.* = i;
    std.mem.sort(usize, permutation, SortContext{
        .descending = descending,
        .sort_values = sort_values,
        .buckets = buckets_ptr.*,
    }, SortContext.lessThan);

    var sorted = try alloc.alloc(SearchAggregationBucket, buckets_ptr.*.len);
    errdefer alloc.free(sorted);
    for (permutation, 0..) |old_idx, new_idx| {
        sorted[new_idx] = buckets_ptr.*[old_idx];
    }

    const original = buckets_ptr.*;
    alloc.free(original);
    buckets_ptr.* = sorted;

    const start: usize = @intCast(request.from);
    if (start >= buckets_ptr.*.len) {
        for (buckets_ptr.*) |*bucket| bucket.deinit(alloc);
        alloc.free(buckets_ptr.*);
        buckets_ptr.* = try alloc.alloc(SearchAggregationBucket, 0);
        return;
    }
    const end: usize = if (request.size > 0)
        @min(buckets_ptr.*.len, start + @as(usize, @intCast(request.size)))
    else
        buckets_ptr.*.len;
    if (start == 0 and end == buckets_ptr.*.len) return;

    const retained = try alloc.alloc(SearchAggregationBucket, end - start);
    errdefer alloc.free(retained);
    for (retained, 0..) |*bucket, i| bucket.* = buckets_ptr.*[start + i];
    for (buckets_ptr.*[0..start]) |*bucket| bucket.deinit(alloc);
    for (buckets_ptr.*[end..]) |*bucket| bucket.deinit(alloc);
    alloc.free(buckets_ptr.*);
    buckets_ptr.* = retained;
}

fn resolvePipelineBucketValue(
    alloc: Allocator,
    bucket: SearchAggregationBucket,
    bucket_path: []const u8,
) !f64 {
    if (std.mem.eql(u8, bucket_path, "_count")) return @floatFromInt(bucket.count);
    for (bucket.aggregations) |aggregation| {
        if (!std.mem.eql(u8, aggregation.name, bucket_path)) continue;
        if (aggregation.value_json) |value_json| return try parseAggregationNumericValue(alloc, value_json);
        return error.InvalidAggregation;
    }
    return error.InvalidAggregation;
}

fn resolvePipelineBucketValueOptional(
    alloc: Allocator,
    bucket: SearchAggregationBucket,
    bucket_path: []const u8,
) !?f64 {
    if (std.mem.eql(u8, bucket_path, "_count")) return @floatFromInt(bucket.count);
    for (bucket.aggregations) |aggregation| {
        if (!std.mem.eql(u8, aggregation.name, bucket_path)) continue;
        if (aggregation.value_json) |value_json| {
            if (std.mem.eql(u8, value_json, "null")) return null;
            return try parseAggregationNumericValue(alloc, value_json);
        }
        return null;
    }
    return error.InvalidAggregation;
}

fn parseAggregationNumericValue(alloc: Allocator, raw: []const u8) !f64 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    return switch (parsed.value) {
        .integer => |v| @floatFromInt(v),
        .float => |v| v,
        .object => |obj| blk: {
            if (obj.get("value")) |value| break :blk try jsonValueRequireF64(value);
            if (obj.get("avg")) |value| break :blk try jsonValueRequireF64(value);
            if (obj.get("sum")) |value| break :blk try jsonValueRequireF64(value);
            break :blk error.InvalidAggregation;
        },
        else => error.InvalidAggregation,
    };
}

const StatsJsonExpectation = struct {
    count: f64,
    sum: f64,
    avg: f64,
    min: f64,
    max: f64,
    sum_squares: f64,
    variance: f64,
    std_dev: f64,
};

fn expectStatsJsonApprox(alloc: Allocator, raw: []const u8, expected: StatsJsonExpectation) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidAggregation,
    };
    try std.testing.expectApproxEqAbs(expected.count, try jsonValueRequireF64(object.get("count") orelse return error.InvalidAggregation), 1e-9);
    try std.testing.expectApproxEqAbs(expected.sum, try jsonValueRequireF64(object.get("sum") orelse return error.InvalidAggregation), 1e-9);
    try std.testing.expectApproxEqAbs(expected.avg, try jsonValueRequireF64(object.get("avg") orelse return error.InvalidAggregation), 1e-9);
    try std.testing.expectApproxEqAbs(expected.min, try jsonValueRequireF64(object.get("min") orelse return error.InvalidAggregation), 1e-9);
    try std.testing.expectApproxEqAbs(expected.max, try jsonValueRequireF64(object.get("max") orelse return error.InvalidAggregation), 1e-9);
    try std.testing.expectApproxEqAbs(expected.sum_squares, try jsonValueRequireF64(object.get("sum_squares") orelse return error.InvalidAggregation), 1e-9);
    try std.testing.expectApproxEqAbs(expected.variance, try jsonValueRequireF64(object.get("variance") orelse return error.InvalidAggregation), 1e-9);
    try std.testing.expectApproxEqAbs(expected.std_dev, try jsonValueRequireF64(object.get("std_dev") orelse return error.InvalidAggregation), 1e-9);
}

fn findAggregationBucketByKey(buckets: []const SearchAggregationBucket, key_json: []const u8) ?SearchAggregationBucket {
    for (buckets) |bucket| {
        if (std.mem.eql(u8, bucket.key_json, key_json)) return bucket;
    }
    return null;
}

fn regexMatches(alloc: Allocator, pattern: []const u8, candidate: []const u8) !bool {
    var regex = try regex_mod.compile(alloc, pattern);
    defer regex.deinit();
    return regex_mod.matchesCompiled(pattern, &regex, candidate);
}

fn jsonValueRequireF64(value: std.json.Value) !f64 {
    return switch (value) {
        .integer => |v| @floatFromInt(v),
        .float => |v| v,
        else => error.InvalidAggregation,
    };
}

fn appendBucketAggregation(
    alloc: Allocator,
    bucket: *SearchAggregationBucket,
    aggregation: SearchAggregationResult,
) !void {
    const existing_len = bucket.aggregations.len;
    const next = try alloc.alloc(SearchAggregationResult, existing_len + 1);
    errdefer alloc.free(next);
    if (existing_len > 0) {
        @memcpy(next[0..existing_len], bucket.aggregations);
        alloc.free(bucket.aggregations);
    }
    next[existing_len] = aggregation;
    bucket.aggregations = next;
}

fn calculateSignificanceScore(
    algorithm: []const u8,
    fg_count: i64,
    fg_total: i64,
    bg_count: i64,
    bg_total: i64,
) f64 {
    if (std.mem.eql(u8, algorithm, "mutual_information")) {
        return calculateMutualInformation(fg_count, fg_total, bg_count, bg_total);
    }
    if (std.mem.eql(u8, algorithm, "chi_squared")) {
        return calculateChiSquared(fg_count, fg_total, bg_count, bg_total);
    }
    if (std.mem.eql(u8, algorithm, "percentage")) {
        return calculatePercentage(fg_count, fg_total, bg_count, bg_total);
    }
    return calculateJLH(fg_count, fg_total, bg_count, bg_total);
}

fn calculateJLH(fg_count: i64, fg_total: i64, bg_count: i64, bg_total: i64) f64 {
    if (fg_total == 0 or bg_total == 0 or bg_count == 0) return 0;
    const fg_rate = @as(f64, @floatFromInt(fg_count)) / @as(f64, @floatFromInt(fg_total));
    const bg_rate = @as(f64, @floatFromInt(bg_count)) / @as(f64, @floatFromInt(bg_total));
    if (bg_rate == 0 or fg_rate <= bg_rate) return 0;
    return fg_rate * std.math.log2(fg_rate / bg_rate);
}

fn calculateMutualInformation(fg_count: i64, fg_total: i64, bg_count: i64, bg_total: i64) f64 {
    const N = @as(f64, @floatFromInt(bg_total));
    if (N == 0) return 0;

    const fixed_bg = if (bg_count < fg_count) fg_count else bg_count;
    const N11 = @as(f64, @floatFromInt(fg_count));
    const N10 = @as(f64, @floatFromInt(fixed_bg - fg_count));
    const N01 = @as(f64, @floatFromInt(fg_total - fg_count));
    const N00 = N - N11 - N10 - N01;
    if (N11 <= 0 or N10 < 0 or N01 < 0 or N00 < 0) return 0;
    if (N10 == 0 or N01 == 0) return @as(f64, @floatFromInt(fg_count)) / @as(f64, @floatFromInt(fg_total));

    const score = (N11 / N) * std.math.log2((N * N11) / ((N11 + N10) * (N11 + N01)));
    if (std.math.isNan(score) or std.math.isInf(score)) return 0;
    return score;
}

fn calculateChiSquared(fg_count: i64, fg_total: i64, bg_count: i64, bg_total: i64) f64 {
    if (fg_total == 0 or bg_total == 0) return 0;
    const N = @as(f64, @floatFromInt(bg_total));
    const observed = @as(f64, @floatFromInt(fg_count));
    const expected = (@as(f64, @floatFromInt(fg_total)) * @as(f64, @floatFromInt(bg_count))) / N;
    if (expected == 0) return 0;
    const chi = std.math.pow(f64, observed - expected, 2) / expected;
    if (std.math.isNan(chi) or std.math.isInf(chi)) return 0;
    return chi;
}

fn calculatePercentage(fg_count: i64, fg_total: i64, bg_count: i64, bg_total: i64) f64 {
    if (fg_total == 0 or bg_total == 0 or bg_count == 0) return 0;
    const fg_rate = @as(f64, @floatFromInt(fg_count)) / @as(f64, @floatFromInt(fg_total));
    const bg_rate = @as(f64, @floatFromInt(bg_count)) / @as(f64, @floatFromInt(bg_total));
    if (bg_rate == 0) return 0;
    const score = (fg_rate / bg_rate) - 1.0;
    if (std.math.isNan(score) or std.math.isInf(score)) return 0;
    return score;
}

fn collectSignificantTermsFromValue(
    alloc: Allocator,
    value: std.json.Value,
    seen_terms: *std.StringHashMap(void),
) !void {
    switch (value) {
        .array => |arr| for (arr.items) |item| try collectSignificantTermsFromValue(alloc, item, seen_terms),
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

fn matchesNumericRangeValue(value: f64, range_spec: NumericRangeRequest) bool {
    if (range_spec.start) |start| {
        if (value < start) return false;
    }
    if (range_spec.end) |end| {
        if (value >= end) return false;
    }
    return true;
}

fn matchesDateRangeValue(value: u64, start_ns: ?u64, end_ns: ?u64) bool {
    if (start_ns) |start| {
        if (value < start) return false;
    }
    if (end_ns) |end| {
        if (value >= end) return false;
    }
    return true;
}

fn matchesGeoDistanceValue(value_meters: f64, from_meters: ?f64, to_meters: ?f64) bool {
    if (from_meters) |from| {
        if (value_meters < from) return false;
    }
    if (to_meters) |to| {
        if (value_meters >= to) return false;
    }
    return true;
}

test "significant_terms can use distributed text stats without a local text index" {
    const alloc = std.testing.allocator;

    const hits = try alloc.alloc(types.SearchHit, 2);
    defer {
        for (hits) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .stored_data = try alloc.dupe(u8, "{\"body\":\"alpha beta\"}"),
    };
    hits[1] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .stored_data = try alloc.dupe(u8, "{\"body\":\"alpha\"}"),
    };

    var term_doc_freqs = [_]distributed_stats_mod.TermDocFreq{
        .{ .term = try alloc.dupe(u8, "alpha"), .doc_freq = 3 },
        .{ .term = try alloc.dupe(u8, "beta"), .doc_freq = 1 },
    };
    defer for (&term_doc_freqs) |*item| item.deinit(alloc);

    const requests = [_]SearchAggregationRequest{.{
        .name = "sig_terms",
        .type = "significant_terms",
        .field = "body",
        .size = 2,
    }};

    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 2,
    };

    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .distributed_text_stats = &.{.{
            .field = "body",
            .global_doc_count = 4,
            .global_total_field_len = 0,
            .term_doc_freqs = &term_doc_freqs,
        }},
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expect(aggregations[0].metadata_json != null);
    try std.testing.expect(std.mem.indexOf(u8, aggregations[0].metadata_json.?, "\"bg_doc_count\":4") != null);
    try std.testing.expectEqual(@as(usize, 2), aggregations[0].buckets.len);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[0].bg_count.?);
    try std.testing.expectEqual(@as(i64, 3), aggregations[0].buckets[1].bg_count.?);
}

test "significant_terms can use distributed background stats without a local text index" {
    const alloc = std.testing.allocator;

    const hits = try alloc.alloc(types.SearchHit, 2);
    defer {
        for (hits) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .stored_data = try alloc.dupe(u8, "{\"body\":\"alpha beta\"}"),
    };
    hits[1] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .stored_data = try alloc.dupe(u8, "{\"body\":\"alpha\"}"),
    };

    var term_doc_freqs = [_]distributed_stats_mod.TermDocFreq{
        .{ .term = try alloc.dupe(u8, "alpha"), .doc_freq = 2 },
        .{ .term = try alloc.dupe(u8, "beta"), .doc_freq = 1 },
    };
    defer for (&term_doc_freqs) |*item| item.deinit(alloc);

    const requests = [_]SearchAggregationRequest{.{
        .name = "sig_terms",
        .type = "significant_terms",
        .field = "body",
        .size = 2,
        .background_query = .{ .match = .{ .field = "body", .text = "alpha" } },
    }};

    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 2,
    };

    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .distributed_background_text_stats = &.{.{
            .aggregation_name = "sig_terms",
            .field = "body",
            .background_doc_count = 2,
            .term_doc_freqs = &term_doc_freqs,
        }},
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expect(aggregations[0].metadata_json != null);
    try std.testing.expect(std.mem.indexOf(u8, aggregations[0].metadata_json.?, "\"bg_doc_count\":2") != null);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[0].bg_count.?);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[1].bg_count.?);
}

test "algebraic distributed partials build exact terms response" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"count_by_customer","op":"count","group_by":["customer"]},
        \\    {"name":"sum_by_customer","op":"sum","group_by":["customer"],"measure":"amount"}
        \\  ]
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":10}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":20}" },
        .{ .key = "l3", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"amount\":3}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":7}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"customer\":\"carol\",\"amount\":5}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const materializations = [_][]const u8{ "count_by_customer", "sum_by_customer" };
    const left_partials = try left_idx.scanDistributedPartials(&left_store, materializations[0..]);
    defer algebraic_mod.distributed.freePartials(alloc, left_partials);
    const right_partials = try right_idx.scanDistributedPartials(&right_store, materializations[0..]);
    defer algebraic_mod.distributed.freePartials(alloc, right_partials);
    var partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_partials.len + right_partials.len);
    defer alloc.free(partials);
    @memcpy(partials[0..left_partials.len], left_partials);
    @memcpy(partials[left_partials.len..], right_partials);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    const nested = [_]SearchAggregationRequest{
        .{
            .name = "total_amount",
            .type = "sum",
            .field = "amount",
        },
        .{
            .name = "running_total",
            .type = "cumulative_sum",
            .field = "",
            .bucket_path = "total_amount",
        },
    };
    const request = SearchAggregationRequest{
        .name = "by_customer",
        .type = "terms",
        .field = "customer",
        .size = 1,
        .aggregations = nested[0..],
    };
    const planned_materializations = (try algebraicDistributedTermsMaterializationsAlloc(alloc, &left_idx, request, &.{})) orelse return error.TestUnexpectedResult;
    defer freeAlgebraicDistributedMaterializations(alloc, planned_materializations);
    try std.testing.expectEqual(@as(usize, 2), planned_materializations.len);
    try std.testing.expectEqualStrings("count_by_customer", planned_materializations[0]);
    try std.testing.expectEqualStrings("sum_by_customer", planned_materializations[1]);

    var aggregation = (try algebraicTermsAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"alice\"", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 3), aggregation.buckets[0].count);
    try std.testing.expectEqual(@as(usize, 2), aggregation.buckets[0].aggregations.len);
    try std.testing.expectEqualStrings("total_amount", aggregation.buckets[0].aggregations[0].name);
    try std.testing.expectEqualStrings("37", aggregation.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("running_total", aggregation.buckets[0].aggregations[1].name);
    try std.testing.expectEqualStrings("37", aggregation.buckets[0].aggregations[1].value_json.?);
}

test "algebraic distributed partials build exact terms response with nested stats" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"count_by_customer","op":"count","group_by":["customer"]},
        \\    {"name":"avg_amount","op":"avg","group_by":["customer"],"measure":"amount"},
        \\    {"name":"min_amount","op":"min","group_by":["customer"],"measure":"amount"},
        \\    {"name":"max_amount","op":"max","group_by":["customer"],"measure":"amount"},
        \\    {"name":"sum_squares_amount","op":"sumsquares","group_by":["customer"],"measure":"amount"}
        \\  ]
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":10}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"amount\":3}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":20}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"customer\":\"carol\",\"amount\":5}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const nested = [_]SearchAggregationRequest{.{
        .name = "amount_stats",
        .type = "stats",
        .field = "amount",
    }};
    const request = SearchAggregationRequest{
        .name = "by_customer",
        .type = "terms",
        .field = "customer",
        .size = 1,
        .aggregations = nested[0..],
    };
    const planned_materializations = (try algebraicDistributedTermsMaterializationsAlloc(alloc, &left_idx, request, &.{})) orelse return error.TestUnexpectedResult;
    defer freeAlgebraicDistributedMaterializations(alloc, planned_materializations);
    try std.testing.expectEqual(@as(usize, 5), planned_materializations.len);
    try std.testing.expectEqualStrings("count_by_customer", planned_materializations[0]);
    try std.testing.expectEqualStrings("avg_amount", planned_materializations[1]);
    try std.testing.expectEqualStrings("min_amount", planned_materializations[2]);
    try std.testing.expectEqualStrings("max_amount", planned_materializations[3]);
    try std.testing.expectEqualStrings("sum_squares_amount", planned_materializations[4]);

    const left_partials = try left_idx.scanDistributedPartials(&left_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, left_partials);
    const right_partials = try right_idx.scanDistributedPartials(&right_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, right_partials);
    var partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_partials.len + right_partials.len);
    defer alloc.free(partials);
    @memcpy(partials[0..left_partials.len], left_partials);
    @memcpy(partials[left_partials.len..], right_partials);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    var aggregation = (try algebraicTermsAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"alice\"", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregation.buckets[0].count);
    try std.testing.expectEqual(@as(usize, 1), aggregation.buckets[0].aggregations.len);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":30,\"avg\":15,\"min\":10,\"max\":20,\"sum_squares\":500,\"variance\":25,\"std_dev\":5}", aggregation.buckets[0].aggregations[0].value_json.?);
}

test "algebraic distributed partials merge canonical axes across independent shard dictionaries" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"count_by_customer","op":"count","group_by":["customer"]},
        \\    {"name":"sum_by_customer","op":"sum","group_by":["customer"],"measure":"amount"},
        \\    {"name":"avg_amount","op":"avg","group_by":["customer"],"measure":"amount"},
        \\    {"name":"min_amount","op":"min","group_by":["customer"],"measure":"amount"},
        \\    {"name":"max_amount","op":"max","group_by":["customer"],"measure":"amount"},
        \\    {"name":"sum_squares_amount","op":"sumsquares","group_by":["customer"],"measure":"amount"}
        \\  ]
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l0", .action = .upsert, .cleaned_value = "{\"customer\":\"left_only\",\"amount\":1}" },
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":10}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":15}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r0", .action = .upsert, .cleaned_value = "{\"customer\":\"right_only\",\"amount\":4}" },
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":20}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":5}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const alice_token = try left_idx.constraintTokenAlloc(alloc, "customer", "alice");
    defer alloc.free(alice_token);
    const alice_axis = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{alice_token});
    defer alloc.free(alice_axis);
    const left_only_token = try left_idx.constraintTokenAlloc(alloc, "customer", "left_only");
    defer alloc.free(left_only_token);
    const left_only_axis = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{left_only_token});
    defer alloc.free(left_only_axis);
    const right_only_token = try right_idx.constraintTokenAlloc(alloc, "customer", "right_only");
    defer alloc.free(right_only_token);
    const right_only_axis = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{right_only_token});
    defer alloc.free(right_only_axis);
    const left_only_symbol = (try left_idx.symbolValueAlloc(&left_store, left_only_axis)) orelse return error.TestUnexpectedResult;
    defer alloc.free(left_only_symbol);
    const right_only_symbol = (try right_idx.symbolValueAlloc(&right_store, right_only_axis)) orelse return error.TestUnexpectedResult;
    defer alloc.free(right_only_symbol);

    const nested = [_]SearchAggregationRequest{.{
        .name = "amount_stats",
        .type = "stats",
        .field = "amount",
    }};
    const request = SearchAggregationRequest{
        .name = "by_customer",
        .type = "terms",
        .field = "customer",
        .size = 1,
        .aggregations = nested[0..],
    };
    const planned_materializations = (try algebraicDistributedTermsMaterializationsAlloc(alloc, &left_idx, request, &.{})) orelse return error.TestUnexpectedResult;
    defer freeAlgebraicDistributedMaterializations(alloc, planned_materializations);

    const left_partials = try left_idx.scanDistributedPartials(&left_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, left_partials);
    const right_partials = try right_idx.scanDistributedPartials(&right_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, right_partials);
    var saw_left_alice = false;
    var saw_right_alice = false;
    for (left_partials) |partial| {
        if (std.mem.eql(u8, partial.canonical_axis, alice_axis)) saw_left_alice = true;
    }
    for (right_partials) |partial| {
        if (std.mem.eql(u8, partial.canonical_axis, alice_axis)) saw_right_alice = true;
    }
    try std.testing.expect(saw_left_alice);
    try std.testing.expect(saw_right_alice);

    var partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_partials.len + right_partials.len);
    defer alloc.free(partials);
    @memcpy(partials[0..left_partials.len], left_partials);
    @memcpy(partials[left_partials.len..], right_partials);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    var aggregation = (try algebraicTermsAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"alice\"", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 4), aggregation.buckets[0].count);
    try std.testing.expectEqual(@as(usize, 1), aggregation.buckets[0].aggregations.len);
    try expectStatsJsonApprox(alloc, aggregation.buckets[0].aggregations[0].value_json.?, .{
        .count = 4,
        .sum = 50,
        .avg = 12.5,
        .min = 5,
        .max = 20,
        .sum_squares = 750,
        .variance = 31.25,
        .std_dev = 5.5901699437494745,
    });
}

test "algebraic distributed partials build exact constrained metric response" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"sum_by_customer","op":"sum","group_by":["customer"],"measure":"amount"}
        \\  ]
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":10}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"amount\":3}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":20}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":7}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const request = SearchAggregationRequest{
        .name = "total_amount",
        .type = "sum",
        .field = "amount",
    };
    const constraints = [_]FixedConstraint{.{ .field = "customer", .value = "alice" }};
    const planned_materializations = (try algebraicDistributedMetricMaterializationsAlloc(alloc, &left_idx, request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer freeAlgebraicDistributedMaterializations(alloc, planned_materializations);
    try std.testing.expectEqual(@as(usize, 1), planned_materializations.len);
    try std.testing.expectEqualStrings("sum_by_customer", planned_materializations[0]);

    const left_partials = try left_idx.scanDistributedPartials(&left_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, left_partials);
    const right_partials = try right_idx.scanDistributedPartials(&right_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, right_partials);
    var partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_partials.len + right_partials.len);
    defer alloc.free(partials);
    @memcpy(partials[0..left_partials.len], left_partials);
    @memcpy(partials[left_partials.len..], right_partials);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    var aggregation = (try algebraicMetricAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, constraints[0..], merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqualStrings("total_amount", aggregation.name);
    try std.testing.expectEqualStrings("37", aggregation.value_json.?);
}

test "algebraic distributed partials build exact constrained stats response" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"avg_amount","op":"avg","group_by":["customer"],"measure":"amount"},
        \\    {"name":"min_amount","op":"min","group_by":["customer"],"measure":"amount"},
        \\    {"name":"max_amount","op":"max","group_by":["customer"],"measure":"amount"},
        \\    {"name":"sum_squares_amount","op":"sumsquares","group_by":["customer"],"measure":"amount"}
        \\  ]
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":10}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"amount\":3}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":20}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\"}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const stats_request = SearchAggregationRequest{
        .name = "amount_stats",
        .type = "stats",
        .field = "amount",
    };
    const constraints = [_]FixedConstraint{.{ .field = "customer", .value = "alice" }};
    const planned_materializations = (try algebraicDistributedStatsMaterializationsAlloc(alloc, &left_idx, stats_request, constraints[0..])) orelse return error.TestUnexpectedResult;
    defer freeAlgebraicDistributedMaterializations(alloc, planned_materializations);
    try std.testing.expectEqual(@as(usize, 4), planned_materializations.len);
    try std.testing.expectEqualStrings("avg_amount", planned_materializations[0]);
    try std.testing.expectEqualStrings("min_amount", planned_materializations[1]);
    try std.testing.expectEqualStrings("max_amount", planned_materializations[2]);
    try std.testing.expectEqualStrings("sum_squares_amount", planned_materializations[3]);

    const left_partials = try left_idx.scanDistributedPartials(&left_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, left_partials);
    const right_partials = try right_idx.scanDistributedPartials(&right_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, right_partials);
    var partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_partials.len + right_partials.len);
    defer alloc.free(partials);
    @memcpy(partials[0..left_partials.len], left_partials);
    @memcpy(partials[left_partials.len..], right_partials);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    var stats_aggregation = (try algebraicStatsAggregationFromDistributedPartialsAlloc(alloc, &left_idx, stats_request, constraints[0..], merged)) orelse return error.TestUnexpectedResult;
    defer stats_aggregation.deinit(alloc);
    try std.testing.expectEqualStrings("amount_stats", stats_aggregation.name);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":30,\"avg\":15,\"min\":10,\"max\":20,\"sum_squares\":500,\"variance\":25,\"std_dev\":5}", stats_aggregation.value_json.?);

    const sumsquares_request = SearchAggregationRequest{
        .name = "amount_squares",
        .type = "sumsquares",
        .field = "amount",
    };
    var sumsquares_aggregation = (try algebraicMetricAggregationFromDistributedPartialsAlloc(alloc, &left_idx, sumsquares_request, constraints[0..], merged)) orelse return error.TestUnexpectedResult;
    defer sumsquares_aggregation.deinit(alloc);
    try std.testing.expectEqualStrings("500", sumsquares_aggregation.value_json.?);
}

test "algebraic distributed partials build exact date histogram response" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [{"name":"created","path":"created_at","type":"timestamp"}],
        \\  "materializations": [
        \\    {"name":"orders_by_day","op":"count","group_by":[],"time":"created","bucket":"day"},
        \\    {"name":"amount_by_day","op":"sum","group_by":[],"measure":"amount","time":"created","bucket":"day"},
        \\    {"name":"avg_amount_by_day","op":"avg","group_by":[],"measure":"amount","time":"created","bucket":"day"},
        \\    {"name":"min_amount_by_day","op":"min","group_by":[],"measure":"amount","time":"created","bucket":"day"},
        \\    {"name":"max_amount_by_day","op":"max","group_by":[],"measure":"amount","time":"created","bucket":"day"},
        \\    {"name":"sum_squares_amount_by_day","op":"sumsquares","group_by":[],"measure":"amount","time":"created","bucket":"day"}
        \\  ]
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"created_at\":\"2026-05-01T10:00:00Z\",\"amount\":10}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"created_at\":\"2026-05-02T09:00:00Z\",\"amount\":5}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"created_at\":\"2026-05-01T11:00:00Z\",\"amount\":20}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"created_at\":\"2026-05-01T12:00:00Z\",\"amount\":7}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const nested = [_]SearchAggregationRequest{
        .{
            .name = "amount_by_day",
            .type = "sum",
            .field = "amount",
        },
        .{
            .name = "amount_stats",
            .type = "stats",
            .field = "amount",
        },
        .{
            .name = "running_amount",
            .type = "cumulative_sum",
            .field = "",
            .bucket_path = "amount_by_day",
        },
    };
    const request = SearchAggregationRequest{
        .name = "orders_by_day",
        .type = "date_histogram",
        .field = "created_at",
        .calendar_interval = "day",
        .aggregations = nested[0..],
    };
    const planned_materializations = (try algebraicDistributedDateHistogramMaterializationsAlloc(alloc, &left_idx, request, &.{})) orelse return error.TestUnexpectedResult;
    defer freeAlgebraicDistributedMaterializations(alloc, planned_materializations);
    try std.testing.expectEqual(@as(usize, 6), planned_materializations.len);
    try std.testing.expectEqualStrings("orders_by_day", planned_materializations[0]);
    try std.testing.expectEqualStrings("amount_by_day", planned_materializations[1]);
    try std.testing.expectEqualStrings("avg_amount_by_day", planned_materializations[2]);
    try std.testing.expectEqualStrings("min_amount_by_day", planned_materializations[3]);
    try std.testing.expectEqualStrings("max_amount_by_day", planned_materializations[4]);
    try std.testing.expectEqualStrings("sum_squares_amount_by_day", planned_materializations[5]);

    const left_partials = try left_idx.scanDistributedPartials(&left_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, left_partials);
    const right_partials = try right_idx.scanDistributedPartials(&right_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, right_partials);
    var partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_partials.len + right_partials.len);
    defer alloc.free(partials);
    @memcpy(partials[0..left_partials.len], left_partials);
    @memcpy(partials[left_partials.len..], right_partials);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    var aggregation = (try algebraicDateHistogramAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"2026-05-01T00:00:00Z\"", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 3), aggregation.buckets[0].count);
    try std.testing.expectEqual(@as(usize, 3), aggregation.buckets[0].aggregations.len);
    try std.testing.expectEqualStrings("37", aggregation.buckets[0].aggregations[0].value_json.?);
    try expectStatsJsonApprox(alloc, aggregation.buckets[0].aggregations[1].value_json.?, .{
        .count = 3,
        .sum = 37,
        .avg = 12.333333333333334,
        .min = 7,
        .max = 20,
        .sum_squares = 549,
        .variance = 30.888888888888886,
        .std_dev = 5.557777333511022,
    });
    try std.testing.expectEqualStrings("37", aggregation.buckets[0].aggregations[2].value_json.?);
    try std.testing.expectEqualStrings("\"2026-05-02T00:00:00Z\"", aggregation.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregation.buckets[1].count);
    try std.testing.expectEqual(@as(usize, 3), aggregation.buckets[1].aggregations.len);
    try std.testing.expectEqualStrings("5", aggregation.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":5,\"avg\":5,\"min\":5,\"max\":5,\"sum_squares\":25,\"variance\":0,\"std_dev\":0}", aggregation.buckets[1].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("42", aggregation.buckets[1].aggregations[2].value_json.?);

    const primary = [_]SearchAggregationResult{aggregation};
    const pipeline_requests = [_]SearchAggregationRequest{.{
        .name = "all_days_amount",
        .type = "sum_bucket",
        .field = "",
        .bucket_path = "orders_by_day>amount_by_day",
    }};
    const pipeline = try computeRootPipelineAggregations(alloc, pipeline_requests[0..], primary[0..]);
    defer {
        for (pipeline) |*result| result.deinit(alloc);
        if (pipeline.len > 0) alloc.free(pipeline);
    }
    try std.testing.expectEqual(@as(usize, 1), pipeline.len);
    try std.testing.expectEqualStrings("all_days_amount", pipeline[0].name);
    try std.testing.expectEqualStrings("42", pipeline[0].value_json.?);
}

test "algebraic distributed partials build exact numeric histogram response" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"score","path":"score","type":"integer"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"count_by_score","op":"count","group_by":["score"]},
        \\    {"name":"amount_by_score","op":"sum","group_by":["score"],"measure":"amount"},
        \\    {"name":"avg_amount_by_score","op":"avg","group_by":["score"],"measure":"amount"},
        \\    {"name":"min_amount_by_score","op":"min","group_by":["score"],"measure":"amount"},
        \\    {"name":"max_amount_by_score","op":"max","group_by":["score"],"measure":"amount"},
        \\    {"name":"sum_squares_amount_by_score","op":"sumsquares","group_by":["score"],"measure":"amount"}
        \\  ]
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"score\":5,\"amount\":10}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"score\":15,\"amount\":20}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"score\":15,\"amount\":7}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"score\":25,\"amount\":5}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const nested = [_]SearchAggregationRequest{
        .{
            .name = "amount_sum",
            .type = "sum",
            .field = "amount",
        },
        .{
            .name = "amount_stats",
            .type = "stats",
            .field = "amount",
        },
    };
    const request = SearchAggregationRequest{
        .name = "score_histogram",
        .type = "histogram",
        .field = "score",
        .interval = 10,
        .aggregations = nested[0..],
    };
    const planned_materializations = (try algebraicDistributedHistogramMaterializationsAlloc(alloc, &left_idx, request, &.{})) orelse return error.TestUnexpectedResult;
    defer freeAlgebraicDistributedMaterializations(alloc, planned_materializations);
    try std.testing.expectEqual(@as(usize, 6), planned_materializations.len);
    try std.testing.expectEqualStrings("count_by_score", planned_materializations[0]);
    try std.testing.expectEqualStrings("amount_by_score", planned_materializations[1]);
    try std.testing.expectEqualStrings("avg_amount_by_score", planned_materializations[2]);
    try std.testing.expectEqualStrings("min_amount_by_score", planned_materializations[3]);
    try std.testing.expectEqualStrings("max_amount_by_score", planned_materializations[4]);
    try std.testing.expectEqualStrings("sum_squares_amount_by_score", planned_materializations[5]);

    const left_partials = try left_idx.scanDistributedPartials(&left_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, left_partials);
    const right_partials = try right_idx.scanDistributedPartials(&right_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, right_partials);
    var partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_partials.len + right_partials.len);
    defer alloc.free(partials);
    @memcpy(partials[0..left_partials.len], left_partials);
    @memcpy(partials[left_partials.len..], right_partials);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    var aggregation = (try algebraicHistogramAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), aggregation.buckets.len);
    try std.testing.expectEqualStrings("0", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregation.buckets[0].count);
    try std.testing.expectEqualStrings("10", aggregation.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":10,\"avg\":10,\"min\":10,\"max\":10,\"sum_squares\":100,\"variance\":0,\"std_dev\":0}", aggregation.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("10", aggregation.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregation.buckets[1].count);
    try std.testing.expectEqualStrings("27", aggregation.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":27,\"avg\":13.5,\"min\":7,\"max\":20,\"sum_squares\":449,\"variance\":42.25,\"std_dev\":6.5}", aggregation.buckets[1].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("20", aggregation.buckets[2].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregation.buckets[2].count);
    try std.testing.expectEqualStrings("5", aggregation.buckets[2].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":5,\"avg\":5,\"min\":5,\"max\":5,\"sum_squares\":25,\"variance\":0,\"std_dev\":0}", aggregation.buckets[2].aggregations[1].value_json.?);
}

test "algebraic distributed partials build measure histogram from docfact tensor program" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"tenant","path":"tenant","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": []
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"tenant\":\"t1\",\"amount\":7}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"tenant\":\"t1\",\"amount\":20}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"tenant\":\"t1\",\"amount\":25}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"tenant\":\"t2\",\"amount\":27}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const constraints = [_]algebraic_mod.ir.Constraint{.{ .field = "tenant", .value = "t1" }};
    var program_plan = (try algebraic_mod.planner.planDocFactBucketFoldTensorProgramAlloc(alloc, &left_idx, .{
        .kind = .histogram,
        .op = .count,
        .bucket_field = "amount",
        .bucket_role = .measure,
        .histogram_interval = 10,
        .constraints = constraints[0..],
    }, "amount_histogram")) orelse return error.TestUnexpectedResult;
    defer program_plan.deinit(alloc);
    const program = program_plan.asProgram();

    const left_partials = (try left_idx.scanDistributedPartialsForTensorProgram(&left_store, program_plan.access_paths, program)) orelse return error.TestUnexpectedResult;
    defer algebraic_mod.distributed.freePartials(alloc, left_partials);
    const right_partials = (try right_idx.scanDistributedPartialsForTensorProgram(&right_store, program_plan.access_paths, program)) orelse return error.TestUnexpectedResult;
    defer algebraic_mod.distributed.freePartials(alloc, right_partials);
    var partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_partials.len + right_partials.len);
    defer alloc.free(partials);
    @memcpy(partials[0..left_partials.len], left_partials);
    @memcpy(partials[left_partials.len..], right_partials);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    const request = SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 10,
    };
    var aggregation = (try algebraicHistogramAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), aggregation.buckets.len);
    try std.testing.expectEqualStrings("0", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregation.buckets[0].count);
    try std.testing.expectEqualStrings("10", aggregation.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 0), aggregation.buckets[1].count);
    try std.testing.expectEqualStrings("20", aggregation.buckets[2].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregation.buckets[2].count);
}

test "algebraic distributed partials build schemaless histogram from pathfact tensor program" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{"version":1,"table":"orders","materializations":[]}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"amount\":7}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"amount\":\"20\"}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"amount\":25}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const folds = [_]algebraic_mod.index.PathFactBucketFoldRequest{
        .{
            .kind = .histogram,
            .op = .count,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .histogram_interval = 10,
        },
        .{
            .kind = .histogram,
            .op = .sum,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .histogram_interval = 10,
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .histogram,
            .op = .avg,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .histogram_interval = 10,
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .histogram,
            .op = .min,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .histogram_interval = 10,
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .histogram,
            .op = .max,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .histogram_interval = 10,
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .histogram,
            .op = .sumsquares,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .histogram_interval = 10,
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .histogram,
            .op = .count,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .histogram_interval = 10,
        },
        .{
            .kind = .histogram,
            .op = .sum,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .histogram_interval = 10,
            .measure_path = "/amount",
            .measure_kind = .string,
        },
        .{
            .kind = .histogram,
            .op = .avg,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .histogram_interval = 10,
            .measure_path = "/amount",
            .measure_kind = .string,
        },
        .{
            .kind = .histogram,
            .op = .min,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .histogram_interval = 10,
            .measure_path = "/amount",
            .measure_kind = .string,
        },
        .{
            .kind = .histogram,
            .op = .max,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .histogram_interval = 10,
            .measure_path = "/amount",
            .measure_kind = .string,
        },
        .{
            .kind = .histogram,
            .op = .sumsquares,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .histogram_interval = 10,
            .measure_path = "/amount",
            .measure_kind = .string,
        },
    };
    var partials_list = std.ArrayListUnmanaged(algebraic_mod.distributed.Partial).empty;
    defer {
        for (partials_list.items) |partial| {
            alloc.free(@constCast(partial.canonical_axis));
            if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
            alloc.free(@constCast(partial.value));
        }
        partials_list.deinit(alloc);
    }
    const access_paths = [_]algebraic_mod.ir.PhysicalAccessPath{algebraic_mod.ir.pathFactAccessPath(left_idx.name)};
    for (folds) |fold| {
        const metadata = try algebraic_mod.index.pathFactBucketFoldMetadataAlloc(alloc, fold);
        defer alloc.free(metadata);
        const semantic_id = switch (fold.op) {
            .count => "amount_histogram",
            .sum => "amount_sum",
            .avg, .min, .max, .sumsquares => "amount_stats",
        };
        const steps = [_]algebraic_mod.ir.TensorProgramStep{
            .{ .expr = .{
                .fragment = .slice,
                .output_dims = &.{ .doc, .path, .kind, .scalar },
                .owner = left_idx.name,
                .layout = .pathfact_rows,
            } },
            .{
                .expr = .{
                    .fragment = .reduce,
                    .input_dims = &.{ .doc, .path, .kind, .scalar },
                    .output_dims = &.{ .bucket, .scalar },
                    .semantic_id = semantic_id,
                    .law_id = algebraic_mod.law.fromOp(fold.op),
                    .metadata = metadata,
                },
                .inputs = &.{.{ .step = 0 }},
            },
        };
        const program = algebraic_mod.ir.TensorProgram{
            .steps = steps[0..],
            .output = .{ .step = 1 },
        };
        const left_partials = (try left_idx.scanDistributedPartialsForTensorProgram(&left_store, access_paths[0..], program)) orelse return error.TestUnexpectedResult;
        defer alloc.free(left_partials);
        try partials_list.appendSlice(alloc, left_partials);
        const right_partials = (try right_idx.scanDistributedPartialsForTensorProgram(&right_store, access_paths[0..], program)) orelse return error.TestUnexpectedResult;
        defer alloc.free(right_partials);
        try partials_list.appendSlice(alloc, right_partials);
    }
    const partials = try alloc.alloc(algebraic_mod.distributed.Partial, partials_list.items.len);
    defer alloc.free(partials);
    @memcpy(partials, partials_list.items);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    const request = SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "/amount",
        .interval = 10,
        .aggregations = &.{
            .{
                .name = "amount_sum",
                .type = "sum",
                .field = "/amount",
            },
            .{
                .name = "amount_stats",
                .type = "stats",
                .field = "/amount",
            },
        },
    };
    var aggregation = (try algebraicHistogramAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), aggregation.buckets.len);
    try std.testing.expectEqualStrings("0", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregation.buckets[0].count);
    try std.testing.expectEqualStrings("7", aggregation.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":7,\"avg\":7,\"min\":7,\"max\":7,\"sum_squares\":49,\"variance\":0,\"std_dev\":0}", aggregation.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("20", aggregation.buckets[2].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregation.buckets[2].count);
    try std.testing.expectEqualStrings("45", aggregation.buckets[2].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":45,\"avg\":22.5,\"min\":20,\"max\":25,\"sum_squares\":1025,\"variance\":6.25,\"std_dev\":2.5}", aggregation.buckets[2].aggregations[1].value_json.?);
}

test "algebraic distributed partials build schemaless range from pathfact tensor program" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{"version":1,"table":"orders","materializations":[]}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"amount\":7}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"amount\":\"20\"}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"amount\":25}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const folds = [_]algebraic_mod.index.PathFactBucketFoldRequest{
        .{
            .kind = .range,
            .op = .count,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .range_start = "0",
            .range_end = "20",
        },
        .{
            .kind = .range,
            .op = .sum,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .range_start = "0",
            .range_end = "20",
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .range,
            .op = .avg,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .range_start = "0",
            .range_end = "20",
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .range,
            .op = .min,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .range_start = "0",
            .range_end = "20",
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .range,
            .op = .max,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .range_start = "0",
            .range_end = "20",
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .range,
            .op = .sumsquares,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .range_start = "0",
            .range_end = "20",
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .range,
            .op = .count,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .range_start = "0",
            .range_end = "20",
        },
        .{
            .kind = .range,
            .op = .sum,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .range_start = "0",
            .range_end = "20",
            .measure_path = "/amount",
            .measure_kind = .string,
        },
        .{
            .kind = .range,
            .op = .avg,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .range_start = "0",
            .range_end = "20",
            .measure_path = "/amount",
            .measure_kind = .string,
        },
        .{
            .kind = .range,
            .op = .min,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .range_start = "0",
            .range_end = "20",
            .measure_path = "/amount",
            .measure_kind = .string,
        },
        .{
            .kind = .range,
            .op = .max,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .range_start = "0",
            .range_end = "20",
            .measure_path = "/amount",
            .measure_kind = .string,
        },
        .{
            .kind = .range,
            .op = .sumsquares,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .range_start = "0",
            .range_end = "20",
            .measure_path = "/amount",
            .measure_kind = .string,
        },
        .{
            .kind = .range,
            .op = .count,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .range_start = "20",
            .range_end = "30",
        },
        .{
            .kind = .range,
            .op = .sum,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .range_start = "20",
            .range_end = "30",
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .range,
            .op = .avg,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .range_start = "20",
            .range_end = "30",
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .range,
            .op = .min,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .range_start = "20",
            .range_end = "30",
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .range,
            .op = .max,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .range_start = "20",
            .range_end = "30",
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .range,
            .op = .sumsquares,
            .bucket_path = "/amount",
            .bucket_kind = .number,
            .range_start = "20",
            .range_end = "30",
            .measure_path = "/amount",
            .measure_kind = .number,
        },
        .{
            .kind = .range,
            .op = .count,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .range_start = "20",
            .range_end = "30",
        },
        .{
            .kind = .range,
            .op = .sum,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .range_start = "20",
            .range_end = "30",
            .measure_path = "/amount",
            .measure_kind = .string,
        },
        .{
            .kind = .range,
            .op = .avg,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .range_start = "20",
            .range_end = "30",
            .measure_path = "/amount",
            .measure_kind = .string,
        },
        .{
            .kind = .range,
            .op = .min,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .range_start = "20",
            .range_end = "30",
            .measure_path = "/amount",
            .measure_kind = .string,
        },
        .{
            .kind = .range,
            .op = .max,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .range_start = "20",
            .range_end = "30",
            .measure_path = "/amount",
            .measure_kind = .string,
        },
        .{
            .kind = .range,
            .op = .sumsquares,
            .bucket_path = "/amount",
            .bucket_kind = .string,
            .range_start = "20",
            .range_end = "30",
            .measure_path = "/amount",
            .measure_kind = .string,
        },
    };
    var partials_list = std.ArrayListUnmanaged(algebraic_mod.distributed.Partial).empty;
    defer {
        for (partials_list.items) |partial| {
            alloc.free(@constCast(partial.canonical_axis));
            if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
            alloc.free(@constCast(partial.value));
        }
        partials_list.deinit(alloc);
    }
    const access_paths = [_]algebraic_mod.ir.PhysicalAccessPath{algebraic_mod.ir.pathFactAccessPath(left_idx.name)};
    for (folds) |fold| {
        const metadata = try algebraic_mod.index.pathFactBucketFoldMetadataAlloc(alloc, fold);
        defer alloc.free(metadata);
        const semantic_id = switch (fold.op) {
            .count => "amount_ranges",
            .sum => "amount_sum",
            .avg, .min, .max, .sumsquares => "amount_stats",
        };
        const steps = [_]algebraic_mod.ir.TensorProgramStep{
            .{ .expr = .{
                .fragment = .slice,
                .output_dims = &.{ .doc, .path, .kind, .scalar },
                .owner = left_idx.name,
                .layout = .pathfact_rows,
            } },
            .{
                .expr = .{
                    .fragment = .reduce,
                    .input_dims = &.{ .doc, .path, .kind, .scalar },
                    .output_dims = &.{ .bucket, .scalar },
                    .semantic_id = semantic_id,
                    .law_id = algebraic_mod.law.fromOp(fold.op),
                    .metadata = metadata,
                },
                .inputs = &.{.{ .step = 0 }},
            },
        };
        const program = algebraic_mod.ir.TensorProgram{
            .steps = steps[0..],
            .output = .{ .step = 1 },
        };
        const left_partials = (try left_idx.scanDistributedPartialsForTensorProgram(&left_store, access_paths[0..], program)) orelse return error.TestUnexpectedResult;
        defer alloc.free(left_partials);
        try partials_list.appendSlice(alloc, left_partials);
        const right_partials = (try right_idx.scanDistributedPartialsForTensorProgram(&right_store, access_paths[0..], program)) orelse return error.TestUnexpectedResult;
        defer alloc.free(right_partials);
        try partials_list.appendSlice(alloc, right_partials);
    }
    const partials = try alloc.alloc(algebraic_mod.distributed.Partial, partials_list.items.len);
    defer alloc.free(partials);
    @memcpy(partials, partials_list.items);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    const request = SearchAggregationRequest{
        .name = "amount_ranges",
        .type = "range",
        .field = "/amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 30 },
        },
        .aggregations = &.{
            .{
                .name = "amount_sum",
                .type = "sum",
                .field = "/amount",
            },
            .{
                .name = "amount_stats",
                .type = "stats",
                .field = "/amount",
            },
        },
    };
    var aggregation = (try algebraicRangeAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"low\"", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregation.buckets[0].count);
    try std.testing.expectEqualStrings("7", aggregation.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":7,\"avg\":7,\"min\":7,\"max\":7,\"sum_squares\":49,\"variance\":0,\"std_dev\":0}", aggregation.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"high\"", aggregation.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregation.buckets[1].count);
    try std.testing.expectEqualStrings("45", aggregation.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":45,\"avg\":22.5,\"min\":20,\"max\":25,\"sum_squares\":1025,\"variance\":6.25,\"std_dev\":2.5}", aggregation.buckets[1].aggregations[1].value_json.?);
}

test "algebraic distributed partials build measure range from docfact tensor program" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"tenant","path":"tenant","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": []
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"tenant\":\"t1\",\"amount\":7}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"tenant\":\"t1\",\"amount\":20}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"tenant\":\"t1\",\"amount\":25}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"tenant\":\"t2\",\"amount\":27}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const constraints = [_]algebraic_mod.ir.Constraint{.{ .field = "tenant", .value = "t1" }};
    const folds = [_]algebraic_mod.index.DocFactBucketFoldRequest{
        .{
            .kind = .range,
            .op = .count,
            .bucket_field = "amount",
            .bucket_role = .measure,
            .range_start = "0",
            .range_end = "20",
            .constraints = constraints[0..],
        },
        .{
            .kind = .range,
            .op = .count,
            .bucket_field = "amount",
            .bucket_role = .measure,
            .range_start = "20",
            .range_end = "30",
            .constraints = constraints[0..],
        },
    };
    var left_partials_list = std.ArrayListUnmanaged(algebraic_mod.distributed.Partial).empty;
    defer {
        for (left_partials_list.items) |partial| {
            alloc.free(@constCast(partial.canonical_axis));
            if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
            alloc.free(@constCast(partial.value));
        }
        left_partials_list.deinit(alloc);
    }
    var right_partials_list = std.ArrayListUnmanaged(algebraic_mod.distributed.Partial).empty;
    defer {
        for (right_partials_list.items) |partial| {
            alloc.free(@constCast(partial.canonical_axis));
            if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
            alloc.free(@constCast(partial.value));
        }
        right_partials_list.deinit(alloc);
    }
    for (folds) |fold| {
        var plan = (try algebraic_mod.planner.planDocFactBucketFoldTensorProgramAlloc(alloc, &left_idx, fold, "amount_ranges")) orelse return error.TestUnexpectedResult;
        defer plan.deinit(alloc);
        const program = plan.asProgram();
        const left_partials = (try left_idx.scanDistributedPartialsForTensorProgram(&left_store, plan.access_paths, program)) orelse return error.TestUnexpectedResult;
        defer alloc.free(left_partials);
        try left_partials_list.appendSlice(alloc, left_partials);
        const right_partials = (try right_idx.scanDistributedPartialsForTensorProgram(&right_store, plan.access_paths, program)) orelse return error.TestUnexpectedResult;
        defer alloc.free(right_partials);
        try right_partials_list.appendSlice(alloc, right_partials);
    }
    var partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_partials_list.items.len + right_partials_list.items.len);
    defer alloc.free(partials);
    @memcpy(partials[0..left_partials_list.items.len], left_partials_list.items);
    @memcpy(partials[left_partials_list.items.len..], right_partials_list.items);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    const request = SearchAggregationRequest{
        .name = "amount_ranges",
        .type = "range",
        .field = "amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 30 },
        },
    };
    var aggregation = (try algebraicRangeAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"low\"", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregation.buckets[0].count);
    try std.testing.expectEqualStrings("\"high\"", aggregation.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregation.buckets[1].count);
}

test "algebraic distributed partials build date range from docfact tensor program" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"tenant","path":"tenant","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [{"name":"created_at","path":"created_at","type":"datetime"}],
        \\  "materializations": []
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"tenant\":\"t1\",\"created_at\":\"2026-05-01T13:00:00Z\",\"amount\":5}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"tenant\":\"t1\",\"created_at\":\"2026-05-02T10:00:00Z\",\"amount\":\"10\"}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"tenant\":\"t1\",\"created_at\":\"2026-05-02T12:00:00Z\",\"amount\":20}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"tenant\":\"t2\",\"created_at\":\"2026-05-02T12:00:00Z\",\"amount\":30}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const constraints = [_]algebraic_mod.ir.Constraint{.{ .field = "tenant", .value = "t1" }};
    const folds = [_]algebraic_mod.index.DocFactBucketFoldRequest{
        .{
            .kind = .date_range,
            .op = .count,
            .bucket_field = "created_at",
            .bucket_role = .time,
            .range_start = "2026-05-01T12:00:00Z",
            .range_end = "2026-05-02T00:00:00Z",
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .sum,
            .bucket_field = "created_at",
            .bucket_role = .time,
            .range_start = "2026-05-01T12:00:00Z",
            .range_end = "2026-05-02T00:00:00Z",
            .measure = "amount",
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .count,
            .bucket_field = "created_at",
            .bucket_role = .time,
            .range_start = "2026-05-02T00:00:00Z",
            .range_end = "2026-05-03T00:00:00Z",
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .sum,
            .bucket_field = "created_at",
            .bucket_role = .time,
            .range_start = "2026-05-02T00:00:00Z",
            .range_end = "2026-05-03T00:00:00Z",
            .measure = "amount",
            .constraints = constraints[0..],
        },
    };
    var left_partials_list = std.ArrayListUnmanaged(algebraic_mod.distributed.Partial).empty;
    defer {
        for (left_partials_list.items) |partial| {
            alloc.free(@constCast(partial.canonical_axis));
            if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
            alloc.free(@constCast(partial.value));
        }
        left_partials_list.deinit(alloc);
    }
    var right_partials_list = std.ArrayListUnmanaged(algebraic_mod.distributed.Partial).empty;
    defer {
        for (right_partials_list.items) |partial| {
            alloc.free(@constCast(partial.canonical_axis));
            if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
            alloc.free(@constCast(partial.value));
        }
        right_partials_list.deinit(alloc);
    }
    for (folds) |fold| {
        const semantic_id = if (fold.op == .count) "created_ranges" else "amount_sum";
        var plan = (try algebraic_mod.planner.planDocFactBucketFoldTensorProgramAlloc(alloc, &left_idx, fold, semantic_id)) orelse return error.TestUnexpectedResult;
        defer plan.deinit(alloc);
        const program = plan.asProgram();
        const left_partials = (try left_idx.scanDistributedPartialsForTensorProgram(&left_store, plan.access_paths, program)) orelse return error.TestUnexpectedResult;
        defer alloc.free(left_partials);
        try left_partials_list.appendSlice(alloc, left_partials);
        const right_partials = (try right_idx.scanDistributedPartialsForTensorProgram(&right_store, plan.access_paths, program)) orelse return error.TestUnexpectedResult;
        defer alloc.free(right_partials);
        try right_partials_list.appendSlice(alloc, right_partials);
    }
    var partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_partials_list.items.len + right_partials_list.items.len);
    defer alloc.free(partials);
    @memcpy(partials[0..left_partials_list.items.len], left_partials_list.items);
    @memcpy(partials[left_partials_list.items.len..], right_partials_list.items);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    const request = SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "created_at",
        .date_ranges = &.{
            .{ .name = "first", .start = "2026-05-01T12:00:00Z", .end = "2026-05-02T00:00:00Z" },
            .{ .name = "second", .start = "2026-05-02T00:00:00Z", .end = "2026-05-03T00:00:00Z" },
        },
        .aggregations = &.{.{
            .name = "amount_sum",
            .type = "sum",
            .field = "amount",
        }},
    };
    var aggregation = (try algebraicDateRangeAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"first\"", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregation.buckets[0].count);
    try std.testing.expectEqualStrings("5", aggregation.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("\"second\"", aggregation.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregation.buckets[1].count);
    try std.testing.expectEqualStrings("30", aggregation.buckets[1].aggregations[0].value_json.?);
}

test "algebraic distributed partials build schemaless date range from pathfact tensor program" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{"version":1,"table":"orders","materializations":[]}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"tenant\":\"t1\",\"created_at\":\"2026-05-01T13:00:00Z\",\"amount\":5}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"tenant\":\"t1\",\"created_at\":\"2026-05-02T10:00:00Z\",\"amount\":\"10\"}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"tenant\":\"t1\",\"created_at\":\"2026-05-02T12:00:00Z\",\"amount\":20}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"tenant\":\"t2\",\"created_at\":\"2026-05-02T12:00:00Z\",\"amount\":30}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const constraints = [_]algebraic_mod.ir.Constraint{.{ .field = "/tenant", .value = "t1" }};
    const folds = [_]algebraic_mod.index.PathFactBucketFoldRequest{
        .{
            .kind = .date_range,
            .op = .count,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-01T12:00:00Z",
            .range_end = "2026-05-02T00:00:00Z",
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .sum,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-01T12:00:00Z",
            .range_end = "2026-05-02T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .number,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .avg,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-01T12:00:00Z",
            .range_end = "2026-05-02T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .number,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .min,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-01T12:00:00Z",
            .range_end = "2026-05-02T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .number,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .max,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-01T12:00:00Z",
            .range_end = "2026-05-02T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .number,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .sumsquares,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-01T12:00:00Z",
            .range_end = "2026-05-02T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .number,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .sum,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-01T12:00:00Z",
            .range_end = "2026-05-02T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .string,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .avg,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-01T12:00:00Z",
            .range_end = "2026-05-02T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .string,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .min,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-01T12:00:00Z",
            .range_end = "2026-05-02T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .string,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .max,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-01T12:00:00Z",
            .range_end = "2026-05-02T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .string,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .sumsquares,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-01T12:00:00Z",
            .range_end = "2026-05-02T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .string,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .count,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-02T00:00:00Z",
            .range_end = "2026-05-03T00:00:00Z",
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .sum,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-02T00:00:00Z",
            .range_end = "2026-05-03T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .number,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .avg,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-02T00:00:00Z",
            .range_end = "2026-05-03T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .number,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .min,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-02T00:00:00Z",
            .range_end = "2026-05-03T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .number,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .max,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-02T00:00:00Z",
            .range_end = "2026-05-03T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .number,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .sumsquares,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-02T00:00:00Z",
            .range_end = "2026-05-03T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .number,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .sum,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-02T00:00:00Z",
            .range_end = "2026-05-03T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .string,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .avg,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-02T00:00:00Z",
            .range_end = "2026-05-03T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .string,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .min,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-02T00:00:00Z",
            .range_end = "2026-05-03T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .string,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .max,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-02T00:00:00Z",
            .range_end = "2026-05-03T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .string,
            .constraints = constraints[0..],
        },
        .{
            .kind = .date_range,
            .op = .sumsquares,
            .bucket_path = "/created_at",
            .bucket_kind = .string,
            .range_start = "2026-05-02T00:00:00Z",
            .range_end = "2026-05-03T00:00:00Z",
            .measure_path = "/amount",
            .measure_kind = .string,
            .constraints = constraints[0..],
        },
    };
    var partials_list = std.ArrayListUnmanaged(algebraic_mod.distributed.Partial).empty;
    defer {
        for (partials_list.items) |partial| {
            alloc.free(@constCast(partial.canonical_axis));
            if (partial.metric.len > 0) alloc.free(@constCast(partial.metric));
            alloc.free(@constCast(partial.value));
        }
        partials_list.deinit(alloc);
    }
    const access_paths = [_]algebraic_mod.ir.PhysicalAccessPath{algebraic_mod.ir.pathFactAccessPath(left_idx.name)};
    for (folds) |fold| {
        const metadata = try algebraic_mod.index.pathFactBucketFoldMetadataAlloc(alloc, fold);
        defer alloc.free(metadata);
        const semantic_id = switch (fold.op) {
            .count => "created_ranges",
            .sum => "amount_sum",
            .avg, .min, .max, .sumsquares => "amount_stats",
        };
        const steps = [_]algebraic_mod.ir.TensorProgramStep{
            .{ .expr = .{
                .fragment = .slice,
                .output_dims = &.{ .doc, .path, .kind, .scalar },
                .owner = left_idx.name,
                .layout = .pathfact_rows,
            } },
            .{
                .expr = .{
                    .fragment = .reduce,
                    .input_dims = &.{ .doc, .path, .kind, .scalar },
                    .output_dims = &.{ .bucket, .scalar },
                    .semantic_id = semantic_id,
                    .law_id = algebraic_mod.law.fromOp(fold.op),
                    .metadata = metadata,
                },
                .inputs = &.{.{ .step = 0 }},
            },
        };
        const program = algebraic_mod.ir.TensorProgram{
            .steps = steps[0..],
            .output = .{ .step = 1 },
        };
        const left_partials = (try left_idx.scanDistributedPartialsForTensorProgram(&left_store, access_paths[0..], program)) orelse return error.TestUnexpectedResult;
        defer alloc.free(left_partials);
        try partials_list.appendSlice(alloc, left_partials);
        const right_partials = (try right_idx.scanDistributedPartialsForTensorProgram(&right_store, access_paths[0..], program)) orelse return error.TestUnexpectedResult;
        defer alloc.free(right_partials);
        try partials_list.appendSlice(alloc, right_partials);
    }
    const partials = try alloc.alloc(algebraic_mod.distributed.Partial, partials_list.items.len);
    defer alloc.free(partials);
    @memcpy(partials, partials_list.items);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    const request = SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "/created_at",
        .date_ranges = &.{
            .{ .name = "first", .start = "2026-05-01T12:00:00Z", .end = "2026-05-02T00:00:00Z" },
            .{ .name = "second", .start = "2026-05-02T00:00:00Z", .end = "2026-05-03T00:00:00Z" },
        },
        .aggregations = &.{
            .{
                .name = "amount_sum",
                .type = "sum",
                .field = "/amount",
            },
            .{
                .name = "amount_stats",
                .type = "stats",
                .field = "/amount",
            },
        },
    };
    var aggregation = (try algebraicDateRangeAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"first\"", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregation.buckets[0].count);
    try std.testing.expectEqualStrings("5", aggregation.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":5,\"avg\":5,\"min\":5,\"max\":5,\"sum_squares\":25,\"variance\":0,\"std_dev\":0}", aggregation.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"second\"", aggregation.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregation.buckets[1].count);
    try std.testing.expectEqualStrings("30", aggregation.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":30,\"avg\":15,\"min\":10,\"max\":20,\"sum_squares\":500,\"variance\":25,\"std_dev\":5}", aggregation.buckets[1].aggregations[1].value_json.?);
}

test "algebraic distributed partials build schemaless terms from pathfact tensor program" {
    const alloc = std.testing.allocator;

    var idx = try algebraic_mod.index.Index.open(alloc, "alg_path_terms_response",
        \\{"version":1,"materializations":[]}
    );
    defer idx.close();

    const gold_axis = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{ "string", "gold" });
    defer alloc.free(gold_axis);
    const silver_axis = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{ "string", "silver" });
    defer alloc.free(silver_axis);
    const bool_axis = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{ "bool", "true" });
    defer alloc.free(bool_axis);
    const null_axis = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{ "null", "" });
    defer alloc.free(null_axis);
    const object_axis = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{ "object", "" });
    defer alloc.free(object_axis);
    const array_axis = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{ "array", "" });
    defer alloc.free(array_axis);
    const partials = [_]algebraic_mod.distributed.Partial{
        .{ .canonical_axis = gold_axis, .metric = "tiers", .law_id = .count, .value = "1" },
        .{ .canonical_axis = silver_axis, .metric = "tiers", .law_id = .count, .value = "1" },
        .{ .canonical_axis = gold_axis, .metric = "tiers", .law_id = .count, .value = "1" },
        .{ .canonical_axis = bool_axis, .metric = "tiers", .law_id = .count, .value = "1" },
        .{ .canonical_axis = null_axis, .metric = "tiers", .law_id = .count, .value = "1" },
        .{ .canonical_axis = object_axis, .metric = "tiers", .law_id = .count, .value = "1" },
        .{ .canonical_axis = array_axis, .metric = "tiers", .law_id = .count, .value = "1" },
    };
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials[0..]);
    defer merged.deinit(alloc);

    var aggregation = (try algebraicTermsAggregationFromDistributedPartialsAlloc(alloc, &idx, .{
        .name = "tiers",
        .type = "terms",
        .field = "/tier",
    }, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 6), aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"gold\"", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregation.buckets[0].count);

    var saw_silver = false;
    var saw_bool = false;
    var saw_null = false;
    var saw_object = false;
    var saw_array = false;
    for (aggregation.buckets[1..]) |bucket| {
        if (std.mem.eql(u8, bucket.key_json, "\"silver\"")) {
            saw_silver = true;
            try std.testing.expectEqual(@as(i64, 1), bucket.count);
        } else if (std.mem.eql(u8, bucket.key_json, "true")) {
            saw_bool = true;
            try std.testing.expectEqual(@as(i64, 1), bucket.count);
        } else if (std.mem.eql(u8, bucket.key_json, "null")) {
            saw_null = true;
            try std.testing.expectEqual(@as(i64, 1), bucket.count);
        } else if (std.mem.eql(u8, bucket.key_json, "{\"kind\":\"object\",\"structural\":true}")) {
            saw_object = true;
            try std.testing.expectEqual(@as(i64, 1), bucket.count);
        } else if (std.mem.eql(u8, bucket.key_json, "{\"kind\":\"array\",\"structural\":true}")) {
            saw_array = true;
            try std.testing.expectEqual(@as(i64, 1), bucket.count);
        }
    }
    try std.testing.expect(saw_silver);
    try std.testing.expect(saw_bool);
    try std.testing.expect(saw_null);
    try std.testing.expect(saw_object);
    try std.testing.expect(saw_array);

    var prefixed = (try algebraicTermsAggregationFromDistributedPartialsAlloc(alloc, &idx, .{
        .name = "tiers",
        .type = "terms",
        .field = "/tier",
        .term_prefix = "g",
    }, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer prefixed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), prefixed.buckets.len);
    try std.testing.expectEqualStrings("\"gold\"", prefixed.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), prefixed.buckets[0].count);
}

test "algebraic distributed partials build exact numeric range response" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"score","path":"score","type":"integer"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"count_by_score","op":"count","group_by":["score"]},
        \\    {"name":"amount_by_score","op":"sum","group_by":["score"],"measure":"amount"},
        \\    {"name":"avg_amount_by_score","op":"avg","group_by":["score"],"measure":"amount"},
        \\    {"name":"min_amount_by_score","op":"min","group_by":["score"],"measure":"amount"},
        \\    {"name":"max_amount_by_score","op":"max","group_by":["score"],"measure":"amount"},
        \\    {"name":"sum_squares_amount_by_score","op":"sumsquares","group_by":["score"],"measure":"amount"}
        \\  ]
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"score\":5,\"amount\":10}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"score\":15,\"amount\":20}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"score\":15,\"amount\":7}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"score\":25,\"amount\":5}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const nested = [_]SearchAggregationRequest{
        .{
            .name = "amount_sum",
            .type = "sum",
            .field = "amount",
        },
        .{
            .name = "amount_stats",
            .type = "stats",
            .field = "amount",
        },
    };
    const request = SearchAggregationRequest{
        .name = "score_ranges",
        .type = "range",
        .field = "score",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 10 },
            .{ .name = "mid", .start = 10, .end = 20 },
            .{ .name = "all", .start = 0, .end = 30 },
        },
        .aggregations = nested[0..],
    };
    const planned_materializations = (try algebraicDistributedRangeMaterializationsAlloc(alloc, &left_idx, request, &.{})) orelse return error.TestUnexpectedResult;
    defer freeAlgebraicDistributedMaterializations(alloc, planned_materializations);
    try std.testing.expectEqual(@as(usize, 6), planned_materializations.len);

    const left_partials = try left_idx.scanDistributedPartials(&left_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, left_partials);
    const right_partials = try right_idx.scanDistributedPartials(&right_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, right_partials);
    var partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_partials.len + right_partials.len);
    defer alloc.free(partials);
    @memcpy(partials[0..left_partials.len], left_partials);
    @memcpy(partials[left_partials.len..], right_partials);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    var aggregation = (try algebraicRangeAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"low\"", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregation.buckets[0].count);
    try std.testing.expectEqualStrings("10", aggregation.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":10,\"avg\":10,\"min\":10,\"max\":10,\"sum_squares\":100,\"variance\":0,\"std_dev\":0}", aggregation.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"mid\"", aggregation.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregation.buckets[1].count);
    try std.testing.expectEqualStrings("27", aggregation.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":27,\"avg\":13.5,\"min\":7,\"max\":20,\"sum_squares\":449,\"variance\":42.25,\"std_dev\":6.5}", aggregation.buckets[1].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"all\"", aggregation.buckets[2].key_json);
    try std.testing.expectEqual(@as(i64, 4), aggregation.buckets[2].count);
    try std.testing.expectEqualStrings("42", aggregation.buckets[2].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":4,\"sum\":42,\"avg\":10.5,\"min\":5,\"max\":20,\"sum_squares\":574,\"variance\":33.25,\"std_dev\":5.766281297335398}", aggregation.buckets[2].aggregations[1].value_json.?);
}

test "algebraic distributed partials build exact aligned date range response" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [{"name":"created","path":"created_at","type":"timestamp"}],
        \\  "materializations": [
        \\    {"name":"orders_by_day","op":"count","group_by":[],"time":"created","bucket":"day"},
        \\    {"name":"amount_by_day","op":"sum","group_by":[],"measure":"amount","time":"created","bucket":"day"}
        \\  ]
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"created_at\":\"2026-05-01T10:00:00Z\",\"amount\":10}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"created_at\":\"2026-05-02T09:00:00Z\",\"amount\":5}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"created_at\":\"2026-05-01T11:00:00Z\",\"amount\":20}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"created_at\":\"2026-05-03T12:00:00Z\",\"amount\":7}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const nested = [_]SearchAggregationRequest{.{
        .name = "amount_sum",
        .type = "sum",
        .field = "amount",
    }};
    const request = SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "created_at",
        .date_ranges = &.{
            .{ .name = "first_two_days", .start = "2026-05-01T00:00:00Z", .end = "2026-05-03T00:00:00Z" },
            .{ .name = "all_days", .start = "2026-05-01T00:00:00Z", .end = "2026-05-04T00:00:00Z" },
        },
        .aggregations = nested[0..],
    };
    const planned_materializations = (try algebraicDistributedDateRangeMaterializationsAlloc(alloc, &left_idx, request, &.{})) orelse return error.TestUnexpectedResult;
    defer freeAlgebraicDistributedMaterializations(alloc, planned_materializations);
    try std.testing.expectEqual(@as(usize, 2), planned_materializations.len);
    try std.testing.expectEqualStrings("orders_by_day", planned_materializations[0]);
    try std.testing.expectEqualStrings("amount_by_day", planned_materializations[1]);

    const unaligned = SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "created_at",
        .date_ranges = &.{.{ .name = "partial_day", .start = "2026-05-01T12:00:00Z", .end = "2026-05-03T00:00:00Z" }},
    };
    try std.testing.expect((try algebraicDistributedDateRangeMaterializationsAlloc(alloc, &left_idx, unaligned, &.{})) == null);

    const left_partials = try left_idx.scanDistributedPartials(&left_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, left_partials);
    const right_partials = try right_idx.scanDistributedPartials(&right_store, planned_materializations);
    defer algebraic_mod.distributed.freePartials(alloc, right_partials);
    var partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_partials.len + right_partials.len);
    defer alloc.free(partials);
    @memcpy(partials[0..left_partials.len], left_partials);
    @memcpy(partials[left_partials.len..], right_partials);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    var aggregation = (try algebraicDateRangeAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"first_two_days\"", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 3), aggregation.buckets[0].count);
    try std.testing.expectEqualStrings("35", aggregation.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("\"all_days\"", aggregation.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 4), aggregation.buckets[1].count);
    try std.testing.expectEqualStrings("42", aggregation.buckets[1].aggregations[0].value_json.?);
}

test "algebraic aggregation planner answers unfiltered terms with nested metric" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"customers","op":"count","group_by":["customer"]},
        \\    {"name":"amount","op":"sum","group_by":["customer"],"measure":"amount"}
        \\  ]
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"amount\":7}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const requests = [_]SearchAggregationRequest{.{
        .name = "customers",
        .type = "terms",
        .field = "customer",
        .aggregations = &.{.{
            .name = "amount",
            .type = "sum",
            .field = "amount",
        }},
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 2), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("\"alice\"", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("30", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("\"bob\"", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[1].count);
}

test "algebraic aggregation planner answers multi field composite terms" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"product","path":"product","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"customer_product_count","op":"count","group_by":["customer","product"]},
        \\    {"name":"customer_product_amount","op":"sum","group_by":["customer","product"],"measure":"amount"}
        \\  ]
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]derived_types.DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pen\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"book\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"book\",\"amount\":5}" },
        .{ .key = "o4", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"product\":\"pen\",\"amount\":7}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const composite_fields = [_][]const u8{ "customer", "product" };
    const requests = [_]SearchAggregationRequest{.{
        .name = "by_customer_product",
        .type = "terms",
        .field = "customer",
        .fields = composite_fields[0..],
        .aggregations = &.{.{
            .name = "amount",
            .type = "sum",
            .field = "amount",
        }},
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 3), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("[\"alice\",\"book\"]", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("25", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("[\"alice\",\"pen\"]", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("10", aggregations[0].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("[\"bob\",\"pen\"]", aggregations[0].buckets[2].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[2].count);
    try std.testing.expectEqualStrings("7", aggregations[0].buckets[2].aggregations[0].value_json.?);

    const status_value = manager.algebraic_indexes.items[0].index.status();
    try std.testing.expectEqual(@as(u64, 1), status_value.planner_algebraic_selected);
    try std.testing.expectEqual(@as(u64, 0), status_value.planner_fallback_count);
}

test "algebraic multi field composite terms sort ties by public key json" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"tenant","path":"tenant_id","type":"string"},
        \\    {"name":"region","path":"region","type":"string"},
        \\    {"name":"product","path":"product","type":"string"},
        \\    {"name":"customer","path":"customer_id","type":"integer"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"wide_orders","op":"count","group_by":["tenant","region","product","customer"]},
        \\    {"name":"wide_amount","op":"sum","group_by":["tenant","region","product","customer"],"measure":"amount"}
        \\  ]
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]derived_types.DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"tenant_id\":\"t000\",\"region\":\"r000\",\"product\":\"p10\",\"customer_id\":2,\"amount\":5}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"tenant_id\":\"t000\",\"region\":\"r000\",\"product\":\"p10\",\"customer_id\":2,\"amount\":7}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"tenant_id\":\"t000\",\"region\":\"r000\",\"product\":\"p2\",\"customer_id\":10,\"amount\":11}" },
        .{ .key = "o4", .action = .upsert, .cleaned_value = "{\"tenant_id\":\"t000\",\"region\":\"r000\",\"product\":\"p2\",\"customer_id\":2,\"amount\":13}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const composite_fields = [_][]const u8{ "tenant_id", "region", "product", "customer_id" };
    const requests = [_]SearchAggregationRequest{.{
        .name = "wide_composite",
        .type = "terms",
        .field = "tenant_id",
        .fields = composite_fields[0..],
        .aggregations = &.{.{
            .name = "wide_amount",
            .type = "sum",
            .field = "amount",
        }},
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 3), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("[\"t000\",\"r000\",\"p10\",2]", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("12", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("[\"t000\",\"r000\",\"p2\",10]", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("[\"t000\",\"r000\",\"p2\",2]", aggregations[0].buckets[2].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[2].count);

    const status_value = manager.algebraic_indexes.items[0].index.status();
    try std.testing.expectEqual(@as(u64, 1), status_value.planner_algebraic_selected);
    try std.testing.expectEqual(@as(u64, 0), status_value.planner_fallback_count);
}

test "terms aggregation computes multi field composite fallback" {
    const alloc = std.testing.allocator;

    const docs = [_][]const u8{
        "{\"customer\":\"alice\",\"product\":\"pen\",\"amount\":10}",
        "{\"customer\":\"alice\",\"product\":\"book\",\"amount\":20}",
        "{\"customer\":\"alice\",\"product\":\"book\",\"amount\":5}",
        "{\"customer\":\"bob\",\"product\":\"pen\",\"amount\":7}",
    };
    var hits = try alloc.alloc(types.SearchHit, docs.len);
    defer {
        for (hits) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    for (docs, 0..) |doc, i| {
        hits[i] = .{
            .id = try std.fmt.allocPrint(alloc, "doc:{d}", .{i}),
            .stored_data = try alloc.dupe(u8, doc),
        };
    }

    const composite_fields = [_][]const u8{ "customer", "product" };
    const requests = [_]SearchAggregationRequest{.{
        .name = "by_customer_product",
        .type = "terms",
        .field = "customer",
        .fields = composite_fields[0..],
        .aggregations = &.{.{
            .name = "amount",
            .type = "sum",
            .field = "amount",
        }},
    }};
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = @intCast(hits.len),
    };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{});
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 3), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("[\"alice\",\"book\"]", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("25", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("[\"alice\",\"pen\"]", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("10", aggregations[0].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("[\"bob\",\"pen\"]", aggregations[0].buckets[2].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[2].count);
    try std.testing.expectEqualStrings("7", aggregations[0].buckets[2].aggregations[0].value_json.?);
}

test "terms aggregation escapes string bucket keys in fallback" {
    const alloc = std.testing.allocator;

    const docs = [_][]const u8{
        "{\"extension\":\".go\",\"file_type\":\"source\\\"code\"}",
        "{\"extension\":\".go\",\"file_type\":\"source\\\"code\"}",
    };
    var hits = try alloc.alloc(types.SearchHit, docs.len);
    defer {
        for (hits) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    for (docs, 0..) |doc, i| {
        hits[i] = .{
            .id = try std.fmt.allocPrint(alloc, "doc:{d}", .{i}),
            .stored_data = try alloc.dupe(u8, doc),
        };
    }

    const requests = [_]SearchAggregationRequest{.{
        .name = "file_types",
        .type = "terms",
        .field = "file_type",
        .size = 10,
    }};
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = @intCast(hits.len),
    };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{});
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 1), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("\"source\\\"code\"", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[0].count);
}

test "algebraic aggregation planner answers schemaless structural path terms" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "docs",
        \\  "materializations": []
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"meta\":{\"tier\":\"gold\"},\"tags\":[\"new\",\"vip\",\"new\"],\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"meta\":{\"tier\":\"silver\"},\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"tags\":[\"returning\"],\"amount\":30}" },
        .{ .key = "o4", .action = .upsert, .cleaned_value = "{\"meta\":{\"tier\":\"gold\"},\"tags\":[\"vip\"],\"amount\":5}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const requests = [_]SearchAggregationRequest{
        .{
            .name = "meta_presence",
            .type = "terms",
            .field = "/meta",
            .aggregations = &.{ .{
                .name = "amount_sum",
                .type = "sum",
                .field = "/amount",
            }, .{
                .name = "amount_stats",
                .type = "stats",
                .field = "/amount",
            } },
        },
        .{
            .name = "tags_presence",
            .type = "terms",
            .field = "/tags",
            .aggregations = &.{.{
                .name = "amount_sum",
                .type = "sum",
                .field = "/amount",
            }},
        },
    };
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 2), aggregations.len);
    try std.testing.expectEqual(@as(usize, 1), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("{\"kind\":\"object\",\"structural\":true}", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 3), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("35", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try expectStatsJsonApprox(alloc, aggregations[0].buckets[0].aggregations[1].value_json.?, .{
        .count = 3,
        .sum = 35,
        .avg = 35.0 / 3.0,
        .min = 5,
        .max = 20,
        .sum_squares = 525,
        .variance = 38.888888888888886,
        .std_dev = 6.236095644623235,
    });
    try std.testing.expectEqual(@as(usize, 3), aggregations[1].buckets.len);
    try std.testing.expectEqualStrings("\"vip\"", aggregations[1].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[1].buckets[0].count);
    try std.testing.expectEqualStrings("15", aggregations[1].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("\"new\"", aggregations[1].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[1].buckets[1].count);
    try std.testing.expectEqualStrings("10", aggregations[1].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("\"returning\"", aggregations[1].buckets[2].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[1].buckets[2].count);
    try std.testing.expectEqualStrings("30", aggregations[1].buckets[2].aggregations[0].value_json.?);
}

test "algebraic aggregation planner applies pathfact predicate constraints" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "docs",
        \\  "materializations": []
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]derived_types.DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"tenant\":\"alpha\",\"meta\":{\"tier\":\"gold\"},\"tags\":[\"new\",\"vip\"],\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"tenant\":\"alpine\",\"meta\":{\"tier\":\"gold\"},\"tags\":[\"vip\"],\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"tenant\":\"beta\",\"meta\":{\"tier\":\"silver\"},\"tags\":[\"vip\",\"sale\"],\"amount\":30}" },
        .{ .key = "o4", .action = .upsert, .cleaned_value = "{\"tenant\":\"delta\",\"meta\":{\"tier\":\"bronze\"},\"tags\":[\"clearance\"],\"amount\":40}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const requests = [_]SearchAggregationRequest{.{
        .name = "tags_for_alpha_tenants",
        .type = "terms",
        .field = "/tags",
        .aggregations = &.{.{
            .name = "amount_sum",
            .type = "sum",
            .field = "/amount",
        }},
    }};
    const prefix_value = try algebraic_mod.index.pathFactStringPrefixConstraintValueAlloc(alloc, "alp");
    defer alloc.free(prefix_value);
    const constraints = [_]FixedConstraint{.{ .field = "/tenant", .value = prefix_value }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
        .algebraic_constraints = constraints[0..],
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 2), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("\"vip\"", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("30", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("\"new\"", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("10", aggregations[0].buckets[1].aggregations[0].value_json.?);

    const status_value = manager.algebraic_indexes.items[0].index.status();
    try std.testing.expectEqual(@as(u64, 1), status_value.planner_algebraic_selected);
    try std.testing.expectEqual(@as(u64, 0), status_value.planner_fallback_count);
}

test "algebraic aggregation planner answers exact cardinality from fact rows" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"product","path":"product","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": []
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pen\",\"amount\":10,\"meta\":{\"tier\":\"gold\"},\"tags\":[\"new\",\"vip\",\"new\"]}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"book\",\"amount\":20,\"meta\":{\"tier\":\"gold\"},\"tags\":[\"vip\"]}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"product\":\"pen\",\"amount\":7,\"meta\":{\"tier\":\"silver\"},\"tags\":[\"sale\"]}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const requests = [_]SearchAggregationRequest{
        .{ .name = "customer_cardinality", .type = "cardinality", .field = "customer" },
        .{ .name = "tier_cardinality", .type = "cardinality", .field = "/meta/tier" },
        .{ .name = "meta_cardinality", .type = "cardinality", .field = "/meta" },
        .{ .name = "tag_cardinality", .type = "cardinality", .field = "/tags" },
    };
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 4), aggregations.len);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":0}", aggregations[2].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":3}", aggregations[3].value_json.?);

    const constrained_requests = [_]SearchAggregationRequest{.{
        .name = "alice_product_cardinality",
        .type = "cardinality",
        .field = "product",
    }};
    const constraints = [_]FixedConstraint{.{ .field = "customer", .value = "alice" }};
    const constrained = try computeSearchAggregations(alloc, &constrained_requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
        .algebraic_constraints = constraints[0..],
    });
    defer deinitResults(alloc, constrained);

    try std.testing.expectEqual(@as(usize, 1), constrained.len);
    try std.testing.expectEqualStrings("{\"value\":2}", constrained[0].value_json.?);
}

test "algebraic aggregation planner constrains root cardinality by document role" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"}
        \\  ],
        \\  "materializations": []
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"alice\"}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"bob\"}" },
        .{ .key = "c1", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"alice\"}" },
        .{ .key = "c2", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"charlie\"}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const requests = [_]SearchAggregationRequest{.{
        .name = "customer_cardinality",
        .type = "cardinality",
        .field = "customer",
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };

    const unconstrained = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, unconstrained);
    try std.testing.expectEqualStrings("{\"value\":3}", unconstrained[0].value_json.?);

    const constraints = [_]FixedConstraint{.{ .field = "kind", .value = "order" }};
    const constrained = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
        .algebraic_constraints = constraints[0..],
    });
    defer deinitResults(alloc, constrained);
    try std.testing.expectEqualStrings("{\"value\":2}", constrained[0].value_json.?);
}

test "algebraic distributed cardinality merges canonical distinct values" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"product","path":"product","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": []
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pen\",\"amount\":10,\"meta\":{\"tier\":\"gold\"},\"tags\":[\"new\",\"vip\",\"new\"]}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"product\":\"book\",\"amount\":20,\"meta\":{\"tier\":\"silver\"},\"tags\":[\"sale\"]}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"notebook\",\"amount\":7,\"meta\":{\"tier\":\"gold\"},\"tags\":[\"vip\",\"clearance\"]}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"customer\":\"carol\",\"product\":\"pen\",\"amount\":5,\"meta\":{\"tier\":\"bronze\"},\"tags\":[\"new\"]}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const customer_request = SearchAggregationRequest{
        .name = "customer_cardinality",
        .type = "cardinality",
        .field = "customer",
    };
    const left_customer_partials = (try left_idx.scanDistributedCardinalityPartials(&left_store, customer_request.name, customer_request.field, &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_customer_partials);
    const right_customer_partials = (try right_idx.scanDistributedCardinalityPartials(&right_store, customer_request.name, customer_request.field, &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_customer_partials);
    var customer_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_customer_partials.len + right_customer_partials.len);
    defer alloc.free(customer_partials);
    @memcpy(customer_partials[0..left_customer_partials.len], left_customer_partials);
    @memcpy(customer_partials[left_customer_partials.len..], right_customer_partials);
    var customer_merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, customer_partials);
    defer customer_merged.deinit(alloc);
    var customer_agg = (try algebraicCardinalityAggregationFromDistributedPartialsAlloc(alloc, customer_request, customer_merged)) orelse return error.TestUnexpectedResult;
    defer customer_agg.deinit(alloc);
    try std.testing.expectEqualStrings("{\"value\":3}", customer_agg.value_json.?);

    const tier_request = SearchAggregationRequest{
        .name = "tier_cardinality",
        .type = "cardinality",
        .field = "/meta/tier",
    };
    const left_tier_partials = (try left_idx.scanDistributedCardinalityPartials(&left_store, tier_request.name, tier_request.field, &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_tier_partials);
    const right_tier_partials = (try right_idx.scanDistributedCardinalityPartials(&right_store, tier_request.name, tier_request.field, &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_tier_partials);
    var tier_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_tier_partials.len + right_tier_partials.len);
    defer alloc.free(tier_partials);
    @memcpy(tier_partials[0..left_tier_partials.len], left_tier_partials);
    @memcpy(tier_partials[left_tier_partials.len..], right_tier_partials);
    var tier_merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, tier_partials);
    defer tier_merged.deinit(alloc);
    var tier_agg = (try algebraicCardinalityAggregationFromDistributedPartialsAlloc(alloc, tier_request, tier_merged)) orelse return error.TestUnexpectedResult;
    defer tier_agg.deinit(alloc);
    try std.testing.expectEqualStrings("{\"value\":3}", tier_agg.value_json.?);

    const meta_request = SearchAggregationRequest{
        .name = "meta_cardinality",
        .type = "cardinality",
        .field = "/meta",
    };
    const left_meta_partials = (try left_idx.scanDistributedCardinalityPartials(&left_store, meta_request.name, meta_request.field, &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_meta_partials);
    const right_meta_partials = (try right_idx.scanDistributedCardinalityPartials(&right_store, meta_request.name, meta_request.field, &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_meta_partials);
    var meta_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_meta_partials.len + right_meta_partials.len);
    defer alloc.free(meta_partials);
    @memcpy(meta_partials[0..left_meta_partials.len], left_meta_partials);
    @memcpy(meta_partials[left_meta_partials.len..], right_meta_partials);
    var meta_merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, meta_partials);
    defer meta_merged.deinit(alloc);
    var meta_agg = (try algebraicCardinalityAggregationFromDistributedPartialsAlloc(alloc, meta_request, meta_merged)) orelse return error.TestUnexpectedResult;
    defer meta_agg.deinit(alloc);
    try std.testing.expectEqualStrings("{\"value\":0}", meta_agg.value_json.?);

    const tags_request = SearchAggregationRequest{
        .name = "tags_cardinality",
        .type = "cardinality",
        .field = "/tags",
    };
    const left_tags_partials = (try left_idx.scanDistributedCardinalityPartials(&left_store, tags_request.name, tags_request.field, &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_tags_partials);
    const right_tags_partials = (try right_idx.scanDistributedCardinalityPartials(&right_store, tags_request.name, tags_request.field, &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_tags_partials);
    var tags_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_tags_partials.len + right_tags_partials.len);
    defer alloc.free(tags_partials);
    @memcpy(tags_partials[0..left_tags_partials.len], left_tags_partials);
    @memcpy(tags_partials[left_tags_partials.len..], right_tags_partials);
    var tags_merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, tags_partials);
    defer tags_merged.deinit(alloc);
    var tags_agg = (try algebraicCardinalityAggregationFromDistributedPartialsAlloc(alloc, tags_request, tags_merged)) orelse return error.TestUnexpectedResult;
    defer tags_agg.deinit(alloc);
    try std.testing.expectEqualStrings("{\"value\":4}", tags_agg.value_json.?);

    const constraints = [_]FixedConstraint{.{ .field = "customer", .value = "alice" }};
    const product_request = SearchAggregationRequest{
        .name = "alice_product_cardinality",
        .type = "cardinality",
        .field = "product",
    };
    const left_product_partials = (try left_idx.scanDistributedCardinalityPartials(&left_store, product_request.name, product_request.field, constraints[0..])).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_product_partials);
    const right_product_partials = (try right_idx.scanDistributedCardinalityPartials(&right_store, product_request.name, product_request.field, constraints[0..])).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_product_partials);
    var product_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_product_partials.len + right_product_partials.len);
    defer alloc.free(product_partials);
    @memcpy(product_partials[0..left_product_partials.len], left_product_partials);
    @memcpy(product_partials[left_product_partials.len..], right_product_partials);
    var product_merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, product_partials);
    defer product_merged.deinit(alloc);
    var product_agg = (try algebraicCardinalityAggregationFromDistributedPartialsAlloc(alloc, product_request, product_merged)) orelse return error.TestUnexpectedResult;
    defer product_agg.deinit(alloc);
    try std.testing.expectEqualStrings("{\"value\":2}", product_agg.value_json.?);

    const gold_value = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{ "string", "gold" });
    defer alloc.free(gold_value);
    const path_constraints = [_]FixedConstraint{.{ .field = "/meta/tier", .value = gold_value }};
    const gold_product_request = SearchAggregationRequest{
        .name = "gold_product_cardinality",
        .type = "cardinality",
        .field = "product",
    };
    const left_gold_product_partials = (try left_idx.scanDistributedCardinalityPartials(&left_store, gold_product_request.name, gold_product_request.field, path_constraints[0..])).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_gold_product_partials);
    const right_gold_product_partials = (try right_idx.scanDistributedCardinalityPartials(&right_store, gold_product_request.name, gold_product_request.field, path_constraints[0..])).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_gold_product_partials);
    var gold_product_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_gold_product_partials.len + right_gold_product_partials.len);
    defer alloc.free(gold_product_partials);
    @memcpy(gold_product_partials[0..left_gold_product_partials.len], left_gold_product_partials);
    @memcpy(gold_product_partials[left_gold_product_partials.len..], right_gold_product_partials);
    var gold_product_merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, gold_product_partials);
    defer gold_product_merged.deinit(alloc);
    var gold_product_agg = (try algebraicCardinalityAggregationFromDistributedPartialsAlloc(alloc, gold_product_request, gold_product_merged)) orelse return error.TestUnexpectedResult;
    defer gold_product_agg.deinit(alloc);
    try std.testing.expectEqualStrings("{\"value\":2}", gold_product_agg.value_json.?);
}

test "algebraic terms aggregation answers nested exact cardinality from fact rows" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"product","path":"product","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": []
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]derived_types.DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pen\",\"amount\":10,\"meta\":{\"tier\":\"gold\"}}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"book\",\"amount\":20,\"meta\":{\"tier\":\"gold\"}}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"product\":\"pen\",\"amount\":7,\"meta\":{\"tier\":\"silver\"}}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const nested = [_]SearchAggregationRequest{
        .{ .name = "product_cardinality", .type = "cardinality", .field = "product" },
        .{ .name = "tier_cardinality", .type = "cardinality", .field = "/meta/tier" },
    };
    const requests = [_]SearchAggregationRequest{.{
        .name = "by_customer",
        .type = "terms",
        .field = "customer",
        .aggregations = nested[0..],
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 2), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("\"alice\"", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", aggregations[0].buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"bob\"", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":1}", aggregations[0].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", aggregations[0].buckets[1].aggregations[1].value_json.?);
}

test "algebraic terms aggregation answers path terms with nested exact cardinality" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"product","path":"product","type":"string"}],
        \\  "materializations": []
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"product\":\"pen\",\"meta\":{\"tier\":\"gold\"}}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"product\":\"book\",\"meta\":{\"tier\":\"gold\"}}" },
        .{ .key = "l3", .action = .upsert, .cleaned_value = "{\"product\":\"pen\",\"meta\":{\"tier\":\"silver\"}}" },
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"product\":\"pen\",\"meta\":{\"tier\":\"silver\"}}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"product\":\"notebook\",\"meta\":{\"tier\":\"silver\"}}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const nested = [_]SearchAggregationRequest{
        .{ .name = "product_cardinality", .type = "cardinality", .field = "product" },
        .{ .name = "tier_cardinality", .type = "cardinality", .field = "/meta/tier" },
    };
    const requests = [_]SearchAggregationRequest{.{
        .name = "by_tier",
        .type = "terms",
        .field = "/meta/tier",
        .aggregations = nested[0..],
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 2), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("\"silver\"", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 3), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", aggregations[0].buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"gold\"", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregations[0].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", aggregations[0].buckets[1].aggregations[1].value_json.?);
}

test "algebraic terms aggregation answers array path terms with nested exact cardinality" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"product","path":"product","type":"string"}],
        \\  "materializations": []
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]derived_types.DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"product\":\"pen\",\"tags\":[\"new\",\"vip\",\"new\"]}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"product\":\"book\",\"tags\":[\"vip\"]}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"product\":\"pen\",\"tags\":[\"sale\"]}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const nested = [_]SearchAggregationRequest{
        .{ .name = "product_cardinality", .type = "cardinality", .field = "product" },
        .{ .name = "tag_cardinality", .type = "cardinality", .field = "/tags" },
    };
    const requests = [_]SearchAggregationRequest{.{
        .name = "by_tag",
        .type = "terms",
        .field = "/tags",
        .aggregations = nested[0..],
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 3), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("\"vip\"", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregations[0].buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"new\"", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":1}", aggregations[0].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregations[0].buckets[1].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"sale\"", aggregations[0].buckets[2].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[2].count);
    try std.testing.expectEqualStrings("{\"value\":1}", aggregations[0].buckets[2].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", aggregations[0].buckets[2].aggregations[1].value_json.?);
}

test "algebraic histogram aggregation answers nested exact cardinality from fact rows" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"product","path":"product","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": []
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]derived_types.DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pen\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"book\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"product\":\"pen\",\"amount\":35}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const nested = [_]SearchAggregationRequest{
        .{ .name = "customer_cardinality", .type = "cardinality", .field = "customer" },
        .{ .name = "product_cardinality", .type = "cardinality", .field = "product" },
    };
    const requests = [_]SearchAggregationRequest{.{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 20,
        .aggregations = nested[0..],
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 2), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("0", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":1}", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", aggregations[0].buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("20", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregations[0].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregations[0].buckets[1].aggregations[1].value_json.?);

    const status_value = manager.algebraic_indexes.items[0].index.status();
    try std.testing.expectEqual(@as(u64, 1), status_value.planner_algebraic_selected);
    try std.testing.expectEqual(@as(u64, 0), status_value.planner_fallback_count);
}

test "algebraic distributed terms aggregation merges nested exact cardinality" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"product","path":"product","type":"string"}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pen\",\"meta\":{\"tier\":\"gold\"},\"tags\":[\"new\",\"vip\",\"new\"]}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"book\",\"meta\":{\"tier\":\"gold\"},\"tags\":[\"vip\"]}" },
        .{ .key = "l3", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"product\":\"pen\",\"meta\":{\"tier\":\"silver\"},\"tags\":[\"sale\"]}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pen\",\"meta\":{\"tier\":\"silver\"},\"tags\":[\"vip\",\"clearance\"]}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"product\":\"notebook\",\"meta\":{\"tier\":\"silver\"},\"tags\":[\"new\"]}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const child_exports = [_]algebraic_mod.index.CardinalityChildRequest{
        .{ .name = "product_cardinality", .field = "product" },
        .{ .name = "tier_cardinality", .field = "/meta/tier" },
    };
    const left_partials = (try left_idx.scanDistributedTermsCardinalityPartials(&left_store, "by_customer", "customer", child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_partials);
    const right_partials = (try right_idx.scanDistributedTermsCardinalityPartials(&right_store, "by_customer", "customer", child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_partials);
    var partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_partials.len + right_partials.len);
    defer alloc.free(partials);
    @memcpy(partials[0..left_partials.len], left_partials);
    @memcpy(partials[left_partials.len..], right_partials);
    var merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, partials);
    defer merged.deinit(alloc);

    const nested = [_]SearchAggregationRequest{
        .{ .name = "product_cardinality", .type = "cardinality", .field = "product" },
        .{ .name = "tier_cardinality", .type = "cardinality", .field = "/meta/tier" },
    };
    const request = SearchAggregationRequest{
        .name = "by_customer",
        .type = "terms",
        .field = "customer",
        .aggregations = nested[0..],
    };
    var aggregation = (try algebraicTermsAggregationFromDistributedPartialsAlloc(alloc, &left_idx, request, &.{}, merged)) orelse return error.TestUnexpectedResult;
    defer aggregation.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"alice\"", aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 3), aggregation.buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregation.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregation.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"bob\"", aggregation.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregation.buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":2}", aggregation.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", aggregation.buckets[1].aggregations[1].value_json.?);

    const left_tier_partials = (try left_idx.scanDistributedTermsCardinalityPartials(&left_store, "by_tier", "/meta/tier", child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_tier_partials);
    const right_tier_partials = (try right_idx.scanDistributedTermsCardinalityPartials(&right_store, "by_tier", "/meta/tier", child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_tier_partials);
    var tier_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_tier_partials.len + right_tier_partials.len);
    defer alloc.free(tier_partials);
    @memcpy(tier_partials[0..left_tier_partials.len], left_tier_partials);
    @memcpy(tier_partials[left_tier_partials.len..], right_tier_partials);
    var merged_tiers = try algebraic_mod.distributed.mergePartialsAlloc(alloc, tier_partials);
    defer merged_tiers.deinit(alloc);

    const tier_request = SearchAggregationRequest{
        .name = "by_tier",
        .type = "terms",
        .field = "/meta/tier",
        .aggregations = nested[0..],
    };
    var tier_aggregation = (try algebraicTermsAggregationFromDistributedPartialsAlloc(alloc, &left_idx, tier_request, &.{}, merged_tiers)) orelse return error.TestUnexpectedResult;
    defer tier_aggregation.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), tier_aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"silver\"", tier_aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 3), tier_aggregation.buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2}", tier_aggregation.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", tier_aggregation.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"gold\"", tier_aggregation.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), tier_aggregation.buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":2}", tier_aggregation.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", tier_aggregation.buckets[1].aggregations[1].value_json.?);

    const tag_child_exports = [_]algebraic_mod.index.CardinalityChildRequest{
        .{ .name = "product_cardinality", .field = "product" },
        .{ .name = "tag_cardinality", .field = "/tags" },
    };
    const left_tag_partials = (try left_idx.scanDistributedTermsCardinalityPartials(&left_store, "by_tag", "/tags", tag_child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_tag_partials);
    const right_tag_partials = (try right_idx.scanDistributedTermsCardinalityPartials(&right_store, "by_tag", "/tags", tag_child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_tag_partials);
    var tag_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_tag_partials.len + right_tag_partials.len);
    defer alloc.free(tag_partials);
    @memcpy(tag_partials[0..left_tag_partials.len], left_tag_partials);
    @memcpy(tag_partials[left_tag_partials.len..], right_tag_partials);
    var merged_tags = try algebraic_mod.distributed.mergePartialsAlloc(alloc, tag_partials);
    defer merged_tags.deinit(alloc);

    const tag_nested = [_]SearchAggregationRequest{
        .{ .name = "product_cardinality", .type = "cardinality", .field = "product" },
        .{ .name = "tag_cardinality", .type = "cardinality", .field = "/tags" },
    };
    const tag_request = SearchAggregationRequest{
        .name = "by_tag",
        .type = "terms",
        .field = "/tags",
        .aggregations = tag_nested[0..],
    };
    var tag_aggregation = (try algebraicTermsAggregationFromDistributedPartialsAlloc(alloc, &left_idx, tag_request, &.{}, merged_tags)) orelse return error.TestUnexpectedResult;
    defer tag_aggregation.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 4), tag_aggregation.buckets.len);
    try std.testing.expectEqualStrings("\"vip\"", tag_aggregation.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 3), tag_aggregation.buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2}", tag_aggregation.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":3}", tag_aggregation.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"new\"", tag_aggregation.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), tag_aggregation.buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":2}", tag_aggregation.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":2}", tag_aggregation.buckets[1].aggregations[1].value_json.?);
    const clearance_bucket = findAggregationBucketByKey(tag_aggregation.buckets, "\"clearance\"") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(i64, 1), clearance_bucket.count);
    try std.testing.expectEqualStrings("{\"value\":1}", clearance_bucket.aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":2}", clearance_bucket.aggregations[1].value_json.?);
    const sale_bucket = findAggregationBucketByKey(tag_aggregation.buckets, "\"sale\"") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(i64, 1), sale_bucket.count);
    try std.testing.expectEqualStrings("{\"value\":1}", sale_bucket.aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", sale_bucket.aggregations[1].value_json.?);
}

test "algebraic distributed range aggregations merge nested exact cardinality" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"product","path":"product","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [{"name":"created_at","path":"created_at","type":"datetime"}],
        \\  "materializations": []
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pen\",\"amount\":10,\"created_at\":\"2026-01-02T00:00:00Z\",\"meta\":{\"tier\":\"gold\"}}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"book\",\"amount\":20,\"created_at\":\"2026-01-03T00:00:00Z\",\"meta\":{\"tier\":\"gold\"}}" },
        .{ .key = "l3", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"product\":\"pen\",\"amount\":30,\"created_at\":\"2026-01-04T00:00:00Z\",\"meta\":{\"tier\":\"silver\"}}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"customer\":\"carol\",\"product\":\"book\",\"amount\":15,\"created_at\":\"2026-01-03T12:00:00Z\",\"meta\":{\"tier\":\"gold\"}}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"notebook\",\"amount\":35,\"created_at\":\"2026-01-05T00:00:00Z\",\"meta\":{\"tier\":\"bronze\"}}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const child_exports = [_]algebraic_mod.index.CardinalityChildRequest{
        .{ .name = "customer_cardinality", .field = "customer" },
        .{ .name = "tier_cardinality", .field = "/meta/tier" },
    };
    const range_exports = [_]algebraic_mod.index.CardinalityRangeRequest{
        .{ .name = "low", .start = "0", .end = "20" },
        .{ .name = "high", .start = "20", .end = "40" },
    };
    const left_range_partials = (try left_idx.scanDistributedRangeCardinalityPartials(&left_store, "amount_ranges", "amount", .numeric, range_exports[0..], child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_range_partials);
    const right_range_partials = (try right_idx.scanDistributedRangeCardinalityPartials(&right_store, "amount_ranges", "amount", .numeric, range_exports[0..], child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_range_partials);
    var range_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_range_partials.len + right_range_partials.len);
    defer alloc.free(range_partials);
    @memcpy(range_partials[0..left_range_partials.len], left_range_partials);
    @memcpy(range_partials[left_range_partials.len..], right_range_partials);
    var range_merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, range_partials);
    defer range_merged.deinit(alloc);

    const nested = [_]SearchAggregationRequest{
        .{ .name = "customer_cardinality", .type = "cardinality", .field = "customer" },
        .{ .name = "tier_cardinality", .type = "cardinality", .field = "/meta/tier" },
    };
    const range_request = SearchAggregationRequest{
        .name = "amount_ranges",
        .type = "range",
        .field = "amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 40 },
        },
        .aggregations = nested[0..],
    };
    var range_agg = (try algebraicRangeAggregationFromDistributedPartialsAlloc(alloc, &left_idx, range_request, &.{}, range_merged)) orelse return error.TestUnexpectedResult;
    defer range_agg.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), range_agg.buckets.len);
    try std.testing.expectEqualStrings("\"low\"", range_agg.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), range_agg.buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2}", range_agg.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", range_agg.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"high\"", range_agg.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 3), range_agg.buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":2}", range_agg.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":3}", range_agg.buckets[1].aggregations[1].value_json.?);

    const left_path_range_partials = (try left_idx.scanDistributedRangeCardinalityPartials(&left_store, "path_amount_ranges", "/amount", .numeric, range_exports[0..], child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_path_range_partials);
    const right_path_range_partials = (try right_idx.scanDistributedRangeCardinalityPartials(&right_store, "path_amount_ranges", "/amount", .numeric, range_exports[0..], child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_path_range_partials);
    var path_range_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_path_range_partials.len + right_path_range_partials.len);
    defer alloc.free(path_range_partials);
    @memcpy(path_range_partials[0..left_path_range_partials.len], left_path_range_partials);
    @memcpy(path_range_partials[left_path_range_partials.len..], right_path_range_partials);
    var path_range_merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, path_range_partials);
    defer path_range_merged.deinit(alloc);

    const path_range_request = SearchAggregationRequest{
        .name = "path_amount_ranges",
        .type = "range",
        .field = "/amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 40 },
        },
        .aggregations = nested[0..],
    };
    var path_range_agg = (try algebraicRangeAggregationFromDistributedPartialsAlloc(alloc, &left_idx, path_range_request, &.{}, path_range_merged)) orelse return error.TestUnexpectedResult;
    defer path_range_agg.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), path_range_agg.buckets.len);
    try std.testing.expectEqualStrings("\"low\"", path_range_agg.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), path_range_agg.buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2}", path_range_agg.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", path_range_agg.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"high\"", path_range_agg.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 3), path_range_agg.buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":2}", path_range_agg.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":3}", path_range_agg.buckets[1].aggregations[1].value_json.?);

    const date_exports = [_]algebraic_mod.index.CardinalityRangeRequest{
        .{ .name = "early", .start = "2026-01-02T00:00:00Z", .end = "2026-01-04T00:00:00Z" },
        .{ .name = "late", .start = "2026-01-04T00:00:00Z", .end = "2026-01-06T00:00:00Z" },
    };
    const left_date_partials = (try left_idx.scanDistributedRangeCardinalityPartials(&left_store, "created_ranges", "created_at", .date, date_exports[0..], child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_date_partials);
    const right_date_partials = (try right_idx.scanDistributedRangeCardinalityPartials(&right_store, "created_ranges", "created_at", .date, date_exports[0..], child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_date_partials);
    var date_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_date_partials.len + right_date_partials.len);
    defer alloc.free(date_partials);
    @memcpy(date_partials[0..left_date_partials.len], left_date_partials);
    @memcpy(date_partials[left_date_partials.len..], right_date_partials);
    var date_merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, date_partials);
    defer date_merged.deinit(alloc);

    const date_request = SearchAggregationRequest{
        .name = "created_ranges",
        .type = "date_range",
        .field = "created_at",
        .date_ranges = &.{
            .{ .name = "early", .start = "2026-01-02T00:00:00Z", .end = "2026-01-04T00:00:00Z" },
            .{ .name = "late", .start = "2026-01-04T00:00:00Z", .end = "2026-01-06T00:00:00Z" },
        },
        .aggregations = nested[0..],
    };
    var date_agg = (try algebraicDateRangeAggregationFromDistributedPartialsAlloc(alloc, &left_idx, date_request, &.{}, date_merged)) orelse return error.TestUnexpectedResult;
    defer date_agg.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), date_agg.buckets.len);
    try std.testing.expectEqualStrings("\"early\"", date_agg.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 3), date_agg.buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2}", date_agg.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", date_agg.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"late\"", date_agg.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), date_agg.buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":2}", date_agg.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":2}", date_agg.buckets[1].aggregations[1].value_json.?);

    const left_path_date_partials = (try left_idx.scanDistributedRangeCardinalityPartials(&left_store, "path_created_ranges", "/created_at", .date, date_exports[0..], child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_path_date_partials);
    const right_path_date_partials = (try right_idx.scanDistributedRangeCardinalityPartials(&right_store, "path_created_ranges", "/created_at", .date, date_exports[0..], child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_path_date_partials);
    var path_date_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_path_date_partials.len + right_path_date_partials.len);
    defer alloc.free(path_date_partials);
    @memcpy(path_date_partials[0..left_path_date_partials.len], left_path_date_partials);
    @memcpy(path_date_partials[left_path_date_partials.len..], right_path_date_partials);
    var path_date_merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, path_date_partials);
    defer path_date_merged.deinit(alloc);

    const path_date_request = SearchAggregationRequest{
        .name = "path_created_ranges",
        .type = "date_range",
        .field = "/created_at",
        .date_ranges = &.{
            .{ .name = "early", .start = "2026-01-02T00:00:00Z", .end = "2026-01-04T00:00:00Z" },
            .{ .name = "late", .start = "2026-01-04T00:00:00Z", .end = "2026-01-06T00:00:00Z" },
        },
        .aggregations = nested[0..],
    };
    var path_date_agg = (try algebraicDateRangeAggregationFromDistributedPartialsAlloc(alloc, &left_idx, path_date_request, &.{}, path_date_merged)) orelse return error.TestUnexpectedResult;
    defer path_date_agg.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), path_date_agg.buckets.len);
    try std.testing.expectEqualStrings("\"early\"", path_date_agg.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 3), path_date_agg.buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2}", path_date_agg.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", path_date_agg.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"late\"", path_date_agg.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), path_date_agg.buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":2}", path_date_agg.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":2}", path_date_agg.buckets[1].aggregations[1].value_json.?);
}

test "algebraic distributed histogram aggregations merge nested exact cardinality" {
    const alloc = std.testing.allocator;
    var left_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer left_backend.close();
    var right_backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer right_backend.close();
    const left_runtime = try left_backend.runtimeStore(alloc, .{});
    const right_runtime = try right_backend.runtimeStore(alloc, .{});
    var left_store = try docstore_mod.DocStore.openRuntime(alloc, left_runtime);
    defer left_store.close();
    var right_store = try docstore_mod.DocStore.openRuntime(alloc, right_runtime);
    defer right_store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"product","path":"product","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [{"name":"created_at","path":"created_at","type":"datetime"}],
        \\  "materializations": []
        \\}
    ;
    var left_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer left_idx.close();
    var right_idx = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    defer right_idx.close();

    const left_docs = [_]derived_types.DerivedDocument{
        .{ .key = "l1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pen\",\"amount\":10,\"created_at\":\"2026-01-02T00:00:00Z\",\"meta\":{\"tier\":\"gold\"}}" },
        .{ .key = "l2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"book\",\"amount\":20,\"created_at\":\"2026-01-03T00:00:00Z\",\"meta\":{\"tier\":\"gold\"}}" },
        .{ .key = "l3", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"product\":\"pen\",\"amount\":30,\"created_at\":\"2026-01-04T00:00:00Z\",\"meta\":{\"tier\":\"silver\"}}" },
    };
    try left_idx.applyBatch(&left_store, .{ .documents = left_docs[0..] });
    const right_docs = [_]derived_types.DerivedDocument{
        .{ .key = "r1", .action = .upsert, .cleaned_value = "{\"customer\":\"carol\",\"product\":\"book\",\"amount\":15,\"created_at\":\"2026-01-03T12:00:00Z\",\"meta\":{\"tier\":\"gold\"}}" },
        .{ .key = "r2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"notebook\",\"amount\":35,\"created_at\":\"2026-01-05T00:00:00Z\",\"meta\":{\"tier\":\"bronze\"}}" },
    };
    try right_idx.applyBatch(&right_store, .{ .documents = right_docs[0..] });

    const child_exports = [_]algebraic_mod.index.CardinalityChildRequest{
        .{ .name = "customer_cardinality", .field = "customer" },
        .{ .name = "tier_cardinality", .field = "/meta/tier" },
    };
    const left_histogram_partials = (try left_idx.scanDistributedHistogramCardinalityPartials(&left_store, "amount_histogram", "amount", .numeric, 10, "", child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_histogram_partials);
    const right_histogram_partials = (try right_idx.scanDistributedHistogramCardinalityPartials(&right_store, "amount_histogram", "amount", .numeric, 10, "", child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_histogram_partials);
    var histogram_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_histogram_partials.len + right_histogram_partials.len);
    defer alloc.free(histogram_partials);
    @memcpy(histogram_partials[0..left_histogram_partials.len], left_histogram_partials);
    @memcpy(histogram_partials[left_histogram_partials.len..], right_histogram_partials);
    var histogram_merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, histogram_partials);
    defer histogram_merged.deinit(alloc);

    const nested = [_]SearchAggregationRequest{
        .{ .name = "customer_cardinality", .type = "cardinality", .field = "customer" },
        .{ .name = "tier_cardinality", .type = "cardinality", .field = "/meta/tier" },
    };
    const histogram_request = SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 10,
        .aggregations = nested[0..],
    };
    var histogram_agg = (try algebraicHistogramAggregationFromDistributedPartialsAlloc(alloc, &left_idx, histogram_request, &.{}, histogram_merged)) orelse return error.TestUnexpectedResult;
    defer histogram_agg.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), histogram_agg.buckets.len);
    try std.testing.expectEqualStrings("10", histogram_agg.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), histogram_agg.buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2}", histogram_agg.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", histogram_agg.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("20", histogram_agg.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), histogram_agg.buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":1}", histogram_agg.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", histogram_agg.buckets[1].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("30", histogram_agg.buckets[2].key_json);
    try std.testing.expectEqual(@as(i64, 2), histogram_agg.buckets[2].count);
    try std.testing.expectEqualStrings("{\"value\":2}", histogram_agg.buckets[2].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":2}", histogram_agg.buckets[2].aggregations[1].value_json.?);

    const left_path_histogram_partials = (try left_idx.scanDistributedHistogramCardinalityPartials(&left_store, "path_amount_histogram", "/amount", .numeric, 10, "", child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_path_histogram_partials);
    const right_path_histogram_partials = (try right_idx.scanDistributedHistogramCardinalityPartials(&right_store, "path_amount_histogram", "/amount", .numeric, 10, "", child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_path_histogram_partials);
    var path_histogram_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_path_histogram_partials.len + right_path_histogram_partials.len);
    defer alloc.free(path_histogram_partials);
    @memcpy(path_histogram_partials[0..left_path_histogram_partials.len], left_path_histogram_partials);
    @memcpy(path_histogram_partials[left_path_histogram_partials.len..], right_path_histogram_partials);
    var path_histogram_merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, path_histogram_partials);
    defer path_histogram_merged.deinit(alloc);

    const path_histogram_request = SearchAggregationRequest{
        .name = "path_amount_histogram",
        .type = "histogram",
        .field = "/amount",
        .interval = 10,
        .aggregations = nested[0..],
    };
    var path_histogram_agg = (try algebraicHistogramAggregationFromDistributedPartialsAlloc(alloc, &left_idx, path_histogram_request, &.{}, path_histogram_merged)) orelse return error.TestUnexpectedResult;
    defer path_histogram_agg.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), path_histogram_agg.buckets.len);
    try std.testing.expectEqualStrings("10", path_histogram_agg.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), path_histogram_agg.buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2}", path_histogram_agg.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", path_histogram_agg.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("20", path_histogram_agg.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), path_histogram_agg.buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":1}", path_histogram_agg.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", path_histogram_agg.buckets[1].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("30", path_histogram_agg.buckets[2].key_json);
    try std.testing.expectEqual(@as(i64, 2), path_histogram_agg.buckets[2].count);
    try std.testing.expectEqualStrings("{\"value\":2}", path_histogram_agg.buckets[2].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":2}", path_histogram_agg.buckets[2].aggregations[1].value_json.?);

    const left_date_partials = (try left_idx.scanDistributedHistogramCardinalityPartials(&left_store, "orders_by_day", "created_at", .date, 0, "day", child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_date_partials);
    const right_date_partials = (try right_idx.scanDistributedHistogramCardinalityPartials(&right_store, "orders_by_day", "created_at", .date, 0, "day", child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_date_partials);
    var date_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_date_partials.len + right_date_partials.len);
    defer alloc.free(date_partials);
    @memcpy(date_partials[0..left_date_partials.len], left_date_partials);
    @memcpy(date_partials[left_date_partials.len..], right_date_partials);
    var date_merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, date_partials);
    defer date_merged.deinit(alloc);

    const date_request = SearchAggregationRequest{
        .name = "orders_by_day",
        .type = "date_histogram",
        .field = "created_at",
        .calendar_interval = "day",
        .aggregations = nested[0..],
    };
    var date_agg = (try algebraicDateHistogramAggregationFromDistributedPartialsAlloc(alloc, &left_idx, date_request, &.{}, date_merged)) orelse return error.TestUnexpectedResult;
    defer date_agg.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), date_agg.buckets.len);
    try std.testing.expectEqualStrings("\"2026-01-02T00:00:00Z\"", date_agg.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), date_agg.buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":1}", date_agg.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", date_agg.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"2026-01-03T00:00:00Z\"", date_agg.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), date_agg.buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":2}", date_agg.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", date_agg.buckets[1].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"2026-01-04T00:00:00Z\"", date_agg.buckets[2].key_json);
    try std.testing.expectEqual(@as(i64, 1), date_agg.buckets[2].count);
    try std.testing.expectEqualStrings("{\"value\":1}", date_agg.buckets[2].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", date_agg.buckets[2].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"2026-01-05T00:00:00Z\"", date_agg.buckets[3].key_json);
    try std.testing.expectEqual(@as(i64, 1), date_agg.buckets[3].count);
    try std.testing.expectEqualStrings("{\"value\":1}", date_agg.buckets[3].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", date_agg.buckets[3].aggregations[1].value_json.?);

    const left_path_date_partials = (try left_idx.scanDistributedHistogramCardinalityPartials(&left_store, "path_orders_by_day", "/created_at", .date, 0, "day", child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, left_path_date_partials);
    const right_path_date_partials = (try right_idx.scanDistributedHistogramCardinalityPartials(&right_store, "path_orders_by_day", "/created_at", .date, 0, "day", child_exports[0..], &.{})).?;
    defer algebraic_mod.distributed.freePartials(alloc, right_path_date_partials);
    var path_date_partials = try alloc.alloc(algebraic_mod.distributed.Partial, left_path_date_partials.len + right_path_date_partials.len);
    defer alloc.free(path_date_partials);
    @memcpy(path_date_partials[0..left_path_date_partials.len], left_path_date_partials);
    @memcpy(path_date_partials[left_path_date_partials.len..], right_path_date_partials);
    var path_date_merged = try algebraic_mod.distributed.mergePartialsAlloc(alloc, path_date_partials);
    defer path_date_merged.deinit(alloc);

    const path_date_request = SearchAggregationRequest{
        .name = "path_orders_by_day",
        .type = "date_histogram",
        .field = "/created_at",
        .calendar_interval = "day",
        .aggregations = nested[0..],
    };
    var path_date_agg = (try algebraicDateHistogramAggregationFromDistributedPartialsAlloc(alloc, &left_idx, path_date_request, &.{}, path_date_merged)) orelse return error.TestUnexpectedResult;
    defer path_date_agg.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), path_date_agg.buckets.len);
    try std.testing.expectEqualStrings("\"2026-01-02T00:00:00Z\"", path_date_agg.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), path_date_agg.buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":1}", path_date_agg.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", path_date_agg.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"2026-01-03T00:00:00Z\"", path_date_agg.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), path_date_agg.buckets[1].count);
    try std.testing.expectEqualStrings("{\"value\":2}", path_date_agg.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", path_date_agg.buckets[1].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"2026-01-04T00:00:00Z\"", path_date_agg.buckets[2].key_json);
    try std.testing.expectEqual(@as(i64, 1), path_date_agg.buckets[2].count);
    try std.testing.expectEqualStrings("{\"value\":1}", path_date_agg.buckets[2].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", path_date_agg.buckets[2].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"2026-01-05T00:00:00Z\"", path_date_agg.buckets[3].key_json);
    try std.testing.expectEqual(@as(i64, 1), path_date_agg.buckets[3].count);
    try std.testing.expectEqualStrings("{\"value\":1}", path_date_agg.buckets[3].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", path_date_agg.buckets[3].aggregations[1].value_json.?);
}

test "algebraic aggregation planner can use configured implicit join materialization" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"integer"},
        \\    {"name":"segment","path":"segment","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer"}
        \\  ],
        \\  "materializations": [
        \\    {"name":"joined_segment_count","op":"count","join":"orders_customers","group_by":["segment"],"group_side":"right","measure_side":"left","implicit_query":true},
        \\    {"name":"joined_segment_amount","op":"sum","join":"orders_customers","group_by":["segment"],"measure":"amount","group_side":"right","measure_side":"left","implicit_query":true}
        \\  ]
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });
    const index = &manager.algebraic_indexes.items[0].index;

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":1,\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":1,\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":2,\"amount\":7}" },
        .{ .key = "c1", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":1,\"segment\":\"enterprise\"}" },
        .{ .key = "c2", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":2,\"segment\":\"startup\"}" },
    };
    try index.applyBatch(&store, .{ .documents = docs[0..] });

    const requests = [_]SearchAggregationRequest{.{
        .name = "segments",
        .type = "terms",
        .field = "segment",
        .aggregations = &.{.{
            .name = "amount",
            .type = "sum",
            .field = "amount",
        }},
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 2), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("\"enterprise\"", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("30", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("\"startup\"", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("7", aggregations[0].buckets[1].aggregations[0].value_json.?);
    const status_value = index.status();
    try std.testing.expectEqual(@as(u64, 1), status_value.planner_algebraic_selected);
    try std.testing.expectEqual(@as(u64, 0), status_value.planner_fallback_count);
}

test "algebraic aggregation planner can execute derived bounded join metric plan" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"region","path":"region","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var index = try algebraic_mod.index.Index.open(alloc, "alg_derived_metric", cfg);
    defer index.close();

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "c1", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c1\",\"region\":\"west\"}" },
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":20}" },
    };
    try index.applyBatch(&store, .{ .documents = docs[0..] });

    const plan = algebraic_mod.planner.planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "joined_amount",
        .metric = .{ .name = "joined_amount", .op = .sum, .field = "amount" },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    try std.testing.expectEqual(algebraic_mod.planner.PlanKind.derived_join_fold, plan.kind);
    const raw = (try algebraicDerivedJoinMetricRawAlloc(&index, &store, plan.derived_join_fold.?)).?;
    defer alloc.free(raw);
    try std.testing.expectEqual(@as(f64, 30), try algebraic_mod.algebra.parseF64(raw));

    const avg_plan = algebraic_mod.planner.planMetricQuery(&index, .{
        .kind = .metric,
        .aggregation_name = "joined_amount_avg",
        .metric = .{ .name = "joined_amount_avg", .op = .avg, .field = "amount" },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    try std.testing.expectEqual(algebraic_mod.planner.PlanKind.derived_join_fold, avg_plan.kind);
    const avg_raw = (try algebraicDerivedJoinMetricRawAlloc(&index, &store, avg_plan.derived_join_fold.?)).?;
    defer alloc.free(avg_raw);
    const avg = try algebraic_mod.algebra.parseAvg(avg_raw);
    try std.testing.expectEqual(@as(f64, 30), avg.sum);
    try std.testing.expectEqual(@as(i64, 2), avg.count);
}

test "algebraic aggregation planner can execute derived bounded join terms plan" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"region","path":"region","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var index = try algebraic_mod.index.Index.open(alloc, "alg_derived_terms", cfg);
    defer index.close();

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "c1", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c1\",\"region\":\"west\"}" },
        .{ .key = "c2", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c2\",\"region\":\"east\"}" },
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c2\",\"amount\":7}" },
    };
    try index.applyBatch(&store, .{ .documents = docs[0..] });

    var plan = try algebraic_mod.planner.planBucketQueryAlloc(alloc, &index, .{
        .kind = .terms,
        .aggregation_name = "regions",
        .bucket_field = "region",
        .child_metrics = &.{
            .{ .name = "amount", .op = .sum, .field = "amount" },
            .{ .name = "amount_stats", .op = .avg, .field = "amount" },
            .{ .name = "amount_stats", .op = .min, .field = "amount" },
            .{ .name = "amount_stats", .op = .max, .field = "amount" },
            .{ .name = "amount_stats", .op = .sumsquares, .field = "amount" },
        },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    defer plan.deinit(alloc);
    try std.testing.expectEqual(algebraic_mod.planner.PlanKind.derived_join_fold, plan.kind);

    const child_requests = [_]SearchAggregationRequest{
        .{ .name = "amount", .type = "sum", .field = "amount" },
        .{ .name = "amount_stats", .type = "stats", .field = "amount" },
    };
    const request = SearchAggregationRequest{
        .name = "regions",
        .type = "terms",
        .field = "region",
        .aggregations = child_requests[0..],
    };
    var result = (try computeDerivedJoinTermsAggregation(
        alloc,
        &index,
        &store,
        request,
        child_requests[0..],
        plan.derived_join_fold.?,
        plan.derived_child_join_folds,
    )).?;
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), result.buckets.len);
    try std.testing.expectEqualStrings("\"west\"", result.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), result.buckets[0].count);
    try std.testing.expectEqualStrings("30", result.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":30,\"avg\":15,\"min\":10,\"max\":20,\"sum_squares\":500,\"variance\":25,\"std_dev\":5}", result.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"east\"", result.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), result.buckets[1].count);
    try std.testing.expectEqualStrings("7", result.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":7,\"avg\":7,\"min\":7,\"max\":7,\"sum_squares\":49,\"variance\":0,\"std_dev\":0}", result.buckets[1].aggregations[1].value_json.?);

    const constraints = [_]algebraic_mod.ir.Constraint{.{ .field = "customer", .value = "c1" }};
    var constrained_plan = try algebraic_mod.planner.planBucketQueryAlloc(alloc, &index, .{
        .kind = .terms,
        .aggregation_name = "regions",
        .bucket_field = "region",
        .constraints = constraints[0..],
        .child_metrics = &.{
            .{ .name = "amount", .op = .sum, .field = "amount" },
        },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    defer constrained_plan.deinit(alloc);
    try std.testing.expectEqual(algebraic_mod.planner.PlanKind.derived_join_fold, constrained_plan.kind);
    const constrained_child_requests = [_]SearchAggregationRequest{
        .{ .name = "amount", .type = "sum", .field = "amount" },
    };
    var constrained_result = (try computeDerivedJoinTermsAggregation(
        alloc,
        &index,
        &store,
        request,
        constrained_child_requests[0..],
        constrained_plan.derived_join_fold.?,
        constrained_plan.derived_child_join_folds,
    )).?;
    defer constrained_result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), constrained_result.buckets.len);
    try std.testing.expectEqualStrings("\"west\"", constrained_result.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), constrained_result.buckets[0].count);
    try std.testing.expectEqualStrings("30", constrained_result.buckets[0].aggregations[0].value_json.?);
}

test "algebraic aggregation planner can execute derived bounded join date histogram plan" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"region","path":"region","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [{"name":"created_at","path":"created_at","type":"datetime"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var index = try algebraic_mod.index.Index.open(alloc, "alg_derived_date", cfg);
    defer index.close();

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "c1", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c1\"}" },
        .{ .key = "c2", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c2\"}" },
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":10,\"created_at\":\"2026-05-01T10:00:00Z\"}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":20,\"created_at\":\"2026-05-01T12:00:00Z\"}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c2\",\"amount\":7,\"created_at\":\"2026-05-02T09:00:00Z\"}" },
    };
    try index.applyBatch(&store, .{ .documents = docs[0..] });

    var plan = try algebraic_mod.planner.planBucketQueryAlloc(alloc, &index, .{
        .kind = .date_histogram,
        .aggregation_name = "orders_by_day",
        .time_field = "created_at",
        .time_bucket = "day",
        .child_metrics = &.{
            .{ .name = "amount", .op = .sum, .field = "amount" },
            .{ .name = "amount_stats", .op = .avg, .field = "amount" },
            .{ .name = "amount_stats", .op = .min, .field = "amount" },
            .{ .name = "amount_stats", .op = .max, .field = "amount" },
            .{ .name = "amount_stats", .op = .sumsquares, .field = "amount" },
        },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    defer plan.deinit(alloc);
    try std.testing.expectEqual(algebraic_mod.planner.PlanKind.derived_join_fold, plan.kind);

    const child_requests = [_]SearchAggregationRequest{
        .{ .name = "amount", .type = "sum", .field = "amount" },
        .{ .name = "amount_stats", .type = "stats", .field = "amount" },
    };
    const request = SearchAggregationRequest{
        .name = "orders_by_day",
        .type = "date_histogram",
        .field = "created_at",
        .calendar_interval = "day",
        .aggregations = child_requests[0..],
    };
    var result = (try computeDerivedJoinDateHistogramAggregation(
        alloc,
        &index,
        &store,
        request,
        try parseDateInterval(request),
        child_requests[0..],
        plan.derived_join_fold.?,
        plan.derived_child_join_folds,
    )).?;
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), result.buckets.len);
    try std.testing.expectEqualStrings("\"2026-05-01T00:00:00Z\"", result.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), result.buckets[0].count);
    try std.testing.expectEqualStrings("30", result.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":30,\"avg\":15,\"min\":10,\"max\":20,\"sum_squares\":500,\"variance\":25,\"std_dev\":5}", result.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"2026-05-02T00:00:00Z\"", result.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), result.buckets[1].count);
    try std.testing.expectEqualStrings("7", result.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":7,\"avg\":7,\"min\":7,\"max\":7,\"sum_squares\":49,\"variance\":0,\"std_dev\":0}", result.buckets[1].aggregations[1].value_json.?);
}

test "algebraic aggregation planner can execute derived bounded join histogram plan" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"region","path":"region","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var index = try algebraic_mod.index.Index.open(alloc, "alg_derived_histogram", cfg);
    defer index.close();

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "c1", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c1\"}" },
        .{ .key = "c2", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c2\"}" },
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c2\",\"amount\":35}" },
    };
    try index.applyBatch(&store, .{ .documents = docs[0..] });

    var plan = try algebraic_mod.planner.planBucketQueryAlloc(alloc, &index, .{
        .kind = .histogram,
        .aggregation_name = "amount_histogram",
        .bucket_field = "amount",
        .bucket_interval = 20,
        .child_metrics = &.{
            .{ .name = "amount_sum", .op = .sum, .field = "amount" },
            .{ .name = "amount_stats", .op = .avg, .field = "amount" },
            .{ .name = "amount_stats", .op = .min, .field = "amount" },
            .{ .name = "amount_stats", .op = .max, .field = "amount" },
            .{ .name = "amount_stats", .op = .sumsquares, .field = "amount" },
        },
        .join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
    });
    defer plan.deinit(alloc);
    try std.testing.expectEqual(algebraic_mod.planner.PlanKind.derived_join_fold, plan.kind);

    const child_requests = [_]SearchAggregationRequest{
        .{ .name = "amount_sum", .type = "sum", .field = "amount" },
        .{ .name = "amount_stats", .type = "stats", .field = "amount" },
    };
    const request = SearchAggregationRequest{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 20,
        .aggregations = child_requests[0..],
    };
    var result = (try computeDerivedJoinHistogramAggregation(
        alloc,
        &index,
        &store,
        request,
        child_requests[0..],
        plan.derived_join_fold.?,
        plan.derived_child_join_folds,
    )).?;
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), result.buckets.len);
    try std.testing.expectEqualStrings("0", result.buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), result.buckets[0].count);
    try std.testing.expectEqualStrings("10", result.buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":10,\"avg\":10,\"min\":10,\"max\":10,\"sum_squares\":100,\"variance\":0,\"std_dev\":0}", result.buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("20", result.buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), result.buckets[1].count);
    try std.testing.expectEqualStrings("55", result.buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":55,\"avg\":27.5,\"min\":20,\"max\":35,\"sum_squares\":1625,\"variance\":56.25,\"std_dev\":7.5}", result.buckets[1].aggregations[1].value_json.?);
}

test "algebraic aggregation public request can execute explicit derived join metric" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg_public_derived_metric", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });
    const index = &manager.algebraic_indexes.items[0].index;

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "c1", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c1\"}" },
        .{ .key = "c2", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c2\"}" },
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c2\",\"amount\":7}" },
    };
    try index.applyBatch(&store, .{ .documents = docs[0..] });

    const requests = [_]SearchAggregationRequest{
        .{
            .name = "joined_amount",
            .type = "sum",
            .field = "amount",
            .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
        },
        .{
            .name = "joined_amount_avg",
            .type = "avg",
            .field = "amount",
            .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
        },
    };
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 0 };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 2), aggregations.len);
    try std.testing.expectEqualStrings("37", aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":3,\"sum\":37,\"avg\":12.333333333333334}", aggregations[1].value_json.?);

    const status_value = index.status();
    try std.testing.expectEqual(@as(u64, 2), status_value.planner_algebraic_selected);
    try std.testing.expectEqual(@as(u64, 0), status_value.planner_fallback_count);
}

test "algebraic aggregation public request can execute explicit derived join terms" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"region","path":"region","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg_public_derived_join", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });
    const index = &manager.algebraic_indexes.items[0].index;

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "c1", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c1\",\"region\":\"west\"}" },
        .{ .key = "c2", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c2\",\"region\":\"east\"}" },
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c2\",\"amount\":7}" },
    };
    try index.applyBatch(&store, .{ .documents = docs[0..] });

    const child_requests = [_]SearchAggregationRequest{
        .{ .name = "amount", .type = "sum", .field = "amount" },
        .{ .name = "amount_stats", .type = "stats", .field = "amount" },
    };
    const requests = [_]SearchAggregationRequest{.{
        .name = "regions",
        .type = "terms",
        .field = "region",
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
        .aggregations = child_requests[0..],
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 0 };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 2), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("\"west\"", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("30", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":30,\"avg\":15,\"min\":10,\"max\":20,\"sum_squares\":500,\"variance\":25,\"std_dev\":5}", aggregations[0].buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"east\"", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("7", aggregations[0].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":7,\"avg\":7,\"min\":7,\"max\":7,\"sum_squares\":49,\"variance\":0,\"std_dev\":0}", aggregations[0].buckets[1].aggregations[1].value_json.?);
}

test "algebraic aggregation public request can execute explicit derived join date histogram" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [{"name":"created_at","path":"created_at","type":"datetime"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg_public_derived_date", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });
    const index = &manager.algebraic_indexes.items[0].index;

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "c1", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c1\"}" },
        .{ .key = "c2", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c2\"}" },
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":10,\"created_at\":\"2026-05-01T10:00:00Z\"}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":20,\"created_at\":\"2026-05-01T12:00:00Z\"}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c2\",\"amount\":7,\"created_at\":\"2026-05-02T09:00:00Z\"}" },
    };
    try index.applyBatch(&store, .{ .documents = docs[0..] });

    const child_requests = [_]SearchAggregationRequest{
        .{ .name = "amount", .type = "sum", .field = "amount" },
        .{ .name = "amount_stats", .type = "stats", .field = "amount" },
    };
    const requests = [_]SearchAggregationRequest{.{
        .name = "orders_by_day",
        .type = "date_histogram",
        .field = "created_at",
        .calendar_interval = "day",
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
        .aggregations = child_requests[0..],
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 0 };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 2), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("\"2026-05-01T00:00:00Z\"", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("30", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":30,\"avg\":15,\"min\":10,\"max\":20,\"sum_squares\":500,\"variance\":25,\"std_dev\":5}", aggregations[0].buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"2026-05-02T00:00:00Z\"", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("7", aggregations[0].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":7,\"avg\":7,\"min\":7,\"max\":7,\"sum_squares\":49,\"variance\":0,\"std_dev\":0}", aggregations[0].buckets[1].aggregations[1].value_json.?);

    const status_value = index.status();
    try std.testing.expectEqual(@as(u64, 1), status_value.planner_algebraic_selected);
    try std.testing.expectEqual(@as(u64, 0), status_value.planner_fallback_count);
}

test "algebraic aggregation public request can execute explicit derived join histogram" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg_public_derived_histogram", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });
    const index = &manager.algebraic_indexes.items[0].index;

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "c1", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c1\"}" },
        .{ .key = "c2", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c2\"}" },
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c2\",\"amount\":35}" },
    };
    try index.applyBatch(&store, .{ .documents = docs[0..] });

    const child_requests = [_]SearchAggregationRequest{
        .{ .name = "amount_sum", .type = "sum", .field = "amount" },
        .{ .name = "amount_stats", .type = "stats", .field = "amount" },
    };
    const requests = [_]SearchAggregationRequest{.{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 20,
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
        .aggregations = child_requests[0..],
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 0 };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 2), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("0", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("10", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":10,\"avg\":10,\"min\":10,\"max\":10,\"sum_squares\":100,\"variance\":0,\"std_dev\":0}", aggregations[0].buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("20", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("55", aggregations[0].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":55,\"avg\":27.5,\"min\":20,\"max\":35,\"sum_squares\":1625,\"variance\":56.25,\"std_dev\":7.5}", aggregations[0].buckets[1].aggregations[1].value_json.?);

    const status_value = index.status();
    try std.testing.expectEqual(@as(u64, 1), status_value.planner_algebraic_selected);
    try std.testing.expectEqual(@as(u64, 0), status_value.planner_fallback_count);
}

test "algebraic aggregation public request can execute explicit derived join range" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"region","path":"region","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg_public_derived_range", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });
    const index = &manager.algebraic_indexes.items[0].index;

    const docs = [_]derived_types.DerivedDocument{
        .{ .key = "c1", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c1\",\"region\":\"west\"}" },
        .{ .key = "c2", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c2\",\"region\":\"east\"}" },
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c2\",\"amount\":35}" },
    };
    try index.applyBatch(&store, .{ .documents = docs[0..] });

    const child_requests = [_]SearchAggregationRequest{
        .{ .name = "amount_sum", .type = "sum", .field = "amount" },
        .{ .name = "amount_stats", .type = "stats", .field = "amount" },
    };
    const requests = [_]SearchAggregationRequest{.{
        .name = "amount_ranges",
        .type = "range",
        .field = "amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 40 },
        },
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
        .aggregations = child_requests[0..],
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 0 };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
        .algebraic_constraints = &.{.{ .field = "region", .value = "west" }},
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 2), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("\"low\"", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("10", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":10,\"avg\":10,\"min\":10,\"max\":10,\"sum_squares\":100,\"variance\":0,\"std_dev\":0}", aggregations[0].buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"high\"", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("20", aggregations[0].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":20,\"avg\":20,\"min\":20,\"max\":20,\"sum_squares\":400,\"variance\":0,\"std_dev\":0}", aggregations[0].buckets[1].aggregations[1].value_json.?);

    const status_value = index.status();
    try std.testing.expectEqual(@as(u64, 1), status_value.planner_algebraic_selected);
    try std.testing.expectEqual(@as(u64, 0), status_value.planner_fallback_count);
}

test "algebraic aggregation public request can execute explicit derived join date range" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "mixed",
        \\  "group_fields": [
        \\    {"name":"kind","path":"kind","type":"string"},
        \\    {"name":"customer","path":"customer","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [{"name":"created_at","path":"created_at","type":"datetime"}],
        \\  "joins": [
        \\    {"name":"orders_customers","left_fields":["customer"],"right_fields":["customer"],"left_type_field":"kind","left_type_value":"order","right_type_field":"kind","right_type_value":"customer","max_fanout":8}
        \\  ],
        \\  "materializations": []
        \\}
    ;
    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg_public_derived_date_range", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });
    const index = &manager.algebraic_indexes.items[0].index;

    const docs = [_]derived_types.DerivedDocument{
        .{ .key = "c1", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c1\"}" },
        .{ .key = "c2", .action = .upsert, .cleaned_value = "{\"kind\":\"customer\",\"customer\":\"c2\"}" },
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":10,\"created_at\":\"2026-05-01T10:00:00Z\"}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c1\",\"amount\":20,\"created_at\":\"2026-05-01T12:00:00Z\"}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"kind\":\"order\",\"customer\":\"c2\",\"amount\":7,\"created_at\":\"2026-05-02T09:00:00Z\"}" },
    };
    try index.applyBatch(&store, .{ .documents = docs[0..] });

    const child_requests = [_]SearchAggregationRequest{
        .{ .name = "amount_sum", .type = "sum", .field = "amount" },
        .{ .name = "amount_stats", .type = "stats", .field = "amount" },
    };
    const requests = [_]SearchAggregationRequest{.{
        .name = "created_ranges",
        .type = "date_range",
        .field = "created_at",
        .date_ranges = &.{
            .{ .name = "may_1", .start = "2026-05-01T00:00:00Z", .end = "2026-05-02T00:00:00Z" },
            .{ .name = "may_2", .start = "2026-05-02T00:00:00Z", .end = "2026-05-03T00:00:00Z" },
        },
        .algebraic_join = .{ .name = "orders_customers", .group_side = "right", .measure_side = "left" },
        .aggregations = child_requests[0..],
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 0 };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 2), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("\"may_1\"", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("30", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":30,\"avg\":15,\"min\":10,\"max\":20,\"sum_squares\":500,\"variance\":25,\"std_dev\":5}", aggregations[0].buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"may_2\"", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("7", aggregations[0].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":7,\"avg\":7,\"min\":7,\"max\":7,\"sum_squares\":49,\"variance\":0,\"std_dev\":0}", aggregations[0].buckets[1].aggregations[1].value_json.?);

    const status_value = index.status();
    try std.testing.expectEqual(@as(u64, 1), status_value.planner_algebraic_selected);
    try std.testing.expectEqual(@as(u64, 0), status_value.planner_fallback_count);
}

test "algebraic aggregation planner answers constrained root and terms metrics" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"product","path":"product","type":"string"},
        \\    {"name":"day","path":"day","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"products","op":"count","group_by":["customer","product","day"]},
        \\    {"name":"amount_by_customer_product","op":"sum","group_by":["customer","product","day"],"measure":"amount"}
        \\  ]
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pen\",\"day\":\"2026-05-01\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"book\",\"day\":\"2026-05-01\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pencil\",\"day\":\"2026-05-01\",\"amount\":30}" },
        .{ .key = "o4", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pencil\",\"day\":\"2026-05-02\",\"amount\":20}" },
        .{ .key = "o5", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"product\":\"pen\",\"day\":\"2026-05-01\",\"amount\":7}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const requests = [_]SearchAggregationRequest{
        .{
            .name = "amount_by_customer",
            .type = "sum",
            .field = "amount",
        },
        .{
            .name = "products",
            .type = "terms",
            .field = "product",
            .aggregations = &.{ .{
                .name = "amount_by_customer_product",
                .type = "sum",
                .field = "amount",
            }, .{
                .name = "top_product",
                .type = "bucket_sort",
                .field = "",
                .bucket_path = "amount_by_customer_product",
                .sort_order = "desc",
                .size = 1,
            } },
        },
    };
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const constraints = [_]FixedConstraint{.{ .field = "customer", .value = "alice" }};
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
        .algebraic_constraints = constraints[0..],
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 2), aggregations.len);
    try std.testing.expectEqualStrings("80", aggregations[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), aggregations[1].buckets.len);
    try std.testing.expectEqualStrings("\"pencil\"", aggregations[1].buckets[0].key_json);
    try std.testing.expectEqualStrings("50", aggregations[1].buckets[0].aggregations[0].value_json.?);
}

test "algebraic aggregation planner answers constrained stats from law folds" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"avg_amount","op":"avg","group_by":["customer"],"measure":"amount"},
        \\    {"name":"min_amount","op":"min","group_by":["customer"],"measure":"amount"},
        \\    {"name":"max_amount","op":"max","group_by":["customer"],"measure":"amount"},
        \\    {"name":"sum_squares_amount","op":"sumsquares","group_by":["customer"],"measure":"amount"}
        \\  ]
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\"}" },
        .{ .key = "o4", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"amount\":50}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const requests = [_]SearchAggregationRequest{
        .{
            .name = "amount_stats",
            .type = "stats",
            .field = "amount",
        },
        .{
            .name = "amount_squares",
            .type = "sumsquares",
            .field = "amount",
        },
    };
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const constraints = [_]FixedConstraint{.{ .field = "customer", .value = "alice" }};
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
        .algebraic_constraints = constraints[0..],
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 2), aggregations.len);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":30,\"avg\":15,\"min\":10,\"max\":20,\"sum_squares\":500,\"variance\":25,\"std_dev\":5}", aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("500", aggregations[1].value_json.?);
    const status_value = manager.algebraic_indexes.items[0].index.status();
    try std.testing.expectEqual(@as(u64, 2), status_value.planner_algebraic_selected);
    try std.testing.expectEqual(@as(u64, 0), status_value.planner_fallback_count);
    try std.testing.expectEqualStrings("selected", status_value.planner_last_decision.?);
}

test "algebraic aggregation planner answers terms with nested stats from law folds" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"count_by_customer","op":"count","group_by":["customer"]},
        \\    {"name":"avg_amount","op":"avg","group_by":["customer"],"measure":"amount"},
        \\    {"name":"min_amount","op":"min","group_by":["customer"],"measure":"amount"},
        \\    {"name":"max_amount","op":"max","group_by":["customer"],"measure":"amount"},
        \\    {"name":"sum_squares_amount","op":"sumsquares","group_by":["customer"],"measure":"amount"}
        \\  ]
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]derived_types.DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"amount\":50}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const nested = [_]SearchAggregationRequest{.{
        .name = "amount_stats",
        .type = "stats",
        .field = "amount",
    }};
    const requests = [_]SearchAggregationRequest{.{
        .name = "customers",
        .type = "terms",
        .field = "customer",
        .aggregations = nested[0..],
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{ .alloc = alloc, .hits = hits, .total_hits = 0 };
    const aggregations = try computeSearchAggregations(alloc, requests[0..], result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 2), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("\"alice\"", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":30,\"avg\":15,\"min\":10,\"max\":20,\"sum_squares\":500,\"variance\":25,\"std_dev\":5}", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("\"bob\"", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":50,\"avg\":50,\"min\":50,\"max\":50,\"sum_squares\":2500,\"variance\":0,\"std_dev\":0}", aggregations[0].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 1), manager.algebraic_indexes.items[0].index.status().planner_algebraic_selected);
    try std.testing.expectEqual(@as(u64, 0), manager.algebraic_indexes.items[0].index.status().planner_fallback_count);
}

test "algebraic aggregation planner falls back when rollup scan budget is exceeded" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "max_planner_scan_rows": 1,
        \\  "group_fields": [
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"product","path":"product","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"amount_by_customer_product","op":"sum","group_by":["customer","product"],"measure":"amount"}
        \\  ]
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pen\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"book\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"product\":\"pen\",\"amount\":7}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const requests = [_]SearchAggregationRequest{.{
        .name = "amount_by_customer",
        .type = "sum",
        .field = "amount",
    }};
    const hits = try alloc.alloc(types.SearchHit, 2);
    defer {
        for (hits) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    hits[0] = .{
        .id = try alloc.dupe(u8, "o1"),
        .stored_data = try alloc.dupe(u8, "{\"customer\":\"alice\",\"product\":\"pen\",\"amount\":10}"),
    };
    hits[1] = .{
        .id = try alloc.dupe(u8, "o2"),
        .stored_data = try alloc.dupe(u8, "{\"customer\":\"alice\",\"product\":\"book\",\"amount\":20}"),
    };
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = @intCast(hits.len),
    };
    const constraints = [_]FixedConstraint{.{ .field = "customer", .value = "alice" }};
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
        .algebraic_constraints = constraints[0..],
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqualStrings("30", aggregations[0].value_json.?);
    const status_value = manager.algebraic_indexes.items[0].index.status();
    try std.testing.expectEqual(@as(u64, 0), status_value.planner_algebraic_selected);
    try std.testing.expectEqual(@as(u64, 1), status_value.planner_fallback_count);
    try std.testing.expectEqualStrings("fallback", status_value.planner_last_decision.?);
    try std.testing.expectEqualStrings("metric_rollup_too_many_rows", status_value.planner_last_fallback_reason.?);
    try std.testing.expect(status_value.planner_lifecycle_ready);
    try std.testing.expect(status_value.planner_lifecycle_blocking_reason == null);
}

test "algebraic aggregation planner applies typed numeric constraints" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"store_id","path":"store_id","type":"integer"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "materializations": [
        \\    {"name":"amount_by_store","op":"sum","group_by":["store_id"],"measure":"amount"}
        \\  ]
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"store_id\":42,\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"store_id\":42,\"amount\":15}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"store_id\":7,\"amount\":99}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const requests = [_]SearchAggregationRequest{.{
        .name = "amount_by_store",
        .type = "sum",
        .field = "amount",
    }};
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const constraints = [_]FixedConstraint{.{ .field = "store_id", .value = "42" }};
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
        .algebraic_constraints = constraints[0..],
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqualStrings("25", aggregations[0].value_json.?);
}

test "algebraic aggregation planner answers range buckets from scalar facts" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [{"name":"created_at","path":"created_at","type":"datetime"}],
        \\  "materializations": [
        \\    {"name":"count_by_customer","op":"count","group_by":["customer"]}
        \\  ]
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":10,\"created_at\":\"2026-01-02T00:00:00Z\"}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"amount\":20,\"created_at\":\"2026-01-03T00:00:00Z\"}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"amount\":30,\"created_at\":\"2026-01-04T00:00:00Z\"}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };

    const range_requests = [_]SearchAggregationRequest{.{
        .name = "amount_ranges",
        .type = "range",
        .field = "amount",
        .ranges = &.{
            .{ .name = "low", .start = 0, .end = 20 },
            .{ .name = "high", .start = 20, .end = 40 },
        },
        .aggregations = &.{
            .{ .name = "amount_sum", .type = "sum", .field = "amount" },
            .{ .name = "amount_stats", .type = "stats", .field = "amount" },
            .{ .name = "customer_cardinality", .type = "cardinality", .field = "customer" },
        },
    }};
    const range_aggs = try computeSearchAggregations(alloc, &range_requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, range_aggs);
    try std.testing.expectEqual(@as(usize, 1), range_aggs.len);
    try std.testing.expectEqual(@as(usize, 2), range_aggs[0].buckets.len);
    try std.testing.expectEqualStrings("\"low\"", range_aggs[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), range_aggs[0].buckets[0].count);
    try std.testing.expectEqualStrings("10", range_aggs[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":10,\"avg\":10,\"min\":10,\"max\":10,\"sum_squares\":100,\"variance\":0,\"std_dev\":0}", range_aggs[0].buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":1}", range_aggs[0].buckets[0].aggregations[2].value_json.?);
    try std.testing.expectEqualStrings("\"high\"", range_aggs[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), range_aggs[0].buckets[1].count);
    try std.testing.expectEqualStrings("50", range_aggs[0].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":50,\"avg\":25,\"min\":20,\"max\":30,\"sum_squares\":1300,\"variance\":25,\"std_dev\":5}", range_aggs[0].buckets[1].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("{\"value\":2}", range_aggs[0].buckets[1].aggregations[2].value_json.?);

    const histogram_requests = [_]SearchAggregationRequest{.{
        .name = "amount_histogram",
        .type = "histogram",
        .field = "amount",
        .interval = 20,
        .aggregations = &.{
            .{ .name = "amount_sum", .type = "sum", .field = "amount" },
            .{ .name = "amount_stats", .type = "stats", .field = "amount" },
        },
    }};
    const histogram_aggs = try computeSearchAggregations(alloc, &histogram_requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, histogram_aggs);
    try std.testing.expectEqual(@as(usize, 1), histogram_aggs.len);
    try std.testing.expectEqual(@as(usize, 2), histogram_aggs[0].buckets.len);
    try std.testing.expectEqualStrings("0", histogram_aggs[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 1), histogram_aggs[0].buckets[0].count);
    try std.testing.expectEqualStrings("10", histogram_aggs[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":10,\"avg\":10,\"min\":10,\"max\":10,\"sum_squares\":100,\"variance\":0,\"std_dev\":0}", histogram_aggs[0].buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("20", histogram_aggs[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 2), histogram_aggs[0].buckets[1].count);
    try std.testing.expectEqualStrings("50", histogram_aggs[0].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":50,\"avg\":25,\"min\":20,\"max\":30,\"sum_squares\":1300,\"variance\":25,\"std_dev\":5}", histogram_aggs[0].buckets[1].aggregations[1].value_json.?);

    const constraints = [_]FixedConstraint{.{ .field = "customer", .value = "alice" }};
    const constrained = [_]SearchAggregationRequest{.{
        .name = "alice_amounts",
        .type = "range",
        .field = "amount",
        .ranges = &.{.{ .name = "all", .start = 0, .end = 40 }},
    }};
    const constrained_aggs = try computeSearchAggregations(alloc, &constrained, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
        .algebraic_constraints = constraints[0..],
    });
    defer deinitResults(alloc, constrained_aggs);
    try std.testing.expectEqual(@as(i64, 2), constrained_aggs[0].buckets[0].count);

    const date_requests = [_]SearchAggregationRequest{.{
        .name = "created_ranges",
        .type = "date_range",
        .field = "created_at",
        .date_ranges = &.{.{ .name = "recent", .start = "2026-01-03T00:00:00Z", .end = "2026-01-05T00:00:00Z" }},
        .aggregations = &.{.{ .name = "customer_cardinality", .type = "cardinality", .field = "customer" }},
    }};
    const date_aggs = try computeSearchAggregations(alloc, &date_requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, date_aggs);
    try std.testing.expectEqual(@as(i64, 2), date_aggs[0].buckets[0].count);
    try std.testing.expectEqualStrings("{\"value\":2}", date_aggs[0].buckets[0].aggregations[0].value_json.?);

    const status_value = manager.algebraic_indexes.items[0].index.status();
    try std.testing.expect(status_value.planner_algebraic_selected >= 3);
}

test "algebraic aggregation planner rolls up date cylinders across extra dimensions" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [
        \\    {"name":"customer","path":"customer","type":"string"},
        \\    {"name":"product","path":"product","type":"string"}
        \\  ],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [{"name":"created","path":"created_at","type":"timestamp"}],
        \\  "materializations": [
        \\    {"name":"orders_by_day","op":"count","group_by":["customer","product"],"time":"created","bucket":"day"},
        \\    {"name":"amount_by_day","op":"sum","group_by":["customer","product"],"measure":"amount","time":"created","bucket":"day"}
        \\  ]
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pen\",\"created_at\":\"2026-05-01T10:00:00Z\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"book\",\"created_at\":\"2026-05-01T11:00:00Z\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"product\":\"pen\",\"created_at\":\"2026-05-02T09:00:00Z\",\"amount\":5}" },
        .{ .key = "o4", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"product\":\"pen\",\"created_at\":\"2026-05-01T10:00:00Z\",\"amount\":7}" },
    };
    try manager.algebraic_indexes.items[0].index.applyBatch(&store, .{ .documents = docs[0..] });

    const requests = [_]SearchAggregationRequest{.{
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
    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const constraints = [_]FixedConstraint{.{ .field = "customer", .value = "alice" }};
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
        .algebraic_constraints = constraints[0..],
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 1), aggregations.len);
    try std.testing.expectEqual(@as(usize, 2), aggregations[0].buckets.len);
    try std.testing.expectEqualStrings("\"2026-05-01T00:00:00Z\"", aggregations[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[0].buckets[0].count);
    try std.testing.expectEqualStrings("30", aggregations[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("\"2026-05-02T00:00:00Z\"", aggregations[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[0].buckets[1].count);
    try std.testing.expectEqualStrings("5", aggregations[0].buckets[1].aggregations[0].value_json.?);

    const unfiltered = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, unfiltered);

    try std.testing.expectEqual(@as(usize, 1), unfiltered.len);
    try std.testing.expectEqual(@as(usize, 2), unfiltered[0].buckets.len);
    try std.testing.expectEqualStrings("\"2026-05-01T00:00:00Z\"", unfiltered[0].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 3), unfiltered[0].buckets[0].count);
    try std.testing.expectEqualStrings("37", unfiltered[0].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("\"2026-05-02T00:00:00Z\"", unfiltered[0].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), unfiltered[0].buckets[1].count);
    try std.testing.expectEqualStrings("5", unfiltered[0].buckets[1].aggregations[0].value_json.?);
}

test "algebraic aggregation planner reads ready adaptive tensor materializations" {
    const alloc = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const cfg =
        \\{
        \\  "version": 1,
        \\  "table": "orders",
        \\  "group_fields": [{"name":"customer","path":"customer","type":"string"}],
        \\  "measure_fields": [{"name":"amount","path":"amount","type":"number"}],
        \\  "time_fields": [{"name":"created","path":"created_at","type":"timestamp"}],
        \\  "adaptive": {"observe": true, "lazy_materialization": true, "min_observations": 1, "max_backfill_rows_per_tick": 10, "min_estimated_scan_rows_saved": 1},
        \\  "materializations": []
        \\}
    ;

    var manager = try index_manager_mod.IndexManager.init(alloc, ".");
    defer manager.deinit();
    const mutex = try alloc.create(std.atomic.Mutex);
    mutex.* = .unlocked;
    const config = try types.IndexConfig.clone(alloc, .{
        .name = "alg",
        .kind = .algebraic,
        .config_json = cfg,
    });
    const alg_index = try algebraic_mod.index.Index.open(alloc, "alg", cfg);
    try manager.algebraic_indexes.append(alloc, .{
        .apply_mutex = mutex,
        .config = config,
        .index = alg_index,
    });
    const index = &manager.algebraic_indexes.items[0].index;

    const docs = [_]@import("derived/derived_types.zig").DerivedDocument{
        .{ .key = "o1", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"created_at\":\"2026-05-01T10:00:00Z\",\"amount\":10}" },
        .{ .key = "o2", .action = .upsert, .cleaned_value = "{\"customer\":\"alice\",\"created_at\":\"2026-05-01T11:00:00Z\",\"amount\":20}" },
        .{ .key = "o3", .action = .upsert, .cleaned_value = "{\"customer\":\"bob\",\"created_at\":\"2026-05-02T09:00:00Z\",\"amount\":7}" },
    };
    try index.applyBatch(&store, .{ .documents = docs[0..] });

    index.recordObservedQueryShapeWithStore(&store, .{
        .kind = .metric,
        .aggregation_name = "amount_total",
        .metric = .{ .name = "amount_total", .op = .sum, .field = "amount" },
    }, "metric_no_materialization");
    index.recordObservedQueryShapeWithStore(&store, .{
        .kind = .terms,
        .aggregation_name = "customers",
        .bucket_field = "customer",
    }, "terms_unsupported");
    const term_child_queries = [_]algebraic_mod.ir.Query{
        .{
            .kind = .terms,
            .aggregation_name = "customer_amount_sum",
            .bucket_field = "customer",
            .metric = .{ .name = "customer_amount_sum", .op = .sum, .field = "amount" },
        },
        .{
            .kind = .terms,
            .aggregation_name = "customer_amount_stats",
            .bucket_field = "customer",
            .metric = .{ .name = "customer_amount_stats", .op = .avg, .field = "amount" },
        },
        .{
            .kind = .terms,
            .aggregation_name = "customer_amount_stats",
            .bucket_field = "customer",
            .metric = .{ .name = "customer_amount_stats", .op = .min, .field = "amount" },
        },
        .{
            .kind = .terms,
            .aggregation_name = "customer_amount_stats",
            .bucket_field = "customer",
            .metric = .{ .name = "customer_amount_stats", .op = .max, .field = "amount" },
        },
        .{
            .kind = .terms,
            .aggregation_name = "customer_amount_stats",
            .bucket_field = "customer",
            .metric = .{ .name = "customer_amount_stats", .op = .sumsquares, .field = "amount" },
        },
    };
    for (term_child_queries) |query| index.recordObservedQueryShapeWithStore(&store, query, "terms_child_unsupported");
    index.recordObservedQueryShapeWithStore(&store, .{
        .kind = .date_histogram,
        .aggregation_name = "orders_by_day",
        .time_field = "created",
        .time_bucket = "day",
    }, "date_histogram_unsupported");
    index.recordObservedQueryShapeWithStore(&store, .{
        .kind = .date_histogram,
        .aggregation_name = "day_amount_sum",
        .time_field = "created",
        .time_bucket = "day",
        .metric = .{ .name = "day_amount_sum", .op = .sum, .field = "amount" },
    }, "date_histogram_child_unsupported");
    const day_stats_queries = [_]algebraic_mod.ir.Query{
        .{
            .kind = .date_histogram,
            .aggregation_name = "day_amount_stats",
            .time_field = "created",
            .time_bucket = "day",
            .metric = .{ .name = "day_amount_stats", .op = .avg, .field = "amount" },
        },
        .{
            .kind = .date_histogram,
            .aggregation_name = "day_amount_stats",
            .time_field = "created",
            .time_bucket = "day",
            .metric = .{ .name = "day_amount_stats", .op = .min, .field = "amount" },
        },
        .{
            .kind = .date_histogram,
            .aggregation_name = "day_amount_stats",
            .time_field = "created",
            .time_bucket = "day",
            .metric = .{ .name = "day_amount_stats", .op = .max, .field = "amount" },
        },
        .{
            .kind = .date_histogram,
            .aggregation_name = "day_amount_stats",
            .time_field = "created",
            .time_bucket = "day",
            .metric = .{ .name = "day_amount_stats", .op = .sumsquares, .field = "amount" },
        },
    };
    for (day_stats_queries) |query| index.recordObservedQueryShapeWithStore(&store, query, "date_histogram_child_unsupported");

    try std.testing.expectEqual(@as(u64, 13), try index.evaluateAdaptiveCandidates(&store, 0));
    while (try index.runAdaptiveWork(&store, 0) != 0) {}

    const metric_recommendation = try algebraic_mod.adaptive.recommendationAlloc(alloc, .{
        .kind = .metric,
        .aggregation_name = "amount_total",
        .metric = .{ .name = "amount_total", .op = .sum, .field = "amount" },
    });
    defer alloc.free(metric_recommendation);
    const empty_axes = try algebraic_mod.token.canonicalTupleAlloc(alloc, &.{});
    defer alloc.free(empty_axes);
    var expression_raw = (try index.adaptiveMaterializedExpressionRawValueForRecommendationAlloc(&store, metric_recommendation, empty_axes, null)).?;
    defer expression_raw.deinit(index.alloc);
    try std.testing.expectEqualStrings("37", expression_raw.raw.?);

    const hits = try alloc.alloc(types.SearchHit, 0);
    defer alloc.free(hits);
    const result = types.SearchResult{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 0,
    };
    const requests = [_]SearchAggregationRequest{
        .{ .name = "amount_total", .type = "sum", .field = "amount" },
        .{
            .name = "customers",
            .type = "terms",
            .field = "customer",
            .aggregations = &.{
                .{ .name = "customer_amount_sum", .type = "sum", .field = "amount" },
                .{ .name = "customer_amount_stats", .type = "stats", .field = "amount" },
            },
        },
        .{
            .name = "orders_by_day",
            .type = "date_histogram",
            .field = "created_at",
            .calendar_interval = "day",
            .aggregations = &.{
                .{ .name = "day_amount_sum", .type = "sum", .field = "amount" },
                .{ .name = "day_amount_stats", .type = "stats", .field = "amount" },
            },
        },
    };
    const aggregations = try computeSearchAggregations(alloc, &requests, result, .{
        .index_manager = &manager,
        .doc_store = &store,
        .algebraic_scope = .root,
        .algebraic_available = true,
    });
    defer deinitResults(alloc, aggregations);

    try std.testing.expectEqual(@as(usize, 3), aggregations.len);
    try std.testing.expectEqualStrings("37", aggregations[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 2), aggregations[1].buckets.len);
    try std.testing.expectEqualStrings("\"alice\"", aggregations[1].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[1].buckets[0].count);
    try std.testing.expectEqualStrings("30", aggregations[1].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":30,\"avg\":15,\"min\":10,\"max\":20,\"sum_squares\":500,\"variance\":25,\"std_dev\":5}", aggregations[1].buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"bob\"", aggregations[1].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[1].buckets[1].count);
    try std.testing.expectEqualStrings("7", aggregations[1].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":7,\"avg\":7,\"min\":7,\"max\":7,\"sum_squares\":49,\"variance\":0,\"std_dev\":0}", aggregations[1].buckets[1].aggregations[1].value_json.?);
    try std.testing.expectEqual(@as(usize, 2), aggregations[2].buckets.len);
    try std.testing.expectEqualStrings("\"2026-05-01T00:00:00Z\"", aggregations[2].buckets[0].key_json);
    try std.testing.expectEqual(@as(i64, 2), aggregations[2].buckets[0].count);
    try std.testing.expectEqualStrings("30", aggregations[2].buckets[0].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":2,\"sum\":30,\"avg\":15,\"min\":10,\"max\":20,\"sum_squares\":500,\"variance\":25,\"std_dev\":5}", aggregations[2].buckets[0].aggregations[1].value_json.?);
    try std.testing.expectEqualStrings("\"2026-05-02T00:00:00Z\"", aggregations[2].buckets[1].key_json);
    try std.testing.expectEqual(@as(i64, 1), aggregations[2].buckets[1].count);
    try std.testing.expectEqualStrings("7", aggregations[2].buckets[1].aggregations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"count\":1,\"sum\":7,\"avg\":7,\"min\":7,\"max\":7,\"sum_squares\":49,\"variance\":0,\"std_dev\":0}", aggregations[2].buckets[1].aggregations[1].value_json.?);

    const status_value = index.status();
    try std.testing.expect(status_value.planner_algebraic_selected >= 3);
    try std.testing.expectEqual(@as(u64, 0), status_value.planner_fallback_count);
}

test "date interval aliases align with algebraic buckets" {
    try std.testing.expectEqual(search_agg_mod.DateInterval.hour, try parseDateInterval(.{ .name = "by_time", .type = "date_histogram", .field = "created_at", .calendar_interval = "60m" }));
    try std.testing.expectEqualStrings("hour", algebraicBucketName(.{ .name = "by_time", .type = "date_histogram", .field = "created_at", .calendar_interval = "60m" }).?);
    try std.testing.expectEqual(search_agg_mod.DateInterval.day, try parseDateInterval(.{ .name = "by_time", .type = "date_histogram", .field = "created_at", .calendar_interval = "24h" }));
    try std.testing.expectEqualStrings("day", algebraicBucketName(.{ .name = "by_time", .type = "date_histogram", .field = "created_at", .calendar_interval = "24h" }).?);
    try std.testing.expectEqual(search_agg_mod.DateInterval.month, try parseDateInterval(.{ .name = "by_time", .type = "date_histogram", .field = "created_at", .calendar_interval = "1M" }));
    try std.testing.expectEqualStrings("month", algebraicBucketName(.{ .name = "by_time", .type = "date_histogram", .field = "created_at", .calendar_interval = "1M" }).?);
}

fn accumulateNumericJsonValue(
    value: std.json.Value,
    sum: *f64,
    sum_squares: *f64,
    count: *i64,
    min_value: *f64,
    max_value: *f64,
) void {
    switch (value) {
        .array => |arr| for (arr.items) |item| {
            accumulateNumericJsonValue(item, sum, sum_squares, count, min_value, max_value);
        },
        else => if (jsonValueToF64(value)) |numeric| {
            sum.* += numeric;
            sum_squares.* += numeric * numeric;
            count.* += 1;
            if (numeric < min_value.*) min_value.* = numeric;
            if (numeric > max_value.*) max_value.* = numeric;
        },
    }
}

fn collectCardinalityValues(alloc: Allocator, seen: *std.StringHashMap(void), value: std.json.Value) !void {
    switch (value) {
        .array => |arr| {
            for (arr.items) |item| try collectCardinalityValues(alloc, seen, item);
        },
        else => {
            const key = try stringifyJsonValueCompact(alloc, value);
            errdefer alloc.free(key);
            const entry = try seen.getOrPut(key);
            if (entry.found_existing) {
                alloc.free(key);
            } else {
                entry.key_ptr.* = key;
                entry.value_ptr.* = {};
            }
        },
    }
}

fn appendTermAggregationValues(
    alloc: Allocator,
    counts: *std.StringHashMap(i64),
    grouped: *std.StringHashMap(std.ArrayListUnmanaged(types.SearchHit)),
    hit: types.SearchHit,
    value: std.json.Value,
) !void {
    switch (value) {
        .array => |arr| {
            for (arr.items) |item| try appendTermAggregationValues(alloc, counts, grouped, hit, item);
        },
        else => {
            const key = try jsonValueToTermKey(alloc, value);
            defer alloc.free(key);

            const count_entry = try counts.getOrPut(key);
            if (count_entry.found_existing) {
                count_entry.value_ptr.* += 1;
            } else {
                count_entry.key_ptr.* = try alloc.dupe(u8, key);
                count_entry.value_ptr.* = 1;
            }

            const group_entry = try grouped.getOrPut(count_entry.key_ptr.*);
            if (!group_entry.found_existing) group_entry.value_ptr.* = .empty;
            try group_entry.value_ptr.append(alloc, hit);
        },
    }
}

fn compositeTermComponentsAlloc(alloc: Allocator, value: std.json.Value) !CompositeTermComponents {
    return switch (value) {
        .array => |arr| blk: {
            const items = try alloc.alloc([]u8, arr.items.len);
            var filled: usize = 0;
            errdefer {
                for (items[0..filled]) |item| alloc.free(item);
                if (items.len > 0) alloc.free(items);
            }
            for (arr.items, 0..) |item, i| {
                items[i] = try stringifyJsonValueCompact(alloc, item);
                filled = i + 1;
            }
            break :blk .{ .items = items };
        },
        else => blk: {
            const items = try alloc.alloc([]u8, 1);
            errdefer alloc.free(items);
            items[0] = try stringifyJsonValueCompact(alloc, value);
            break :blk .{ .items = items };
        },
    };
}

fn appendCompositeTermAggregationValues(
    alloc: Allocator,
    counts: *std.StringHashMap(i64),
    grouped: *std.StringHashMap(std.ArrayListUnmanaged(types.SearchHit)),
    hit: types.SearchHit,
    components: []const CompositeTermComponents,
) !void {
    const selected = try alloc.alloc([]const u8, components.len);
    defer alloc.free(selected);
    try appendCompositeTermAggregationValuesAt(alloc, counts, grouped, hit, components, selected, 0);
}

fn appendCompositeTermAggregationValuesAt(
    alloc: Allocator,
    counts: *std.StringHashMap(i64),
    grouped: *std.StringHashMap(std.ArrayListUnmanaged(types.SearchHit)),
    hit: types.SearchHit,
    components: []const CompositeTermComponents,
    selected: [][]const u8,
    field_idx: usize,
) !void {
    if (field_idx == components.len) {
        const key = try compositeTermKeyJsonAlloc(alloc, selected);
        defer alloc.free(key);

        const count_entry = try counts.getOrPut(key);
        if (count_entry.found_existing) {
            count_entry.value_ptr.* += 1;
        } else {
            count_entry.key_ptr.* = try alloc.dupe(u8, key);
            count_entry.value_ptr.* = 1;
        }

        const group_entry = try grouped.getOrPut(count_entry.key_ptr.*);
        if (!group_entry.found_existing) group_entry.value_ptr.* = .empty;
        try group_entry.value_ptr.append(alloc, hit);
        return;
    }

    for (components[field_idx].items) |component| {
        selected[field_idx] = component;
        try appendCompositeTermAggregationValuesAt(alloc, counts, grouped, hit, components, selected, field_idx + 1);
    }
}

fn compositeTermKeyJsonAlloc(alloc: Allocator, components: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '[');
    for (components, 0..) |component, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, component);
    }
    try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

fn jsonValueToTermKey(alloc: Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => try alloc.dupe(u8, value.string),
        .bool => if (value.bool) try alloc.dupe(u8, "true") else try alloc.dupe(u8, "false"),
        .integer => try std.fmt.allocPrint(alloc, "{d}", .{value.integer}),
        .float => try std.fmt.allocPrint(alloc, "{d}", .{value.float}),
        .number_string => try alloc.dupe(u8, value.number_string),
        .null => try alloc.dupe(u8, "null"),
        else => try stringifyJsonValueCompact(alloc, value),
    };
}

fn stringifyJsonValueCompact(alloc: Allocator, value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn distanceToMeters(value: f64, unit: []const u8) !f64 {
    if (unit.len == 0 or std.mem.eql(u8, unit, "m") or std.mem.eql(u8, unit, "meter") or std.mem.eql(u8, unit, "meters")) return value;
    if (std.mem.eql(u8, unit, "km") or std.mem.eql(u8, unit, "kilometer") or std.mem.eql(u8, unit, "kilometers")) return value * 1000.0;
    if (std.mem.eql(u8, unit, "mi") or std.mem.eql(u8, unit, "mile") or std.mem.eql(u8, unit, "miles")) return value * 1609.344;
    if (std.mem.eql(u8, unit, "ft") or std.mem.eql(u8, unit, "foot") or std.mem.eql(u8, unit, "feet")) return value * 0.3048;
    return error.UnsupportedAggregation;
}

fn extractGeoPointFieldFromStoredJson(alloc: Allocator, raw_json: []const u8, field_path: []const u8) !?geo_mod.GeoPoint {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{});
    defer parsed.deinit();

    const value = extractValueAtPath(parsed.value, field_path) orelse return null;
    return switch (value) {
        .object => |obj| blk: {
            const lat_value = obj.get("lat") orelse break :blk null;
            const lon_value = obj.get("lon") orelse break :blk null;
            const lat = jsonValueToF64(lat_value) orelse break :blk null;
            const lon = jsonValueToF64(lon_value) orelse break :blk null;
            break :blk .{ .lat = lat, .lon = lon };
        },
        else => null,
    };
}

fn jsonValueToF64(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => @floatFromInt(value.integer),
        .float => value.float,
        .number_string => std.fmt.parseFloat(f64, value.number_string) catch null,
        else => null,
    };
}

fn fillHistogramBucketKeys(alloc: Allocator, first_key: i64, last_key: i64) ![]i64 {
    if (last_key < first_key) return &.{};
    const len: usize = @intCast(last_key - first_key + 1);
    const keys = try alloc.alloc(i64, len);
    for (keys, 0..) |*slot, idx| {
        slot.* = first_key + @as(i64, @intCast(idx));
    }
    return keys;
}

fn fillDateHistogramBucketKeys(
    alloc: Allocator,
    first_key: u64,
    last_key: u64,
    interval: search_agg_mod.DateInterval,
) ![]u64 {
    var keys: std.ArrayList(u64) = .empty;
    errdefer keys.deinit(alloc);

    var current = first_key;
    while (current <= last_key) {
        try keys.append(alloc, current);
        const next = try nextDateHistogramBucketKey(current, interval);
        if (next <= current) break;
        current = next;
    }
    return keys.toOwnedSlice(alloc);
}

fn nextDateHistogramBucketKey(current: u64, interval: search_agg_mod.DateInterval) !u64 {
    return switch (interval) {
        .minute => current + 60 * std.time.ns_per_s,
        .hour => current + std.time.ns_per_hour,
        .day => current + std.time.ns_per_day,
        .week => current + 7 * std.time.ns_per_day,
        .month => try addCalendarMonths(current, 1),
        .year => try addCalendarYears(current, 1),
    };
}

fn addCalendarMonths(current: u64, delta_months: i64) !u64 {
    const total_seconds: u64 = @intCast(@divFloor(current, std.time.ns_per_s));
    const days: i64 = @intCast(@divFloor(total_seconds, 86_400));
    const civil = civilFromDays(days);
    const month_index = (civil.year * 12 + (civil.month - 1)) + delta_months;
    var year = @divFloor(month_index, 12);
    var month = @mod(month_index, 12) + 1;
    if (month <= 0) {
        month += 12;
        year -= 1;
    }
    return civilDateToBucketNs(year, month, 1);
}

fn addCalendarYears(current: u64, delta_years: i64) !u64 {
    const total_seconds: u64 = @intCast(@divFloor(current, std.time.ns_per_s));
    const days: i64 = @intCast(@divFloor(total_seconds, 86_400));
    const civil = civilFromDays(days);
    return civilDateToBucketNs(civil.year + delta_years, 1, 1);
}

fn civilDateToBucketNs(year: i64, month: i64, day: i64) !u64 {
    const days = daysFromCivil(year, month, day);
    if (days < 0) return error.InvalidAggregation;
    return @as(u64, @intCast(days)) * std.time.ns_per_day;
}

fn extractNumericFieldFromStoredJson(alloc: Allocator, raw_json: []const u8, field_path: []const u8) !?f64 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{});
    defer parsed.deinit();
    const value = extractValueAtPath(parsed.value, field_path) orelse return null;
    return jsonValueToF64(value);
}

fn extractTimestampFieldFromStoredJson(alloc: Allocator, raw_json: []const u8, field_path: []const u8) !?u64 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{});
    defer parsed.deinit();
    const value = extractValueAtPath(parsed.value, field_path) orelse return null;
    return switch (value) {
        .integer => @intCast(value.integer),
        .float => @intFromFloat(value.float),
        .number_string => std.fmt.parseInt(u64, value.number_string, 10) catch null,
        .string => try parseRfc3339ToNs(value.string),
        else => null,
    };
}

fn parseDateInterval(request: SearchAggregationRequest) !search_agg_mod.DateInterval {
    const value = if (request.calendar_interval.len > 0) request.calendar_interval else request.fixed_interval;
    if (std.mem.eql(u8, value, "minute") or std.mem.eql(u8, value, "1m")) return .minute;
    if (std.mem.eql(u8, value, "hour") or std.mem.eql(u8, value, "1h") or std.mem.eql(u8, value, "60m")) return .hour;
    if (std.mem.eql(u8, value, "day") or std.mem.eql(u8, value, "1d") or std.mem.eql(u8, value, "24h")) return .day;
    if (std.mem.eql(u8, value, "week") or std.mem.eql(u8, value, "1w")) return .week;
    if (std.mem.eql(u8, value, "month") or std.mem.eql(u8, value, "1M")) return .month;
    if (std.mem.eql(u8, value, "year") or std.mem.eql(u8, value, "1y")) return .year;
    return error.UnsupportedAggregation;
}

fn parseRfc3339ToNs(text: []const u8) !?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return null;

    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;

    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len or text[idx] != 'Z' or idx + 1 != text.len) return null;

    const days = daysFromCivil(year, month, day);
    if (days < 0) return null;
    const secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    if (secs < 0) return null;
    return @as(u64, @intCast(secs)) * std.time.ns_per_s + nanos;
}

fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) @as(i64, 1) else @as(i64, 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

fn formatRfc3339Bucket(alloc: Allocator, ns: u64) ![]const u8 {
    const total_seconds: u64 = @intCast(@divFloor(ns, std.time.ns_per_s));
    const days: i64 = @intCast(@divFloor(total_seconds, 86_400));
    const secs_of_day: u64 = total_seconds % 86_400;
    const civil = civilFromDays(days);
    const hour: u64 = secs_of_day / 3_600;
    const minute: u64 = (secs_of_day % 3_600) / 60;
    const second: u64 = secs_of_day % 60;
    return try std.fmt.allocPrint(alloc, "{:0>4}-{:0>2}-{:0>2}T{:0>2}:{:0>2}:{:0>2}Z", .{
        @as(u64, @intCast(civil.year)),
        @as(u64, @intCast(civil.month)),
        @as(u64, @intCast(civil.day)),
        hour,
        minute,
        second,
    });
}

const CivilDate = struct {
    year: i64,
    month: i64,
    day: i64,
};

fn civilFromDays(days: i64) CivilDate {
    const z = days + 719_468;
    const era = @divFloor(if (z >= 0) z else z - 146_096, 146_097);
    const doe = z - era * 146_097;
    const yoe = @divFloor(doe - @divFloor(doe, 1_460) + @divFloor(doe, 36_524) - @divFloor(doe, 146_096), 365);
    var y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const d = doy - @divFloor(153 * mp + 2, 5) + 1;
    const m = mp + (if (mp < 10) @as(i64, 3) else @as(i64, -9));
    y += if (m <= 2) @as(i64, 1) else @as(i64, 0);
    return .{ .year = y, .month = m, .day = d };
}

fn extractValueAtPath(root: std.json.Value, field_path: []const u8) ?std.json.Value {
    if (root != .object and std.mem.eql(u8, field_path, "body")) return root;
    var current = root;
    var parts = std.mem.splitScalar(u8, field_path, '.');
    while (parts.next()) |part| {
        switch (current) {
            .object => |obj| current = obj.get(part) orelse return null,
            else => return null,
        }
    }
    return current;
}
