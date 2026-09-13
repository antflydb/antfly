// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Storage-independent aggregation values shared by query control and the
//! compiled storage kernel. Physical aggregation execution lives in
//! `aggregations.zig`; keeping that implementation out of this module is the
//! source-level half of the archive boundary.

const std = @import("std");
const Allocator = std.mem.Allocator;
const algebraic_ir = @import("algebraic/ir.zig");
const background_text_stats = @import("background_text_stats.zig");
const distributed_stats_mod = @import("../../search/distributed_stats.zig");
const introducer_mod = @import("../../introducer.zig");

pub const max_aggregation_source_hits: usize = 100_000;
const max_significant_terms_candidates: usize = 4096;
const min_significant_terms_candidates: usize = 256;
const significant_terms_candidate_multiplier: usize = 8;

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

pub const CardinalityMode = enum { auto, exact, approximate };

pub const SearchAggregationRequest = struct {
    name: []const u8,
    type: []const u8,
    field: []const u8,
    // Exact-vs-approximate selection for cardinality aggregations; ignored for
    // other types. `auto` keeps the existing behavior (sketch when it applies,
    // else exact).
    cardinality_mode: CardinalityMode = .auto,
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
    algebraic_join: ?algebraic_ir.JoinRef = null,
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
    owns_labels: bool = false,
    value_json: ?[]const u8 = null,
    metadata_json: ?[]const u8 = null,
    buckets: []SearchAggregationBucket = &.{},

    pub fn deinit(self: *SearchAggregationResult, alloc: Allocator) void {
        if (self.owns_labels) {
            if (self.name.len > 0) alloc.free(self.name);
            if (self.field.len > 0) alloc.free(self.field);
            if (self.type.len > 0) alloc.free(self.type);
        }
        if (self.value_json) |value_json| alloc.free(value_json);
        if (self.metadata_json) |metadata_json| alloc.free(metadata_json);
        for (self.buckets) |*bucket| bucket.deinit(alloc);
        if (self.buckets.len > 0) alloc.free(self.buckets);
        self.* = undefined;
    }
};

pub const DistributedBackgroundTextStats = background_text_stats.DistributedBackgroundTextStats;

pub fn deinitDistributedBackgroundTextStats(
    alloc: Allocator,
    items: []const DistributedBackgroundTextStats,
) void {
    background_text_stats.deinitAll(alloc, items);
}

pub fn deinitResults(alloc: Allocator, results: []SearchAggregationResult) void {
    for (results) |*result| result.deinit(alloc);
    if (results.len > 0) alloc.free(results);
}

pub fn cloneSearchAggregationResultLabelsDeep(alloc: Allocator, result: *SearchAggregationResult) !void {
    if (!result.owns_labels) {
        result.name = try alloc.dupe(u8, result.name);
        errdefer alloc.free(result.name);
        result.field = try alloc.dupe(u8, result.field);
        errdefer alloc.free(result.field);
        result.type = try alloc.dupe(u8, result.type);
        result.owns_labels = true;
    }
    for (result.buckets) |*bucket| {
        for (bucket.aggregations) |*child| try cloneSearchAggregationResultLabelsDeep(alloc, child);
    }
}

/// Control-side aggregation context. Physical consumers extend this contract
/// with live index-manager and document-store handles in `aggregations.zig`.
pub const Context = struct {
    text_analysis: ?*const introducer_mod.TextAnalysisConfig = null,
    distributed_text_stats: []const distributed_stats_mod.TextFieldStats = &.{},
    distributed_background_text_stats: []const DistributedBackgroundTextStats = &.{},
};

pub const FixedConstraint = algebraic_ir.Constraint;

/// The outer kernel ABI carries operation inputs and versioning. Only this
/// comparatively small context is JSON encoded; merged hit bodies cross as
/// one borrowed descriptor array and are never copied into an input document.
pub const ComputeContextWire = struct {
    text_analysis: ?introducer_mod.TextAnalysisConfig = null,
    distributed_text_stats: []const distributed_stats_mod.TextFieldStats = &.{},
    distributed_background_text_stats: []const DistributedBackgroundTextStats = &.{},
};

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

pub fn significantTermsCandidateLimit(size: usize) !usize {
    if (size > max_significant_terms_candidates) return error.QueryCandidateBudgetExceeded;
    const scaled = std.math.mul(usize, size, significant_terms_candidate_multiplier) catch
        max_significant_terms_candidates;
    return @min(max_significant_terms_candidates, @max(min_significant_terms_candidates, scaled));
}

test "aggregation contract owns recursive result labels" {
    const alloc = std.testing.allocator;
    var children = try alloc.alloc(SearchAggregationResult, 1);
    children[0] = .{ .name = "child", .field = "amount", .type = "sum" };
    var buckets = try alloc.alloc(SearchAggregationBucket, 1);
    buckets[0] = .{ .key_json = try alloc.dupe(u8, "\"a\""), .count = 1, .aggregations = children };
    var result = SearchAggregationResult{ .name = "root", .field = "kind", .type = "terms", .buckets = buckets };
    try cloneSearchAggregationResultLabelsDeep(alloc, &result);
    try std.testing.expect(result.owns_labels);
    try std.testing.expect(result.buckets[0].aggregations[0].owns_labels);
    result.deinit(alloc);
}

test "aggregation contract preserves candidate budget identity" {
    try std.testing.expectError(error.QueryCandidateBudgetExceeded, significantTermsCandidateLimit(max_significant_terms_candidates + 1));
}
