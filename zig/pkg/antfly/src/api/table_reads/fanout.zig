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
const db_mod = @import("../../storage/db/mod.zig");
const db_query_search = @import("../../storage/db/query/search_exec.zig");
const distributed_stats_mod = @import("../../search/distributed_stats.zig");
const json_helpers = @import("../json_helpers.zig");
const search_analysis = @import("../../search/analysis.zig");
const table_read_remote_wire = @import("remote_wire.zig");

const OwnedTextStatsFieldRequest = table_read_remote_wire.OwnedTextStatsFieldRequest;
const OwnedBackgroundTextStatsFieldRequest = table_read_remote_wire.OwnedBackgroundTextStatsFieldRequest;

pub const ParallelFanoutKind = enum {
    text_stats,
    query,
    preflight,
};

pub const FanoutPlanReason = enum {
    no_io,
    single_group,
    small_request,
    parallel,
};

pub const FanoutPlan = struct {
    parallel: bool,
    width: usize,
    reason: FanoutPlanReason,
};

pub const ParallelFanoutMetricsSnapshot = struct {
    text_stats_parallel_total: u64 = 0,
    text_stats_parallel_ns_total: u64 = 0,
    text_stats_fallback_total: u64 = 0,
    text_stats_planned_parallel_total: u64 = 0,
    text_stats_planned_sequential_total: u64 = 0,
    text_stats_planned_width_total: u64 = 0,
    text_stats_plan_no_io_total: u64 = 0,
    text_stats_plan_single_group_total: u64 = 0,
    text_stats_plan_small_request_total: u64 = 0,
    query_parallel_total: u64 = 0,
    query_parallel_ns_total: u64 = 0,
    query_fallback_total: u64 = 0,
    query_planned_parallel_total: u64 = 0,
    query_planned_sequential_total: u64 = 0,
    query_planned_width_total: u64 = 0,
    query_plan_no_io_total: u64 = 0,
    query_plan_single_group_total: u64 = 0,
    query_plan_small_request_total: u64 = 0,
    preflight_parallel_total: u64 = 0,
    preflight_parallel_ns_total: u64 = 0,
    preflight_fallback_total: u64 = 0,
    preflight_planned_parallel_total: u64 = 0,
    preflight_planned_sequential_total: u64 = 0,
    preflight_planned_width_total: u64 = 0,
    preflight_plan_no_io_total: u64 = 0,
    preflight_plan_single_group_total: u64 = 0,
    preflight_plan_small_request_total: u64 = 0,
};

var parallel_text_stats_total: std.atomic.Value(u64) = .init(0);
var parallel_text_stats_ns_total: std.atomic.Value(u64) = .init(0);
var fallback_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_parallel_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_sequential_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_width_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_no_io_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_single_group_text_stats_total: std.atomic.Value(u64) = .init(0);
var planned_small_request_text_stats_total: std.atomic.Value(u64) = .init(0);
var parallel_query_total: std.atomic.Value(u64) = .init(0);
var parallel_query_ns_total: std.atomic.Value(u64) = .init(0);
var fallback_query_total: std.atomic.Value(u64) = .init(0);
var planned_parallel_query_total: std.atomic.Value(u64) = .init(0);
var planned_sequential_query_total: std.atomic.Value(u64) = .init(0);
var planned_width_query_total: std.atomic.Value(u64) = .init(0);
var planned_no_io_query_total: std.atomic.Value(u64) = .init(0);
var planned_single_group_query_total: std.atomic.Value(u64) = .init(0);
var planned_small_request_query_total: std.atomic.Value(u64) = .init(0);
var parallel_preflight_total: std.atomic.Value(u64) = .init(0);
var parallel_preflight_ns_total: std.atomic.Value(u64) = .init(0);
var fallback_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_parallel_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_sequential_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_width_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_no_io_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_single_group_preflight_total: std.atomic.Value(u64) = .init(0);
var planned_small_request_preflight_total: std.atomic.Value(u64) = .init(0);

pub fn recordFanoutPlan(kind: ParallelFanoutKind, plan: FanoutPlan) void {
    switch (kind) {
        .text_stats => {
            if (plan.parallel) {
                _ = planned_parallel_text_stats_total.fetchAdd(1, .monotonic);
            } else {
                _ = planned_sequential_text_stats_total.fetchAdd(1, .monotonic);
            }
            _ = planned_width_text_stats_total.fetchAdd(plan.width, .monotonic);
            switch (plan.reason) {
                .no_io => _ = planned_no_io_text_stats_total.fetchAdd(1, .monotonic),
                .single_group => _ = planned_single_group_text_stats_total.fetchAdd(1, .monotonic),
                .small_request => _ = planned_small_request_text_stats_total.fetchAdd(1, .monotonic),
                .parallel => {},
            }
        },
        .query => {
            if (plan.parallel) {
                _ = planned_parallel_query_total.fetchAdd(1, .monotonic);
            } else {
                _ = planned_sequential_query_total.fetchAdd(1, .monotonic);
            }
            _ = planned_width_query_total.fetchAdd(plan.width, .monotonic);
            switch (plan.reason) {
                .no_io => _ = planned_no_io_query_total.fetchAdd(1, .monotonic),
                .single_group => _ = planned_single_group_query_total.fetchAdd(1, .monotonic),
                .small_request => _ = planned_small_request_query_total.fetchAdd(1, .monotonic),
                .parallel => {},
            }
        },
        .preflight => {
            if (plan.parallel) {
                _ = planned_parallel_preflight_total.fetchAdd(1, .monotonic);
            } else {
                _ = planned_sequential_preflight_total.fetchAdd(1, .monotonic);
            }
            _ = planned_width_preflight_total.fetchAdd(plan.width, .monotonic);
            switch (plan.reason) {
                .no_io => _ = planned_no_io_preflight_total.fetchAdd(1, .monotonic),
                .single_group => _ = planned_single_group_preflight_total.fetchAdd(1, .monotonic),
                .small_request => _ = planned_small_request_preflight_total.fetchAdd(1, .monotonic),
                .parallel => {},
            }
        },
    }
}

pub fn recordParallelFanout(kind: ParallelFanoutKind, elapsed_ns: u64) void {
    switch (kind) {
        .text_stats => {
            _ = parallel_text_stats_total.fetchAdd(1, .monotonic);
            _ = parallel_text_stats_ns_total.fetchAdd(elapsed_ns, .monotonic);
        },
        .query => {
            _ = parallel_query_total.fetchAdd(1, .monotonic);
            _ = parallel_query_ns_total.fetchAdd(elapsed_ns, .monotonic);
        },
        .preflight => {
            _ = parallel_preflight_total.fetchAdd(1, .monotonic);
            _ = parallel_preflight_ns_total.fetchAdd(elapsed_ns, .monotonic);
        },
    }
}

pub fn recordParallelFanoutFallback(kind: ParallelFanoutKind) void {
    switch (kind) {
        .text_stats => _ = fallback_text_stats_total.fetchAdd(1, .monotonic),
        .query => _ = fallback_query_total.fetchAdd(1, .monotonic),
        .preflight => _ = fallback_preflight_total.fetchAdd(1, .monotonic),
    }
}

pub fn parallelFanoutMetricsSnapshot() ParallelFanoutMetricsSnapshot {
    return .{
        .text_stats_parallel_total = parallel_text_stats_total.load(.monotonic),
        .text_stats_parallel_ns_total = parallel_text_stats_ns_total.load(.monotonic),
        .text_stats_fallback_total = fallback_text_stats_total.load(.monotonic),
        .text_stats_planned_parallel_total = planned_parallel_text_stats_total.load(.monotonic),
        .text_stats_planned_sequential_total = planned_sequential_text_stats_total.load(.monotonic),
        .text_stats_planned_width_total = planned_width_text_stats_total.load(.monotonic),
        .text_stats_plan_no_io_total = planned_no_io_text_stats_total.load(.monotonic),
        .text_stats_plan_single_group_total = planned_single_group_text_stats_total.load(.monotonic),
        .text_stats_plan_small_request_total = planned_small_request_text_stats_total.load(.monotonic),
        .query_parallel_total = parallel_query_total.load(.monotonic),
        .query_parallel_ns_total = parallel_query_ns_total.load(.monotonic),
        .query_fallback_total = fallback_query_total.load(.monotonic),
        .query_planned_parallel_total = planned_parallel_query_total.load(.monotonic),
        .query_planned_sequential_total = planned_sequential_query_total.load(.monotonic),
        .query_planned_width_total = planned_width_query_total.load(.monotonic),
        .query_plan_no_io_total = planned_no_io_query_total.load(.monotonic),
        .query_plan_single_group_total = planned_single_group_query_total.load(.monotonic),
        .query_plan_small_request_total = planned_small_request_query_total.load(.monotonic),
        .preflight_parallel_total = parallel_preflight_total.load(.monotonic),
        .preflight_parallel_ns_total = parallel_preflight_ns_total.load(.monotonic),
        .preflight_fallback_total = fallback_preflight_total.load(.monotonic),
        .preflight_planned_parallel_total = planned_parallel_preflight_total.load(.monotonic),
        .preflight_planned_sequential_total = planned_sequential_preflight_total.load(.monotonic),
        .preflight_planned_width_total = planned_width_preflight_total.load(.monotonic),
        .preflight_plan_no_io_total = planned_no_io_preflight_total.load(.monotonic),
        .preflight_plan_single_group_total = planned_single_group_preflight_total.load(.monotonic),
        .preflight_plan_small_request_total = planned_small_request_preflight_total.load(.monotonic),
    };
}

fn ioAsyncLimitWidth(io_impl: *std.Io.Threaded, group_count: usize) usize {
    const raw = @intFromEnum(io_impl.async_limit);
    if (raw == 0) return 1;
    if (raw == std.math.maxInt(usize)) return @max(@as(usize, 1), group_count);
    return @max(@as(usize, 1), @min(group_count, raw));
}

pub fn ioAsyncLimitCap(io_impl: *std.Io.Threaded) usize {
    const raw = @intFromEnum(io_impl.async_limit);
    if (raw == 0) return 1;
    if (raw == std.math.maxInt(usize)) return std.math.maxInt(usize);
    return @max(@as(usize, 1), raw);
}

pub fn planFanout(kind: ParallelFanoutKind, io_impl: ?*std.Io.Threaded, group_count: usize) FanoutPlan {
    const attached_io = io_impl orelse return .{
        .parallel = false,
        .width = 1,
        .reason = .no_io,
    };
    if (group_count <= 1) return .{
        .parallel = false,
        .width = 1,
        .reason = .single_group,
    };

    const width_cap = ioAsyncLimitWidth(attached_io, group_count);
    const target_width = switch (kind) {
        .text_stats, .preflight => @min(width_cap, @min(group_count, @as(usize, 4))),
        .query => @min(width_cap, @min(group_count, @as(usize, 4))),
    };
    return .{
        .parallel = target_width > 1,
        .width = if (target_width > 0) target_width else 1,
        .reason = if (target_width > 1) .parallel else .small_request,
    };
}

pub fn planQueryFanout(
    io_impl: ?*std.Io.Threaded,
    group_count: usize,
    req: db_mod.types.SearchRequest,
) FanoutPlan {
    const attached_io = io_impl orelse return .{
        .parallel = false,
        .width = 1,
        .reason = .no_io,
    };
    if (group_count <= 1) return .{
        .parallel = false,
        .width = 1,
        .reason = .single_group,
    };
    if (group_count <= 2 and req.limit > 0 and req.limit <= 32) return .{
        .parallel = false,
        .width = 1,
        .reason = .small_request,
    };

    const width_cap = ioAsyncLimitWidth(attached_io, group_count);
    const result_window = req.limit + req.offset;
    const target_width: usize = if (result_window > 0 and result_window <= 32)
        @min(width_cap, @min(group_count, @as(usize, 4)))
    else
        @min(width_cap, @min(group_count, @as(usize, 8)));
    return .{
        .parallel = target_width > 1,
        .width = if (target_width > 0) target_width else 1,
        .reason = if (target_width > 1) .parallel else .small_request,
    };
}

pub fn queryNeedsDistributedTextStats(req: db_mod.types.SearchRequest) bool {
    if (req.distributed_text_stats.len > 0) return false;
    if (req.full_text != null) return true;
    if (db_query_search.isTextQuery(req.query) and !db_query_search.isDefaultMatchAll(req.query)) return true;
    return req.full_text_queries.len > 0;
}

pub fn collectSignificantTermsFieldRequests(
    alloc: std.mem.Allocator,
    requests: []const db_mod.aggregations.SearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) ![]OwnedTextStatsFieldRequest {
    var grouped = std.StringHashMapUnmanaged(std.StringHashMapUnmanaged(void)){};
    defer {
        var it = grouped.iterator();
        while (it.next()) |entry| {
            var term_it = entry.value_ptr.keyIterator();
            while (term_it.next()) |term| alloc.free(term.*);
            entry.value_ptr.deinit(alloc);
            alloc.free(entry.key_ptr.*);
        }
        grouped.deinit(alloc);
    }

    try collectSignificantTermsFieldRequestsRecursive(alloc, &grouped, requests, hits);
    if (grouped.count() == 0) return &.{};

    const out = try alloc.alloc(OwnedTextStatsFieldRequest, grouped.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }

    var it = grouped.iterator();
    while (it.next()) |entry| {
        const terms = try alloc.alloc([]const u8, entry.value_ptr.count());
        var term_index: usize = 0;
        var term_it = entry.value_ptr.keyIterator();
        while (term_it.next()) |term| {
            terms[term_index] = try alloc.dupe(u8, term.*);
            term_index += 1;
        }
        out[initialized] = .{
            .field = try alloc.dupe(u8, entry.key_ptr.*),
            .terms = terms,
        };
        initialized += 1;
    }
    return out;
}

pub fn collectSignificantTermsBackgroundFieldRequests(
    alloc: std.mem.Allocator,
    requests: []const db_mod.aggregations.SearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) ![]OwnedBackgroundTextStatsFieldRequest {
    var out = std.ArrayListUnmanaged(OwnedBackgroundTextStatsFieldRequest).empty;
    errdefer {
        for (out.items) |*item| item.deinit(alloc);
        out.deinit(alloc);
    }
    try collectSignificantTermsBackgroundFieldRequestsRecursive(alloc, &out, requests, hits);
    return try out.toOwnedSlice(alloc);
}

fn collectSignificantTermsBackgroundFieldRequestsRecursive(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(OwnedBackgroundTextStatsFieldRequest),
    requests: []const db_mod.aggregations.SearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) !void {
    for (requests) |request| {
        if (std.mem.eql(u8, request.type, "significant_terms") and request.background_query != null) {
            var seen_terms = std.StringHashMapUnmanaged(void){};
            defer {
                var term_it = seen_terms.keyIterator();
                while (term_it.next()) |term| alloc.free(term.*);
                seen_terms.deinit(alloc);
            }
            try collectSignificantTermsFromHits(alloc, hits, request.field, &seen_terms);
            if (seen_terms.count() > 0) {
                const terms = try alloc.alloc([]const u8, seen_terms.count());
                var term_index: usize = 0;
                var term_it = seen_terms.keyIterator();
                while (term_it.next()) |term| {
                    terms[term_index] = try alloc.dupe(u8, term.*);
                    term_index += 1;
                }
                try out.append(alloc, .{
                    .aggregation_name = try alloc.dupe(u8, request.name),
                    .field = try alloc.dupe(u8, request.field),
                    .terms = terms,
                    .background_query = try cloneBackgroundQuery(alloc, request.background_query.?),
                });
            }
        }
        try collectSignificantTermsBackgroundFieldRequestsRecursive(alloc, out, request.aggregations, hits);
    }
}

fn cloneBackgroundQuery(
    alloc: std.mem.Allocator,
    query: db_mod.aggregations.BackgroundQuery,
) !db_mod.aggregations.BackgroundQuery {
    return switch (query) {
        .match_all => .{ .match_all = {} },
        .match => |match| .{ .match = .{
            .field = try alloc.dupe(u8, match.field),
            .text = try alloc.dupe(u8, match.text),
        } },
        .term => |term| .{ .term = .{
            .field = try alloc.dupe(u8, term.field),
            .term = try alloc.dupe(u8, term.term),
        } },
    };
}

fn collectSignificantTermsFieldRequestsRecursive(
    alloc: std.mem.Allocator,
    grouped: *std.StringHashMapUnmanaged(std.StringHashMapUnmanaged(void)),
    requests: []const db_mod.aggregations.SearchAggregationRequest,
    hits: []const db_mod.types.SearchHit,
) !void {
    for (requests) |request| {
        if (std.mem.eql(u8, request.type, "significant_terms") and request.background_query == null) {
            const gop = try grouped.getOrPut(alloc, request.field);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, request.field);
                gop.value_ptr.* = .{};
            }
            try collectSignificantTermsFromHits(alloc, hits, request.field, gop.value_ptr);
        }
        try collectSignificantTermsFieldRequestsRecursive(alloc, grouped, request.aggregations, hits);
    }
}

fn collectSignificantTermsFromHits(
    alloc: std.mem.Allocator,
    hits: []const db_mod.types.SearchHit,
    field: []const u8,
    seen_terms: *std.StringHashMapUnmanaged(void),
) !void {
    for (hits) |hit| try collectSignificantTermsFromStoredAlloc(alloc, hit.stored_data orelse continue, field, seen_terms);
}

fn collectSignificantTermsFromStoredAlloc(
    alloc: std.mem.Allocator,
    stored: []const u8,
    field: []const u8,
    seen_terms: *std.StringHashMapUnmanaged(void),
) !void {
    var parsed = (try json_helpers.parseJsonPathValueAlloc(alloc, stored, field)) orelse return;
    defer parsed.deinit();
    try collectSignificantTermsFromValue(alloc, parsed.value, seen_terms);
}

fn collectSignificantTermsFromValue(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    seen_terms: *std.StringHashMapUnmanaged(void),
) !void {
    switch (value) {
        .array => |arr| for (arr.items) |item| try collectSignificantTermsFromValue(alloc, item, seen_terms),
        .string => {
            const tokens = try search_analysis.default_analyzer.analyze(alloc, value.string);
            defer search_analysis.Analyzer.freeTokens(alloc, tokens);
            for (tokens) |tok| {
                const entry = try seen_terms.getOrPut(alloc, tok.term);
                if (entry.found_existing) continue;
                entry.key_ptr.* = try alloc.dupe(u8, tok.term);
            }
        },
        else => {},
    }
}

pub fn extractJsonValueAtPath(value: std.json.Value, path: []const u8) ?std.json.Value {
    return json_helpers.extractJsonPathValue(value, path);
}

pub const TextStatsFanoutSlot = struct {
    arena: std.heap.ArenaAllocator,
    fields: []const distributed_stats_mod.TextFieldStats = &.{},
    err: ?anyerror = null,

    fn init() TextStatsFanoutSlot {
        return .{
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
    }

    fn deinit(self: *TextStatsFanoutSlot) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const SearchFanoutSlot = struct {
    arena: std.heap.ArenaAllocator,
    result: ?db_mod.types.SearchResult = null,
    err: ?anyerror = null,

    fn init() SearchFanoutSlot {
        return .{
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
    }

    fn deinit(self: *SearchFanoutSlot) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const PreflightFanoutSlot = struct {
    arena: std.heap.ArenaAllocator,
    summary: ?db_mod.RuntimePreflightSummary = null,
    err: ?anyerror = null,

    fn init() PreflightFanoutSlot {
        return .{
            .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
    }

    fn deinit(self: *PreflightFanoutSlot) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn initTextStatsFanoutSlots(alloc: std.mem.Allocator, count: usize) ![]TextStatsFanoutSlot {
    const slots = try alloc.alloc(TextStatsFanoutSlot, count);
    errdefer alloc.free(slots);
    for (slots) |*slot| slot.* = .init();
    return slots;
}

pub fn deinitTextStatsFanoutSlots(alloc: std.mem.Allocator, slots: []TextStatsFanoutSlot) void {
    for (slots) |*slot| slot.deinit();
    alloc.free(slots);
}

pub fn initSearchFanoutSlots(alloc: std.mem.Allocator, count: usize) ![]SearchFanoutSlot {
    const slots = try alloc.alloc(SearchFanoutSlot, count);
    errdefer alloc.free(slots);
    for (slots) |*slot| slot.* = .init();
    return slots;
}

pub fn deinitSearchFanoutSlots(alloc: std.mem.Allocator, slots: []SearchFanoutSlot) void {
    for (slots) |*slot| slot.deinit();
    alloc.free(slots);
}

pub fn initPreflightFanoutSlots(alloc: std.mem.Allocator, count: usize) ![]PreflightFanoutSlot {
    const slots = try alloc.alloc(PreflightFanoutSlot, count);
    errdefer alloc.free(slots);
    for (slots) |*slot| slot.* = .init();
    return slots;
}

pub fn deinitPreflightFanoutSlots(alloc: std.mem.Allocator, slots: []PreflightFanoutSlot) void {
    for (slots) |*slot| slot.deinit();
    alloc.free(slots);
}

pub fn cloneRuntimePreflightSummary(
    alloc: std.mem.Allocator,
    summary: db_mod.RuntimePreflightSummary,
) !db_mod.RuntimePreflightSummary {
    var cloned: db_mod.RuntimePreflightSummary = .{};
    errdefer cloned.deinit(alloc);
    try mergeRuntimePreflightSummaryNoFree(alloc, &cloned, summary);
    return cloned;
}

pub fn mergeRuntimePreflightSummary(
    alloc: std.mem.Allocator,
    target: *db_mod.RuntimePreflightSummary,
    extra: db_mod.RuntimePreflightSummary,
) !void {
    defer {
        var owned = extra;
        owned.deinit(alloc);
    }

    try mergeRuntimePreflightSummaryNoFree(alloc, target, extra);
}

pub fn mergeRuntimePreflightSummaryNoFree(
    alloc: std.mem.Allocator,
    target: *db_mod.RuntimePreflightSummary,
    extra: db_mod.RuntimePreflightSummary,
) !void {
    try mergeRuntimePreflightStrings(alloc, &target.result_refs, extra.result_refs);
    try mergeRuntimePreflightStrings(alloc, &target.graph_query_order, extra.graph_query_order);
    try mergeRuntimePreflightTextEstimates(alloc, &target.text_indexes, extra.text_indexes);
    try mergeRuntimePreflightEmbeddingEstimates(alloc, &target.embedding_indexes, extra.embedding_indexes);
    try mergeRuntimePreflightGraphEstimates(alloc, &target.graph_indexes, extra.graph_indexes);
    try mergeRuntimePreflightTextQueryStats(alloc, &target.text_query_stats, extra.text_query_stats);
    target.doc_id_value_count = @max(target.doc_id_value_count, extra.doc_id_value_count);
    target.filter_id_count = @max(target.filter_id_count, extra.filter_id_count);
    target.exclude_id_count = @max(target.exclude_id_count, extra.exclude_id_count);
    target.numeric_range_clause_count = @max(target.numeric_range_clause_count, extra.numeric_range_clause_count);
    target.term_range_clause_count = @max(target.term_range_clause_count, extra.term_range_clause_count);
    target.ip_range_clause_count = @max(target.ip_range_clause_count, extra.ip_range_clause_count);
    target.bool_field_clause_count = @max(target.bool_field_clause_count, extra.bool_field_clause_count);
    target.geo_filter_clause_count = @max(target.geo_filter_clause_count, extra.geo_filter_clause_count);
    target.positive_id_result_upper_bound = if (target.positive_id_result_upper_bound) |existing|
        if (extra.positive_id_result_upper_bound) |incoming|
            @min(existing, incoming)
        else
            existing
    else
        extra.positive_id_result_upper_bound;
    const target_pre_merge_lower_bound = if (target.structured_filter_doc_count_lower_bound) |value|
        value
    else if (target.structured_filter_count_exact)
        target.structured_filter_doc_count_estimate
    else
        target.structured_filter_doc_count_estimate;
    const extra_pre_merge_lower_bound = if (extra.structured_filter_doc_count_lower_bound) |value|
        value
    else if (extra.structured_filter_count_exact)
        extra.structured_filter_doc_count_estimate
    else
        extra.structured_filter_doc_count_estimate;
    if (target.structured_filter_count_exact and extra.structured_filter_count_exact) {
        if (target.structured_filter_doc_count_estimate) |existing| {
            if (extra.structured_filter_doc_count_estimate) |incoming| {
                target.structured_filter_doc_count_estimate = existing + incoming;
                target.structured_filter_count_exact = true;
            } else {
                target.structured_filter_doc_count_estimate = null;
                target.structured_filter_count_exact = false;
            }
        } else if (extra.structured_filter_doc_count_estimate) |incoming| {
            target.structured_filter_doc_count_estimate = incoming;
            target.structured_filter_count_exact = true;
        } else {
            target.structured_filter_doc_count_estimate = null;
            target.structured_filter_count_exact = false;
        }
    } else {
        target.structured_filter_doc_count_estimate = null;
        target.structured_filter_count_exact = false;
    }
    target.structured_filter_doc_count_sample_estimate = if (target.structured_filter_doc_count_sample_estimate) |existing|
        if (extra.structured_filter_doc_count_sample_estimate) |incoming|
            existing + incoming
        else
            existing
    else
        extra.structured_filter_doc_count_sample_estimate;
    target.structured_filter_count_sample_size += extra.structured_filter_count_sample_size;
    if (target.structured_filter_count_exact) {
        target.structured_filter_doc_count_sample_estimate = null;
        target.structured_filter_count_sample_size = 0;
    }
    if (target.structured_filter_count_exact) {
        target.structured_filter_doc_count_lower_bound = null;
    } else {
        target.structured_filter_doc_count_lower_bound = if (target_pre_merge_lower_bound != null or extra_pre_merge_lower_bound != null)
            (target_pre_merge_lower_bound orelse 0) + (extra_pre_merge_lower_bound orelse 0)
        else
            null;
    }
    target.structured_filter_count_budget_limit = if (target.structured_filter_count_budget_limit) |existing|
        if (extra.structured_filter_count_budget_limit) |incoming|
            @max(existing, incoming)
        else
            existing
    else
        extra.structured_filter_count_budget_limit;
    target.shard_result_window = @max(target.shard_result_window, extra.shard_result_window);
    target.shard_result_window_total += extra.shard_result_window_total;
    target.stored_projection_doc_upper_bound_total += extra.stored_projection_doc_upper_bound_total;
    target.rerank_doc_upper_bound = @max(target.rerank_doc_upper_bound, extra.rerank_doc_upper_bound);
    target.aggregation_may_scan_full_results = target.aggregation_may_scan_full_results or extra.aggregation_may_scan_full_results;
    target.shard_count += extra.shard_count;
    target.remote_shard_count += extra.remote_shard_count;
    target.dense_query_count += extra.dense_query_count;
    target.vector_worker_candidate_count += extra.vector_worker_candidate_count;
    target.vector_worker_fallback_count += extra.vector_worker_fallback_count;
    target.vector_worker_filter_constraint_count += extra.vector_worker_filter_constraint_count;
    target.vector_worker_requires_algebraic_filter_resolution = target.vector_worker_requires_algebraic_filter_resolution or
        extra.vector_worker_requires_algebraic_filter_resolution;
    target.dense_effective_k_total += extra.dense_effective_k_total;
    target.dense_search_width_total += extra.dense_search_width_total;
    target.dense_search_width_max = @max(target.dense_search_width_max, extra.dense_search_width_max);
    target.dense_epsilon_max = @max(target.dense_epsilon_max, extra.dense_epsilon_max);
    db_mod.deriveRuntimePreflightEstimates(target);
}

test "merge runtime preflight summary preserves structured filter exact counts only when every shard is exact" {
    const alloc = std.testing.allocator;

    var exact_left: db_mod.RuntimePreflightSummary = .{
        .structured_filter_doc_count_estimate = 2,
        .structured_filter_count_exact = true,
    };
    defer exact_left.deinit(alloc);
    try mergeRuntimePreflightSummary(alloc, &exact_left, .{
        .structured_filter_doc_count_estimate = 3,
        .structured_filter_count_exact = true,
    });
    try std.testing.expectEqual(@as(?u64, 5), exact_left.structured_filter_doc_count_estimate);
    try std.testing.expect(exact_left.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u32, 5), exact_left.result_doc_estimate);

    var mixed: db_mod.RuntimePreflightSummary = .{
        .structured_filter_doc_count_estimate = 2,
        .structured_filter_count_exact = true,
        .vector_worker_candidate_count = 1,
        .vector_worker_filter_constraint_count = 2,
        .vector_worker_requires_algebraic_filter_resolution = true,
    };
    defer mixed.deinit(alloc);
    try mergeRuntimePreflightSummary(alloc, &mixed, .{
        .structured_filter_doc_count_estimate = 7,
        .structured_filter_count_exact = false,
        .vector_worker_fallback_count = 1,
        .vector_worker_filter_constraint_count = 1,
    });
    try std.testing.expectEqual(@as(?u64, null), mixed.structured_filter_doc_count_estimate);
    try std.testing.expect(!mixed.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u64, 9), mixed.structured_filter_doc_count_lower_bound);
    try std.testing.expectEqual(@as(?u32, null), mixed.result_doc_estimate);
    try std.testing.expectEqual(@as(u32, 1), mixed.vector_worker_candidate_count);
    try std.testing.expectEqual(@as(u32, 1), mixed.vector_worker_fallback_count);
    try std.testing.expectEqual(@as(u32, 3), mixed.vector_worker_filter_constraint_count);
    try std.testing.expect(mixed.vector_worker_requires_algebraic_filter_resolution);

    var lower_bounds: db_mod.RuntimePreflightSummary = .{
        .structured_filter_doc_count_lower_bound = 4,
        .structured_filter_count_exact = false,
    };
    defer lower_bounds.deinit(alloc);
    try mergeRuntimePreflightSummary(alloc, &lower_bounds, .{
        .structured_filter_doc_count_lower_bound = 6,
        .structured_filter_count_exact = false,
    });
    try std.testing.expectEqual(@as(?u64, 10), lower_bounds.structured_filter_doc_count_lower_bound);
    try std.testing.expect(!lower_bounds.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u32, null), lower_bounds.result_doc_estimate);

    var sampled: db_mod.RuntimePreflightSummary = .{
        .structured_filter_doc_count_sample_estimate = 4,
        .structured_filter_count_sample_size = 8,
        .structured_filter_count_exact = false,
    };
    defer sampled.deinit(alloc);
    try mergeRuntimePreflightSummary(alloc, &sampled, .{
        .structured_filter_doc_count_sample_estimate = 6,
        .structured_filter_count_sample_size = 12,
        .structured_filter_count_exact = false,
    });
    try std.testing.expectEqual(@as(?u64, 10), sampled.structured_filter_doc_count_sample_estimate);
    try std.testing.expectEqual(@as(u32, 20), sampled.structured_filter_count_sample_size);
    try std.testing.expect(!sampled.structured_filter_count_exact);
    try std.testing.expectEqual(@as(?u32, 10), sampled.result_doc_estimate);
}

fn mergeRuntimePreflightTextQueryStats(
    alloc: std.mem.Allocator,
    target: *[]const distributed_stats_mod.TextFieldStats,
    extra: []const distributed_stats_mod.TextFieldStats,
) !void {
    const merged = try mergeDistributedTextStats(alloc, &[_][]const distributed_stats_mod.TextFieldStats{
        target.*,
        extra,
    });
    distributed_stats_mod.deinitTextFieldStats(alloc, target.*);
    target.* = merged;
}

fn mergeRuntimePreflightStrings(
    alloc: std.mem.Allocator,
    target: *[]const []const u8,
    extra: []const []const u8,
) !void {
    var items = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (items.items) |item| alloc.free(item);
        items.deinit(alloc);
    }
    for (target.*) |item| try appendUniqueRuntimePreflightString(alloc, &items, item);
    for (extra) |item| try appendUniqueRuntimePreflightString(alloc, &items, item);
    freeRuntimePreflightStringSlice(alloc, target.*);
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

fn appendUniqueRuntimePreflightString(
    alloc: std.mem.Allocator,
    items: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    for (items.items) |existing| {
        if (std.mem.eql(u8, existing, value)) return;
    }
    try items.append(alloc, try alloc.dupe(u8, value));
}

fn freeRuntimePreflightStringSlice(alloc: std.mem.Allocator, items: []const []const u8) void {
    for (items) |item| alloc.free(@constCast(item));
    if (items.len > 0) alloc.free(@constCast(items));
}

fn mergeRuntimePreflightTextEstimates(
    alloc: std.mem.Allocator,
    target: *[]const db_mod.TextIndexEstimate,
    extra: []const db_mod.TextIndexEstimate,
) !void {
    var items = std.ArrayListUnmanaged(db_mod.TextIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    for (target.*) |item| try items.append(alloc, .{
        .name = try alloc.dupe(u8, item.name),
        .doc_count = item.doc_count,
        .chunk_backed = item.chunk_backed,
        .group_chunk_parents = item.group_chunk_parents,
    });
    for (extra) |item| {
        for (items.items) |*existing| {
            if (!std.mem.eql(u8, existing.name, item.name)) continue;
            existing.doc_count += item.doc_count;
            existing.chunk_backed = existing.chunk_backed or item.chunk_backed;
            existing.group_chunk_parents = existing.group_chunk_parents or item.group_chunk_parents;
            break;
        } else {
            try items.append(alloc, .{
                .name = try alloc.dupe(u8, item.name),
                .doc_count = item.doc_count,
                .chunk_backed = item.chunk_backed,
                .group_chunk_parents = item.group_chunk_parents,
            });
        }
    }

    for (target.*) |*item| item.deinit(alloc);
    if (target.*.len > 0) alloc.free(@constCast(target.*));
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

fn mergeRuntimePreflightEmbeddingEstimates(
    alloc: std.mem.Allocator,
    target: *[]const db_mod.EmbeddingIndexEstimate,
    extra: []const db_mod.EmbeddingIndexEstimate,
) !void {
    var items = std.ArrayListUnmanaged(db_mod.EmbeddingIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    for (target.*) |item| try items.append(alloc, .{
        .name = try alloc.dupe(u8, item.name),
        .sparse = item.sparse,
        .doc_count = item.doc_count,
        .dims = item.dims,
        .chunk_backed = item.chunk_backed,
    });
    for (extra) |item| {
        for (items.items) |*existing| {
            if (!std.mem.eql(u8, existing.name, item.name) or existing.sparse != item.sparse) continue;
            existing.doc_count += item.doc_count;
            existing.chunk_backed = existing.chunk_backed or item.chunk_backed;
            if (existing.dims == 0) existing.dims = item.dims;
            break;
        } else {
            try items.append(alloc, .{
                .name = try alloc.dupe(u8, item.name),
                .sparse = item.sparse,
                .doc_count = item.doc_count,
                .dims = item.dims,
                .chunk_backed = item.chunk_backed,
            });
        }
    }

    for (target.*) |*item| item.deinit(alloc);
    if (target.*.len > 0) alloc.free(@constCast(target.*));
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

fn mergeRuntimePreflightGraphEstimates(
    alloc: std.mem.Allocator,
    target: *[]const db_mod.GraphIndexEstimate,
    extra: []const db_mod.GraphIndexEstimate,
) !void {
    var items = std.ArrayListUnmanaged(db_mod.GraphIndexEstimate).empty;
    errdefer {
        for (items.items) |*item| item.deinit(alloc);
        items.deinit(alloc);
    }

    for (target.*) |item| try items.append(alloc, .{
        .name = try alloc.dupe(u8, item.name),
        .edge_count = item.edge_count,
        .node_count = item.node_count,
    });
    for (extra) |item| {
        for (items.items) |*existing| {
            if (!std.mem.eql(u8, existing.name, item.name)) continue;
            existing.edge_count += item.edge_count;
            existing.node_count += item.node_count;
            break;
        } else {
            try items.append(alloc, .{
                .name = try alloc.dupe(u8, item.name),
                .edge_count = item.edge_count,
                .node_count = item.node_count,
            });
        }
    }

    for (target.*) |*item| item.deinit(alloc);
    if (target.*.len > 0) alloc.free(@constCast(target.*));
    target.* = if (items.items.len == 0) &.{} else try items.toOwnedSlice(alloc);
}

pub fn mergeDistributedTextStats(
    alloc: std.mem.Allocator,
    groups: []const []const distributed_stats_mod.TextFieldStats,
) ![]const distributed_stats_mod.TextFieldStats {
    var fields = std.StringHashMapUnmanaged(struct {
        doc_count: u32 = 0,
        total_field_len: u64 = 0,
        terms: std.StringHashMapUnmanaged(u32) = .{},
    }){};
    defer {
        var it = fields.iterator();
        while (it.next()) |entry| {
            var term_it = entry.value_ptr.terms.keyIterator();
            while (term_it.next()) |term| alloc.free(term.*);
            entry.value_ptr.terms.deinit(alloc);
            alloc.free(entry.key_ptr.*);
        }
        fields.deinit(alloc);
    }

    for (groups) |items| {
        for (items) |item| {
            const gop = try fields.getOrPut(alloc, item.field);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, item.field);
                gop.value_ptr.* = .{};
            }
            gop.value_ptr.doc_count +|= item.global_doc_count;
            gop.value_ptr.total_field_len +|= item.global_total_field_len;
            for (item.term_doc_freqs) |term| {
                const term_gop = try gop.value_ptr.terms.getOrPut(alloc, term.term);
                if (!term_gop.found_existing) {
                    term_gop.key_ptr.* = try alloc.dupe(u8, term.term);
                    term_gop.value_ptr.* = 0;
                }
                term_gop.value_ptr.* +|= term.doc_freq;
            }
        }
    }

    const out = try alloc.alloc(distributed_stats_mod.TextFieldStats, fields.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    var it = fields.iterator();
    while (it.next()) |entry| {
        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, entry.value_ptr.terms.count());
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        var term_it = entry.value_ptr.terms.iterator();
        while (term_it.next()) |term_entry| {
            term_doc_freqs[initialized_terms] = .{
                .term = try alloc.dupe(u8, term_entry.key_ptr.*),
                .doc_freq = term_entry.value_ptr.*,
            };
            initialized_terms += 1;
        }
        out[initialized] = .{
            .field = try alloc.dupe(u8, entry.key_ptr.*),
            .global_doc_count = entry.value_ptr.doc_count,
            .global_total_field_len = entry.value_ptr.total_field_len,
            .term_doc_freqs = term_doc_freqs,
        };
        std.mem.sort(distributed_stats_mod.TermDocFreq, term_doc_freqs, {}, termDocFreqLessThan);
        initialized += 1;
    }
    std.mem.sort(distributed_stats_mod.TextFieldStats, out, {}, textFieldStatsLessThan);
    return out;
}

pub fn mergeDistributedBackgroundTextStats(
    alloc: std.mem.Allocator,
    groups: []const []const db_mod.aggregations.DistributedBackgroundTextStats,
) ![]const db_mod.aggregations.DistributedBackgroundTextStats {
    var fields = std.StringHashMapUnmanaged(struct {
        aggregation_name: []const u8,
        field: []const u8,
        background_doc_count: u32 = 0,
        terms: std.StringHashMapUnmanaged(u32) = .{},
    }){};
    defer {
        var it = fields.iterator();
        while (it.next()) |entry| {
            var term_it = entry.value_ptr.terms.keyIterator();
            while (term_it.next()) |term| alloc.free(term.*);
            entry.value_ptr.terms.deinit(alloc);
            alloc.free(entry.value_ptr.aggregation_name);
            alloc.free(entry.value_ptr.field);
            alloc.free(entry.key_ptr.*);
        }
        fields.deinit(alloc);
    }

    for (groups) |items| {
        for (items) |item| {
            const map_key = try textStatsTupleKeyAlloc(alloc, &.{ item.aggregation_name, item.field });
            defer alloc.free(map_key);
            const gop = try fields.getOrPut(alloc, map_key);
            if (!gop.found_existing) {
                gop.key_ptr.* = try alloc.dupe(u8, map_key);
                gop.value_ptr.* = .{
                    .aggregation_name = try alloc.dupe(u8, item.aggregation_name),
                    .field = try alloc.dupe(u8, item.field),
                };
            }
            gop.value_ptr.background_doc_count +|= item.background_doc_count;
            for (item.term_doc_freqs) |term| {
                const term_gop = try gop.value_ptr.terms.getOrPut(alloc, term.term);
                if (!term_gop.found_existing) {
                    term_gop.key_ptr.* = try alloc.dupe(u8, term.term);
                    term_gop.value_ptr.* = 0;
                }
                term_gop.value_ptr.* +|= term.doc_freq;
            }
        }
    }

    const out = try alloc.alloc(db_mod.aggregations.DistributedBackgroundTextStats, fields.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    var it = fields.iterator();
    while (it.next()) |entry| {
        const term_doc_freqs = try alloc.alloc(distributed_stats_mod.TermDocFreq, entry.value_ptr.terms.count());
        var initialized_terms: usize = 0;
        errdefer {
            for (term_doc_freqs[0..initialized_terms]) |*item| item.deinit(alloc);
            if (term_doc_freqs.len > 0) alloc.free(term_doc_freqs);
        }
        var term_it = entry.value_ptr.terms.iterator();
        while (term_it.next()) |term_entry| {
            term_doc_freqs[initialized_terms] = .{
                .term = try alloc.dupe(u8, term_entry.key_ptr.*),
                .doc_freq = term_entry.value_ptr.*,
            };
            initialized_terms += 1;
        }
        out[initialized] = .{
            .aggregation_name = try alloc.dupe(u8, entry.value_ptr.aggregation_name),
            .field = try alloc.dupe(u8, entry.value_ptr.field),
            .background_doc_count = entry.value_ptr.background_doc_count,
            .term_doc_freqs = term_doc_freqs,
        };
        std.mem.sort(distributed_stats_mod.TermDocFreq, term_doc_freqs, {}, termDocFreqLessThan);
        initialized += 1;
    }
    std.mem.sort(db_mod.aggregations.DistributedBackgroundTextStats, out, {}, backgroundTextStatsLessThan);
    return out;
}

fn termDocFreqLessThan(_: void, lhs: distributed_stats_mod.TermDocFreq, rhs: distributed_stats_mod.TermDocFreq) bool {
    return std.mem.lessThan(u8, lhs.term, rhs.term);
}

fn textFieldStatsLessThan(_: void, lhs: distributed_stats_mod.TextFieldStats, rhs: distributed_stats_mod.TextFieldStats) bool {
    return std.mem.lessThan(u8, lhs.field, rhs.field);
}

fn backgroundTextStatsLessThan(_: void, lhs: db_mod.aggregations.DistributedBackgroundTextStats, rhs: db_mod.aggregations.DistributedBackgroundTextStats) bool {
    const aggregation_order = std.mem.order(u8, lhs.aggregation_name, rhs.aggregation_name);
    return switch (aggregation_order) {
        .lt => true,
        .gt => false,
        .eq => std.mem.lessThan(u8, lhs.field, rhs.field),
    };
}

fn textStatsTupleKeyAlloc(alloc: std.mem.Allocator, components: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    for (components) |component| {
        if (component.len > std.math.maxInt(u32)) return error.KeyComponentTooLarge;
        var len_buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(component.len), .big);
        try out.appendSlice(alloc, &len_buf);
        try out.appendSlice(alloc, component);
    }

    return try out.toOwnedSlice(alloc);
}

fn backgroundTermDocFreq(items: []const distributed_stats_mod.TermDocFreq, term: []const u8) ?u32 {
    for (items) |item| {
        if (std.mem.eql(u8, item.term, term)) return item.doc_freq;
    }
    return null;
}

test "fanout planner uses io cap and request shape" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .limited(8),
    });
    defer io_impl.deinit();

    const no_io_plan = planFanout(.text_stats, null, 4);
    try std.testing.expect(!no_io_plan.parallel);
    try std.testing.expectEqual(@as(usize, 1), no_io_plan.width);
    try std.testing.expectEqual(FanoutPlanReason.no_io, no_io_plan.reason);

    const text_stats_plan = planFanout(.text_stats, &io_impl, 6);
    try std.testing.expect(text_stats_plan.parallel);
    try std.testing.expectEqual(@as(usize, 4), text_stats_plan.width);
    try std.testing.expectEqual(FanoutPlanReason.parallel, text_stats_plan.reason);

    const small_query_plan = planQueryFanout(&io_impl, 2, .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .limit = 10,
    });
    try std.testing.expect(!small_query_plan.parallel);
    try std.testing.expectEqual(@as(usize, 1), small_query_plan.width);
    try std.testing.expectEqual(FanoutPlanReason.small_request, small_query_plan.reason);

    const larger_query_plan = planQueryFanout(&io_impl, 6, .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
        .limit = 100,
    });
    try std.testing.expect(larger_query_plan.parallel);
    try std.testing.expectEqual(@as(usize, 6), larger_query_plan.width);
    try std.testing.expectEqual(FanoutPlanReason.parallel, larger_query_plan.reason);
}

test "merge distributed text stats sums shard corpus stats by field and term" {
    const alloc = std.testing.allocator;

    const merged = try mergeDistributedTextStats(alloc, &.{
        &.{.{
            .field = "body",
            .global_doc_count = 2,
            .global_total_field_len = 9,
            .term_doc_freqs = &.{
                .{ .term = "alpha", .doc_freq = 2 },
                .{ .term = "beta", .doc_freq = 1 },
            },
        }},
        &.{
            .{
                .field = "body",
                .global_doc_count = 3,
                .global_total_field_len = 15,
                .term_doc_freqs = &.{
                    .{ .term = "alpha", .doc_freq = 1 },
                    .{ .term = "gamma", .doc_freq = 2 },
                },
            },
            .{
                .field = "title",
                .global_doc_count = 3,
                .global_total_field_len = 12,
                .term_doc_freqs = &.{
                    .{ .term = "hello", .doc_freq = 3 },
                },
            },
        },
    });
    defer distributed_stats_mod.deinitTextFieldStats(alloc, merged);

    try std.testing.expectEqual(@as(usize, 2), merged.len);
    try std.testing.expectEqualStrings("body", merged[0].field);
    try std.testing.expectEqualStrings("title", merged[1].field);

    const body = for (merged) |item| {
        if (std.mem.eql(u8, item.field, "body")) break item;
    } else unreachable;
    try std.testing.expectEqual(@as(u32, 5), body.global_doc_count);
    try std.testing.expectEqual(@as(u64, 24), body.global_total_field_len);
    try std.testing.expectEqualStrings("alpha", body.term_doc_freqs[0].term);
    try std.testing.expectEqualStrings("beta", body.term_doc_freqs[1].term);
    try std.testing.expectEqualStrings("gamma", body.term_doc_freqs[2].term);
    try std.testing.expectEqual(@as(?u32, 3), body.termDocFreq("alpha"));
    try std.testing.expectEqual(@as(?u32, 1), body.termDocFreq("beta"));
    try std.testing.expectEqual(@as(?u32, 2), body.termDocFreq("gamma"));

    const title = for (merged) |item| {
        if (std.mem.eql(u8, item.field, "title")) break item;
    } else unreachable;
    try std.testing.expectEqual(@as(u32, 3), title.global_doc_count);
    try std.testing.expectEqual(@as(?u32, 3), title.termDocFreq("hello"));
}

test "merge distributed background text stats keys preserve embedded separators" {
    const alloc = std.testing.allocator;

    const merged = try mergeDistributedBackgroundTextStats(alloc, &.{
        &.{.{
            .aggregation_name = "agg\x1ffield",
            .field = "name",
            .background_doc_count = 2,
            .term_doc_freqs = &.{.{ .term = "alpha", .doc_freq = 2 }},
        }},
        &.{.{
            .aggregation_name = "agg",
            .field = "field\x1fname",
            .background_doc_count = 3,
            .term_doc_freqs = &.{.{ .term = "beta", .doc_freq = 3 }},
        }},
    });
    defer db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, merged);

    try std.testing.expectEqual(@as(usize, 2), merged.len);
    try std.testing.expectEqualStrings("agg", merged[0].aggregation_name);
    try std.testing.expectEqualStrings("field\x1fname", merged[0].field);
    try std.testing.expectEqualStrings("agg\x1ffield", merged[1].aggregation_name);
    try std.testing.expectEqualStrings("name", merged[1].field);

    const left = for (merged) |item| {
        if (std.mem.eql(u8, item.aggregation_name, "agg\x1ffield")) break item;
    } else unreachable;
    try std.testing.expectEqualStrings("name", left.field);
    try std.testing.expectEqual(@as(u32, 2), left.background_doc_count);
    try std.testing.expectEqual(@as(?u32, 2), backgroundTermDocFreq(left.term_doc_freqs, "alpha"));

    const right = for (merged) |item| {
        if (std.mem.eql(u8, item.aggregation_name, "agg")) break item;
    } else unreachable;
    try std.testing.expectEqualStrings("field\x1fname", right.field);
    try std.testing.expectEqual(@as(u32, 3), right.background_doc_count);
    try std.testing.expectEqual(@as(?u32, 3), backgroundTermDocFreq(right.term_doc_freqs, "beta"));
}

test "collect significant terms field requests gathers unique field terms from hits" {
    const alloc = std.testing.allocator;

    const hits = try alloc.alloc(db_mod.types.SearchHit, 2);
    defer {
        for (hits) |*hit| hit.deinit(alloc);
        alloc.free(hits);
    }
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .stored_data = try alloc.dupe(u8, "{\"body\":\"alpha beta\",\"nested\":{\"body\":\"beta gamma\"}}"),
    };
    hits[1] = .{
        .id = try alloc.dupe(u8, "doc:b"),
        .stored_data = try alloc.dupe(u8, "{\"body\":\"alpha\",\"nested\":{\"body\":\"gamma\"}}"),
    };

    const requests = [_]db_mod.aggregations.SearchAggregationRequest{
        .{
            .name = "sig_body",
            .type = "significant_terms",
            .field = "body",
        },
        .{
            .name = "outer_terms",
            .type = "terms",
            .field = "status",
            .aggregations = &.{
                .{
                    .name = "nested_sig_body",
                    .type = "significant_terms",
                    .field = "nested.body",
                },
            },
        },
    };

    const field_requests = try collectSignificantTermsFieldRequests(alloc, &requests, hits);
    defer {
        for (field_requests) |*item| item.deinit(alloc);
        if (field_requests.len > 0) alloc.free(field_requests);
    }

    try std.testing.expectEqual(@as(usize, 2), field_requests.len);

    const body = for (field_requests) |item| {
        if (std.mem.eql(u8, item.field, "body")) break item;
    } else unreachable;
    try std.testing.expectEqual(@as(usize, 2), body.terms.len);
    try std.testing.expect(std.mem.eql(u8, body.terms[0], "alpha") or std.mem.eql(u8, body.terms[1], "alpha"));
    try std.testing.expect(std.mem.eql(u8, body.terms[0], "beta") or std.mem.eql(u8, body.terms[1], "beta"));

    const nested = for (field_requests) |item| {
        if (std.mem.eql(u8, item.field, "nested.body")) break item;
    } else unreachable;
    try std.testing.expectEqual(@as(usize, 2), nested.terms.len);
    try std.testing.expect(std.mem.eql(u8, nested.terms[0], "beta") or std.mem.eql(u8, nested.terms[1], "beta"));
    try std.testing.expect(std.mem.eql(u8, nested.terms[0], "gamma") or std.mem.eql(u8, nested.terms[1], "gamma"));
}
