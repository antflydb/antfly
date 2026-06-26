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
const distributed_stats_mod = @import("../../search/distributed_stats.zig");

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
