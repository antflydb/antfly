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
