// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const sources = @import("antfly_local_sources");
const compiler = sources.sql_compiler;
const scheduling = sources.common_request_admission.runnable_scheduler;

/// Classification uses compiler-owned structure, never client cost hints.
/// Constant SQL still passes through catalog/provider setup, so it cannot yet
/// claim verified bounded execution. LIMIT alone does not bound sorting or joins.
pub fn classify(compiled: *const compiler.Compiled) scheduling.ReadLane {
    const select = switch (compiled.statement) {
        .select => |value| value,
        else => return .general,
    };
    if (select.source != null or select.ctes.len != 0 or select.set_operation != null or
        select.values_arms.len != 0 or select.group_by.len != 0 or select.count_all or
        select.having != null or select.order_by.len != 0 or select.windows.len != 0 or
        (select.table != null and select.limit == null)) return .analytical;
    for (select.columns) |column| {
        const expression = column.expression orelse continue;
        // Function calls can include aggregate/window, provider or unknown work.
        if (expression.* != .literal and expression.* != .column) return .analytical;
    }
    return .general;
}

pub fn apply(execution: *scheduling.Execution, compiled: *const compiler.Compiled) !void {
    try execution.classifyRead(classify(compiled));
}

test "SQL read lane classification uses verified structure rather than limits or hints" {
    const cases = .{
        .{ "SELECT 1", scheduling.ReadLane.general },
        .{ "SELECT n FROM docs LIMIT 8", scheduling.ReadLane.general },
        .{ "SELECT n FROM docs", scheduling.ReadLane.analytical },
        .{ "SELECT n FROM docs ORDER BY n LIMIT 1", scheduling.ReadLane.analytical },
        .{ "SELECT count(*) FROM docs LIMIT 1", scheduling.ReadLane.analytical },
        .{ "SELECT n, count(*) FROM docs GROUP BY n", scheduling.ReadLane.analytical },
        .{ "SELECT a.n FROM docs a JOIN peers b ON a.n = b.n LIMIT 1", scheduling.ReadLane.analytical },
    };
    inline for (cases) |case| {
        var compiled = try compiler.compile(std.testing.allocator, case[0], .{});
        defer compiled.deinit();
        try std.testing.expectEqual(case[1], classify(&compiled));
    }
}

test "SQL read lane classification preserves the original root and admitted footprint" {
    var owner = try scheduling.Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true });
    defer owner.close();
    var execution: scheduling.Execution = .{};
    defer execution.finish();
    try execution.start(&owner, .{ .io = std.testing.io, .retained_bytes = 512 }, .query);
    const entry = &execution.entry.?;
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM docs", .{});
    defer compiled.deinit();
    try apply(&execution, &compiled);
    try std.testing.expect(entry == &execution.entry.?);
    try std.testing.expectEqual(scheduling.ReadLane.analytical, entry.read_lane);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 512), owner.stats().retained_bytes);
    try std.testing.expectError(error.InvalidRunnableTransition, execution.classifyRead(.bounded));
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "SQL read lane transition cannot refresh expired or canceled authority" {
    const Clock = struct {
        var tick: u64 = 100;
        fn now() u64 {
            return tick;
        }
        fn requested(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    var compiled = try compiler.compile(std.testing.allocator, "SELECT count(*) FROM docs", .{});
    defer compiled.deinit();
    for ([_]bool{ false, true }) |expire| {
        Clock.tick = 100;
        var canceled = false;
        var owner = try scheduling.Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true });
        defer owner.close();
        var execution: scheduling.Execution = .{};
        defer execution.finish();
        try execution.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 200, .cancellation = .{ .ptr = &canceled, .requested = Clock.requested }, .retained_bytes = 512 }, .query);
        try apply(&execution, &compiled);
        try std.testing.expectEqual(@as(?u64, 200), execution.entry.?.options.deadline_ns);
        const grants = owner.stats().grants;
        if (expire) Clock.tick = 201 else canceled = true;
        try std.testing.expectError(if (expire) error.DeadlineExceeded else error.Canceled, execution.classifyRead(.general));
        try std.testing.expectEqualDeep(grants, owner.stats().grants);
        try std.testing.expectEqual(scheduling.ReadLane.analytical, execution.entry.?.read_lane);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 512), owner.stats().retained_bytes);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    }
}
