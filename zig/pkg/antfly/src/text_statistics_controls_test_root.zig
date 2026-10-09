// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const sources = @import("antfly_local_sources");
const controls = sources.storage_local_query_controls;
const types = sources.storage_db_types;
const runtime = @import("antfly_runtime_abi");
const native = sources.common_request_admission.runnable_scheduler;
const clock = @import("antfly_platform").time;
const Read = @import("api/runnable_local_read.zig").Read;

fn requested(raw: *const anyopaque) bool {
    return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
}
fn checked(borrow: runtime.runnable_abi.Borrowed) !void {
    try borrow.validate();
    const status = borrow.checkpoint(borrow.context);
    if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
}

test "text statistics trusted controls retain original phase capacity and tighter deadline" {
    for ([_]native.Class{ .query, .inference }) |phase| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        const deadline = clock.monotonicNs() + 60 * std.time.ns_per_s;
        var canceled = std.atomic.Value(bool).init(false);
        var parent: native.Execution = .{};
        defer parent.finish();
        try parent.start(&scheduler, .{
            .io = std.testing.io,
            .native_now_ns = clock.monotonicNs,
            .deadline_ns = deadline,
            .retained_bytes = 23,
            .cancellation = .{ .ptr = &canceled, .requested = requested },
        }, phase);
        var read: Read = .{ .execution = &parent, .allocator = std.testing.allocator };
        const team = try read.team();
        const trusted: types.SearchRequest = .{
            .execution_runnable = team.borrow,
            .execution_deadline_ns = deadline,
            .cancellation = .{ .ptr = &canceled, .is_cancelled_fn = requested },
        };
        for ([_]?u64{ null, deadline - std.time.ns_per_s, deadline + std.time.ns_per_s }) |parsed_deadline| {
            var parsed: types.SearchRequest = .{ .execution_deadline_ns = parsed_deadline, .index_name = "ft", .include_stored = false };
            controls.applyTrustedControls(&parsed, trusted);
            try std.testing.expectEqual(@as(?u64, if (parsed_deadline) |value| @min(value, deadline) else deadline), parsed.execution_deadline_ns);
            try std.testing.expect(parsed.execution_runnable.?.context == team.borrow.context);
            try std.testing.expectEqual(@as(runtime.runnable_abi.Class, @fromBackingInt(@intCast(@backingInt(phase)))), try parsed.execution_runnable.?.currentClass());
            try checked(parsed.execution_runnable.?);
            try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
            try std.testing.expectEqualStrings("ft", parsed.index_name.?);
            try std.testing.expect(!parsed.include_stored);
            canceled.store(true, .release);
            try std.testing.expect(parsed.cancellation.?.isCancelled());
            try std.testing.expectError(error.Canceled, checked(parsed.execution_runnable.?));
            canceled.store(false, .release);
        }
    }
}
