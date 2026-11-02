// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
test {
    _ = @import("search/scorer.zig");
    _ = @import("search/search.zig");
}

test "native text lifetime retains owner clock and never yields under index locks" {
    const std = @import("std");
    const controls = @import("storage/query_controls.zig");
    const types = @import("storage/db/types.zig");
    const runtime = @import("antfly_runtime_abi");
    const State = struct {
        checks: usize = 0,
        checkpoints: usize = 0,
        failure: runtime.error_abi.Status = .{},
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            return self.failure;
        }
        fn checkpoint(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checkpoints += 1;
            return .{};
        }
        fn transition(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            return .{};
        }
    };
    var state: State = .{};
    var req: types.SearchRequest = .{ .execution_deadline_ns = 0, .execution_runnable = .{
        .context = &state,
        .cpu_slots = 1,
        .parent_class = @backingInt(runtime.runnable_abi.Class.query),
        .checkpoint = State.checkpoint,
        .park = State.transition,
        .enter = State.transition,
        .check_lifetime = State.lifetime,
    } };
    // A copied timestamp in a different clock cannot supersede the owner.
    try controls.nativeTextAuthority(&req).check();
    try std.testing.expectEqual(@as(usize, 1), state.checks);
    for ([_]anyerror{ error.Timeout, error.Canceled, error.SchedulerClosed }) |failure| {
        state.failure = runtime.error_abi.statusFromError(failure);
        try std.testing.expectError(failure, controls.nativeTextAuthority(&req).check());
    }
    req.execution_runnable.?.check_lifetime = null;
    try std.testing.expectError(error.UnsupportedOperation, controls.nativeTextAuthority(&req).check());
    try std.testing.expectEqual(@as(usize, 0), state.checkpoints);
    req.execution_runnable = null;
    try std.testing.expectError(error.Timeout, controls.nativeTextAuthority(&req).check());
}
