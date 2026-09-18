// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const callback = @import("runtime_callback_abi.zig");
const error_abi = @import("runtime_error_abi.zig");
const VTable = struct { query: *const fn (u8) anyerror!void };
extern fn runtime_callback_test_dispatch() callback.CallbackDispatch;
extern fn runtime_callback_test_query() *const anyopaque;

test "callback archive boundary preserves dense admission and query lifetime failures" {
    const boundary = callback.Boundary(VTable);
    const foreign = runtime_callback_test_dispatch();
    // Prevent the local-dispatch optimization from masking a missing status.
    try std.testing.expect(foreign != boundary.local_dispatch);
    const query: @FieldType(VTable, "query") = @ptrCast(@alignCast(runtime_callback_test_query()));
    const failures = [_]anyerror{
        error.AdmissionFull,
        error.AdmissionQueueFull,
        error.AdmissionBytesExhausted,
        error.AdmissionRequestTooLarge,
        error.AdmissionWaitTimeout,
        error.AdmissionClosed,
        error.DeadlineExceeded,
        error.Canceled,
    };
    for (failures, 0..) |failure, index| {
        try std.testing.expectError(failure, boundary.call("query", foreign, query, .{@as(u8, @intCast(index))}));
        try std.testing.expect(error_abi.errorHasStableDetail(failure));
    }
    // Do not relabel ownership bugs as recoverable overload.
    try std.testing.expectEqual(error.RuntimeBoundaryFailure, error_abi.errorFromStatus(error_abi.statusFromError(error.InvalidLease)));
}
