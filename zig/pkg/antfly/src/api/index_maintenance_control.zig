// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2
//! Shared control-plane lifetime for graph metric and relational maintenance.
//! Resource adapters retain their own generation proofs and durable executors.
const operation = @import("operation.zig");

pub const Control = struct {
    request: operation.RequestContext,

    pub fn cancellation(self: *const Control) operation.CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = cancelled, .check_fn = check };
    }

    fn cancelled(ptr: *const anyopaque) bool {
        check(ptr) catch return true;
        return false;
    }

    fn check(ptr: *const anyopaque) !void {
        const self: *const Control = @ptrCast(@alignCast(ptr));
        try self.request.ensureActive();
    }
};

test "index maintenance shared control preserves deadline and cancellation" {
    const std = @import("std");
    const control = Control{ .request = .{ .deadline_ns = 0 } };
    try std.testing.expect(control.cancellation().isCancelled());
    try std.testing.expectError(error.DeadlineExceeded, control.cancellation().check());
}
