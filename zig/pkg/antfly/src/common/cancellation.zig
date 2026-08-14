// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Transport-neutral borrowed cancellation contract.
//!
//! A callback is the semantic representation. Atomic flags are one adapter,
//! not part of the contract, so cancellation survives compiled runtime and
//! foreign-function boundaries without exposing a Zig atomic layout.

const std = @import("std");

pub const CancellationToken = struct {
    ptr: ?*const anyopaque = null,
    is_cancelled_fn: ?*const fn (*const anyopaque) bool = null,

    pub const none: CancellationToken = .{};

    pub fn fromAtomic(signal: *const std.atomic.Value(bool)) CancellationToken {
        return .{
            .ptr = signal,
            .is_cancelled_fn = struct {
                fn call(ptr: *const anyopaque) bool {
                    const value: *const std.atomic.Value(bool) = @ptrCast(@alignCast(ptr));
                    return value.load(.acquire);
                }
            }.call,
        };
    }

    pub fn isCancelled(self: CancellationToken) bool {
        const ptr = self.ptr orelse return false;
        const callback = self.is_cancelled_fn orelse return false;
        return callback(ptr);
    }

    pub fn check(self: CancellationToken) !void {
        if (self.isCancelled()) return error.Canceled;
    }
};

test "semantic cancellation token adapts an atomic source" {
    var signal = std.atomic.Value(bool).init(false);
    const token = CancellationToken.fromAtomic(&signal);
    try std.testing.expect(!token.isCancelled());
    signal.store(true, .release);
    try std.testing.expect(token.isCancelled());
    try std.testing.expectError(error.Canceled, token.check());
}

test "incomplete semantic cancellation token is safely inactive" {
    var state = false;
    try std.testing.expect(!(CancellationToken{ .ptr = &state }).isCancelled());
    try std.testing.expect(!(CancellationToken{ .is_cancelled_fn = struct {
        fn call(_: *const anyopaque) bool {
            return true;
        }
    }.call }).isCancelled());
}
