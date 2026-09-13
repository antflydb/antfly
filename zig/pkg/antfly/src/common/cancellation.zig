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

//! Transport-neutral borrowed cancellation contract.
//!
//! A callback is the semantic representation. Atomic flags are one adapter,
//! not part of the contract, so cancellation survives compiled runtime and
//! foreign-function boundaries without exposing a Zig atomic layout.

const std = @import("std");

pub const CancellationToken = struct {
    ptr: ?*const anyopaque = null,
    is_cancelled_fn: ?*const fn (*const anyopaque) bool = null,
    /// Optional fallible checkpoint for scoped execution (for example a
    /// renewable publication lease). This is authoritative when supplied;
    /// the boolean callback remains available to transport-only adapters.
    check_fn: ?*const fn (*const anyopaque) anyerror!void = null,

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
        if (self.check_fn) |check_fn| {
            check_fn(ptr) catch return true;
            return false;
        }
        const callback = self.is_cancelled_fn orelse return false;
        return callback(ptr);
    }

    pub fn check(self: CancellationToken) !void {
        if (self.ptr) |ptr| if (self.check_fn) |check_fn| return check_fn(ptr);
        if (self.ptr) |ptr| if (self.is_cancelled_fn) |callback| {
            if (callback(ptr)) return error.Canceled;
        };
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
