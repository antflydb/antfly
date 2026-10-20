// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Transport-neutral borrowed cancellation contract.
//!
//! A callback is the semantic representation. Atomic flags are one adapter,
//! not part of the contract, so cancellation survives compiled runtime and
//! foreign-function boundaries without exposing a Zig atomic layout.

const std = @import("std");

pub const CancellationToken = struct {
    const Callbacks = struct { check_fn: *const fn (*const anyopaque) anyerror!void };
    const Boundary = @import("antfly_runtime_abi").callback_abi.Boundary(Callbacks);
    /// The dispatcher belongs to the compilation unit that owns check_fn.
    /// Zig error integers must never be interpreted by a different archive.
    check_dispatch: Boundary.Dispatch = Boundary.local_dispatch,
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
        if (self.check_fn != null) {
            self.check() catch return true;
            return false;
        }
        const callback = self.is_cancelled_fn orelse return false;
        return callback(ptr);
    }

    pub fn check(self: CancellationToken) !void {
        if (self.ptr) |ptr| if (self.check_fn) |check_fn| return Boundary.call("check_fn", self.check_dispatch, check_fn, .{ptr});
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

test "cancellation checkpoint uses its foreign dispatcher" {
    const abi = @import("antfly_runtime_abi");
    const Foreign = struct {
        fn dispatch(_: *const abi.native_abi.CallContract, _: *const anyopaque, _: *const anyopaque, _: ?*anyopaque) callconv(.c) abi.error_abi.Status {
            return abi.error_abi.statusFromError(error.DeadlineExceeded);
        }
        fn forbidden(_: *const anyopaque) !void {
            return error.TestUnexpectedResult;
        }
    };
    var context: u8 = 0;
    const token: CancellationToken = .{ .ptr = &context, .check_fn = Foreign.forbidden, .check_dispatch = Foreign.dispatch };
    try std.testing.expectError(error.DeadlineExceeded, token.check());
    try std.testing.expect(token.isCancelled());
}
