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

//! Borrow request execution IO without creating another worker pool.

const std = @import("std");
const runtime = @import("antfly_runtime_abi");

/// The caller keeps this adapter stable through every physical join. A deadline
/// clock is not execution authority and is deliberately not an input here.
pub const Borrow = struct {
    receiver: ?runtime.io_abi.Receiver = null,

    pub fn bind(self: *@This(), original: ?runtime.io_abi.Borrow, configured: ?std.Io) !std.Io {
        self.receiver = if (original) |value| try value.receive() else null;
        if (self.receiver) |*receiver| return receiver.io();
        return configured orelse error.BackendRuntimeIoUnavailable;
    }
};

test "source execution IO retains caller executor ahead of configured fallback" {
    const Executor = struct {
        value: i96,
        calls: usize = 0,
        fn now(raw: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            return .{ .nanoseconds = self.value };
        }
    };
    var original_executor: Executor = .{ .value = 1 };
    var configured_executor: Executor = .{ .value = 2 };
    var vtable = std.Io.failing.vtable.*;
    vtable.now = Executor.now;
    const original: std.Io = .{ .userdata = &original_executor, .vtable = &vtable };
    const fallback: std.Io = .{ .userdata = &configured_executor, .vtable = &vtable };
    var receiver: Borrow = .{};
    const io = try receiver.bind(runtime.io_abi.Borrow.init(&original), fallback);
    try std.testing.expectEqual(@as(i96, 1), std.Io.Clock.awake.now(io).nanoseconds);
    try std.testing.expectEqual(@as(usize, 1), original_executor.calls);
    try std.testing.expectEqual(@as(usize, 0), configured_executor.calls);
    const configured = try receiver.bind(null, fallback);
    try std.testing.expectEqual(@as(i96, 2), std.Io.Clock.awake.now(configured).nanoseconds);
    try std.testing.expectEqual(@as(usize, 1), configured_executor.calls);
}

test "source execution IO rejects missing authority and malformed explicit executor" {
    var receiver: Borrow = .{};
    try std.testing.expectError(error.BackendRuntimeIoUnavailable, receiver.bind(null, null));
    var invalid = runtime.io_abi.Borrow.init(&std.testing.io);
    invalid.contract = .of(u8);
    try std.testing.expectError(error.InvalidArgument, receiver.bind(invalid, std.testing.io));
}

test "source execution IO preserves caller capacity refusal without another executor" {
    var original = std.Io.Threaded.init(std.testing.allocator, .{ .async_limit = .nothing, .concurrent_limit = .nothing });
    defer original.deinit();
    const original_io = original.io();
    var receiver: Borrow = .{};
    const io = try receiver.bind(runtime.io_abi.Borrow.init(&original_io), std.testing.io);
    const Task = struct {
        fn run(called: *bool) void {
            called.* = true;
        }
    };
    var called = false;
    if (io.concurrent(Task.run, .{&called})) |result| {
        var future = result;
        future.await(io);
        return error.TestUnexpectedResult;
    } else |err| try std.testing.expectEqual(error.ConcurrencyUnavailable, err);
    try std.testing.expect(!called);
}
