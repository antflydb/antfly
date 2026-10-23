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

const std = @import("std");
const CancellationToken = @import("antfly_cancellation").CancellationToken;

/// Borrowed cooperative cancellation for one synchronous maintenance pass.
/// The atomic flag covers graceful shutdown before Future cancellation is
/// armed; `checkCancel` also observes cancellation delivered by std.Io.
pub const Token = struct {
    io: std.Io,
    requested: ?*const std.atomic.Value(bool) = null,
    cooperative: CancellationToken = .none,
    deadline_ns: ?u64 = null,
    checkpoint_ptr: ?*anyopaque = null,
    checkpoint_fn: ?*const fn (*anyopaque) anyerror!void = null,

    pub fn check(self: Token) !void {
        if (self.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        try self.cooperative.check();
        if (self.requested) |requested| {
            if (requested.load(.acquire)) return error.Canceled;
        }
        try self.io.checkCancel();
        if (self.checkpoint_ptr) |ptr| {
            if (self.checkpoint_fn) |checkpoint_fn| try checkpoint_fn(ptr);
        }
    }

    pub fn withCheckpoint(
        self: Token,
        ptr: *anyopaque,
        checkpoint_fn: *const fn (*anyopaque) anyerror!void,
    ) Token {
        var result = self;
        result.checkpoint_ptr = ptr;
        result.checkpoint_fn = checkpoint_fn;
        return result;
    }
};

pub fn check(token: ?Token) !void {
    if (token) |value| try value.check();
}

/// Borrowed bridge for synchronous graph preparation and its joined parallel
/// kernels. Lease checkpoints mutate renewal state, so concurrent workers must
/// serialize them rather than race on the enclosing HeldLease.
pub const GraphBridge = struct {
    maintenance: ?Token,
    mutex: std.Io.Mutex = .init,
    failure: ?anyerror = null,

    pub fn token(self: *GraphBridge) CancellationToken {
        if (self.maintenance == null) return .none;
        return .{ .ptr = self, .check_fn = checkpoint, .is_cancelled_fn = isCancelled };
    }

    fn isCancelled(ptr: *const anyopaque) bool {
        checkpoint(ptr) catch return true;
        return false;
    }

    fn checkpoint(ptr: *const anyopaque) !void {
        const self: *GraphBridge = @ptrCast(@alignCast(@constCast(ptr)));
        const maintenance = self.maintenance.?;
        self.mutex.lockUncancelable(maintenance.io);
        defer self.mutex.unlock(maintenance.io);
        if (self.failure) |err| return err;
        maintenance.check() catch |err| {
            self.failure = err;
            return err;
        };
    }
};

test "serverless graph maintenance bridge serializes renewal and preserves lease failures" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const State = struct {
        checks: usize = 0,
        lost: bool = false,
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.lost) return error.WorkLeaseLost;
            self.checks += 1;
        }
        fn worker(token: CancellationToken) anyerror!void {
            for (0..1024) |_| try token.check();
        }
    };
    var state = State{};
    var bridge = GraphBridge{ .maintenance = (Token{ .io = io }).withCheckpoint(&state, State.checkpoint) };
    var futures: [4]std.Io.Future(anyerror!void) = undefined;
    var active: usize = 0;
    defer while (active > 0) {
        active -= 1;
        _ = futures[active].cancel(io) catch {};
    };
    for (&futures) |*future| {
        future.* = try io.concurrent(State.worker, .{bridge.token()});
        active += 1;
    }
    while (active > 0) {
        active -= 1;
        try futures[active].await(io);
    }
    try std.testing.expectEqual(@as(usize, 4096), state.checks);
    state.lost = true;
    try std.testing.expectError(error.WorkLeaseLost, bridge.token().check());
    try std.testing.expect(bridge.token().isCancelled());
    // Object-store transports borrow only the boolean callback.
    const transport_token = bridge.token();
    try std.testing.expect(transport_token.is_cancelled_fn.?(transport_token.ptr.?));
    state.lost = false;
    try std.testing.expectError(error.WorkLeaseLost, bridge.token().check());
}
