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
    checkpoint_ptr: ?*anyopaque = null,
    checkpoint_fn: ?*const fn (*anyopaque) anyerror!void = null,

    pub fn check(self: Token) !void {
        try self.cooperative.check();
        if (self.requested) |requested| {
            if (requested.load(.acquire)) return error.Canceled;
        }
        try self.io.checkCancel();
        if (self.checkpoint_ptr) |ptr| {
            if (self.checkpoint_fn) |checkpoint_fn| try checkpoint_fn(ptr);
        }
    }

    /// Transport watchdogs run in separately canceled tasks. Poll only shared
    /// sources here: never run a lease renewal or inspect that task's Io state.
    pub fn isRequested(self: Token) bool {
        if (self.requested) |requested| if (requested.load(.acquire)) return true;
        if (self.cooperative.ptr) |ptr| if (self.cooperative.is_cancelled_fn) |probe| return probe(ptr);
        return false;
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
    failed: std.atomic.Value(bool) = .init(false),

    pub fn token(self: *GraphBridge) CancellationToken {
        if (self.maintenance == null) return .none;
        return .{ .ptr = self, .check_fn = checkpoint, .is_cancelled_fn = isCancelled };
    }

    fn isCancelled(ptr: *const anyopaque) bool {
        const self: *const GraphBridge = @ptrCast(@alignCast(ptr));
        return self.failed.load(.acquire) or self.maintenance.?.isRequested();
    }

    fn checkpoint(ptr: *const anyopaque) !void {
        const self: *GraphBridge = @ptrCast(@alignCast(@constCast(ptr)));
        const maintenance = self.maintenance.?;
        self.mutex.lockUncancelable(maintenance.io);
        defer self.mutex.unlock(maintenance.io);
        if (self.failure) |err| return err;
        maintenance.check() catch |err| {
            self.failure = err;
            self.failed.store(true, .release);
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

test "serverless graph transport polling cannot poison a request when its watchdog is canceled" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const Poll = struct {
        ready: std.atomic.Value(bool) = .init(false),
        bridge: *GraphBridge,
        fn run(self: *@This(), task_io: std.Io) bool {
            self.ready.store(true, .release);
            task_io.sleep(.fromSeconds(60), .awake) catch {};
            // HTTP cancellation joins a stopped watchdog. A probe in that
            // canceled task must not cache its task-local cancellation as a
            // terminal error belonging to the still-live parent request.
            return self.bridge.token().is_cancelled_fn.?(self.bridge);
        }
    };
    var requested = std.atomic.Value(bool).init(false);
    var bridge = GraphBridge{ .maintenance = .{ .io = io, .requested = &requested } };
    var poll = Poll{ .bridge = &bridge };
    var future = try io.concurrent(Poll.run, .{ &poll, io });
    while (!poll.ready.load(.acquire)) try io.sleep(.fromMilliseconds(1), .awake);
    try std.testing.expect(!future.cancel(io));
    try bridge.token().check();
    requested.store(true, .release);
    try std.testing.expect(bridge.token().is_cancelled_fn.?(&bridge));
}

test "serverless multiprovider transport cancellation probes never run renewal I/O" {
    const State = struct {
        checks: usize = 0,
        fn checkpoint(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            return error.WorkLeaseLost;
        }
    };
    var state = State{};
    var bridge = GraphBridge{ .maintenance = (Token{ .io = std.testing.io }).withCheckpoint(&state, State.checkpoint) };
    try std.testing.expect(!bridge.token().is_cancelled_fn.?(&bridge));
    try std.testing.expectEqual(@as(usize, 0), state.checks);
    try std.testing.expectError(error.WorkLeaseLost, bridge.token().check());
    try std.testing.expect(bridge.token().is_cancelled_fn.?(&bridge));
}
