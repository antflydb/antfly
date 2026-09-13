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

const std = @import("std");
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;

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
