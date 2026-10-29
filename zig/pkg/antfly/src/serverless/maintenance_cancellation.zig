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
const Runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;

/// Borrowed cooperative cancellation for one synchronous maintenance pass.
/// The atomic flag covers graceful shutdown before Future cancellation is
/// armed; `checkCancel` also observes cancellation delivered by std.Io.
pub const Token = struct {
    io: std.Io,
    /// Main-task authority only. Parallel cancellation checks never mutate it.
    execution: ?*Runnable.Execution = null,
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

    /// Called only by the owner, after its parallel children have joined.
    pub fn ownerCheckpoint(self: Token) !void {
        try self.check();
        if (self.execution) |execution| execution.checkpoint() catch |err| {
            try self.check();
            return err;
        };
    }

    /// Resume before attempting the lock: CPU reentry must never retain a
    /// mutex needed by another owner that could hold the available CPU.
    pub fn lock(self: Token, mutex: anytype) !void {
        try self.ownerCheckpoint();
        while (!mutex.tryLock()) {
            try self.sleep(.fromMilliseconds(1));
            try self.ownerCheckpoint();
        }
        errdefer {
            if (comptime @TypeOf(mutex) == *std.atomic.Mutex) mutex.unlock() else mutex.unlock(self.io);
        }
        try self.check();
    }

    /// One physical wait, on the caller's executor and original execution.
    pub fn sleep(self: Token, duration: std.Io.Duration) !void {
        try self.check();
        if (self.execution) |execution| {
            const class = try execution.retainedClass();
            try execution.park(.resource_blocked);
            const result = self.io.sleep(duration, .awake);
            execution.enter(class) catch |err| {
                try self.check();
                return err;
            };
            try self.check();
            try result;
        } else {
            try self.io.sleep(duration, .awake);
            try self.check();
        }
    }

    pub const CpuPhase = struct {
        token: Token,
        previous_slots: usize = 1,
        active: bool = false,

        pub fn finish(self: *CpuPhase) !void {
            if (self.active) {
                self.token.execution.?.resizeTeam(self.previous_slots) catch |err| {
                    try self.token.check();
                    return err;
                };
                self.active = false;
            }
            try self.token.check();
        }

        pub fn deinit(self: *CpuPhase) void {
            // Return excess CPU even when a namespace lease was lost. A
            // rejected execution retains its root until its enclosing owner
            // physically retires it; this destructor never renews authority.
            if (self.active) self.token.execution.?.resizeTeam(self.previous_slots) catch {};
            self.active = false;
        }
    };

    pub fn beginCpuPhase(self: Token, slots: usize) !CpuPhase {
        try self.ownerCheckpoint();
        var phase: CpuPhase = .{ .token = self };
        if (self.execution) |execution| {
            phase.previous_slots = if (execution.entry) |entry| entry.slots else 1;
            if (slots != phase.previous_slots) {
                execution.resizeTeam(slots) catch |err| {
                    try self.check();
                    return err;
                };
                phase.active = true;
            }
        }
        return phase;
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

/// Owner-only boundaries in synchronous publication orchestration. Parallel
/// graph callbacks use `check`, retaining lifetime-only behavior.
pub fn ownerCheckpoint(token: ?Token) !void {
    if (token) |value| try value.ownerCheckpoint();
}

/// Borrowed bridge for synchronous graph preparation and its joined parallel
/// kernels. Lease checkpoints mutate renewal state, so concurrent workers must
/// serialize them rather than race on the enclosing HeldLease.
pub const GraphBridge = struct {
    maintenance: ?Token,
    mutex: std.Io.Mutex = .init,
    failure: ?anyerror = null,

    /// Borrow the same lifetime into physical provider operations. Children
    /// replace only execution ownership; renewal remains serialized here.
    pub fn readContext(self: *GraphBridge) @import("antfly_local_sources").serverless_query_lake_read_context.Context {
        const maintenance = self.maintenance orelse return .{};
        var context: @import("antfly_local_sources").serverless_query_lake_read_context.Context = .{
            .io = maintenance.io,
            .deadline_ns = maintenance.deadline_ns,
            .authority = .{ .ptr = self, .check = checkOriginal },
        };
        if (maintenance.execution) |execution| context = context.withExecution(execution);
        return context;
    }
    fn checkOriginal(raw: *anyopaque) !void {
        try checkpoint(raw);
    }

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
