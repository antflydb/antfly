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

//! One ownership boundary for lane, mutex, request budget and backend state.
const std = @import("std");
const ops = @import("ops.zig");
const control_mod = @import("../execution_control.zig");
const Control = control_mod.InferenceExecutionControl;

pub const Phase = enum { setup, step, release, cleanup };
pub const OwnerSnapshot = struct { transitions: u64 = 0, reuses: u64 = 0, cleanups: u64 = 0, releases: u64 = 0, resets: u64 = 0 };

/// Access last_owner only under the model execution mutex. Counters are atomic
/// so metrics can observe transitions without racing backend execution.
pub const OwnerState = struct {
    last_owner: u64 = 0,
    transitions: std.atomic.Value(u64) = .init(0),
    reuses: std.atomic.Value(u64) = .init(0),
    cleanups: std.atomic.Value(u64) = .init(0),
    releases: std.atomic.Value(u64) = .init(0),
    resets: std.atomic.Value(u64) = .init(0),

    pub fn enter(self: *OwnerState, owner: u64, context: *anyopaque, prepare: *const fn (*anyopaque) anyerror!void) !void {
        if (owner == 0) return error.InvalidExecutionOwner;
        if (self.last_owner == owner) {
            _ = self.reuses.fetchAdd(1, .monotonic);
            return;
        }
        self.last_owner = 0;
        try prepare(context);
        self.last_owner = owner;
        _ = self.transitions.fetchAdd(1, .monotonic);
    }

    /// Plain tensor retirement preserves the current owner's request state.
    /// A different owner's release may mutate shared caches, so invalidate it.
    /// Graph/captured-address routes do not declare cooperative sharing.
    pub fn release(self: *OwnerState, owner: u64) void {
        if (self.last_owner != owner) self.last_owner = 0;
        _ = self.releases.fetchAdd(1, .monotonic);
    }

    pub fn cleanup(self: *OwnerState) void {
        self.last_owner = 0;
        _ = self.cleanups.fetchAdd(1, .monotonic);
    }

    pub fn reset(self: *OwnerState) void {
        self.last_owner = 0;
        _ = self.resets.fetchAdd(1, .monotonic);
    }

    pub fn snapshot(self: *const OwnerState) OwnerSnapshot {
        return .{
            .transitions = self.transitions.load(.monotonic),
            .reuses = self.reuses.load(.monotonic),
            .cleanups = self.cleanups.load(.monotonic),
            .releases = self.releases.load(.monotonic),
            .resets = self.resets.load(.monotonic),
        };
    }
};

pub const Scope = struct {
    mutex: *std.atomic.Mutex,
    lane: ?control_mod.ExecutionLane.Lease,
    backend: ?ops.ComputeBackend = null,
    phase: Phase,
    active: bool = true,
    batch_active: bool = false,
    batch_drained: bool = false,

    pub const Options = struct {
        mutex: *std.atomic.Mutex,
        lane: ?*control_mod.ExecutionLane = null,
        interactive: bool = false,
        contention_wait_ns: u64 = 0,
        phase: Phase = .step,
    };

    pub fn acquire(control: Control, options: Options) !Scope {
        const queued = if (options.lane) |lane| try lane.acquireWithPriority(control, options.interactive) else null;
        errdefer if (queued) |lease| lease.release();
        try control.lockWithPoll(options.mutex, options.contention_wait_ns);
        return .{ .mutex = options.mutex, .lane = queued, .phase = options.phase };
    }

    /// Cleanup is non-cancellable once borrowed work has drained. Backend
    /// cleanup transitions are intentionally non-failing and do not reset a
    /// whole request while retiring its KV storage.
    pub fn acquireCleanup(io: ?std.Io, options: Options) Scope {
        const queued = if (options.lane) |lane| lane.acquireForCleanup(io, options.interactive) else null;
        (Control{}).lockWithPoll(options.mutex, options.contention_wait_ns) catch unreachable;
        return .{ .mutex = options.mutex, .lane = queued, .phase = .cleanup };
    }

    pub fn acquireRelease(io: ?std.Io, options: Options) Scope {
        var scope = acquireCleanup(io, options);
        scope.phase = .release;
        return scope;
    }

    pub fn bind(self: *Scope, backend: ops.ComputeBackend) !void {
        std.debug.assert(self.active and self.backend == null);
        backend.bindExecutionBudget();
        errdefer backend.clearExecutionBudget();
        if (self.phase == .cleanup) {
            if (backend.execution_cleanup) |cleanup| cleanup(backend.ptr, backend.execution_owner);
        } else if (self.phase == .release) {
            if (backend.execution_release) |release| release(backend.ptr, backend.execution_owner);
        } else if (backend.execution_enter) |enter| try enter(backend.ptr, backend.execution_owner);
        self.backend = backend;
    }

    /// The scope owns this borrowed aggregate until GPU work and publication
    /// drain. The request-local backend wrapper remains unchanged.
    pub fn bindDecodeBatch(self: *Scope, budget: *@import("../runtime/tier/memory.zig").RunBudget, rows: usize) void {
        var backend = self.backend orelse unreachable;
        backend.execution_budget = budget;
        backend.bindExecutionBudget();
        self.backend = backend;
        if (backend.execution_batch_rows) |set| set(backend.ptr, rows);
        self.batch_active = true;
    }

    /// No borrowed row may publish ready before its queued GPU accesses drain.
    /// A sticky driver failure poisons this worker; unwinding borrowed pointers
    /// would permit use-after-free and is not a recoverable request error.
    pub fn drainBatch(self: *Scope) void {
        if (!self.batch_active or self.batch_drained) return;
        const backend = self.backend.?;
        if (backend.execution_batch_drain) |drain| drain(backend.ptr) catch |err|
            std.debug.panic("CUDA cohort stream cannot drain; worker restart required: {s}", .{@errorName(err)});
        self.batch_drained = true;
    }

    pub fn bindCleanup(self: *Scope, backend: ops.ComputeBackend) void {
        std.debug.assert(self.phase == .cleanup or self.phase == .release);
        self.bind(backend) catch unreachable;
    }

    pub fn deinit(self: *Scope) void {
        if (!self.active) return;
        self.drainBatch();
        if (self.backend) |backend| {
            if (self.batch_active) if (backend.execution_batch_rows) |set| set(backend.ptr, 0);
            backend.clearExecutionBudget();
        }
        self.mutex.unlock();
        if (self.lane) |lease| lease.release();
        self.active = false;
    }
};

test "execution owner transitions reset once and preserve same-request steps" {
    const Probe = struct {
        resets: usize = 0,
        fail: bool = false,
        fn prepare(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.resets += 1;
            if (self.fail) return error.TestTransitionFailed;
        }
    };
    var state = OwnerState{};
    var probe = Probe{};
    try state.enter(1, &probe, Probe.prepare);
    try state.enter(1, &probe, Probe.prepare);
    try state.enter(1, &probe, Probe.prepare);
    try std.testing.expectEqual(@as(usize, 1), probe.resets);
    try state.enter(2, &probe, Probe.prepare);
    state.cleanup();
    try state.enter(2, &probe, Probe.prepare);
    try std.testing.expectEqual(@as(usize, 3), probe.resets);
    probe.fail = true;
    try std.testing.expectError(error.TestTransitionFailed, state.enter(3, &probe, Probe.prepare));
    try std.testing.expectEqual(@as(u64, 0), state.last_owner);
    const stats = state.snapshot();
    try std.testing.expectEqual(@as(u64, 3), stats.transitions);
    try std.testing.expectEqual(@as(u64, 2), stats.reuses);
}

test "execution scope rolls back failed binding and permits cancelled cleanup" {
    const Probe = struct {
        budget: ?*@import("../runtime/tier/memory.zig").RunBudget = null,
        fail: bool = true,
        cleanups: usize = 0,
        fn bind(raw: *anyopaque, budget: ?*@import("../runtime/tier/memory.zig").RunBudget) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.budget = budget;
        }
        fn enter(raw: *anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail) return error.TestTransitionFailed;
        }
        fn cleanup(raw: *anyopaque, _: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.cleanups += 1;
        }
    };
    var mutex = std.atomic.Mutex.unlocked;
    var lane = control_mod.ExecutionLane{};
    var probe = Probe{};
    var budget = @import("../runtime/tier/memory.zig").RunBudget.init(.{});
    const backend = ops.ComputeBackend{ .ptr = &probe, .vtable = undefined, .execution_budget = &budget, .execution_budget_bind = Probe.bind, .execution_enter = Probe.enter, .execution_cleanup = Probe.cleanup, .execution_owner = 1 };
    {
        var scope = try Scope.acquire(.{}, .{ .mutex = &mutex, .lane = &lane });
        defer scope.deinit();
        try std.testing.expectError(error.TestTransitionFailed, scope.bind(backend));
        try std.testing.expect(probe.budget == null);
    }
    try std.testing.expect(mutex.tryLock());
    mutex.unlock();
    try std.testing.expectError(error.Timeout, Scope.acquire(.{ .deadline_ns = 0 }, .{ .mutex = &mutex, .lane = &lane }));
    var cleanup = Scope.acquireCleanup(null, .{ .mutex = &mutex, .lane = &lane });
    cleanup.bindCleanup(backend);
    try std.testing.expect(probe.budget == &budget);
    cleanup.deinit();
    cleanup.deinit();
    // Resource retirement also ignores cancellation, binds the right budget,
    // and never enters a request (the probe's entry deliberately still fails).
    var release = Scope.acquireRelease(null, .{ .mutex = &mutex, .lane = &lane });
    release.bindCleanup(backend);
    try std.testing.expect(probe.budget == &budget);
    release.deinit();
    try std.testing.expect(probe.budget == null);
    try std.testing.expectEqual(@as(usize, 1), probe.cleanups);
    try std.testing.expect(mutex.tryLock());
    mutex.unlock();
}

test "execution resource release preserves same owner and invalidates a peer without a request reset" {
    const Probe = struct {
        fn prepare(_: *anyopaque) !void {}
    };
    var state = OwnerState{};
    var marker: u8 = 0;
    try state.enter(1, &marker, Probe.prepare);
    state.release(1);
    try state.enter(1, &marker, Probe.prepare);
    try std.testing.expectEqual(@as(u64, 1), state.snapshot().transitions);
    try std.testing.expectEqual(@as(u64, 1), state.snapshot().reuses);
    state.release(2);
    try std.testing.expectEqual(@as(u64, 0), state.last_owner);
    try state.enter(1, &marker, Probe.prepare);
    try std.testing.expectEqual(@as(u64, 2), state.snapshot().transitions);
    state.cleanup();
    try std.testing.expectEqual(@as(u64, 0), state.last_owner);
}

test "execution scope binds a borrowed cohort budget only until shared retirement" {
    const Memory = @import("../runtime/tier/memory.zig");
    const Probe = struct {
        bound: ?*Memory.RunBudget = null,
        rows: usize = 0,
        drains: usize = 0,
        fn bind(raw: *anyopaque, budget: ?*Memory.RunBudget) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.bound = budget;
        }
        fn setRows(raw: *anyopaque, rows: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.rows = rows;
        }
        fn drain(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.bound != null and self.rows == 2);
            self.drains += 1;
        }
    };
    var probe = Probe{};
    var leader = Memory.RunBudget.init(.{});
    var cohort = Memory.RunBudget.init(.{});
    const cb = ops.ComputeBackend{ .ptr = &probe, .vtable = undefined, .execution_budget = &leader, .execution_budget_bind = Probe.bind, .execution_batch_rows = Probe.setRows, .execution_batch_drain = Probe.drain };
    var mutex = std.atomic.Mutex.unlocked;
    var scope = try Scope.acquire(.{}, .{ .mutex = &mutex });
    try scope.bind(cb);
    try std.testing.expect(probe.bound == &leader);
    scope.bindDecodeBatch(&cohort, 2);
    try std.testing.expect(probe.bound == &cohort);
    try std.testing.expect(cb.execution_budget == &leader);
    scope.drainBatch();
    try std.testing.expectEqual(@as(usize, 1), probe.drains);
    scope.deinit();
    try std.testing.expect(probe.bound == null and probe.rows == 0);
    try std.testing.expectEqual(@as(usize, 1), probe.drains);
}
