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
const platform_time = @import("antfly_platform").time;

/// A synchronous caller may lend its scheduler checkpoint to this OS worker.
/// Controls copied to watchdogs or parallel children carry no callback: those
/// threads have their own empty TLS slot. The host must join synchronous work
/// and restore this scope before returning the caller's borrowed grant.
pub const SynchronousCheckpoint = struct {
    context: *anyopaque,
    check_fn: *const fn (*anyopaque) anyerror!void,
    wait_begin_fn: ?*const fn (*anyopaque) anyerror!void = null,
    wait_end_fn: ?*const fn (*anyopaque) anyerror!void = null,
};
threadlocal var synchronous_checkpoint: ?SynchronousCheckpoint = null;
threadlocal var synchronous_checkpoint_depth: usize = 0;
threadlocal var checking_synchronous_checkpoint: bool = false;
threadlocal var waiting_synchronous_resource: bool = false;

pub const CheckpointScope = struct {
    pub fn hasCurrent() bool {
        return synchronous_checkpoint != null and !waiting_synchronous_resource;
    }

    previous: ?SynchronousCheckpoint,
    previous_waiting: bool,
    depth: usize,
    thread: if (@import("builtin").single_threaded) void else std.Thread.Id,
    active: bool = true,

    pub fn init(checkpoint: SynchronousCheckpoint) CheckpointScope {
        std.debug.assert((checkpoint.wait_begin_fn == null) == (checkpoint.wait_end_fn == null));
        const previous = synchronous_checkpoint;
        const previous_waiting = waiting_synchronous_resource;
        waiting_synchronous_resource = false;
        synchronous_checkpoint_depth += 1;
        synchronous_checkpoint = checkpoint;
        return .{ .previous = previous, .previous_waiting = previous_waiting, .depth = synchronous_checkpoint_depth, .thread = if (comptime @import("builtin").single_threaded) {} else std.Thread.getCurrentId() };
    }

    pub fn deinit(self: *CheckpointScope) void {
        if (comptime !@import("builtin").single_threaded) std.debug.assert(self.thread == std.Thread.getCurrentId());
        std.debug.assert(self.active and self.depth == synchronous_checkpoint_depth and !waiting_synchronous_resource);
        synchronous_checkpoint = self.previous;
        waiting_synchronous_resource = self.previous_waiting;
        synchronous_checkpoint_depth -= 1;
        self.active = false;
    }
};

fn checkSynchronousCheckpoint() !void {
    if (checking_synchronous_checkpoint or waiting_synchronous_resource) return;
    const checkpoint = synchronous_checkpoint orelse return;
    checking_synchronous_checkpoint = true;
    defer checking_synchronous_checkpoint = false;
    try checkpoint.check_fn(checkpoint.context);
}

/// Park only a contended resource wait. Original cancellation/deadline checks
/// continue while parked, but a runnable-only scheduler checkpoint cannot run.
/// The owning caller retires the operation if reacquisition fails.
pub const ResourceWait = struct {
    checkpoint: ?SynchronousCheckpoint = null,
    scope_depth: usize = 0,

    pub fn init() !ResourceWait {
        if (waiting_synchronous_resource) return .{};
        const checkpoint = synchronous_checkpoint orelse return .{};
        const begin = checkpoint.wait_begin_fn orelse return .{};
        try checkSynchronousCheckpoint();
        try begin(checkpoint.context);
        waiting_synchronous_resource = true;
        return .{ .checkpoint = checkpoint, .scope_depth = synchronous_checkpoint_depth };
    }

    pub fn finish(self: *ResourceWait) !void {
        const checkpoint = self.checkpoint orelse return;
        std.debug.assert(waiting_synchronous_resource and self.scope_depth == synchronous_checkpoint_depth);
        self.checkpoint = null;
        waiting_synchronous_resource = false;
        try checkpoint.wait_end_fn.?(checkpoint.context);
    }
};

/// Stable phases shared by API, enrichment, model loading, pipelines, and
/// backends. Progress is advisory; cancellation and deadlines are mandatory.
pub const Phase = enum(u8) {
    queued,
    loading_model,
    loading_weights,
    preparing_weights,
    tokenizing,
    executing,
    serializing,
    publishing,
};

pub const Progress = struct {
    phase: Phase,
    completed: u64 = 0,
    total: u64 = 0,
    model: []const u8 = "",
    backend: []const u8 = "",
};

pub const Cancellation = struct {
    ptr: ?*anyopaque = null,
    is_cancelled_fn: *const fn (?*anyopaque) bool,

    pub fn isCancelled(self: Cancellation) bool {
        return self.is_cancelled_fn(self.ptr);
    }
};

pub const ProgressSink = struct {
    ptr: ?*anyopaque = null,
    update_fn: *const fn (?*anyopaque, Progress) void,

    pub fn update(self: ProgressSink, progress: Progress) void {
        self.update_fn(self.ptr, progress);
    }
};

/// Describes the strongest interruption boundary required by an execution
/// primitive. Cooperative work may run in-process. Driver calls which cannot be
/// interrupted must be owned by a supervised worker process.
pub const Interruption = enum {
    cooperative,
    process_required,
};

/// Allocation-free view retained by a process watchdog while native code is
/// executing. The enclosing guard guarantees every borrowed pointer remains
/// alive until `disarm` returns.
pub const MonitorControl = struct {
    deadline_ns: ?u64 = null,
    cancellation: ?Cancellation = null,
    cancellation_grace_ns: ?u64 = null,
    ptr: ?*anyopaque = null,
    check_fn: ?*const fn (?*anyopaque) anyerror!void = null,

    pub fn check(self: MonitorControl) !void {
        if (self.deadline_ns) |deadline_ns| {
            if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
        }
        if (self.check_fn) |check_fn| try check_fn(self.ptr);
        if (self.cancellation) |token| {
            if (token.isCancelled()) return error.Cancelled;
        }
    }
};

/// Set once by a host that runs process-required backends (Metal, CUDA,
/// ONNX loads) inside its own process instead of a supervised worker, as
/// libantfly does: the host process is the caller's program, which cannot be
/// killed and restarted. Such calls then run without a hard-cancellation
/// boundary. Once one enters the backend it cannot be interrupted; its
/// deadline and cancellation take effect when it returns, and a driver fault
/// takes down the host.
var uninterruptible_in_process = std.atomic.Value(bool).init(false);

pub fn allowUninterruptibleInProcess() void {
    uninterruptible_in_process.store(true, .release);
}

pub fn uninterruptibleInProcessAllowed() bool {
    return uninterruptible_in_process.load(.acquire);
}

pub const HardCancellationBoundary = struct {
    ptr: *anyopaque,
    arm_fn: *const fn (*anyopaque, MonitorControl) anyerror!u64,
    disarm_fn: *const fn (*anyopaque, u64) void,

    fn arm(self: HardCancellationBoundary, control: MonitorControl) !u64 {
        return self.arm_fn(self.ptr, control);
    }

    fn disarm(self: HardCancellationBoundary, token: u64) void {
        self.disarm_fn(self.ptr, token);
    }
};

pub const UninterruptibleGuard = struct {
    boundary: ?HardCancellationBoundary = null,
    token: u64 = 0,

    pub fn deinit(self: *UninterruptibleGuard) void {
        const boundary = self.boundary orelse return;
        self.boundary = null;
        boundary.disarm(self.token);
    }
};

/// Request-scoped execution contract. `deadline_ns` is always in the process
/// monotonic clock domain. The legacy check hook is retained so existing
/// callers can migrate without weakening cancellation coverage.
pub const InferenceExecutionControl = struct {
    /// Runtime used for cancellation monitors and other structured background
    /// work. HTTP requests always supply their owning runtime; library callers
    /// may omit it when no monitor is required.
    io: ?std.Io = null,
    deadline_ns: ?u64 = null,
    cancellation: ?Cancellation = null,
    progress: ?ProgressSink = null,
    ptr: ?*anyopaque = null,
    check_fn: ?*const fn (?*anyopaque) anyerror!void = null,
    /// Present only inside a worker process whose parent owns restart. It is
    /// deliberately request-scoped; a boolean "running out of process" flag
    /// cannot notice that one particular request was cancelled while blocked
    /// in a driver.
    hard_cancellation: ?HardCancellationBoundary = null,
    /// A cancelled request may finish an in-flight native call before the
    /// watchdog replaces the worker. Checks outside native calls stay immediate.
    cancellation_grace_ns: ?u64 = null,

    /// Borrow this control for structured image work. The caller must retain
    /// it until all preprocessing workers have joined.
    pub fn imageWorkControl(self: *const InferenceExecutionControl) @import("antfly_image").work_control.Control {
        const Adapter = struct {
            fn check(raw: ?*const anyopaque) !void {
                const control: *const InferenceExecutionControl = @ptrCast(@alignCast(raw.?));
                try control.check();
            }
        };
        return .{ .context = self, .check_fn = Adapter.check };
    }

    pub fn check(self: InferenceExecutionControl) !void {
        try checkSynchronousCheckpoint();
        if (self.check_fn) |check_fn| try check_fn(self.ptr);
        if (self.cancellation) |token| {
            if (token.isCancelled()) return error.Cancelled;
        }
        if (self.deadline_ns) |deadline_ns| {
            if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
        }
    }

    pub fn update(self: InferenceExecutionControl, phase: Phase, completed: u64, total: u64) !void {
        return self.updateDetail(phase, completed, total, "", "");
    }

    pub fn updateDetail(
        self: InferenceExecutionControl,
        phase: Phase,
        completed: u64,
        total: u64,
        model: []const u8,
        backend: []const u8,
    ) !void {
        try self.check();
        if (self.progress) |sink| sink.update(.{
            .phase = phase,
            .completed = completed,
            .total = total,
            .model = model,
            .backend = backend,
        });
    }

    /// Acquire a process-local execution gate without turning queueing behind
    /// another inference request into an uncancellable wait.
    pub fn lock(self: InferenceExecutionControl, mutex: *std.atomic.Mutex) !void {
        if (mutex.tryLock()) {
            self.check() catch |err| {
                mutex.unlock();
                return err;
            };
            return;
        }
        var wait = try ResourceWait.init();
        var spins: usize = 0;
        while (true) {
            self.check() catch |err| {
                try wait.finish();
                return err;
            };
            if (mutex.tryLock()) break;
            if (spins < 64) {
                std.atomic.spinLoopHint();
                spins += 1;
            } else {
                @import("antfly_platform").time.yieldNow();
            }
        }
        wait.finish() catch |err| {
            mutex.unlock();
            return err;
        };
        self.check() catch |err| {
            mutex.unlock();
            return err;
        };
    }

    /// Only the queued mutex wait parks capacity. Reacquire before returning
    /// a held lock; an active worker RPC must retain its physical CPU grant.
    pub fn lockIo(self: InferenceExecutionControl, io: std.Io, mutex: *std.Io.Mutex) !void {
        try self.check();
        if (mutex.tryLock()) return;
        var wait = try ResourceWait.init();
        while (true) {
            self.check() catch |err| {
                try wait.finish();
                return err;
            };
            if (mutex.tryLock()) break;
            io.sleep(.fromMilliseconds(1), .awake) catch |err| {
                try wait.finish();
                return err;
            };
        }
        wait.finish() catch |err| {
            mutex.unlock(io);
            return err;
        };
        self.check() catch |err| {
            mutex.unlock(io);
            return err;
        };
    }

    pub fn enterUninterruptible(
        self: InferenceExecutionControl,
        interruption: Interruption,
    ) !UninterruptibleGuard {
        if (interruption != .process_required) return .{};
        const boundary = self.hard_cancellation orelse {
            if (uninterruptibleInProcessAllowed()) return .{};
            return error.ProcessIsolationRequired;
        };
        return .{
            .boundary = boundary,
            .token = try boundary.arm(.{
                .deadline_ns = self.deadline_ns,
                .cancellation = self.cancellation,
                .cancellation_grace_ns = self.cancellation_grace_ns,
                .ptr = self.ptr,
                .check_fn = self.check_fn,
            }),
        };
    }
};

test "execution control distinguishes cancellation and timeout" {
    const State = struct {
        cancelled: bool = false,
        fn isCancelled(raw: ?*anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.cancelled;
        }
    };
    var state = State{ .cancelled = true };
    const cancelled = InferenceExecutionControl{
        .cancellation = .{ .ptr = &state, .is_cancelled_fn = State.isCancelled },
    };
    try std.testing.expectError(error.Cancelled, cancelled.check());

    const timed_out = InferenceExecutionControl{ .deadline_ns = 0 };
    try std.testing.expectError(error.Timeout, timed_out.check());
}

test "execution control reports progress after checking" {
    const Capture = struct {
        last: ?Progress = null,
        fn update(raw: ?*anyopaque, progress: Progress) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.last = progress;
        }
    };
    var capture = Capture{};
    const control = InferenceExecutionControl{
        .progress = .{ .ptr = &capture, .update_fn = Capture.update },
    };
    try control.update(.executing, 3, 24);
    try std.testing.expectEqual(Phase.executing, capture.last.?.phase);
    try std.testing.expectEqual(@as(u64, 3), capture.last.?.completed);
}

test "execution control expires while waiting for an execution gate" {
    var mutex = std.atomic.Mutex.unlocked;
    try std.testing.expect(mutex.tryLock());
    defer mutex.unlock();
    const control = InferenceExecutionControl{
        .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_ms,
    };
    try std.testing.expectError(error.Timeout, control.lock(&mutex));
}

test "uninterruptible work fails closed without a process owner" {
    const control = InferenceExecutionControl{};
    try std.testing.expectError(
        error.ProcessIsolationRequired,
        control.enterUninterruptible(.process_required),
    );
    var cooperative = try control.enterUninterruptible(.cooperative);
    cooperative.deinit();
}

pub fn testSynchronousCheckpoint() !void {
    const Probe = struct {
        calls: usize = 0,
        fail: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.fail) return error.Forbidden;
        }
        fn child() void {
            const control = InferenceExecutionControl{};
            control.check() catch @panic("child inherited parent checkpoint");
        }
    };
    const control = InferenceExecutionControl{};
    var parent = Probe{};
    var child = Probe{ .fail = true };
    var outer = CheckpointScope.init(.{ .context = &parent, .check_fn = Probe.check });
    defer if (outer.active) outer.deinit();
    try control.check();
    var inner = CheckpointScope.init(.{ .context = &child, .check_fn = Probe.check });
    try std.testing.expectError(error.Forbidden, control.check());
    inner.deinit();
    try control.check();
    if (comptime !@import("builtin").single_threaded) {
        const worker = try std.Thread.spawn(.{}, Probe.child, .{});
        worker.join();
    }
    try std.testing.expectEqual(@as(usize, 2), parent.calls);
    try std.testing.expectEqual(@as(usize, 1), child.calls);
    outer.deinit();
    try control.check();
    try std.testing.expectEqual(@as(usize, 2), parent.calls);
}

test "runnable execution synchronous checkpoint stays on its worker and restores nested scopes" {
    try testSynchronousCheckpoint();
}

pub fn testSynchronousResourceWait() !void {
    const Mode = enum { success, canceled, resume_rejected };
    const Probe = struct {
        gate: std.atomic.Mutex = .unlocked,
        mode: Mode,
        parked: bool = false,
        canceled: bool = false,
        waits: usize = 0,
        resumes: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(!self.parked);
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.parked = true;
            self.waits += 1;
            self.canceled = self.mode == .canceled;
            self.gate.unlock();
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.parked);
            self.resumes += 1;
            if (self.mode == .resume_rejected) return error.Forbidden;
            self.parked = false;
        }
        fn canceledNow(raw: ?*anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.canceled;
        }
    };
    for ([_]Mode{ .success, .canceled, .resume_rejected }) |mode| {
        var probe = Probe{ .mode = mode };
        const control = InferenceExecutionControl{ .cancellation = .{ .ptr = &probe, .is_cancelled_fn = Probe.canceledNow } };
        var scope = CheckpointScope.init(.{ .context = &probe, .check_fn = Probe.check, .wait_begin_fn = Probe.begin, .wait_end_fn = Probe.end });
        defer if (scope.active) scope.deinit();
        try std.testing.expect(probe.gate.tryLock());
        switch (mode) {
            .success => {
                try control.lock(&probe.gate);
                probe.gate.unlock();
            },
            .canceled => try std.testing.expectError(error.Cancelled, control.lock(&probe.gate)),
            .resume_rejected => try std.testing.expectError(error.Forbidden, control.lock(&probe.gate)),
        }
        try std.testing.expectEqual(@as(usize, 1), probe.waits);
        try std.testing.expectEqual(@as(usize, 1), probe.resumes);
        try std.testing.expect(probe.gate.tryLock());
        probe.gate.unlock();
        scope.deinit();
        try (InferenceExecutionControl{}).check();
    }
}

test "runnable execution synchronous resource wait preserves cancellation and unlocks failures" {
    try testSynchronousResourceWait();
}

pub fn testNestedSynchronousResourceWait() !void {
    const Probe = struct {
        parked: bool = false,
        checks: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(!self.parked);
            self.checks += 1;
            // Collective controls may check their members recursively. The
            // owner checkpoint must not recursively yield the same capsule.
            try (InferenceExecutionControl{}).check();
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(!self.parked);
            self.parked = true;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.parked);
            self.parked = false;
        }
        fn checkpoint(self: *@This()) SynchronousCheckpoint {
            return .{ .context = self, .check_fn = check, .wait_begin_fn = begin, .wait_end_fn = end };
        }
    };
    var parent = Probe{};
    var child = Probe{};
    var outer = CheckpointScope.init(parent.checkpoint());
    defer if (outer.active) outer.deinit();
    var parent_wait = try ResourceWait.init();
    var inner = CheckpointScope.init(child.checkpoint());
    defer if (inner.active) inner.deinit();
    try (InferenceExecutionControl{}).check();
    var child_wait = try ResourceWait.init();
    try (InferenceExecutionControl{}).check();
    try child_wait.finish();
    inner.deinit();
    // The parent's checkpoint stays suspended until its own wait finishes.
    try (InferenceExecutionControl{}).check();
    try std.testing.expectEqual(@as(usize, 1), parent.checks);
    try std.testing.expectEqual(@as(usize, 2), child.checks);
    try parent_wait.finish();
    try (InferenceExecutionControl{}).check();
    try std.testing.expectEqual(@as(usize, 2), parent.checks);
    outer.deinit();
    try (InferenceExecutionControl{}).check();
}

test "runnable execution nested admitted scope restores a parked caller and prevents recursive checkpoints" {
    try testNestedSynchronousResourceWait();
}
