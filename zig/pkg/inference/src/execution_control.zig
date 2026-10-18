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
const CheckBoundary = @import("antfly_runtime_abi").callback_abi.Boundary(struct {
    check_fn: *const fn (?*anyopaque) anyerror!void,
});
const platform_time = @import("antfly_platform").time;

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
/// primitive. Uninterruptible calls require either a supervised boundary or
/// an explicit in-process policy from the runtime owner.
pub const Interruption = enum {
    cooperative,
    uninterruptible,
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
    check_dispatch: CheckBoundary.Dispatch = CheckBoundary.local_dispatch,

    pub fn check(self: MonitorControl) !void {
        if (self.deadline_ns) |deadline_ns| {
            if (platform_time.monotonicNs() >= deadline_ns) return error.Timeout;
        }
        if (self.check_fn) |check_fn| try CheckBoundary.call("check_fn", self.check_dispatch, check_fn, .{self.ptr});
        if (self.cancellation) |token| {
            if (token.isCancelled()) return error.Cancelled;
        }
    }
};

/// Chosen by the runtime owner, never by a process-global exception. Embedded
/// callers retain buffers and sessions until native calls return. Server callers
/// require a real watchdog boundary for calls which cannot stop cooperatively.
pub const NativeCallPolicy = enum {
    require_supervision,
    in_process,
};

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
    check_dispatch: CheckBoundary.Dispatch = CheckBoundary.local_dispatch,
    /// Present only inside a worker process whose parent owns restart. It is
    /// deliberately request-scoped; a boolean "running out of process" flag
    /// cannot notice that one particular request was cancelled while blocked
    /// in a driver.
    hard_cancellation: ?HardCancellationBoundary = null,
    native_call_policy: NativeCallPolicy = .require_supervision,
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

    /// Replace a checkpoint in this compilation unit while retaining the
    /// request's IO, watchdog, deadline and progress capabilities. A callback
    /// and its error-domain dispatcher must always change together.
    pub fn withCheckpoint(self: InferenceExecutionControl, ptr: ?*anyopaque, callback: *const fn (?*anyopaque) anyerror!void) InferenceExecutionControl {
        var result = self;
        result.ptr = ptr;
        result.check_fn = callback;
        result.check_dispatch = CheckBoundary.local_dispatch;
        return result;
    }

    pub fn check(self: InferenceExecutionControl) !void {
        if (self.check_fn) |check_fn| try CheckBoundary.call("check_fn", self.check_dispatch, check_fn, .{self.ptr});
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
        var spins: usize = 0;
        while (!mutex.tryLock()) {
            try self.check();
            if (spins < 64) {
                std.atomic.spinLoopHint();
                spins += 1;
            } else {
                @import("antfly_platform").time.yieldNow();
            }
        }
        self.check() catch |err| {
            mutex.unlock();
            return err;
        };
    }

    pub fn enterUninterruptible(
        self: InferenceExecutionControl,
        interruption: Interruption,
    ) !UninterruptibleGuard {
        if (interruption != .uninterruptible) return .{};
        const boundary = self.hard_cancellation orelse {
            if (self.native_call_policy == .in_process) return .{};
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
                .check_dispatch = self.check_dispatch,
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
        control.enterUninterruptible(.uninterruptible),
    );
    var cooperative = try control.enterUninterruptible(.cooperative);
    cooperative.deinit();
}

test "native call policy is scoped to the execution control" {
    const embedded = InferenceExecutionControl{ .native_call_policy = .in_process };
    var guard = try embedded.enterUninterruptible(.uninterruptible);
    defer guard.deinit();
    try std.testing.expectError(error.ProcessIsolationRequired, (InferenceExecutionControl{}).enterUninterruptible(.uninterruptible));
    const cancelled = InferenceExecutionControl{ .native_call_policy = .in_process, .check_fn = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    }.check };
    try std.testing.expectError(error.Cancelled, cancelled.check());
}

test "execution control checkpoint replacement preserves carrier and resets foreign dispatch" {
    const abi = @import("antfly_runtime_abi");
    const Foreign = struct {
        fn dispatch(_: *const abi.native_abi.CallContract, _: *const anyopaque, _: *const anyopaque, _: ?*anyopaque) callconv(.c) abi.error_abi.Status {
            return abi.error_abi.statusFromError(error.DeadlineExceeded);
        }
        fn stop(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    const original: InferenceExecutionControl = .{ .io = std.testing.io, .check_fn = Foreign.stop, .check_dispatch = Foreign.dispatch, .deadline_ns = std.math.maxInt(u64), .native_call_policy = .in_process, .cancellation_grace_ns = 99 };
    try std.testing.expectError(error.DeadlineExceeded, original.check());
    const rebound = original.withCheckpoint(null, Foreign.stop);
    try std.testing.expectError(error.Cancelled, rebound.check());
    try std.testing.expectEqual(original.io.?.userdata, rebound.io.?.userdata);
    try std.testing.expectEqual(original.deadline_ns, rebound.deadline_ns);
    try std.testing.expectEqual(original.native_call_policy, rebound.native_call_policy);
    try std.testing.expectEqual(original.cancellation_grace_ns, rebound.cancellation_grace_ns);
}
