// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
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

/// Request-scoped execution contract. `deadline_ns` is always in the process
/// monotonic clock domain. The legacy check hook is retained so existing
/// callers can migrate without weakening cancellation coverage.
pub const InferenceExecutionControl = struct {
    deadline_ns: ?u64 = null,
    cancellation: ?Cancellation = null,
    progress: ?ProgressSink = null,
    ptr: ?*anyopaque = null,
    check_fn: ?*const fn (?*anyopaque) anyerror!void = null,

    pub fn check(self: InferenceExecutionControl) !void {
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
        var spins: usize = 0;
        while (!mutex.tryLock()) {
            try self.check();
            if (spins < 64) {
                std.atomic.spinLoopHint();
                spins += 1;
            } else {
                std.Thread.yield() catch {};
            }
        }
        self.check() catch |err| {
            mutex.unlock();
            return err;
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
