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

//! Provider-neutral inference request cancellation, deadline, and progress.

const std = @import("std");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const platform_time = @import("antfly_platform").time;

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
    deadline_ns: ?u64 = null,
};

pub const ProgressSink = struct {
    ptr: ?*anyopaque = null,
    update_fn: *const fn (?*anyopaque, Progress) void,

    pub fn update(self: ProgressSink, progress: Progress) void {
        self.update_fn(self.ptr, progress);
    }
};

/// Invocation-local control plane shared by all model families and checked
/// callback boundaries. It borrows its cancellation and progress targets for
/// the duration of one admitted invocation.
pub const RequestContext = struct {
    io: std.Io,
    deadline_ns: ?u64,
    cancellation: ?CancellationToken = null,
    progress: ?ProgressSink = null,
    /// Synchronous provider phases may borrow this caller's execution grant.
    /// Never forward its callbacks to watchdogs or parallel children.
    runnable: ?@import("antfly_runtime_abi").runnable_abi.Borrowed = null,

    /// A finite deadline, execution owner or fallible authority requires a
    /// provider that accepts the actual controls. A transport-only atomic
    /// cancellation token retains the legacy compatibility path.
    pub fn requiresControlledProvider(self: RequestContext) bool {
        return self.deadline_ns != null or self.runnable != null or
            (if (self.cancellation) |token| token.check_fn != null else false);
    }

    pub fn check(self: RequestContext) !void {
        if (self.cancellation) |value| value.check() catch |err| return switch (err) {
            error.Canceled => error.Cancelled,
            error.DeadlineExceeded => error.Timeout,
            else => err,
        };
        // Physical borrowers may poll while the parent is parked. This
        // optional owner capability observes retained lifetime without a CPU
        // checkpoint or phase transition.
        if (self.runnable) |borrow| if (borrow.check_lifetime != null) {
            borrow.checkLifetime() catch |err| return switch (err) {
                error.Canceled => error.Cancelled,
                error.DeadlineExceeded => error.Timeout,
                else => err,
            };
        };
        const deadline = self.deadline_ns orelse return;
        if (platform_time.monotonicNs() >= deadline) return error.Timeout;
    }

    pub fn remainingTimeoutMs(self: RequestContext) !?u64 {
        try self.check();
        const deadline = self.deadline_ns orelse return null;
        const remaining_ns = deadline -| platform_time.monotonicNs();
        return @max(@as(u64, 1), std.math.divCeil(u64, remaining_ns, std.time.ns_per_ms) catch 1);
    }

    pub fn update(self: RequestContext, phase: Phase, completed: u64, total: u64) !void {
        try self.check();
        if (self.progress) |sink| sink.update(.{ .phase = phase, .completed = completed, .total = total, .deadline_ns = self.deadline_ns });
    }

    pub fn updateDetail(self: RequestContext, phase: Phase, completed: u64, total: u64, model: []const u8, backend: []const u8) !void {
        try self.check();
        if (self.progress) |sink| sink.update(.{
            .phase = phase,
            .completed = completed,
            .total = total,
            .model = model,
            .backend = backend,
            .deadline_ns = self.deadline_ns,
        });
    }
};

test "provider request lifetime polling preserves parked owner without CPU checkpoints" {
    const runtime = @import("antfly_runtime_abi");
    const Fake = struct {
        calls: usize = 0,
        failure: ?anyerror = null,
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return if (self.failure) |failure| runtime.error_abi.statusFromError(failure) else .ok;
        }
        fn checkpoint(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.InvalidRunnableTransition);
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.InvalidRunnableTransition);
        }
    };
    var fake: Fake = .{};
    const request: RequestContext = .{ .io = std.testing.io, .deadline_ns = null, .runnable = .{ .context = &fake, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.query), .checkpoint = Fake.checkpoint, .park = Fake.phase, .enter = Fake.phase, .check_lifetime = Fake.lifetime } };
    try request.check();
    fake.failure = error.Canceled;
    try std.testing.expectError(error.Cancelled, request.check());
    fake.failure = error.DeadlineExceeded;
    try std.testing.expectError(error.Timeout, request.check());
    fake.failure = error.SchedulerClosed;
    try std.testing.expectError(error.SchedulerClosed, request.check());
    try std.testing.expectEqual(@as(usize, 4), fake.calls);
}
