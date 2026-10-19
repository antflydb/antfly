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

//! Shared callback contracts for manual and Io-derived execution.

const derived_types = @import("derived_types.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
pub const RuntimeError = error{AsyncWorkerFailed};

/// A batch carries the session opened by BeginCatchUpFn. Its token is explicit
/// even when callbacks resume on another worker or share one cooperative thread.
pub const ApplyFn = *const fn (ctx: *anyopaque, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef, token: CatchUpSessionToken) anyerror!bool;
pub const PersistFn = *const fn (ctx: *anyopaque, index_name: []const u8, sequence: u64, force: bool) anyerror!bool;
pub const TruncateFn = *const fn (ctx: *anyopaque, sequence: u64) anyerror!void;
pub const CatchUpSessionToken = struct {
    value: u64 = 0,

    pub fn isNone(self: @This()) bool {
        return self.value == 0;
    }
};
pub const CatchUpFinishResult = struct {
    applied_sequence_persisted: bool = false,
};
pub const BeginCatchUpFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) anyerror!CatchUpSessionToken;
pub const FinishCatchUpFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, token: CatchUpSessionToken, applied_sequence: u64, success: bool) anyerror!CatchUpFinishResult;
pub const CanAdvanceToTargetFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, from_sequence: u64, target_sequence: u64) anyerror!bool;
pub const AppliedSequenceAdvancedFn = *const fn (ctx: *anyopaque, index_name: []const u8, applied_sequence: u64) void;

const std = @import("std");
const types = @import("../types.zig");
const platform_clock = @import("antfly_platform").clock;
const platform_time = @import("antfly_platform").time;

/// Deadline and its clock travel together across executor implementations.
pub const VisibilityWait = struct {
    cancellation: types.CancellationToken = .none,
    deadline_ns: ?u64 = null,
    clock: ?platform_clock.Clock = null,
    /// Supplied only when the caller has no service holder forbidding lending.
    execution_wait: ?@import("../../../common/execution_wait.zig").Boundary = null,

    pub fn check(self: @This()) !void {
        // A scoped fallible token is authoritative: retain its original
        // lifetime cause instead of collapsing it to a transport cancel flag.
        if (self.cancellation.check_fn != null) {
            try self.cancellation.check();
        } else if (self.cancellation.isCancelled()) return error.EnrichmentWaitCanceled;
        if (self.deadline_ns) |deadline_ns| {
            const now_ns = if (self.clock) |clock|
                clock.nowRealtimeNs()
            else
                platform_time.monotonicNs();
            if (now_ns >= deadline_ns) return error.EnrichmentWaitTimeout;
        }
    }

    /// Only the physical polling sleep lends CPU. Reentry precedes consuming
    /// its result, and neither operation ownership nor deadline is replaced.
    pub fn waitForProgress(self: @This(), io: std.Io, duration: std.Io.Duration) !void {
        if (self.execution_wait) |boundary| {
            try self.check();
            try boundary.beginWait();
            const result = io.sleep(duration, .awake);
            try boundary.endWait();
            try self.check();
            try result;
        } else {
            io.sleep(duration, .awake) catch {};
            try self.check();
        }
    }
};

test "derived visibility wait retains its deadline clock and cancellation" {
    var clock = platform_clock.ManualClock{};
    var cancelled = std.atomic.Value(bool).init(false);
    const wait = VisibilityWait{
        .clock = clock.clock(),
        .deadline_ns = 100,
        .cancellation = types.CancellationToken.fromAtomic(&cancelled),
    };
    try wait.check();
    clock.advanceNs(99);
    try wait.check();
    clock.advanceNs(1);
    try std.testing.expectError(error.EnrichmentWaitTimeout, wait.check());
    cancelled.store(true, .release);
    try std.testing.expectError(error.EnrichmentWaitCanceled, wait.check());
}

test "controlled derived visibility wait preserves authoritative one-shot lifetime rejection" {
    const Scope = struct {
        calls: usize = 0,
        cause: anyerror,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == 1) return self.cause;
        }
    };
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged, error.DerivedTargetAdvanceStuck }) |cause| {
        var scope: Scope = .{ .cause = cause };
        const wait: VisibilityWait = .{ .cancellation = .{ .ptr = &scope, .check_fn = Scope.check } };
        try std.testing.expectError(cause, wait.check());
        try std.testing.expectEqual(@as(usize, 1), scope.calls);
    }
}

test "controlled derived visibility retains CPU by default and skips polling after refused lending" {
    const native = @import("../../../common/runnable_scheduler.zig");
    const Probe = struct {
        scheduler: *native.Scheduler,
        checks: usize = 0,
        begins: usize = 0,
        ends: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
            try std.testing.expectEqual(@as(u64, 23), self.scheduler.stats().retained_bytes);
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.begins += 1;
            return error.CatalogGenerationChanged;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ends += 1;
        }
    };
    var scheduler = try native.Scheduler.init(.{ .slots = 2 });
    defer scheduler.close();
    var execution: native.Execution = .{};
    try execution.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 23 }, .query);
    defer execution.finish();
    var probe: Probe = .{ .scheduler = &scheduler };
    var wait: VisibilityWait = .{ .cancellation = .{ .ptr = &probe, .check_fn = Probe.check } };
    try wait.waitForProgress(std.testing.io, .zero);
    try std.testing.expectEqual(@as(usize, 1), probe.checks);
    wait.execution_wait = .{ .ptr = &probe, .begin = Probe.begin, .end = Probe.end };
    try std.testing.expectError(error.CatalogGenerationChanged, wait.waitForProgress(std.testing.io, .zero));
    try std.testing.expectEqual(@as(usize, 2), probe.checks);
    try std.testing.expectEqual(@as(usize, 1), probe.begins);
    try std.testing.expectEqual(@as(usize, 0), probe.ends);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
}
