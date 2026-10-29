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
const RequestContext = @import("antfly_inference_execution_context").RequestContext;
const maintenance = @import("maintenance_cancellation.zig");
const Read = @import("../api/runnable_local_read.zig").Read;

/// One synchronous, physically joined model invocation. Construct at its final
/// address before binding; neither its callbacks nor its grant may be cached.
pub const Scope = struct {
    allocator: std.mem.Allocator,
    maintenance: ?maintenance.Token,
    authority: CancellationToken = .none,
    slots: usize = 1,
    previous: ?RequestContext = null,
    phase: ?maintenance.Token.CpuPhase = null,
    read: ?Read = null,
    mutex: std.Io.Mutex = .init,

    pub fn bind(self: *Scope, previous: ?RequestContext) !?RequestContext {
        self.previous = previous;
        try self.ready();
        if (self.maintenance) |token| self.phase = try token.beginCpuPhaseWithCheck(self.slots, .{ .context = self, .check = admissionCheck });
        const io = if (self.maintenance) |token| token.io else if (previous) |context| context.io else return null;
        var context = previous orelse RequestContext{ .io = io, .deadline_ns = null };
        context.io = io;
        if (self.maintenance) |token| {
            if (token.deadline_ns) |deadline| context.deadline_ns = if (context.deadline_ns) |old| @min(old, deadline) else deadline;
            if (token.execution) |execution| if (execution.entry != null or execution.borrowed != null) {
                self.read = .{ .execution = execution, .allocator = self.allocator };
                context.runnable = (try self.read.?.team()).borrow;
            };
        }
        context.cancellation = .{ .ptr = self, .check_fn = check, .is_cancelled_fn = canceled };
        try context.check();
        return context;
    }

    fn admissionCheck(raw: *anyopaque) !void {
        const self: *Scope = @ptrCast(@alignCast(raw));
        try self.ready();
    }

    /// Lifetime checks also run on watchdogs. Serialize renewable lease state;
    /// scheduler checkpoints/phase transitions stay on the invocation owner.
    fn check(raw: *const anyopaque) !void {
        const self: *Scope = @ptrCast(@alignCast(@constCast(raw)));
        const io = if (self.maintenance) |token| token.io else if (self.previous) |context| context.io else return self.checkOriginal();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        try self.checkOriginal();
    }
    fn canceled(raw: *const anyopaque) bool {
        check(raw) catch return true;
        return false;
    }
    fn checkOriginal(self: *Scope) !void {
        if (self.maintenance) |token| {
            try token.check();
            if (token.execution) |execution| try execution.checkLifetime();
        }
        try self.authority.check();
        if (self.previous) |context| try context.check();
    }
    pub fn ready(self: *Scope) !void {
        try check(self);
    }
    pub fn finish(self: *Scope) !void {
        if (self.phase) |*phase| phase.finish() catch |err| {
            try self.ready();
            return err;
        };
        try self.ready();
    }
    pub fn deinit(self: *Scope) void {
        if (self.phase) |*phase| phase.deinit();
    }
};

test "serverless execution model scope fences queued team admission before capacity returns" {
    const Runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const Job = struct {
        execution: *Runnable.Execution,
        revoked: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn previous(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            if (self.revoked.load(.acquire)) return error.ModelScopeRevoked;
        }
        fn run(self: *@This()) void {
            defer self.done.store(true, .release);
            defer self.execution.finish();
            var scope: Scope = .{ .allocator = std.testing.allocator, .maintenance = .{ .io = std.testing.io, .execution = self.execution }, .slots = 2 };
            defer scope.deinit();
            _ = scope.bind(.{ .io = std.testing.io, .deadline_ns = null, .cancellation = .{ .ptr = self, .check_fn = previous } }) catch |err| {
                self.failure = err;
                return;
            };
            self.failure = error.RevokedModelWasAdmitted;
        }
    };
    const io = std.testing.io;
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .max_wait_ms = 5000 });
    defer scheduler.close();
    var blocker: Runnable.Execution = .{};
    defer blocker.finish();
    try blocker.start(&scheduler, .{ .io = io }, .query);
    var execution: Runnable.Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = io }, .background);
    var job: Job = .{ .execution = &execution };
    var future = try io.concurrent(Job.run, .{&job});
    defer future.cancel(io);
    const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
    while (scheduler.stats().queued == 0) {
        if (job.done.load(.acquire)) return job.failure orelse error.ModelDidNotQueue;
        if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.ModelDidNotQueue;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    job.revoked.store(true, .release);
    while (!job.done.load(.acquire)) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.ModelScopeDidNotInterruptAdmission;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    future.await(io);
    try std.testing.expectEqual(error.ModelScopeRevoked, job.failure.?);
    // The unrelated owner never released its lane to make admission possible.
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    blocker.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}
