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

//! Borrowed original commit controls at pre-decision native service fences.
//! The synchronous caller owns this budget, its team, clock and cancellation
//! until admission and all mandatory mutation work return. Never serialize it.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const transaction = @import("transaction_contract.zig");
const routing = @import("routing_budget.zig");
const lock = @import("../storage/db/apply_rw_lock.zig");

pub fn checkPreDecision(context: transaction.PreDecisionContext) !void {
    if (context.execution_runnable) |borrow| try borrow.checkLifetime();
    try context.cancellation.check();
    if (context.deadline_ns) |deadline| {
        // Keep the caller's clock domain and the proven pre-proposal error.
        if ((routing.RoutingBudget{ .io = context.deadline_io }).nowNs() >= deadline)
            return error.PreDecisionDeadlineExceeded;
    }
}

const Controls = struct {
    execution_runnable: ?runtime.runnable_abi.Borrowed = null,
    pre_decision: transaction.PreDecisionContext,
};

fn check(controls: Controls) !void {
    // Lifetime never yields while a pinned service lock may be retained.
    if (controls.execution_runnable) |borrow| try borrow.checkLifetime();
    try checkPreDecision(controls.pre_decision);
}

pub const Budget = @import("../common/read_execution_budget.zig").Budget(Controls, check);

pub fn init(pre_decision: transaction.PreDecisionContext, borrow: ?runtime.runnable_abi.Borrowed, io: std.Io) Budget {
    // A group-local invocation may carry its independently owned worker in
    // the original context without an enclosing coordinator team. Preserve it
    // for native publication waits as well as lifetime checks. An explicit
    // synchronous team borrow remains authoritative when one was supplied.
    return .{ .controls = .{ .execution_runnable = borrow orelse pre_decision.execution_runnable, .pre_decision = pre_decision }, .io = io };
}

/// Group-local admission receives one independently owned worker token in its
/// process-local context. Its original clock/cancellation remain authoritative.
pub fn initParticipant(context: transaction.PreDecisionContext, io: std.Io) Budget {
    return init(context, context.execution_runnable, io);
}

pub fn acquisition(budget: *Budget) lock.ApplyRwLock.Acquisition {
    return .{
        .lifetime = .{ .context = budget, .check_fn = checkBudget },
        .execution_wait = budget.executionBoundary(),
        .publication = if (budget.controls.pre_decision.deadline_ns != null) budget.publication() else null,
        .retained_physical_wait = budget.controls.execution_runnable != null and budget.controls.pre_decision.deadline_ns != null,
    };
}

fn checkBudget(raw: *const anyopaque) !void {
    const budget: *Budget = @ptrCast(@alignCast(@constCast(raw)));
    try budget.checkpoint();
}

test "mutation admission controls preserve original pre-decision clock and sticky cancellation" {
    const vopr = @import("vopr");
    var clock = try vopr.vopr_io.VoprIo.init(.{});
    defer clock.deinit();
    const original_io = clock.io();
    var budget = init(.{ .deadline_ns = 1, .deadline_io = runtime.io_abi.Borrow.init(&original_io) }, null, std.testing.io);
    // The service lock's real clock is unrelated to this simulated owner clock.
    try acquisition(&budget).lifetime.check();
    try std.testing.expect(acquisition(&budget).execution_wait == null);
    budget.controls.pre_decision.deadline_ns = 0;
    try std.testing.expectError(error.PreDecisionDeadlineExceeded, acquisition(&budget).lifetime.check());
    budget.controls.pre_decision.deadline_ns = 1;
    try std.testing.expectError(error.PreDecisionDeadlineExceeded, acquisition(&budget).lifetime.check());
    var canceled = std.atomic.Value(bool).init(false);
    budget = init(.{ .cancellation = @import("../storage/db/types.zig").CancellationToken.fromAtomic(&canceled) }, null, std.testing.io);
    try acquisition(&budget).lifetime.check();
    canceled.store(true, .release);
    try std.testing.expectError(error.Canceled, acquisition(&budget).lifetime.check());
    canceled.store(false, .release);
    try std.testing.expectError(error.Canceled, acquisition(&budget).lifetime.check());
}

test "mutation admission controls preserve borrowed class and reacquire before rejecting wakes" {
    const Fixture = struct {
        mutex: *lock.ApplyRwLock,
        canceled: *std.atomic.Value(bool),
        held: bool = true,
        deny_resume: bool = false,
        cancel_parked: bool = false,
        lifetime_failure: ?anyerror = null,
        parks: usize = 0,
        resumes: usize = 0,
        checkpoints: usize = 0,
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (self.lifetime_failure) |err| runtime.error_abi.statusFromError(err) else .ok;
        }
        fn checkpoint(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checkpoints += 1;
            return runtime.error_abi.statusFromError(error.InvalidRunnableTransition);
        }
        fn current(_: *anyopaque, output: *c_int) callconv(.c) runtime.error_abi.Status {
            output.* = @backingInt(runtime.runnable_abi.Class.write);
            return .ok;
        }
        fn park(raw: *anyopaque, phase: c_int) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (phase != @backingInt(runtime.runnable_abi.Wait.resource)) return runtime.error_abi.statusFromError(error.InvalidArgument);
            self.parks += 1;
            self.mutex.unlockShared();
            self.held = false;
            if (self.cancel_parked) self.canceled.store(true, .release);
            return .ok;
        }
        fn enter(raw: *anyopaque, phase: c_int) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (phase != @backingInt(runtime.runnable_abi.Class.write)) return runtime.error_abi.statusFromError(error.InvalidArgument);
            self.resumes += 1;
            return if (self.deny_resume) runtime.error_abi.statusFromError(error.SchedulerClosed) else .ok;
        }
    };
    for (0..3) |scenario| {
        var mutex: lock.ApplyRwLock = .{};
        mutex.lockShared();
        var canceled = std.atomic.Value(bool).init(false);
        var fixture: Fixture = .{ .mutex = &mutex, .canceled = &canceled, .deny_resume = scenario == 1, .cancel_parked = scenario == 2 };
        defer if (fixture.held) mutex.unlockShared();
        const borrow: runtime.runnable_abi.Borrowed = .{
            .context = &fixture,
            .cpu_slots = 1,
            // Resume the captured current write class, not the parent query.
            .parent_class = @backingInt(runtime.runnable_abi.Class.query),
            .checkpoint = Fixture.checkpoint,
            .park = Fixture.park,
            .enter = Fixture.enter,
            .check_lifetime = Fixture.lifetime,
            .current_class = Fixture.current,
        };
        var budget = initParticipant(.{ .cancellation = @import("../storage/db/types.zig").CancellationToken.fromAtomic(&canceled), .execution_runnable = borrow }, std.testing.io);
        if (scenario == 0) {
            try mutex.lockExclusiveControlledIo(std.testing.io, acquisition(&budget));
            mutex.unlockExclusive();
        } else {
            const expected = if (scenario == 1) error.SchedulerClosed else error.Canceled;
            try std.testing.expectError(expected, mutex.lockExclusiveControlledIo(std.testing.io, acquisition(&budget)));
        }
        try std.testing.expectEqual(@as(usize, 1), fixture.parks);
        try std.testing.expectEqual(fixture.parks, fixture.resumes);
        try std.testing.expectEqual(@as(usize, 0), fixture.checkpoints);
        try std.testing.expectEqual(@as(u32, 0), mutex.parked_waiters.load(.acquire));
        try std.testing.expect(mutex.tryLockExclusive());
        mutex.unlockExclusive();
    }
    var mutex: lock.ApplyRwLock = .{};
    var canceled = std.atomic.Value(bool).init(false);
    var fixture: Fixture = .{ .mutex = &mutex, .canceled = &canceled, .held = false };
    var borrow: runtime.runnable_abi.Borrowed = .{ .context = &fixture, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.write), .checkpoint = Fixture.checkpoint, .park = Fixture.park, .enter = Fixture.enter };
    var budget = initParticipant(.{ .execution_runnable = borrow }, std.testing.io);
    try std.testing.expectError(error.UnsupportedOperation, mutex.lockExclusiveControlledIo(std.testing.io, acquisition(&budget)));
    borrow.check_lifetime = Fixture.lifetime;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
        fixture.lifetime_failure = cause;
        budget = initParticipant(.{ .execution_runnable = borrow }, std.testing.io);
        try std.testing.expectError(cause, mutex.lockExclusiveControlledIo(std.testing.io, acquisition(&budget)));
        try std.testing.expect(mutex.tryLockExclusive());
        mutex.unlockExclusive();
    }
    try std.testing.expectEqual(@as(usize, 0), fixture.parks);
    try std.testing.expectEqual(@as(usize, 0), fixture.checkpoints);
}

test "mutation admission controls reject stopped participant authority before setup without CPU acquisition" {
    const Fixture = struct {
        cause: ?anyerror = null,
        checks: usize = 0,
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            return if (self.cause) |cause| runtime.error_abi.statusFromError(cause) else .ok;
        }
        fn checkpoint(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            @panic("participant setup lifetime must not acquire or yield CPU");
        }
        fn transition(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            @panic("participant setup lifetime must not transfer execution");
        }
    };
    var fixture: Fixture = .{};
    var context: transaction.PreDecisionContext = .{ .execution_runnable = .{
        .context = &fixture,
        .cpu_slots = 1,
        .parent_class = @backingInt(runtime.runnable_abi.Class.write),
        .checkpoint = Fixture.checkpoint,
        .park = Fixture.transition,
        .enter = Fixture.transition,
    } };
    try std.testing.expectError(error.UnsupportedOperation, checkPreDecision(context));
    try std.testing.expectEqual(@as(usize, 0), fixture.checks);
    context.execution_runnable.?.check_lifetime = Fixture.lifetime;
    try checkPreDecision(context);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
        fixture.cause = cause;
        // Owner rejection remains the original cause even when a portable
        // deadline has also expired; no setup or fresh authority may follow.
        context.deadline_ns = 0;
        try std.testing.expectError(cause, checkPreDecision(context));
    }
    fixture.cause = null;
    try std.testing.expectError(error.PreDecisionDeadlineExceeded, checkPreDecision(context));
    try std.testing.expectEqual(@as(usize, 5), fixture.checks);
}
