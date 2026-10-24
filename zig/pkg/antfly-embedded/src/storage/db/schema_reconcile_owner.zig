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
const runtime = @import("../background_runtime.zig");
const admission = @import("coalesced_job_admission.zig");
/// Supervision only. DB's run-one-pass port retains schema-version fencing and
/// durable building/failed/ready transitions. The durable owner lane must drain
/// before this owner or its borrowed operation context is destroyed.
pub const Owner = struct {
    state: std.atomic.Value(u8) = .init(0),
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    in_flight: std.atomic.Value(u32) = .init(0),
    stopping: std.atomic.Value(bool) = .init(false),
    bound: bool = false,
    port: Port = undefined,
    pub const Port = struct {
        ptr: *anyopaque,
        lane: runtime.DurableJobLane,
        owner_id: u64,
        stable_address: bool,
        closing: *const std.atomic.Value(bool),
        pass: *const fn (*anyopaque) void,
    };
    fn jobRun(ptr: *anyopaque) !void {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        self.run();
    }
    fn jobDeinit(_: *anyopaque) void {}
    fn lock(self: *Owner) void {
        while (!self.lifecycle_mutex.tryLock()) @import("antfly_platform").time.yieldNow();
    }
    pub fn schedule(self: *Owner, port: Port) void {
        self.lock();
        if (self.stopping.load(.acquire) or port.closing.load(.acquire) or self.in_flight.load(.monotonic) == std.math.maxInt(u32)) {
            self.lifecycle_mutex.unlock();
            return;
        }
        _ = self.in_flight.fetchAdd(1, .acquire);
        self.lifecycle_mutex.unlock();
        defer _ = self.in_flight.fetchSub(1, .release);
        // Value-returning handles may move. Inline and movable handles retain
        // no borrowed context and complete reconciliation on the caller.
        if (!port.stable_address or port.lane.executesInline()) {
            port.pass(port.ptr);
            return;
        }
        if (!admission.request(&self.state)) return;
        // Immutable across queued callbacks and successor flights. Direct
        // admission retains this owner until publication/submission returns.
        self.lock();
        if (!self.bound) {
            self.port = port;
            self.bound = true;
        } else {
            std.debug.assert(self.port.ptr == port.ptr and self.port.owner_id == port.owner_id);
        }
        self.lifecycle_mutex.unlock();
        port.lane.submit(.{ .owner_id = port.owner_id, .class = .maintenance, .ptr = self, .run = jobRun, .deinit = jobDeinit }) catch |err| {
            std.log.warn("schema index reconciliation queue unavailable; using caller fallback err={s}", .{@errorName(err)});
            self.run();
        };
    }

    fn fence(self: *Owner) void {
        self.lock();
        self.stopping.store(true, .release);
        self.lifecycle_mutex.unlock();
    }
    /// Compatibility cleanup must already own a quiesced execution context.
    pub fn stop(self: *Owner) void {
        self.fence();
        while (self.in_flight.load(.acquire) != 0) @import("antfly_platform").time.yieldNow();
    }
    pub fn stopWithAdmission(self: *Owner, backend: *runtime.BackendRuntime, owner_id: u64, original: @import("../../common/read_admission.zig").Port) !void {
        self.lock();
        if (self.bound and (self.port.owner_id != owner_id or self.port.lane.ptr != backend.durable_jobs.ptr)) {
            self.lifecycle_mutex.unlock();
            return error.InvalidBackgroundOwner;
        }
        self.stopping.store(true, .release);
        self.lifecycle_mutex.unlock();
        try backend.durable_jobs.quiesceOwnerWithAdmission(owner_id, original);
        try runtime.waitForDirectAdmissionsWithAdmission(backend, &self.in_flight, original);
    }
    fn run(self: *Owner) void {
        const port = self.port;
        while (true) {
            if (self.stopping.load(.acquire) or port.closing.load(.acquire)) {
                self.state.store(0, .release);
                return;
            }
            port.pass(port.ptr);
            if (admission.settled(&self.state)) return;
        }
    }
};

const Fixture = struct {
    owner: Owner = .{},
    closing: std.atomic.Value(bool) = .init(false),
    queued: ?runtime.Job = null,
    refuse: bool = false,
    rerun: bool = true,
    inline_lane: bool = false,
    calls: usize = 0,
    submissions: usize = 0,
    fn submit(ptr: *anyopaque, job: runtime.Job) !void {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        self.submissions += 1;
        if (self.refuse) return error.QueueRefused;
        self.queued = job;
    }
    fn noop(_: *anyopaque, _: u64) void {}
    fn poll(_: *anyopaque, _: usize) !usize {
        return 0;
    }
    fn pass(ptr: *anyopaque) void {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        if (self.rerun and self.calls == 1) self.owner.schedule(self.port());
    }
    fn port(self: *Fixture) Owner.Port {
        return .{ .ptr = self, .lane = .{ .ptr = self, .vtable = if (self.inline_lane) &.{ .submit = submit, .drain_owner = noop, .close_owner = noop, .poll = poll, .executes_inline = true } else &.{ .submit = submit, .drain_owner = noop, .close_owner = noop, .poll = poll } }, .owner_id = 1, .stable_address = true, .closing = &self.closing, .pass = pass };
    }
    fn drain(self: *Fixture) !void {
        const job = self.queued.?;
        self.queued = null;
        try job.run(job.ptr);
        job.deinit(job.ptr);
    }
};
test "schema reconciliation coalesces publication during a pass" {
    var f: Fixture = .{};
    f.owner.schedule(f.port());
    f.owner.schedule(f.port());
    try f.drain();
    try std.testing.expectEqual(@as(usize, 2), f.calls);
    try std.testing.expectEqual(@as(usize, 1), f.submissions);
    try std.testing.expectEqual(@as(u8, 0), f.owner.state.load(.acquire));
}
test "schema reconciliation falls back on rejected admission and never retains movable handles" {
    var f: Fixture = .{ .refuse = true };
    f.owner.schedule(f.port());
    try std.testing.expectEqual(@as(usize, 2), f.calls);
    try std.testing.expectEqual(@as(u8, 0), f.owner.state.load(.acquire));
    var movable: Fixture = .{ .rerun = false };
    var port = movable.port();
    port.stable_address = false;
    movable.owner.schedule(port);
    try std.testing.expectEqual(@as(usize, 1), movable.calls);
    try std.testing.expect(!movable.owner.bound);
    try std.testing.expectEqual(@as(usize, 0), movable.submissions);
}
test "schema reconciliation close rejects registration and cancels queued reruns" {
    var f: Fixture = .{};
    f.owner.schedule(f.port());
    f.owner.stop();
    try f.drain();
    f.owner.schedule(f.port());
    try std.testing.expectEqual(@as(usize, 0), f.calls);
    try std.testing.expectEqual(@as(usize, 1), f.submissions);
    try std.testing.expectEqual(@as(u8, 0), f.owner.state.load(.acquire));
}

test "schema reconciliation executes inline without retaining a callback" {
    var f: Fixture = .{ .inline_lane = true, .rerun = false };
    f.owner.schedule(f.port());
    try std.testing.expectEqual(@as(usize, 1), f.calls);
    try std.testing.expect(!f.owner.bound);
    try std.testing.expectEqual(@as(usize, 0), f.submissions);
}

test "maintenance runtime schema stop drains queued and inline producers with original controls" {
    const controls = @import("../../common/read_admission.zig");
    const Case = enum { success, begin_failure, end_failure, lifetime_in_wait };
    for ([_]bool{ false, true }) |inline_mode| for (std.enums.values(Case)) |case| {
        var backend = try runtime.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = if (inline_mode) .manual else .io_threaded, .filesystem_io = if (inline_mode) std.testing.io else null });
        defer backend.deinit();
        const lane = backend.ptr().durable_jobs;
        const id = try backend.ptr().allocOwnerId();
        var owner: Owner = .{};
        const F = struct {
            owner: *Owner,
            lane: runtime.DurableJobLane,
            id: u64,
            io: std.Io,
            closing: std.atomic.Value(bool) = .init(false),
            entered: std.Io.Event = .unset,
            release: std.Io.Event = .unset,
            calls: usize = 0,
            fn pass(raw: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.calls += 1;
                self.entered.set(self.io);
                self.release.waitUncancelable(self.io);
            }
            fn port(self: *@This()) Owner.Port {
                return .{ .ptr = self, .lane = self.lane, .owner_id = self.id, .stable_address = true, .closing = &self.closing, .pass = pass };
            }
            fn schedule(self: *@This()) void {
                self.owner.schedule(self.port());
            }
        };
        const Control = struct {
            f: *F,
            case: Case,
            begins: usize = 0,
            ends: usize = 0,
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.case == .lifetime_in_wait and self.begins != 0) return error.DeadlineExceeded;
            }
            fn deadline(_: *anyopaque) !?u64 {
                return 1000;
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.begins += 1;
                if (self.case == .begin_failure) return error.NotFound;
                if (self.case != .lifetime_in_wait) self.f.release.set(self.f.io);
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.ends += 1;
                self.f.release.set(self.f.io);
                if (self.case == .end_failure) return error.Canceled;
            }
        };
        var f: F = .{ .owner = &owner, .lane = lane, .id = id, .io = backend.ptr().io() orelse std.testing.io };
        var thread: ?std.Thread = null;
        defer {
            f.release.set(f.io);
            if (thread) |active| active.join();
            owner.stop();
            lane.closeOwner(id);
        }
        if (inline_mode) thread = try std.Thread.spawn(.{}, F.schedule, .{&f}) else f.schedule();
        try f.entered.wait(f.io);
        if (!inline_mode) f.schedule(); // Coalesced demand must not run after stop.
        var control: Control = .{ .f = &f, .case = case };
        var payload: controls.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
        const result = owner.stopWithAdmission(backend.ptr(), id, .init(&payload));
        if (case == .success) try result else try std.testing.expectError(switch (case) {
            .begin_failure => error.NotFound,
            .end_failure => error.Canceled,
            .lifetime_in_wait => error.DeadlineExceeded,
            .success => unreachable,
        }, result);
        try std.testing.expectError(error.BackgroundOwnerIdExhausted, lane.reopenOwner(id));
        f.release.set(f.io);
        if (thread) |active| active.join();
        thread = null;
        owner.stop();
        lane.drainOwner(id);
        f.schedule();
        try std.testing.expect(owner.stopping.load(.acquire));
        try std.testing.expectEqual(@as(u32, 0), owner.in_flight.load(.acquire));
        try std.testing.expectEqual(@as(u8, 0), owner.state.load(.acquire));
        try std.testing.expectEqual(@as(usize, 1), f.calls);
        try std.testing.expectEqual(@as(usize, 1), control.begins);
        try std.testing.expectEqual(@as(usize, if (case == .begin_failure) 0 else 1), control.ends);
    };
}
