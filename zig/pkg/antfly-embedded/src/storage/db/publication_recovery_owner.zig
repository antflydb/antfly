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

//! Stable, close-safe registration of publication retry jobs and probes.
//! Storage supplies durable operations; Driver owns retry policy.
const std = @import("std");
const background = @import("../background_runtime.zig");
const policy = @import("publication_outbox_recovery.zig");
pub const Owner = struct {
    runtime: *background.BackendRuntime,
    owner_id: u64,
    driver: policy.Driver = .{},
    mutex: std.atomic.Mutex = .unlocked,
    stopping: std.atomic.Value(bool) = .init(false),
    in_flight: std.atomic.Value(u32) = .init(0),
    bound: bool = false,
    maintenance_only: std.atomic.Value(bool) = .init(false),
    port: Port = undefined,
    pub const Port = struct {
        ptr: *anyopaque,
        eligible: *const fn (*anyopaque) bool,
        closing: *const fn (*anyopaque) bool,
        pending: *const fn (*anyopaque) bool,
        may_disarm: *const fn (*anyopaque) bool,
        now: *const fn (*anyopaque) u64,
        drain: *const fn (*anyopaque) anyerror!void,
        /// Opt-in finite step. Unsupported/legacy debt must return the marker
        /// before materialization or any blocking publisher operation.
        bounded_drain: ?*const fn (*anyopaque) anyerror!void = null,
        progress: ?*const fn (*anyopaque) policy.Driver.Progress = null,
        retry_delay: *const fn (*anyopaque, u32) u64,
        report: *const fn (*anyopaque, policy.Driver.Failure, anyerror, u32, u64) void,
    };
    pub fn init(runtime: *background.BackendRuntime) !Owner {
        return .{ .runtime = runtime, .owner_id = try runtime.allocOwnerId() };
    }
    fn lock(self: *Owner) void {
        while (!self.mutex.tryLock()) @import("antfly_platform").time.yieldNow();
    }
    /// Binding happens after installation at a stable address and stays
    /// immutable through the last queued job and claimed probe.
    fn begin(self: *Owner, port: Port) bool {
        self.lock();
        defer self.mutex.unlock();
        if (self.stopping.load(.acquire) or !port.eligible(port.ptr)) return false;
        if (!self.bound) {
            self.port = port;
            self.bound = true;
        } else std.debug.assert(self.port.ptr == port.ptr);
        _ = self.in_flight.fetchAdd(1, .acquire);
        return true;
    }
    pub fn schedule(self: *Owner, port: Port) void {
        if (!self.begin(port)) return;
        defer _ = self.in_flight.fetchSub(1, .release);
        self.driver.schedule(self.driverPort(false));
    }
    /// Retain observation even while the durable outbox is currently empty.
    pub fn watch(self: *Owner, port: Port) void {
        if (!self.begin(port)) return;
        defer _ = self.in_flight.fetchSub(1, .release);
        arm(self);
    }
    pub fn deinit(self: *Owner) void {
        self.lock();
        if (self.stopping.swap(true, .acq_rel)) {
            self.mutex.unlock();
            return;
        }
        self.mutex.unlock();
        self.runtime.disarmOwnerMaintenanceProbe(self.owner_id);
        self.runtime.durable_jobs.closeOwner(self.owner_id);
        // Direct admission callers may have raced the close. The closed
        // registry rejects their enqueue/rearm, and their borrow must retire.
        while (self.in_flight.load(.acquire) != 0) @import("antfly_platform").time.yieldNow();
    }
    fn cast(ptr: *anyopaque) *Owner {
        return @ptrCast(@alignCast(ptr));
    }
    fn driverPort(self: *Owner, comptime bounded: bool) policy.Driver.Port {
        return .{ .ptr = self, .eligible = eligible, .closing = closing, .pending = pending, .may_disarm = mayDisarm, .now = now, .drain = if (bounded) boundedDrain else drain, .progress = if (self.port.progress != null) progress else null, .submit = submit, .arm = arm, .disarm = disarm, .retry_delay = delay, .report = report };
    }
    fn eligible(ptr: *anyopaque) bool {
        const self = cast(ptr);
        return !self.stopping.load(.acquire) and self.port.eligible(self.port.ptr);
    }
    fn closing(ptr: *anyopaque) bool {
        const self = cast(ptr);
        return self.stopping.load(.acquire) or self.port.closing(self.port.ptr);
    }
    fn pending(ptr: *anyopaque) bool {
        const self = cast(ptr);
        return self.port.pending(self.port.ptr);
    }
    fn mayDisarm(ptr: *anyopaque) bool {
        const self = cast(ptr);
        return self.port.may_disarm(self.port.ptr);
    }
    fn now(ptr: *anyopaque) u64 {
        const self = cast(ptr);
        return self.port.now(self.port.ptr);
    }
    fn drain(ptr: *anyopaque) !void {
        const self = cast(ptr);
        try self.port.drain(self.port.ptr);
        self.maintenance_only.store(false, .release);
    }
    fn boundedDrain(ptr: *anyopaque) !void {
        const self = cast(ptr);
        self.port.bounded_drain.?(self.port.ptr) catch |err| {
            if (err == error.PublicationRecoveryNeedsMaintenance) {
                self.maintenance_only.store(true, .release);
                return error.CommitCompletionPending;
            }
            return err;
        };
    }
    fn progress(ptr: *anyopaque) policy.Driver.Progress {
        const self = cast(ptr);
        return self.port.progress.?(self.port.ptr);
    }
    fn delay(ptr: *anyopaque, failures: u32) u64 {
        const self = cast(ptr);
        return self.port.retry_delay(self.port.ptr, failures);
    }
    fn report(ptr: *anyopaque, failure: policy.Driver.Failure, err: anyerror, failures: u32, delay_ns: u64) void {
        const self = cast(ptr);
        self.port.report(self.port.ptr, failure, err, failures, delay_ns);
    }
    fn submit(ptr: *anyopaque) !void {
        const self = cast(ptr);
        if (self.stopping.load(.acquire)) return error.Canceled;
        const bounded = self.port.bounded_drain != null and !self.maintenance_only.load(.acquire);
        // Registration prepaid this owner's intrusive queue slot; runtime
        // construction prepaid recovery workers and their dispatch futures.
        // Bind the callback to the submitted class, not a later mutable flag.
        try self.runtime.durable_jobs.submit(.{ .owner_id = self.owner_id, .class = if (bounded) .recovery else .maintenance, .ptr = self, .run = if (bounded) runRecovery else run, .deinit = jobDeinit });
    }
    fn run(ptr: *anyopaque) !void {
        const self = cast(ptr);
        if (closing(ptr)) {
            self.driver.state.store(0, .release);
            return;
        }
        self.driver.run(self.driverPort(false));
    }
    fn runRecovery(ptr: *anyopaque) !void {
        const self = cast(ptr);
        if (closing(ptr)) {
            self.driver.state.store(0, .release);
            return;
        }
        self.driver.run(self.driverPort(true));
    }
    fn jobDeinit(_: *anyopaque) void {}
    fn arm(ptr: *anyopaque) void {
        const self = cast(ptr);
        if (self.stopping.load(.acquire)) return;
        self.runtime.armOwnerMaintenanceProbe(self.owner_id, .{ .ptr = self, .run = probe }) catch |err| {
            std.log.warn("publication recovery supervisor unavailable err={s}", .{@errorName(err)});
        };
    }
    fn disarm(ptr: *anyopaque) void {
        const self = cast(ptr);
        self.runtime.disarmOwnerMaintenanceProbe(self.owner_id);
    }
    fn probe(ptr: *anyopaque) void {
        const self = cast(ptr);
        self.schedule(self.port);
    }
};

test "publication recovery registration delivers and drains borrowed callbacks before retirement" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    var runtime = try background.BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer runtime.deinit();
    const F = struct {
        has_pending: std.atomic.Value(bool) = .init(true),
        closed: std.atomic.Value(bool) = .init(false),
        drains: std.atomic.Value(u32) = .init(0),
        block: std.atomic.Value(bool) = .init(false),
        entered: std.atomic.Value(bool) = .init(false),
        released: std.atomic.Value(bool) = .init(false),
        fn f(ptr: *anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr));
        }
        fn yes(_: *anyopaque) bool {
            return true;
        }
        fn closing(ptr: *anyopaque) bool {
            return f(ptr).closed.load(.acquire);
        }
        fn pending(ptr: *anyopaque) bool {
            return f(ptr).has_pending.load(.acquire);
        }
        fn now(_: *anyopaque) u64 {
            return @import("antfly_platform").time.monotonicNs();
        }
        fn drain(ptr: *anyopaque) !void {
            const self = f(ptr);
            std.debug.assert(!self.closed.load(.acquire));
            if (self.block.load(.acquire)) {
                self.entered.store(true, .release);
                while (!self.released.load(.acquire)) @import("antfly_platform").time.yieldNow();
                std.debug.assert(!self.closed.load(.acquire));
            }
            _ = self.drains.fetchAdd(1, .release);
            self.has_pending.store(false, .release);
        }
        fn delay(_: *anyopaque, _: u32) u64 {
            return std.time.ns_per_ms;
        }
        fn report(_: *anyopaque, _: policy.Driver.Failure, _: anyerror, _: u32, _: u64) void {}
    };
    var f: F = .{};
    var owner = try Owner.init(runtime.ptr());
    defer owner.deinit();
    defer f.released.store(true, .release);
    const port: Owner.Port = .{ .ptr = &f, .eligible = F.yes, .closing = F.closing, .pending = F.pending, .may_disarm = F.yes, .now = F.now, .drain = F.drain, .retry_delay = F.delay, .report = F.report };
    owner.watch(port);
    owner.schedule(port);
    const deadline = F.now(&f) + 5 * std.time.ns_per_s;
    while (f.has_pending.load(.acquire) and F.now(&f) < deadline) {
        _ = try runtime.ptr().durable_jobs.poll(8);
        if (runtime.ptr().io()) |io| io.sleep(.fromMilliseconds(1), .awake) catch {};
    }
    try std.testing.expect(!f.has_pending.load(.acquire));
    owner.watch(port);
    f.block.store(true, .release);
    f.has_pending.store(true, .release);
    owner.schedule(port);
    const entry_deadline = F.now(&f) + 5 * std.time.ns_per_s;
    while (!f.entered.load(.acquire) and F.now(&f) < entry_deadline) @import("antfly_platform").time.yieldNow();
    try std.testing.expect(f.entered.load(.acquire));
    const Close = struct {
        owner: *Owner,
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.owner.deinit();
            self.done.store(true, .release);
        }
    };
    var close: Close = .{ .owner = &owner };
    const thread = try std.Thread.spawn(.{}, Close.run, .{&close});
    defer thread.join();
    defer f.released.store(true, .release);
    const close_deadline = F.now(&f) + 5 * std.time.ns_per_s;
    while (!owner.stopping.load(.acquire) and F.now(&f) < close_deadline) @import("antfly_platform").time.yieldNow();
    try std.testing.expect(owner.stopping.load(.acquire));
    try std.testing.expect(!close.done.load(.acquire));
    f.released.store(true, .release);
    while (!close.done.load(.acquire)) @import("antfly_platform").time.yieldNow();
    f.closed.store(true, .release);
    const delivered = f.drains.load(.acquire);
    f.has_pending.store(true, .release);
    owner.watch(port);
    owner.schedule(port);
    _ = try runtime.ptr().durable_jobs.poll(8);
    try std.testing.expectEqual(delivered, f.drains.load(.acquire));
    try std.testing.expect(f.has_pending.load(.acquire));
}

const ReservedPublicationTest = struct {
    pending_work: std.atomic.Value(bool) = .init(false),
    bounded_calls: std.atomic.Value(usize) = .init(0),
    ordinary_calls: std.atomic.Value(usize) = .init(0),
    admission_failures: std.atomic.Value(usize) = .init(0),
    needs_maintenance: bool = false,
    now_ns: std.atomic.Value(u64) = .init(1),
    fn cast(ptr: *anyopaque) *@This() {
        return @ptrCast(@alignCast(ptr));
    }
    fn yes(_: *anyopaque) bool {
        return true;
    }
    fn no(_: *anyopaque) bool {
        return false;
    }
    fn pending(ptr: *anyopaque) bool {
        return cast(ptr).pending_work.load(.acquire);
    }
    fn now(ptr: *anyopaque) u64 {
        return cast(ptr).now_ns.load(.acquire);
    }
    fn bounded(ptr: *anyopaque) !void {
        const self = cast(ptr);
        _ = self.bounded_calls.fetchAdd(1, .release);
        if (self.needs_maintenance) return error.PublicationRecoveryNeedsMaintenance;
        self.pending_work.store(false, .release);
    }
    fn ordinary(ptr: *anyopaque) !void {
        const self = cast(ptr);
        _ = self.ordinary_calls.fetchAdd(1, .release);
        self.pending_work.store(false, .release);
    }
    fn deinitJob(_: *anyopaque) void {}
    fn delay(_: *anyopaque, _: u32) u64 {
        return std.time.ns_per_s;
    }
    fn report(ptr: *anyopaque, kind: policy.Driver.Failure, _: anyerror, _: u32, _: u64) void {
        if (kind == .admission) _ = cast(ptr).admission_failures.fetchAdd(1, .release);
    }
    fn port(self: *@This()) Owner.Port {
        return .{ .ptr = self, .eligible = yes, .closing = no, .pending = pending, .may_disarm = no, .now = now, .drain = ordinary, .bounded_drain = bounded, .retry_delay = delay, .report = report };
    }
    fn waitForClear(self: *@This(), runtime: *background.BackendRuntime) !void {
        const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
        while (self.pending_work.load(.acquire) and @import("antfly_platform").time.monotonicNs() < deadline) {
            if (runtime.io()) |io| try io.sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expect(!self.pending_work.load(.acquire));
    }
};

test "publication recovery registration uses prepaid dispatch while ordinary parent allocation is exhausted" {
    const resources = @import("../resource_manager.zig");
    const a = std.testing.allocator;
    const limit = 2048 << 20;
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = limit } });
    defer manager.deinit(a);
    var runtime = try background.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded, .memory_parent = manager.memoryParentPort(), .lane_limits = .{ .worker_capacity = 2 } });
    defer runtime.deinit();
    var fixture: ReservedPublicationTest = .{};
    var owner = try Owner.init(runtime.ptr());
    defer owner.deinit();
    owner.watch(fixture.port());
    const pressure = limit - manager.snapshot().memory.used_bytes;
    try std.testing.expect(manager.reserveExternal(pressure));
    defer manager.releaseExternal(pressure);
    try std.testing.expectError(error.OutOfMemory, runtime.ptr().durable_jobs.submit(.{ .owner_id = owner.owner_id, .class = .maintenance, .ptr = &fixture, .run = ReservedPublicationTest.ordinary, .deinit = ReservedPublicationTest.deinitJob }));
    fixture.pending_work.store(true, .release);
    owner.schedule(fixture.port());
    try fixture.waitForClear(runtime.ptr());
    runtime.ptr().durable_jobs.drainOwner(owner.owner_id);
    try std.testing.expectEqual(@as(usize, 1), fixture.bounded_calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), fixture.ordinary_calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), fixture.admission_failures.load(.acquire));
    try std.testing.expectEqual(@as(u32, 0), owner.driver.failure_streak);
}

test "publication recovery registration keeps legacy materialization on ordinary capacity" {
    const resources = @import("../resource_manager.zig");
    const a = std.testing.allocator;
    const limit = 2048 << 20;
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = limit } });
    defer manager.deinit(a);
    var runtime = try background.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded, .memory_parent = manager.memoryParentPort(), .lane_limits = .{ .worker_capacity = 2 } });
    defer runtime.deinit();
    var fixture: ReservedPublicationTest = .{ .needs_maintenance = true };
    var owner = try Owner.init(runtime.ptr());
    defer owner.deinit();
    owner.watch(fixture.port());
    fixture.pending_work.store(true, .release);
    owner.schedule(fixture.port());
    runtime.ptr().durable_jobs.drainOwner(owner.owner_id);
    try std.testing.expect(owner.maintenance_only.load(.acquire));
    try std.testing.expect(fixture.pending_work.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), fixture.bounded_calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), fixture.ordinary_calls.load(.acquire));
    const pressure = limit - manager.snapshot().memory.used_bytes;
    try std.testing.expect(manager.reserveExternal(pressure));
    var pressured = true;
    defer if (pressured) manager.releaseExternal(pressure);
    fixture.now_ns.store(owner.driver.next_attempt_ns.load(.acquire), .release);
    owner.schedule(fixture.port());
    try std.testing.expectEqual(@as(usize, 0), fixture.ordinary_calls.load(.acquire));
    try std.testing.expect(fixture.pending_work.load(.acquire));
    try std.testing.expect(fixture.admission_failures.load(.acquire) > 0);
    manager.releaseExternal(pressure);
    pressured = false;
    fixture.now_ns.store(owner.driver.next_attempt_ns.load(.acquire), .release);
    owner.schedule(fixture.port());
    try fixture.waitForClear(runtime.ptr());
    runtime.ptr().durable_jobs.drainOwner(owner.owner_id);
    try std.testing.expectEqual(@as(usize, 1), fixture.bounded_calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), fixture.ordinary_calls.load(.acquire));
    try std.testing.expect(!owner.maintenance_only.load(.acquire));
    try std.testing.expectEqual(@as(u32, 0), owner.driver.failure_streak);
}
