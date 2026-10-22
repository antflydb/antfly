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
const runtime_mod = @import("../background_runtime.zig");

/// Bounded local maintenance ordering and cadence. DB retains each operation's
/// data preparation and publication fences; server upload policy is already
/// supplied through its separate dispatcher.
pub const Owner = struct {
    pub const Work = enum { upload, baseline, producer, footprint, repair, column, index, authority };
    pub const Activity = struct {
        artifacts: bool = false,
        producers: bool = false,
        indexes: bool = false,
        columns: bool = false,
        scan_pause_ns: ?u64 = null,
        background_checkpoint: bool = false,
    };
    pub const Port = struct {
        ptr: *anyopaque,
        runtime: *runtime_mod.BackendRuntime,
        now: *const fn (*anyopaque) u64,
        ready: *const fn (*anyopaque) bool,
        run: *const fn (*anyopaque, Work) anyerror!bool,
        failed: *const fn (*anyopaque, Work) void,
        activity: *const fn (*anyopaque) Activity,
    };
    port: Port = undefined,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    future: ?runtime_mod.MaintenanceScheduler.Handle = null,
    stopping: std.atomic.Value(bool) = .init(false),
    stop_started: bool = false,
    joined: std.Io.Event = .unset,
    stop_progress: std.Io.Event = .unset,
    repair_retry_after_ns: u64 = 0,
    pub const idle_poll_ns: u64 = 5 * std.time.ns_per_s;
    pub const active_poll_ns: u64 = 100 * std.time.ns_per_ms;

    fn lock(self: *Owner) void {
        while (!self.lifecycle_mutex.tryLock()) @import("antfly_platform").time.yieldNow();
    }
    pub fn start(self: *Owner, port: Port) void {
        self.lock();
        defer self.lifecycle_mutex.unlock();
        if (self.stopping.load(.acquire) or self.future != null) return;
        const scheduler = port.runtime.maintenanceScheduler() catch |err| {
            std.log.warn("artifact repair scheduler unavailable: {}", .{err});
            return;
        };
        self.port = port;
        self.future = scheduler.register(self, scheduledStep) catch |err| {
            std.log.warn("artifact repair metadata worker spawn failed: {}", .{err});
            return;
        };
    }
    fn shutdown(self: *Owner) @import("maintenance_stop.zig").Borrowed {
        return .{ .mutex = &self.lifecycle_mutex, .registration = &self.future, .stopping = &self.stopping, .finisher = &self.stop_started, .joined = &self.joined, .changed = &self.stop_progress };
    }
    pub fn stop(self: *Owner, runtime: *runtime_mod.BackendRuntime) void {
        self.shutdown().stop(runtime.io(), false);
    }
    pub fn stopWithAdmission(self: *Owner, runtime: *runtime_mod.BackendRuntime, original: @import("../../common/read_admission.zig").Port) !void {
        try self.shutdown().stopWithAdmission(runtime.io(), original);
    }
    fn scheduledStep(self: *Owner) ?u64 {
        return self.step(self.port);
    }
    pub fn step(self: *Owner, port: Port) ?u64 {
        if (self.stopping.load(.acquire)) return null;
        self.run(port);
        if (self.stopping.load(.acquire)) return null;
        return self.delayMs(port.now(port.ptr), port.activity(port.ptr));
    }
    pub fn delayMs(self: *const Owner, now_ns: u64, activity: Activity) u64 {
        const active = (now_ns >= self.repair_retry_after_ns and activity.artifacts) or activity.producers or activity.indexes or activity.columns;
        if (activity.background_checkpoint and activity.scan_pause_ns == null and !active) return 50;
        return std.math.divCeil(u64, activity.scan_pause_ns orelse if (active) active_poll_ns else idle_poll_ns, std.time.ns_per_ms) catch unreachable;
    }
    pub fn run(self: *Owner, port: Port) void {
        if (self.stopping.load(.acquire) or !port.ready(port.ptr)) return;
        // Each check admits one stage. Shutdown joins the already admitted
        // stage through this callback's future, then denies later stages.
        if (self.stopping.load(.acquire)) return;
        _ = port.run(port.ptr, .upload) catch |err| switch (err) {
            error.NotLeader, error.Canceled, error.ResourceLimitExceeded, error.ResourceBudgetExceeded, error.ArtifactCatalogDrift => {},
            else => std.log.warn("artifact upload recovery failed: {s}", .{@errorName(err)}),
        };
        if (self.stopping.load(.acquire)) return;
        _ = port.run(port.ptr, .authority) catch |err| switch (err) {
            error.NotLeader, error.ReadOnly, error.Canceled, error.WriterLocked, error.OnlineSourcePinPending, error.ResourceBudgetExceeded, error.DocumentAuthorityCursorChanged, error.PortableRuntimeActivationPending => {},
            else => std.log.warn("document authority cache migration failed: {s}", .{@errorName(err)}),
        };
        if (self.stopping.load(.acquire)) return;
        _ = port.run(port.ptr, .baseline) catch |err| blk: {
            port.failed(port.ptr, .baseline);
            switch (err) {
                error.OnlineSourcePinPending, error.WriterLocked, error.Canceled, error.ResourceBudgetExceeded, error.ArtifactCatalogDrift => {},
                else => std.log.warn("artifact producer baseline failed: {s}", .{@errorName(err)}),
            }
            break :blk false;
        };
        if (self.stopping.load(.acquire)) return;
        _ = port.run(port.ptr, .producer) catch |err| switch (err) {
            error.NotLeader, error.Canceled, error.ResourceLimitExceeded, error.ResourceBudgetExceeded, error.ArtifactCatalogDrift, error.EnrichmentSourceChanged, error.OnlineSourcePinPending, error.WriterLocked => {},
            else => std.log.warn("artifact producer scheduling failed: {s}", .{@errorName(err)}),
        };
        if (self.stopping.load(.acquire)) return;
        _ = port.run(port.ptr, .footprint) catch |err| blk: {
            // A prepared source pin and writer contention are temporary. Keep
            // the normal idle retry cadence instead of spinning on a fence.
            port.failed(port.ptr, .footprint);
            switch (err) {
                error.OnlineSourcePinPending, error.WriterLocked, error.Canceled, error.ResourceBudgetExceeded => {},
                else => std.log.warn("artifact footprint reconciliation failed: {s}", .{@errorName(err)}),
            }
            break :blk false;
        };
        if (self.stopping.load(.acquire)) return;
        if (port.now(port.ptr) >= self.repair_retry_after_ns) {
            if (self.stopping.load(.acquire)) return;
            _ = port.run(port.ptr, .repair) catch |err| failed: {
                if (err == error.PortableRuntimeActivationPending) return;
                self.repair_retry_after_ns = port.now(port.ptr) +| idle_poll_ns;
                std.log.warn("artifact repair metadata maintenance pass failed: {}", .{err});
                break :failed false;
            };
        }
        // Drain a time/range budget, then yield to other owner work. A
        // backlog uses the active cadence rather than the idle poll.
        if (self.stopping.load(.acquire)) return;
        _ = port.run(port.ptr, .column) catch |err| switch (err) {
            error.Canceled, error.PreparedGenerationChanged, error.ResourceBudgetExceeded => {},
            else => std.log.warn("relational column maintenance failed: {s}", .{@errorName(err)}),
        };
        if (self.stopping.load(.acquire)) return;
        _ = port.run(port.ptr, .index) catch |err| switch (err) {
            error.Canceled, error.PreparedGenerationChanged, error.IntentConflict, error.ResourceBudgetExceeded, error.PortableRuntimeActivationPending, error.IndexNotFound => {},
            else => std.log.warn("relational index maintenance failed: {s}", .{@errorName(err)}),
        };
    }
};

test "independent maintenance cadence honors source pauses and due work" {
    var owner: Owner = .{};
    owner.repair_retry_after_ns = 20;
    try std.testing.expectEqual(@as(u64, 5000), owner.delayMs(10, .{ .artifacts = true }));
    try std.testing.expectEqual(@as(u64, 100), owner.delayMs(20, .{ .artifacts = true }));
    try std.testing.expectEqual(@as(u64, 100), owner.delayMs(10, .{ .producers = true }));
    try std.testing.expectEqual(@as(u64, 50), owner.delayMs(10, .{ .background_checkpoint = true }));
    try std.testing.expectEqual(@as(u64, 2), owner.delayMs(20, .{ .artifacts = true, .scan_pause_ns = std.time.ns_per_ms + 1 }));
}

test "independent maintenance gates all work and yields activation contention" {
    const Fixture = struct {
        admitted: bool = false,
        calls: std.ArrayListUnmanaged(Owner.Work) = .empty,
        fn ready(ptr: *anyopaque) bool {
            return (@as(*@This(), @ptrCast(@alignCast(ptr)))).admitted;
        }
        fn now(_: *anyopaque) u64 {
            return 10;
        }
        fn run(ptr: *anyopaque, work: Owner.Work) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, work);
            if (work == .repair) return error.PortableRuntimeActivationPending;
            return false;
        }
        fn failed(_: *anyopaque, _: Owner.Work) void {}
        fn activity(_: *anyopaque) Owner.Activity {
            return .{};
        }
    };
    var fixture: Fixture = .{};
    defer fixture.calls.deinit(std.testing.allocator);
    var owner: Owner = .{};
    const port: Owner.Port = .{ .ptr = &fixture, .runtime = undefined, .now = Fixture.now, .ready = Fixture.ready, .run = Fixture.run, .failed = Fixture.failed, .activity = Fixture.activity };
    owner.run(port);
    try std.testing.expectEqual(@as(usize, 0), fixture.calls.items.len);
    fixture.admitted = true;
    owner.run(port);
    try std.testing.expectEqualSlices(Owner.Work, &.{ .upload, .authority, .baseline, .producer, .footprint, .repair }, fixture.calls.items);
    try std.testing.expectEqual(@as(u64, 0), owner.repair_retry_after_ns);
}

test "independent maintenance joins callbacks outside its lifecycle lock" {
    const builtin = @import("builtin");
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    var runtime = try runtime_mod.BackendRuntimeHandle.init(std.testing.allocator, .{});
    defer runtime.deinit();
    var owner: Owner = .{};
    defer owner.stop(runtime.ptr());
    const Fixture = struct {
        owner: *Owner,
        entered: std.atomic.Value(bool) = .init(false),
        unlocked: std.atomic.Value(bool) = .init(false),
        work_calls: [7]std.atomic.Value(usize) = @splat(.init(0)),
        fn now(_: *anyopaque) u64 {
            return 0;
        }
        fn ready(_: *anyopaque) bool {
            return true;
        }
        fn run(ptr: *anyopaque, work: Owner.Work) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.work_calls[@backingInt(work)].fetchAdd(1, .acq_rel);
            if (work == .upload) {
                self.entered.store(true, .release);
                while (!self.owner.stopping.load(.acquire)) @import("antfly_platform").time.yieldNow();
                // The stop flag is published while the mutex is held. Allow
                // that critical section to finish, but bound the wait so a
                // join-under-lock regression fails instead of hanging.
                const probe_deadline = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s;
                var unlocked = false;
                while (@import("antfly_platform").time.monotonicNs() < probe_deadline) {
                    if (self.owner.lifecycle_mutex.tryLock()) {
                        self.owner.lifecycle_mutex.unlock();
                        unlocked = true;
                        break;
                    }
                    @import("antfly_platform").time.yieldNow();
                }
                self.unlocked.store(unlocked, .release);
            }
            return false;
        }
        fn failed(_: *anyopaque, _: Owner.Work) void {}
        fn activity(_: *anyopaque) Owner.Activity {
            return .{};
        }
    };
    var fixture: Fixture = .{ .owner = &owner };
    owner.start(.{ .ptr = &fixture, .runtime = runtime.ptr(), .now = Fixture.now, .ready = Fixture.ready, .run = Fixture.run, .failed = Fixture.failed, .activity = Fixture.activity });
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!fixture.entered.load(.acquire) and @import("antfly_platform").time.monotonicNs() < deadline) {
        runtime.ptr().io().?.sleep(.fromMilliseconds(1), .awake) catch {};
    }
    try std.testing.expect(fixture.entered.load(.acquire));
    owner.stop(runtime.ptr());
    try std.testing.expect(fixture.unlocked.load(.acquire));
    try std.testing.expect(owner.future == null);
    try std.testing.expectEqual(@as(usize, 1), fixture.work_calls[0].load(.acquire));
    for (fixture.work_calls[1..]) |*calls| try std.testing.expectEqual(@as(usize, 0), calls.load(.acquire));
}

test "independent maintenance shutdown fences every later stage and preserves current failure cleanup" {
    const Fixture = struct {
        owner: *Owner,
        stop_after: Owner.Work,
        fail_current: bool,
        calls: [std.meta.tags(Owner.Work).len]Owner.Work = undefined,
        count: usize = 0,
        failed_count: usize = 0,
        fn ready(_: *anyopaque) bool {
            return true;
        }
        fn now(_: *anyopaque) u64 {
            return 10;
        }
        fn run(raw: *anyopaque, work: Owner.Work) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls[self.count] = work;
            self.count += 1;
            if (work == self.stop_after) {
                self.owner.stopping.store(true, .release);
                if (self.fail_current) return error.Canceled;
            }
            return false;
        }
        fn failed(raw: *anyopaque, work: Owner.Work) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.owner.stopping.load(.acquire));
            std.debug.assert(work == self.stop_after);
            self.failed_count += 1;
        }
        fn activity(_: *anyopaque) Owner.Activity {
            @panic("shutdown must not start another activity observation");
        }
    };
    const order = [_]Owner.Work{ .upload, .authority, .baseline, .producer, .footprint, .repair, .column, .index };
    for (order, 0..) |work, index| {
        for ([_]bool{ false, true }) |fail_current| {
            if (fail_current and work != .baseline and work != .footprint) continue;
            var owner: Owner = .{};
            var fixture: Fixture = .{ .owner = &owner, .stop_after = work, .fail_current = fail_current };
            const port: Owner.Port = .{ .ptr = &fixture, .runtime = undefined, .now = Fixture.now, .ready = Fixture.ready, .run = Fixture.run, .failed = Fixture.failed, .activity = Fixture.activity };
            try std.testing.expect(owner.step(port) == null);
            try std.testing.expectEqualSlices(Owner.Work, order[0 .. index + 1], fixture.calls[0..fixture.count]);
            try std.testing.expectEqual(@as(usize, @intFromBool(fail_current)), fixture.failed_count);
            // Already stopped direct callers cannot bypass the stage gate.
            owner.run(port);
            try std.testing.expectEqual(index + 1, fixture.count);
        }
    }
}

test "independent maintenance shutdown during readiness denies the first stage" {
    const Fixture = struct {
        owner: *Owner,
        fn ready(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.owner.stopping.store(true, .release);
            return true;
        }
        fn now(_: *anyopaque) u64 {
            @panic("shutdown must not schedule a retry");
        }
        fn run(_: *anyopaque, _: Owner.Work) !bool {
            @panic("shutdown during readiness must deny work");
        }
        fn failed(_: *anyopaque, _: Owner.Work) void {
            @panic("no stage was admitted");
        }
        fn activity(_: *anyopaque) Owner.Activity {
            @panic("shutdown must not observe activity");
        }
    };
    var owner: Owner = .{};
    var fixture: Fixture = .{ .owner = &owner };
    const port: Owner.Port = .{ .ptr = &fixture, .runtime = undefined, .now = Fixture.now, .ready = Fixture.ready, .run = Fixture.run, .failed = Fixture.failed, .activity = Fixture.activity };
    try std.testing.expect(owner.step(port) == null);
}

test "independent maintenance concurrent shutdown callers both join physical callback retirement" {
    const builtin = @import("builtin");
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    var runtime = try runtime_mod.BackendRuntimeHandle.init(std.testing.allocator, .{});
    defer runtime.deinit();
    const io = runtime.ptr().io().?;
    var owner: Owner = .{};
    const Fixture = struct {
        io: std.Io,
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        retired: std.atomic.Value(bool) = .init(false),
        later_stages: std.atomic.Value(usize) = .init(0),
        fn now(_: *anyopaque) u64 {
            return 0;
        }
        fn ready(_: *anyopaque) bool {
            return true;
        }
        fn run(raw: *anyopaque, work: Owner.Work) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (work == .upload) {
                self.entered.set(self.io);
                self.release.waitUncancelable(self.io);
                self.retired.store(true, .release);
            } else _ = self.later_stages.fetchAdd(1, .acq_rel);
            return false;
        }
        fn failed(_: *anyopaque, _: Owner.Work) void {}
        fn activity(_: *anyopaque) Owner.Activity {
            return .{};
        }
    };
    const Stopper = struct {
        owner: *Owner,
        runtime: *runtime_mod.BackendRuntime,
        returned: std.atomic.Value(bool) = .init(false),
        fn stop(self: *@This()) void {
            self.owner.stop(self.runtime);
            self.returned.store(true, .release);
        }
    };
    var fixture: Fixture = .{ .io = io };
    defer owner.stop(runtime.ptr());
    // Always release the callback before joining stoppers, even if a bounded
    // observation fails. A regression must fail rather than strand shutdown.
    owner.start(.{ .ptr = &fixture, .runtime = runtime.ptr(), .now = Fixture.now, .ready = Fixture.ready, .run = Fixture.run, .failed = Fixture.failed, .activity = Fixture.activity });
    const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    defer fixture.release.set(io);
    while (!fixture.entered.isSet() and @import("antfly_platform").time.monotonicNs() < deadline)
        io.sleep(.fromMilliseconds(1), .awake) catch {};
    try std.testing.expect(fixture.entered.isSet());
    var first: Stopper = .{ .owner = &owner, .runtime = runtime.ptr() };
    const first_thread = try std.Thread.spawn(.{}, Stopper.stop, .{&first});
    defer first_thread.join();
    defer fixture.release.set(io);
    while (!owner.stopping.load(.acquire) and @import("antfly_platform").time.monotonicNs() < deadline)
        io.sleep(.fromMilliseconds(1), .awake) catch {};
    try std.testing.expect(owner.stopping.load(.acquire));
    var second: Stopper = .{ .owner = &owner, .runtime = runtime.ptr() };
    const second_thread = try std.Thread.spawn(.{}, Stopper.stop, .{&second});
    defer second_thread.join();
    defer fixture.release.set(io);
    while (@atomicLoad(std.Io.Event, &owner.stop_progress, .acquire) != .waiting and
        !second.returned.load(.acquire) and @import("antfly_platform").time.monotonicNs() < deadline)
        io.sleep(.fromMilliseconds(1), .awake) catch {};
    // The second caller really reached the retirement wait; this is not an
    // inference from an arbitrary sleep while a thread might be unscheduled.
    try std.testing.expectEqual(std.Io.Event.waiting, @atomicLoad(std.Io.Event, &owner.stop_progress, .acquire));
    try std.testing.expect(!first.returned.load(.acquire));
    try std.testing.expect(!second.returned.load(.acquire));
    try std.testing.expect(!fixture.retired.load(.acquire));
    const scheduler = try runtime.ptr().maintenanceScheduler();
    const held = scheduler.snapshot();
    try std.testing.expectEqual(@as(usize, 0), held.registrations);
    try std.testing.expectEqual(@as(usize, 1), held.active);
    fixture.release.set(io);
    owner.joined.waitUncancelable(io);
    try std.testing.expect(fixture.retired.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), fixture.later_stages.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), scheduler.snapshot().active);
    owner.stop(runtime.ptr());
}
