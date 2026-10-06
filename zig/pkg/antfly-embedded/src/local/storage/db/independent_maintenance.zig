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
    pub const Work = enum { upload, baseline, producer, footprint, repair, column, index };
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
    pub fn stop(self: *Owner, runtime: *runtime_mod.BackendRuntime) void {
        self.lock();
        self.stopping.store(true, .release);
        var future = self.future;
        self.future = null;
        self.lifecycle_mutex.unlock();
        // The callback may invoke another local lifecycle operation. Join
        // outside admission so its completion cannot deadlock shutdown.
        if (future) |*task| {
            if (runtime.io()) |io| task.await(io);
        }
        self.lock();
        self.lifecycle_mutex.unlock();
    }
    fn scheduledStep(self: *Owner) ?u64 {
        return self.step(self.port);
    }
    pub fn step(self: *Owner, port: Port) ?u64 {
        if (self.stopping.load(.acquire)) return null;
        self.run(port);
        return self.delayMs(port.now(port.ptr), port.activity(port.ptr));
    }
    pub fn delayMs(self: *const Owner, now_ns: u64, activity: Activity) u64 {
        const active = (now_ns >= self.repair_retry_after_ns and activity.artifacts) or activity.producers or activity.indexes or activity.columns;
        if (activity.background_checkpoint and activity.scan_pause_ns == null and !active) return 50;
        return std.math.divCeil(u64, activity.scan_pause_ns orelse if (active) active_poll_ns else idle_poll_ns, std.time.ns_per_ms) catch unreachable;
    }
    pub fn run(self: *Owner, port: Port) void {
        if (!port.ready(port.ptr)) return;
        _ = port.run(port.ptr, .upload) catch |err| switch (err) {
            error.NotLeader, error.Canceled, error.ResourceLimitExceeded, error.ResourceBudgetExceeded, error.ArtifactCatalogDrift => {},
            else => std.log.warn("artifact upload recovery failed: {s}", .{@errorName(err)}),
        };
        _ = port.run(port.ptr, .baseline) catch |err| blk: {
            port.failed(port.ptr, .baseline);
            switch (err) {
                error.OnlineSourcePinPending, error.WriterLocked, error.Canceled, error.ResourceBudgetExceeded, error.ArtifactCatalogDrift => {},
                else => std.log.warn("artifact producer baseline failed: {s}", .{@errorName(err)}),
            }
            break :blk false;
        };
        _ = port.run(port.ptr, .producer) catch |err| switch (err) {
            error.NotLeader, error.Canceled, error.ResourceLimitExceeded, error.ResourceBudgetExceeded, error.ArtifactCatalogDrift, error.EnrichmentSourceChanged, error.OnlineSourcePinPending, error.WriterLocked => {},
            else => std.log.warn("artifact producer scheduling failed: {s}", .{@errorName(err)}),
        };
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
        if (port.now(port.ptr) >= self.repair_retry_after_ns) {
            _ = port.run(port.ptr, .repair) catch |err| failed: {
                if (err == error.PortableRuntimeActivationPending) return;
                self.repair_retry_after_ns = port.now(port.ptr) +| idle_poll_ns;
                std.log.warn("artifact repair metadata maintenance pass failed: {}", .{err});
                break :failed false;
            };
        }
        // Drain a time/range budget, then yield to other owner work. A
        // backlog uses the active cadence rather than the idle poll.
        _ = port.run(port.ptr, .column) catch |err| switch (err) {
            error.Canceled, error.PreparedGenerationChanged, error.ResourceBudgetExceeded => {},
            else => std.log.warn("relational column maintenance failed: {s}", .{@errorName(err)}),
        };
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
    try std.testing.expectEqualSlices(Owner.Work, &.{ .upload, .baseline, .producer, .footprint, .repair }, fixture.calls.items);
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
        fn now(_: *anyopaque) u64 {
            return 0;
        }
        fn ready(_: *anyopaque) bool {
            return true;
        }
        fn run(ptr: *anyopaque, work: Owner.Work) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
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
}
