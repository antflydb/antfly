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
const platform_time = @import("antfly_platform").time;
const AtomicU64 = @import("antfly_platform").atomic.Value(u64);
var portable_activation_retry_jitter_nonce: AtomicU64 = .init(0);
fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) @import("antfly_platform").time.yieldNow();
}
fn monotonicTimeNs() u64 {
    return platform_time.monotonicNs();
}

/// Owner-scoped local activation supervision. DB supplies the fenced activation
/// operation; this owner controls admission, retry cadence, and close barriers.
pub const Owner = struct {
    pub const Port = struct {
        ptr: *anyopaque,
        path: []const u8,
        runtime: *runtime_mod.BackendRuntime,
        owner_id: u64,
        pending: *std.atomic.Value(bool),
        retry: *const fn (*anyopaque) anyerror!bool,
        before_start: ?*const fn (*anyopaque) void = null,
        refuse_launch: ?*const fn (*anyopaque) bool = null,
        now: *const fn (*anyopaque) u64 = systemNow,
    };
    port: Port = undefined,
    bound: bool = false,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    worker_running: std.atomic.Value(bool) = .init(false),
    in_flight: std.atomic.Value(u32) = .init(0),
    stopping: std.atomic.Value(bool) = .init(false),
    jitter_salt: u64 = 0,
    launch_failure_streak: u32 = 0,
    next_launch_ns: u64 = 0,
    activation_failure_streak: u32 = 0,
    next_attempt_ns: u64 = 0,
    pub const portable_activation_retry_base_ns: u64 = 250 * std.time.ns_per_ms;
    pub const portable_activation_retry_max_ns: u64 = 30 * std.time.ns_per_s;
    fn systemNow(_: *anyopaque) u64 {
        return monotonicTimeNs();
    }

    fn armLocked(self: *Owner) void {
        self.port.runtime.armOwnerMaintenanceProbe(self.port.owner_id, .{
            .ptr = self,
            .run = portableActivationRetryMaintenanceProbeMain,
        }) catch |err| {
            std.log.warn("portable activation recovery arm failed path={s} err={s}", .{ self.port.path, @errorName(err) });
        };
    }
    pub fn start(self: *Owner, port: Port) void {
        lockAtomic(&self.lifecycle_mutex);
        if (self.stopping.load(.acquire) or self.in_flight.load(.monotonic) == std.math.maxInt(u32)) {
            self.lifecycle_mutex.unlock();
            return;
        }
        _ = self.in_flight.fetchAdd(1, .acquire);
        self.lifecycle_mutex.unlock();
        defer _ = self.in_flight.fetchSub(1, .release);
        if (port.before_start) |hook| hook(port.ptr);
        _ = lockAtomic(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        // This lifecycle-owned stop flag is permanent once close begins. A
        // probe may already have been claimed by the reaper when close disarms
        // it, so recheck under the same mutex used by stop before launching.
        if (self.stopping.load(.acquire)) {
            port.runtime.disarmOwnerMaintenanceProbe(port.owner_id);
            return;
        }
        // A claimed probe reads this port outside the lifecycle lock. Publish
        // it once before queue admission and keep it immutable through close.
        if (!self.bound) {
            self.port = port;
            self.bound = true;
        } else {
            std.debug.assert(self.port.ptr == port.ptr and self.port.runtime == port.runtime and self.port.owner_id == port.owner_id);
        }
        if (!port.pending.load(.acquire)) {
            self.activation_failure_streak = 0;
            self.next_attempt_ns = 0;
            self.port.runtime.disarmOwnerMaintenanceProbe(self.port.owner_id);
            return;
        }
        if (self.worker_running.load(.acquire)) {
            self.port.runtime.disarmOwnerMaintenanceProbe(self.port.owner_id);
            return;
        }
        const now_ns = self.port.now(self.port.ptr);
        if (now_ns < self.next_launch_ns or now_ns < self.next_attempt_ns) return;
        if (self.jitter_salt == 0) {
            const nonce = portable_activation_retry_jitter_nonce.fetchAdd(1, .monotonic);
            var entropy: [16]u8 = undefined;
            std.mem.writeInt(u64, entropy[0..8], now_ns, .little);
            std.mem.writeInt(u64, entropy[8..16], nonce, .little);
            const path_hash = std.hash.Wyhash.hash(0x5052544143545048, self.port.path);
            self.jitter_salt = std.hash.Wyhash.hash(path_hash, &entropy) | 1;
        }
        // Preserve the initial delay, but do not pin a worker during it. The
        // first admitted step only arms the deadline probe if it is not due.
        if (self.next_attempt_ns == 0)
            self.next_attempt_ns = now_ns +| portableActivationRetryDelayNs(self.port.path, self.jitter_salt, self.activation_failure_streak);
        self.worker_running.store(true, .release);
        _ = self.launchPortableActivationRetryWorkerLocked() catch |err| {
            self.worker_running.store(false, .release);
            self.launch_failure_streak +|= 1;
            const retry_delay_ns = portableActivationRetryDelayNs(
                self.port.path,
                self.jitter_salt,
                self.launch_failure_streak - 1,
            );
            self.next_launch_ns = now_ns +| retry_delay_ns;
            self.armLocked();
            std.log.warn(
                "portable activation retry launch deferred path={s} err={s} failures={d} next_retry_ms={d}",
                .{
                    self.port.path,
                    @errorName(err),
                    self.launch_failure_streak,
                    retry_delay_ns / std.time.ns_per_ms,
                },
            );
            return;
        };
        self.launch_failure_streak = 0;
        self.next_launch_ns = 0;
        self.port.runtime.disarmOwnerMaintenanceProbe(self.port.owner_id);
    }

    pub const PortableActivationRetryLaunch = enum {
        backend_runtime,
    };

    /// Each dispatch makes at most one activation attempt. Retry deadlines live
    /// in the owner, so backoff never occupies a recovery worker.
    pub fn launchPortableActivationRetryWorkerLocked(self: *Owner) !PortableActivationRetryLaunch {
        std.debug.assert(self.worker_running.load(.acquire));
        if (self.port.runtime.durable_jobs.executesInline()) return error.BackgroundRuntimeUnavailable;
        if (self.port.refuse_launch) |hook| {
            if (hook(self.port.ptr)) return error.InjectedPortableActivationRetrySubmitFailure;
        }
        try self.port.runtime.durable_jobs.submit(.{
            .owner_id = self.port.owner_id,
            .class = .recovery,
            .ptr = self,
            .run = portableActivationRetryDurableJobMain,
            .deinit = portableActivationRetryDurableJobDeinit,
        });
        return .backend_runtime;
    }

    fn portableActivationRetryDurableJobMain(ptr: *anyopaque) !void {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        self.portableActivationRetryWorkerMain();
    }

    fn portableActivationRetryDurableJobDeinit(_: *anyopaque) void {}

    fn portableActivationRetryMaintenanceProbeMain(ptr: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        self.start(self.port);
    }

    pub fn stop(self: *Owner, port: Port) void {
        _ = lockAtomic(&self.lifecycle_mutex);
        self.stopping.store(true, .release);
        // Serialize disarm with the step's final rearm; a claimed probe still
        // rechecks stopping under this lock before it can submit anything.
        port.runtime.disarmOwnerMaintenanceProbe(port.owner_id);
        self.lifecycle_mutex.unlock();

        // The owner-scoped worker publishes completion. Wait outside the
        // lifecycle mutex so its final handshake cannot deadlock shutdown.
        while (self.worker_running.load(.acquire) or self.in_flight.load(.acquire) != 0) {
            const io = port.runtime.io() orelse std.Options.debug_io;
            io.sleep(.fromMilliseconds(1), .awake) catch {};
        }

        // Completion is published immediately before the worker releases this
        // mutex. Reacquiring it is the final memory-lifetime barrier.
        _ = lockAtomic(&self.lifecycle_mutex);
        std.debug.assert(!self.worker_running.load(.acquire));
        self.lifecycle_mutex.unlock();
    }

    pub fn stopWithAdmission(self: *Owner, port: Port, original: @import("../../common/read_admission.zig").Port) !void {
        lockAtomic(&self.lifecycle_mutex);
        if (self.bound and (self.port.ptr != port.ptr or self.port.runtime != port.runtime or self.port.owner_id != port.owner_id)) {
            self.lifecycle_mutex.unlock();
            return error.InvalidBackgroundOwner;
        }
        self.stopping.store(true, .release);
        self.lifecycle_mutex.unlock();
        // The dedicated owner counts both claimed probes and the worker's
        // final lifecycle handshake; quiescence never closes backend work.
        try port.runtime.durable_jobs.quiesceOwnerWithAdmission(port.owner_id, original);
        try runtime_mod.waitForDirectAdmissionsWithAdmission(port.runtime, &self.in_flight, original);
    }

    pub fn portableActivationRetryDelayNs(path: []const u8, jitter_salt: u64, failure_streak: u32) u64 {
        const exponent: u6 = @intCast(@min(failure_streak, 7));
        const nominal = @min(portable_activation_retry_base_ns << exponent, portable_activation_retry_max_ns);
        // Stable 80-100% jitter desynchronizes restored replicas without ever
        // exceeding the operator-facing retry cap. Including the DB path keeps
        // independent databases from marching in lockstep after process start.
        var streak_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &streak_bytes, failure_streak, .little);
        const path_hash = std.hash.Wyhash.hash(0x5052544143545259, path);
        const entropy = std.hash.Wyhash.hash(path_hash ^ jitter_salt, &streak_bytes);
        const spread = @max(@as(u64, 1), nominal / 5);
        return nominal - spread + entropy % (spread + 1);
    }

    pub fn portableActivationRetryWorkerMain(self: *Owner) void {
        _ = lockAtomic(&self.lifecycle_mutex);
        const should_attempt = !self.stopping.load(.acquire) and
            self.port.pending.load(.acquire) and self.port.now(self.port.ptr) >= self.next_attempt_ns;
        self.lifecycle_mutex.unlock();

        var failure: ?anyerror = null;
        if (should_attempt) {
            _ = self.port.retry(self.port.ptr) catch |err| result: {
                failure = err;
                break :result false;
            };
        }

        _ = lockAtomic(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        // This lock is the last owner access. stop() acquires it after the
        // false publication, including a stop racing this rearm handshake.
        self.worker_running.store(false, .release);
        if (self.stopping.load(.acquire)) return;
        if (!self.port.pending.load(.acquire)) {
            self.activation_failure_streak = 0;
            self.next_attempt_ns = 0;
            self.next_launch_ns = 0;
            self.port.runtime.disarmOwnerMaintenanceProbe(self.port.owner_id);
            return;
        }
        if (should_attempt) {
            // Single-flight contention is not another activation failure.
            if (failure) |err| {
                if (err != error.PortableRuntimeActivationPending) self.activation_failure_streak +|= 1;
            } else self.activation_failure_streak = 0;
            const delay = portableActivationRetryDelayNs(self.port.path, self.jitter_salt, self.activation_failure_streak);
            self.next_attempt_ns = self.port.now(self.port.ptr) +| delay;
            if (failure) |err| if (err != error.PortableRuntimeActivationPending) {
                std.log.warn("portable runtime activation retry failed path={s} class={s} failures={d} next_retry_ms={d}", .{
                    self.port.path, @errorName(err), self.activation_failure_streak, delay / std.time.ns_per_ms,
                });
            };
        }
        self.armLocked();
    }
};

const TestFixture = struct {
    owner: Owner = .{},
    pending: std.atomic.Value(bool) = .init(true),
    clock: AtomicU64 = .init(1),
    attempts: std.atomic.Value(u32) = .init(0),
    result: enum { fail, contended, success } = .fail,
    fn cast(ptr: *anyopaque) *@This() {
        return @ptrCast(@alignCast(ptr));
    }
    fn now(ptr: *anyopaque) u64 {
        return cast(ptr).clock.load(.acquire);
    }
    fn retry(ptr: *anyopaque) !bool {
        const self = cast(ptr);
        _ = self.attempts.fetchAdd(1, .release);
        switch (self.result) {
            .fail => return error.InjectedActivationFailure,
            .contended => return error.PortableRuntimeActivationPending,
            .success => self.pending.store(false, .release),
        }
        return true;
    }
    fn port(self: *@This(), runtime: *runtime_mod.BackendRuntime, id: u64) Owner.Port {
        return .{ .ptr = self, .path = "finite-recovery-test", .runtime = runtime, .owner_id = id, .pending = &self.pending, .retry = retry, .now = now };
    }
    fn due(self: *@This()) u64 {
        lockAtomic(&self.owner.lifecycle_mutex);
        defer self.owner.lifecycle_mutex.unlock();
        return @max(self.owner.next_attempt_ns, self.owner.next_launch_ns);
    }
    fn streak(self: *@This()) u32 {
        lockAtomic(&self.owner.lifecycle_mutex);
        defer self.owner.lifecycle_mutex.unlock();
        return self.owner.activation_failure_streak;
    }
};

test "portable activation finite recovery waiting owners leave both reserved workers available" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    var runtime = try runtime_mod.BackendRuntimeHandle.init(std.testing.allocator, .{});
    defer runtime.deinit();
    var fixtures: [24]TestFixture = @splat(.{});
    var ids: [fixtures.len]u64 = undefined;
    var count: usize = 0;
    defer for (fixtures[0..count], ids[0..count]) |*fixture, id| {
        fixture.owner.stop(fixture.port(runtime.ptr(), id));
        runtime.ptr().durable_jobs.closeOwner(id);
    };
    for (&fixtures, &ids) |*fixture, *id| {
        id.* = try runtime.ptr().allocOwnerId();
        count += 1;
        fixture.owner.start(fixture.port(runtime.ptr(), id.*));
    }
    // Each initial step must retire without waiting for its future deadline.
    for (&fixtures, ids) |*fixture, id| {
        runtime.ptr().durable_jobs.drainOwner(id);
        try std.testing.expect(!fixture.owner.worker_running.load(.acquire));
        try std.testing.expectEqual(@as(u32, 0), fixture.attempts.load(.acquire));
        try std.testing.expect(fixture.due() > fixture.clock.load(.acquire));
    }
    // Due work for a different owner still executes while all 24 are waiting.
    var ready: TestFixture = .{ .result = .success };
    const ready_id = try runtime.ptr().allocOwnerId();
    const ready_port = ready.port(runtime.ptr(), ready_id);
    defer runtime.ptr().durable_jobs.closeOwner(ready_id);
    defer ready.owner.stop(ready_port);
    ready.owner.start(ready_port);
    runtime.ptr().durable_jobs.drainOwner(ready_id);
    ready.clock.store(ready.due(), .release);
    ready.owner.start(ready_port);
    runtime.ptr().durable_jobs.drainOwner(ready_id);
    try std.testing.expect(!ready.pending.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), ready.attempts.load(.acquire));
}

test "portable activation finite recovery retains backoff and executes under ordinary parent exhaustion" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    var resources = @import("../resource_manager.zig").ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 2048 << 20 } });
    defer resources.deinit(std.testing.allocator);
    var runtime = try runtime_mod.BackendRuntimeHandle.init(std.testing.allocator, .{ .memory_parent = resources.memoryParentPort() });
    defer runtime.deinit();
    var fixture: TestFixture = .{};
    const id = try runtime.ptr().allocOwnerId();
    const port = fixture.port(runtime.ptr(), id);
    defer runtime.ptr().durable_jobs.closeOwner(id);
    defer fixture.owner.stop(port);
    const held = (2048 << 20) - resources.snapshot().memory.used_bytes;
    try std.testing.expect(resources.reserveExternal(held));
    defer resources.releaseExternal(held);
    fixture.owner.start(port);
    runtime.ptr().durable_jobs.drainOwner(id);
    try std.testing.expectEqual(@as(u32, 0), fixture.attempts.load(.acquire));
    // One attempt per dispatch, retaining the failure streak between steps.
    for (1..3) |expected| {
        fixture.clock.store(fixture.due(), .release);
        fixture.owner.start(port);
        runtime.ptr().durable_jobs.drainOwner(id);
        try std.testing.expectEqual(@as(u32, @intCast(expected)), fixture.attempts.load(.acquire));
        try std.testing.expectEqual(@as(u32, @intCast(expected)), fixture.streak());
        const expected_delay = Owner.portableActivationRetryDelayNs(port.path, fixture.owner.jitter_salt, @intCast(expected));
        try std.testing.expectEqual(fixture.clock.load(.acquire) + expected_delay, fixture.due());
        // Repeated early notifications cannot bypass the timer or pin a worker.
        fixture.owner.start(port);
        runtime.ptr().durable_jobs.drainOwner(id);
        try std.testing.expectEqual(@as(u32, @intCast(expected)), fixture.attempts.load(.acquire));
    }
    fixture.result = .contended;
    fixture.clock.store(fixture.due(), .release);
    fixture.owner.start(port);
    runtime.ptr().durable_jobs.drainOwner(id);
    try std.testing.expectEqual(@as(u32, 2), fixture.streak());
    fixture.result = .success;
    fixture.clock.store(fixture.due(), .release);
    fixture.owner.start(port);
    runtime.ptr().durable_jobs.drainOwner(id);
    try std.testing.expect(!fixture.pending.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), fixture.due());
    try std.testing.expectEqual(@as(u32, 0), fixture.streak());
    try std.testing.expectEqual(@as(u32, 4), fixture.attempts.load(.acquire));
}

test "maintenance runtime portable activation stop retains retry and direct-start borrows after interruption" {
    const admission = @import("../../common/read_admission.zig");
    const Case = enum { success, begin_failure, end_failure, lifetime_in_wait };
    for ([_]bool{ false, true }) |direct_start| for (std.enums.values(Case)) |case| {
        var backend = try runtime_mod.BackendRuntimeHandle.init(std.testing.allocator, .{});
        defer backend.deinit();
        const id = try backend.ptr().allocOwnerId();
        const io = backend.ptr().io().?;
        var owner: Owner = .{};
        const F = struct {
            owner: *Owner,
            backend: *runtime_mod.BackendRuntime,
            id: u64,
            io: std.Io,
            direct: bool,
            entered: std.Io.Event = .unset,
            release: std.Io.Event = .unset,
            pending: std.atomic.Value(bool) = .init(true),
            attempts: usize = 0,
            fn retry(raw: *anyopaque) !bool {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.attempts += 1;
                self.entered.set(self.io);
                self.release.waitUncancelable(self.io);
                self.pending.store(false, .release);
                return true;
            }
            fn before(raw: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (!self.direct) return;
                self.entered.set(self.io);
                self.release.waitUncancelable(self.io);
            }
            fn now(_: *anyopaque) u64 {
                return 1;
            }
            fn port(self: *@This()) Owner.Port {
                return .{ .ptr = self, .path = "controlled-activation", .runtime = self.backend, .owner_id = self.id, .pending = &self.pending, .retry = retry, .before_start = before, .now = now };
            }
            fn start(self: *@This()) void {
                self.owner.start(self.port());
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
        var f: F = .{ .owner = &owner, .backend = backend.ptr(), .id = id, .io = io, .direct = direct_start };
        var thread: ?std.Thread = null;
        defer {
            f.release.set(io);
            if (thread) |active| active.join();
            owner.stop(f.port());
            backend.ptr().durable_jobs.closeOwner(id);
        }
        if (direct_start) thread = try std.Thread.spawn(.{}, F.start, .{&f}) else {
            owner.next_attempt_ns = 1;
            f.start();
        }
        try f.entered.wait(io);
        var control: Control = .{ .f = &f, .case = case };
        var payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
        const result = owner.stopWithAdmission(f.port(), .init(&payload));
        if (case == .success) try result else try std.testing.expectError(switch (case) {
            .begin_failure => error.NotFound,
            .end_failure => error.Canceled,
            .lifetime_in_wait => error.DeadlineExceeded,
            .success => unreachable,
        }, result);
        try std.testing.expectError(error.BackgroundOwnerIdExhausted, backend.ptr().durable_jobs.reopenOwner(id));
        f.release.set(io);
        if (thread) |active| active.join();
        thread = null;
        owner.stop(f.port());
        backend.ptr().durable_jobs.drainOwner(id);
        f.start();
        try std.testing.expect(owner.stopping.load(.acquire));
        try std.testing.expect(!owner.worker_running.load(.acquire));
        try std.testing.expectEqual(@as(u32, 0), owner.in_flight.load(.acquire));
        try std.testing.expectEqual(@as(usize, if (direct_start) 0 else 1), f.attempts);
        try std.testing.expectEqual(@as(usize, 1), control.begins);
        try std.testing.expectEqual(@as(usize, if (case == .begin_failure) 0 else 1), control.ends);
    };
}
