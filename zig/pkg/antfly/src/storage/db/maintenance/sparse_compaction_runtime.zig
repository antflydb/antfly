// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const apply_rw_lock_mod = @import("../apply_rw_lock.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const platform_clock = @import("antfly_platform").clock;
const background_runtime_mod = @import("../../background_runtime.zig");

pub const Config = struct {
    enabled: bool = builtin.os.tag != .freestanding and !builtin.is_test,
    idle_interval_ms: u64 = 50,
    error_interval_ms: u64 = 250,
    clock: platform_clock.Clock = platform_clock.Clock.real(),
};

pub var test_start_failures_remaining: std.atomic.Value(u32) = .init(0);

pub const SparseCompactionRuntime = if (builtin.os.tag == .freestanding) struct {
    config: Config,

    pub fn init(
        _: Allocator,
        _: *index_manager_mod.IndexManager,
        _: *apply_rw_lock_mod.ApplyRwLock,
        _: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !@This() {
        return .{ .config = config };
    }

    pub fn deinit(self: *@This()) void {
        self.* = undefined;
    }

    pub fn start(self: *@This()) !void {
        if (self.config.enabled) return error.UnsupportedPlatform;
    }

    pub fn stop(_: *@This()) bool {
        return false;
    }

    pub fn pause(_: *@This()) bool {
        return false;
    }

    pub fn resumeAfterPause(_: *@This()) !void {}

    pub fn ensureRunning(_: *@This()) !bool {
        return true;
    }

    pub fn isStarted(_: *const @This()) bool {
        return false;
    }

    pub fn notify(self: *@This()) void {
        _ = self;
    }

    pub fn runOnce(self: *@This()) !bool {
        _ = self;
        return false;
    }
} else struct {
    alloc: Allocator,
    io_impl: ?*Io.Threaded,
    index_manager: *index_manager_mod.IndexManager,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
    config: Config,
    mutex: Io.Mutex = .init,
    cond: Io.Condition = .init,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    desired_running: bool = false,
    paused: bool = false,
    shutdown: bool = false,
    notified: bool = false,
    future: ?Io.Future(void) = null,

    pub fn init(
        alloc: Allocator,
        index_manager: *index_manager_mod.IndexManager,
        apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
        backend_runtime: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !SparseCompactionRuntime {
        const io_impl = backend_runtime.io_impl;
        if (config.enabled and io_impl == null) return error.MissingBackendRuntimeIo;
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .index_manager = index_manager,
            .apply_mutex = apply_mutex,
            .config = config,
        };
    }

    pub fn deinit(self: *SparseCompactionRuntime) void {
        _ = self.stop();
        self.* = undefined;
    }

    pub fn start(self: *SparseCompactionRuntime) !void {
        if (!self.config.enabled) return;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        self.desired_running = true;
        self.paused = false;
        try self.startLocked();
    }

    pub fn stop(self: *SparseCompactionRuntime) bool {
        if (!self.config.enabled) return false;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        self.desired_running = false;
        self.paused = true;
        return self.stopLocked();
    }

    pub fn pause(self: *SparseCompactionRuntime) bool {
        if (!self.config.enabled) return false;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        self.paused = true;
        const desired = self.desired_running;
        _ = self.stopLocked();
        return desired;
    }

    pub fn resumeAfterPause(self: *SparseCompactionRuntime) !void {
        if (!self.config.enabled) return;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        self.paused = false;
        if (self.desired_running) try self.startLocked();
    }

    pub fn ensureRunning(self: *SparseCompactionRuntime) !bool {
        if (!self.config.enabled) return true;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (!self.desired_running) return true;
        if (self.paused) return false;
        try self.startLocked();
        return true;
    }

    pub fn isStarted(self: *SparseCompactionRuntime) bool {
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        return self.future != null;
    }

    fn startLocked(self: *SparseCompactionRuntime) !void {
        if (self.future != null or self.paused or !self.desired_running) return;
        const io_impl = self.io_impl orelse return error.MissingBackendRuntimeIo;
        const io = io_impl.io();
        if (builtin.is_test and consumeTestStartFailure()) return error.TestTransientMaintenanceRestart;
        self.mutex.lockUncancelable(io);
        self.shutdown = false;
        self.notified = true;
        self.mutex.unlock(io);
        self.future = try io.concurrent(workerMain, .{self});
    }

    fn stopLocked(self: *SparseCompactionRuntime) bool {
        const io_impl = self.io_impl orelse return false;
        const io = io_impl.io();
        if (self.future == null) return false;

        self.mutex.lockUncancelable(io);
        self.shutdown = true;
        self.notified = true;
        self.cond.broadcast(io);
        self.mutex.unlock(io);

        _ = self.future.?.await(io);
        self.future = null;
        return true;
    }

    pub fn notify(self: *SparseCompactionRuntime) void {
        if (!self.config.enabled) return;
        const io_impl = self.io_impl orelse return;
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        self.notified = true;
        self.cond.broadcast(io);
        self.mutex.unlock(io);
    }

    pub fn runOnce(self: *SparseCompactionRuntime) !bool {
        var maybe_task: ?index_manager_mod.IndexManager.SparseCompactionTask = null;
        if (!try lockApplyExclusiveCancellable(self)) return false;
        maybe_task = self.index_manager.beginSparseCompactionTask() catch |err| {
            self.apply_mutex.unlockExclusive();
            return err;
        };
        self.apply_mutex.unlockExclusive();

        var task = maybe_task orelse return false;
        const work_alloc = self.index_manager.alloc;
        defer task.deinit(work_alloc);

        var result = index_manager_mod.IndexManager.executeSparseCompactionTask(work_alloc, &task) catch |err| {
            if (builtin.os.tag != .freestanding) {
                std.log.warn("sparse segment compaction failed index={s}: {s}", .{ task.index_name, @errorName(err) });
            }
            return err;
        };
        defer result.deinit(work_alloc);

        // A started task must retire before structural mutation can close its
        // generation. Block std.Io cancellation only across that mandatory
        // reacquire-and-finish section: lock acquisition remains cooperative,
        // and a pending cancellation is re-observed immediately afterward.
        var retirement = try MandatoryApplyRetirement.acquire(self);
        defer retirement.deinit();
        const finish_result = self.index_manager.finishSparseCompactionTask(&task, &result);
        try retirement.releaseAndCheckCancellation();
        _ = try finish_result;
        return true;
    }
};

/// Owns the apply lock and cancellation protection needed to retire a task
/// which has already begun. `deinit` makes every error path safe, while the
/// successful path explicitly observes cancellation only after releasing the
/// apply lock.
const MandatoryApplyRetirement = struct {
    io: Io,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
    previous_cancel_protection: Io.CancelProtection,
    apply_lock_held: bool = true,
    cancellation_protected: bool = true,

    fn acquire(runtime: *SparseCompactionRuntime) !MandatoryApplyRetirement {
        const io_impl = runtime.io_impl orelse return error.MissingBackendRuntimeIo;
        const io = io_impl.io();
        const previous_cancel_protection = io.swapCancelProtection(.blocked);
        errdefer _ = io.swapCancelProtection(previous_cancel_protection);

        const NeverCancelled = struct {
            pub fn isCancelled(_: @This()) bool {
                return false;
            }
        };
        runtime.apply_mutex.lockExclusiveIo(
            io,
            @as(?NeverCancelled, null),
        ) catch |err| switch (err) {
            // Cancellation is blocked above and the explicit token is absent.
            error.Canceled, error.Cancelled => unreachable,
        };
        return .{
            .io = io,
            .apply_mutex = runtime.apply_mutex,
            .previous_cancel_protection = previous_cancel_protection,
        };
    }

    fn deinit(self: *MandatoryApplyRetirement) void {
        if (self.apply_lock_held) {
            self.apply_mutex.unlockExclusive();
            self.apply_lock_held = false;
        }
        if (self.cancellation_protected) {
            _ = self.io.swapCancelProtection(self.previous_cancel_protection);
            self.cancellation_protected = false;
        }
    }

    fn releaseAndCheckCancellation(self: *MandatoryApplyRetirement) Io.Cancelable!void {
        std.debug.assert(self.apply_lock_held);
        std.debug.assert(self.cancellation_protected);
        self.apply_mutex.unlockExclusive();
        self.apply_lock_held = false;
        _ = self.io.swapCancelProtection(self.previous_cancel_protection);
        self.cancellation_protected = false;
        try self.io.checkCancel();
    }
};

fn workerMain(runtime: *SparseCompactionRuntime) void {
    while (true) {
        if (isShutdown(runtime)) return;
        const ran = runtime.runOnce() catch |err| {
            // `std.Io` cancellation is a task-lifetime boundary, not a
            // recoverable compaction failure. Do not consume its one-shot
            // notification and re-enter the worker loop.
            if (err == error.Canceled) return;
            if (builtin.os.tag != .freestanding) {
                std.log.warn("sparse compaction worker failed: {s}", .{@errorName(err)});
            }
            sleepMs(runtime, runtime.config.error_interval_ms);
            continue;
        };
        if (ran) continue;
        waitForWork(runtime);
    }
}

fn waitForWork(runtime: *SparseCompactionRuntime) void {
    var remaining_ms = runtime.config.idle_interval_ms;
    if (remaining_ms == 0) remaining_ms = 1;

    const io_impl = runtime.io_impl orelse return;
    const io = io_impl.io();
    runtime.mutex.lockUncancelable(io);
    if (runtime.notified or runtime.shutdown) {
        runtime.notified = false;
        runtime.mutex.unlock(io);
        return;
    }
    runtime.mutex.unlock(io);

    while (remaining_ms > 0) {
        if (isShutdown(runtime)) return;
        const slice_ms: u64 = @min(remaining_ms, 10);
        runtime.config.clock.sleepMs(slice_ms);
        remaining_ms -= slice_ms;
        runtime.mutex.lockUncancelable(io);
        const notified = runtime.notified;
        runtime.notified = false;
        runtime.mutex.unlock(io);
        if (notified) return;
    }
}

fn sleepMs(runtime: *SparseCompactionRuntime, ms: u64) void {
    var remaining_ms = if (ms == 0) 1 else ms;
    while (remaining_ms > 0) {
        if (isShutdown(runtime)) return;
        const slice_ms: u64 = @min(remaining_ms, 10);
        runtime.config.clock.sleepMs(slice_ms);
        remaining_ms -= slice_ms;
    }
}

fn isShutdown(runtime: *SparseCompactionRuntime) bool {
    const io_impl = runtime.io_impl orelse return runtime.shutdown;
    const io = io_impl.io();
    runtime.mutex.lockUncancelable(io);
    defer runtime.mutex.unlock(io);
    return runtime.shutdown;
}

fn lockApplyExclusiveCancellable(runtime: *SparseCompactionRuntime) !bool {
    const io_impl = runtime.io_impl orelse return false;
    const Cancellation = struct {
        runtime: *SparseCompactionRuntime,

        pub fn isCancelled(self: @This()) bool {
            return isShutdown(self.runtime);
        }
    };
    runtime.apply_mutex.lockExclusiveIo(
        io_impl.io(),
        @as(?Cancellation, .{ .runtime = runtime }),
    ) catch |err| switch (err) {
        // The component stop token makes this optional pass a no-op. Backend
        // task cancellation must propagate to the worker boundary instead of
        // being consumed as an ordinary idle iteration.
        error.Cancelled => return false,
        error.Canceled => return error.Canceled,
    };
    return true;
}

fn lockAtomicWithBackoff(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.Thread.yield() catch {};
}

fn consumeTestStartFailure() bool {
    var remaining = test_start_failures_remaining.load(.acquire);
    while (remaining != 0) {
        if (test_start_failures_remaining.cmpxchgWeak(
            remaining,
            remaining - 1,
            .acq_rel,
            .acquire,
        )) |actual| {
            remaining = actual;
            continue;
        }
        return true;
    }
    return false;
}

test "sparse compaction propagates backend cancellation before task start" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const Waiter = struct {
        fn run(runtime: *SparseCompactionRuntime) Io.Cancelable!bool {
            return lockApplyExclusiveCancellable(runtime);
        }
    };

    var io_impl = Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var apply_lock: apply_rw_lock_mod.ApplyRwLock = .{};
    var runtime: SparseCompactionRuntime = .{
        .alloc = std.testing.allocator,
        .io_impl = &io_impl,
        .index_manager = undefined,
        .apply_mutex = &apply_lock,
        .config = .{ .enabled = true },
    };

    apply_lock.lockShared();
    var shared_held = true;
    var waiter = Io.async(io, Waiter.run, .{&runtime});
    var waiter_active = true;
    defer {
        // Release the blocker first so cleanup cannot hang even if the
        // cancellation assertion above the normal release path fails.
        if (shared_held) apply_lock.unlockShared();
        if (waiter_active) _ = waiter.cancel(io) catch {};
    }
    var waiter_joined = false;
    for (0..5_000) |_| {
        if (apply_lock.exclusive_waiters.load(.acquire) != 0) {
            waiter_joined = true;
            break;
        }
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(waiter_joined);

    const result = waiter.cancel(io);
    waiter_active = false;
    try std.testing.expectError(error.Canceled, result);
    try std.testing.expectEqual(@as(u64, 0), apply_lock.exclusive_waiters.load(.acquire));
    apply_lock.unlockShared();
    shared_held = false;
    try std.testing.expect(apply_lock.tryLockExclusive());
    apply_lock.unlockExclusive();
}

test "sparse compaction defers backend cancellation through mandatory retirement" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const Retirer = struct {
        fn run(runtime: *SparseCompactionRuntime, retired: *std.atomic.Value(bool)) Io.Cancelable!void {
            var retirement = MandatoryApplyRetirement.acquire(runtime) catch unreachable;
            defer retirement.deinit();
            retired.store(true, .release);
            try retirement.releaseAndCheckCancellation();
        }
    };
    const CancelContext = struct {
        future: *Io.Future(Io.Cancelable!void),
        io: Io,
        started: std.atomic.Value(bool) = .init(false),
        cancel_error: ?anyerror = null,

        fn run(ctx: *@This()) void {
            ctx.started.store(true, .release);
            ctx.future.cancel(ctx.io) catch |err| {
                ctx.cancel_error = err;
            };
        }
    };

    var io_impl = Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .limited(1),
    });
    defer io_impl.deinit();
    const io = io_impl.io();
    var apply_lock: apply_rw_lock_mod.ApplyRwLock = .{};
    var runtime: SparseCompactionRuntime = .{
        .alloc = std.testing.allocator,
        .io_impl = &io_impl,
        .index_manager = undefined,
        .apply_mutex = &apply_lock,
        .config = .{ .enabled = true },
    };
    var retired = std.atomic.Value(bool).init(false);

    apply_lock.lockShared();
    var shared_held = true;
    var future = Io.async(io, Retirer.run, .{ &runtime, &retired });
    var future_active = true;
    defer {
        // A retirement waiter has cancellation blocked by design. Always
        // unblock the lock before awaiting it during failure cleanup.
        if (shared_held) apply_lock.unlockShared();
        if (future_active) _ = future.cancel(io) catch {};
    }
    var waiter_joined = false;
    for (0..5_000) |_| {
        if (apply_lock.exclusive_waiters.load(.acquire) != 0) {
            waiter_joined = true;
            break;
        }
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(waiter_joined);

    var cancel_ctx = CancelContext{ .future = &future, .io = io };
    const cancel_thread = try std.Thread.spawn(.{}, CancelContext.run, .{&cancel_ctx});
    var cancel_thread_active = true;
    defer if (cancel_thread_active) {
        if (shared_held) {
            apply_lock.unlockShared();
            shared_held = false;
        }
        cancel_thread.join();
    };
    while (!cancel_ctx.started.load(.acquire)) std.Thread.yield() catch {};
    // `Future.cancel` blocks until the protected task reaches its explicit
    // post-retirement cancellation point. Give the canceling thread time to
    // publish its request before allowing the apply lock to become available.
    try io.sleep(.fromMilliseconds(10), .awake);
    apply_lock.unlockShared();
    shared_held = false;
    cancel_thread.join();
    cancel_thread_active = false;
    future_active = false;

    try std.testing.expect(retired.load(.acquire));
    try std.testing.expectEqual(error.Canceled, cancel_ctx.cancel_error.?);
    try std.testing.expectEqual(@as(u64, 0), apply_lock.exclusive_waiters.load(.acquire));
    try std.testing.expect(apply_lock.tryLockExclusive());
    apply_lock.unlockExclusive();
}
