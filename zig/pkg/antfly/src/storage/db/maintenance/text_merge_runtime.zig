// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const apply_rw_lock_mod = @import("../apply_rw_lock.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const types = @import("../types.zig");
const platform_clock = @import("antfly_platform").clock;
const background_runtime_mod = @import("../../background_runtime.zig");

pub const Config = struct {
    enabled: bool = builtin.os.tag != .freestanding and !builtin.is_test,
    idle_interval_ms: u64 = 50,
    error_interval_ms: u64 = 250,
    max_pending_segments: u64 = 512,
    max_pending_bytes: u64 = 256 * 1024 * 1024,
    backpressure_merge_steps: usize = 1,
    backpressure_sleep_ms: u64 = 0,
    clock: platform_clock.Clock = platform_clock.Clock.real(),
};

pub var test_block_after_task_begin: std.atomic.Value(bool) = .init(false);
pub var test_task_begin_entered: std.atomic.Value(bool) = .init(false);
pub var test_release_after_task_begin: std.atomic.Value(bool) = .init(false);

pub const TextMergeRuntime = if (builtin.os.tag == .freestanding) struct {
    config: Config,
    defer_flag: ?*const std.atomic.Value(bool),

    pub fn init(
        _: Allocator,
        _: *index_manager_mod.IndexManager,
        _: *apply_rw_lock_mod.ApplyRwLock,
        defer_flag: ?*const std.atomic.Value(bool),
        _: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !@This() {
        return .{
            .config = config,
            .defer_flag = defer_flag,
        };
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

    pub fn notify(self: *@This()) void {
        _ = self;
    }

    pub fn runOnce(self: *@This()) !bool {
        _ = self;
        return false;
    }

    pub fn applyBackpressure(self: *@This()) void {
        _ = self;
    }

    pub fn stats(self: *@This()) types.TextMergeStats {
        return self.statsAssumeApplyLockHeld();
    }

    pub fn statsAssumeApplyLockHeld(self: *@This()) types.TextMergeStats {
        return .{
            .enabled = self.config.enabled,
            .max_pending_segments = self.config.max_pending_segments,
            .max_pending_bytes = self.config.max_pending_bytes,
        };
    }
} else struct {
    alloc: Allocator,
    io_impl: ?*Io.Threaded,
    index_manager: *index_manager_mod.IndexManager,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
    defer_flag: ?*const std.atomic.Value(bool),
    config: Config,
    mutex: Io.Mutex = .init,
    cond: Io.Condition = .init,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    shutdown: bool = false,
    notified: bool = false,
    backpressure_events: u64 = 0,
    backpressure_ns: u64 = 0,
    future: ?Io.Future(void) = null,

    pub fn init(
        alloc: Allocator,
        index_manager: *index_manager_mod.IndexManager,
        apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
        defer_flag: ?*const std.atomic.Value(bool),
        backend_runtime: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !TextMergeRuntime {
        const io_impl = backend_runtime.io_impl;
        if (config.enabled and io_impl == null) return error.MissingBackendRuntimeIo;
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .index_manager = index_manager,
            .apply_mutex = apply_mutex,
            .defer_flag = defer_flag,
            .config = config,
        };
    }

    pub fn deinit(self: *TextMergeRuntime) void {
        _ = self.stop();
        self.* = undefined;
    }

    pub fn start(self: *TextMergeRuntime) !void {
        if (!self.config.enabled) return;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (self.future != null) return;
        const io_impl = self.io_impl orelse return error.MissingBackendRuntimeIo;
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        self.shutdown = false;
        self.notified = true;
        self.mutex.unlock(io);
        self.future = try io.concurrent(workerMain, .{self});
    }

    /// Gracefully drains the active merge, if any, and stops the worker.
    /// Structural catalog mutations call this before moving or closing an
    /// inline index runtime, so no task can retain a pointer across mutation.
    pub fn stop(self: *TextMergeRuntime) bool {
        if (!self.config.enabled) return false;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
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

    pub fn notify(self: *TextMergeRuntime) void {
        if (!self.config.enabled) return;
        const io_impl = self.io_impl orelse return;
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        self.notified = true;
        self.cond.broadcast(io);
        self.mutex.unlock(io);
    }

    pub fn runOnce(self: *TextMergeRuntime) !bool {
        if (self.workDeferred()) return false;
        var maybe_task: ?index_manager_mod.IndexManager.TextMergeTask = null;
        if (!self.apply_mutex.tryLockExclusive()) return false;
        maybe_task = self.index_manager.beginTextMergeTask() catch |err| {
            self.apply_mutex.unlockExclusive();
            return err;
        };
        self.apply_mutex.unlockExclusive();

        var task = maybe_task orelse return false;
        const work_alloc = self.index_manager.alloc;
        defer task.deinit(work_alloc);

        if (builtin.is_test and test_block_after_task_begin.load(.acquire)) {
            test_task_begin_entered.store(true, .release);
            while (!test_release_after_task_begin.load(.acquire)) std.Thread.yield() catch {};
        }

        var result = index_manager_mod.IndexManager.executeTextMergeTask(work_alloc, &task) catch |err| {
            // Once a task has borrowed an index runtime it must retire its
            // scheduler state before a graceful stop can complete. Structural
            // mutation waits for stop before taking the apply lock, so this
            // acquisition cannot form a shutdown lock cycle.
            lockApplyExclusive(self.apply_mutex);
            if (err == error.ResourceBudgetExceeded) {
                self.index_manager.cancelTextMergeTask(&task);
                self.apply_mutex.unlockExclusive();
                return false;
            }
            self.index_manager.noteTextMergeFailure(&task, err);
            self.apply_mutex.unlockExclusive();
            return err;
        };
        defer result.deinit(work_alloc);

        lockApplyExclusive(self.apply_mutex);
        defer self.apply_mutex.unlockExclusive();
        _ = self.index_manager.finishTextMergeTask(&task, &result) catch |err| {
            self.index_manager.noteTextMergeFailure(&task, err);
            return err;
        };
        return true;
    }

    pub fn applyBackpressure(self: *TextMergeRuntime) void {
        if (!self.config.enabled) return;
        if (self.workDeferred()) return;
        if (self.config.max_pending_segments == 0 and self.config.max_pending_bytes == 0) return;
        if (!self.backpressureNeeded()) return;

        const started_ns = self.config.clock.nowRealtimeNs();
        if (self.config.backpressure_sleep_ms > 0) {
            sleepMs(self, self.config.backpressure_sleep_ms);
        }
        const elapsed_ns = self.config.clock.nowRealtimeNs() -| started_ns;
        if (self.io_impl) |io_impl| {
            const io = io_impl.io();
            self.mutex.lockUncancelable(io);
            self.backpressure_events += 1;
            self.backpressure_ns += elapsed_ns;
            self.mutex.unlock(io);
        } else {
            self.backpressure_events += 1;
            self.backpressure_ns += elapsed_ns;
        }
    }

    pub fn stats(self: *TextMergeRuntime) types.TextMergeStats {
        lockApplyShared(self.apply_mutex);
        defer self.apply_mutex.unlockShared();
        return self.statsAssumeApplyLockHeld();
    }

    pub fn statsAssumeApplyLockHeld(self: *TextMergeRuntime) types.TextMergeStats {
        var snapshot = self.index_manager.textMergeStatsSnapshot();

        const backpressure = if (self.io_impl) |io_impl| blk: {
            const io = io_impl.io();
            self.mutex.lockUncancelable(io);
            const events = self.backpressure_events;
            const ns = self.backpressure_ns;
            self.mutex.unlock(io);
            break :blk .{ events, ns };
        } else .{ self.backpressure_events, self.backpressure_ns };
        snapshot.enabled = self.config.enabled;
        snapshot.backpressure_events = backpressure[0];
        snapshot.backpressure_ns = backpressure[1];
        snapshot.max_pending_segments = self.config.max_pending_segments;
        snapshot.max_pending_bytes = self.config.max_pending_bytes;
        return snapshot;
    }

    fn backpressureNeeded(self: *TextMergeRuntime) bool {
        if (self.workDeferred()) return false;
        lockApplyShared(self.apply_mutex);
        const stats_snapshot = self.index_manager.textMergeStatsSnapshot();
        self.apply_mutex.unlockShared();
        return (self.config.max_pending_segments > 0 and stats_snapshot.pending_segments > self.config.max_pending_segments) or
            (self.config.max_pending_bytes > 0 and stats_snapshot.pending_bytes > self.config.max_pending_bytes);
    }

    fn workDeferred(self: *const TextMergeRuntime) bool {
        const flag = self.defer_flag orelse return false;
        return flag.load(.acquire);
    }
};

fn workerMain(runtime: *TextMergeRuntime) void {
    while (true) {
        if (isShutdown(runtime)) return;
        const ran = runtime.runOnce() catch |err| {
            if (err == error.ResourceBudgetExceeded) {
                sleepMs(runtime, runtime.config.error_interval_ms);
                continue;
            }
            if (builtin.os.tag != .freestanding) {
                std.log.err("text merge worker failed: {s}", .{@errorName(err)});
            }
            sleepMs(runtime, runtime.config.error_interval_ms);
            continue;
        };
        if (ran) continue;
        waitForWork(runtime);
    }
}

fn waitForWork(runtime: *TextMergeRuntime) void {
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

fn sleepMs(runtime: *TextMergeRuntime, ms: u64) void {
    var remaining_ms = if (ms == 0) 1 else ms;
    while (remaining_ms > 0) {
        if (isShutdown(runtime)) return;
        const slice_ms: u64 = @min(remaining_ms, 10);
        runtime.config.clock.sleepMs(slice_ms);
        remaining_ms -= slice_ms;
    }
}

fn isShutdown(runtime: *TextMergeRuntime) bool {
    const io_impl = runtime.io_impl orelse return runtime.shutdown;
    const io = io_impl.io();
    runtime.mutex.lockUncancelable(io);
    defer runtime.mutex.unlock(io);
    return runtime.shutdown;
}

fn lockApplyExclusive(lock: *apply_rw_lock_mod.ApplyRwLock) void {
    lock.lockExclusive();
}

fn lockAtomicWithBackoff(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.Thread.yield() catch {};
}

fn lockApplyShared(lock: *apply_rw_lock_mod.ApplyRwLock) void {
    lock.lockShared();
}
