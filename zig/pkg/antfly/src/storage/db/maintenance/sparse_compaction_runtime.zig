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
const platform_clock = @import("../../../platform/clock.zig");
const background_runtime_mod = @import("../../background_runtime.zig");

pub const Config = struct {
    enabled: bool = builtin.os.tag != .freestanding and !builtin.is_test,
    idle_interval_ms: u64 = 50,
    error_interval_ms: u64 = 250,
    clock: platform_clock.Clock = platform_clock.Clock.real(),
};

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
        if (self.io_impl) |io_impl| {
            const io = io_impl.io();
            self.mutex.lockUncancelable(io);
            self.shutdown = true;
            self.notified = true;
            self.cond.broadcast(io);
            self.mutex.unlock(io);

            if (self.future) |*future| _ = future.await(io);
        }
        self.future = null;
        self.* = undefined;
    }

    pub fn start(self: *SparseCompactionRuntime) !void {
        if (!self.config.enabled) return;
        const io_impl = self.io_impl orelse return error.MissingBackendRuntimeIo;
        self.future = try io_impl.io().concurrent(workerMain, .{self});
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
        if (!lockApplyExclusiveBackoff(self)) return false;
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

        if (!lockApplyExclusiveBackoff(self)) return false;
        defer self.apply_mutex.unlockExclusive();
        _ = try self.index_manager.finishSparseCompactionTask(&task, &result);
        return true;
    }
};

fn workerMain(runtime: *SparseCompactionRuntime) void {
    while (true) {
        if (isShutdown(runtime)) return;
        const ran = runtime.runOnce() catch |err| {
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

fn lockApplyExclusiveBackoff(runtime: *SparseCompactionRuntime) bool {
    while (!runtime.apply_mutex.tryLockExclusive()) {
        if (isShutdown(runtime)) return false;
        sleepMs(runtime, 1);
    }
    return true;
}
