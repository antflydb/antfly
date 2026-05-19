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
const Allocator = std.mem.Allocator;
const replay_source_mod = @import("replay_source.zig");
const derived_types = @import("derived_types.zig");
const backlog_tracker_mod = @import("backlog_tracker.zig");
const resource_manager_mod = @import("../../resource_manager.zig");
const runtime_backend = @import("../../runtime_backend.zig");
const background_runtime_mod = @import("../../background_runtime.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const types = @import("../types.zig");

const ApplyFnType = *const fn (ctx: *anyopaque, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) anyerror!bool;
const PersistFnType = *const fn (ctx: *anyopaque, index_name: []const u8, sequence: u64, force: bool) anyerror!bool;
const TruncateFnType = *const fn (ctx: *anyopaque, sequence: u64) anyerror!void;
const BeginCatchUpFnType = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) anyerror!void;
const FinishCatchUpFnType = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, success: bool) anyerror!void;
const CanAdvanceToTargetFnType = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, from_sequence: u64, target_sequence: u64) anyerror!bool;

const async_runtime_mod = if (builtin.os.tag == .freestanding) struct {
    pub const RuntimeError = error{AsyncWorkerFailed};
    pub const ApplyFn = ApplyFnType;
    pub const PersistFn = PersistFnType;
    pub const TruncateFn = TruncateFnType;
    pub const BeginCatchUpFn = BeginCatchUpFnType;
    pub const FinishCatchUpFn = FinishCatchUpFnType;
    pub const CanAdvanceToTargetFn = CanAdvanceToTargetFnType;

    pub const DerivedRuntime = struct {
        pub fn init(
            alloc: Allocator,
            replay_source: replay_source_mod.Source,
            ctx: *anyopaque,
            apply_fn: ApplyFnType,
            persist_fn: PersistFnType,
            truncate_fn: TruncateFnType,
            begin_catch_up_fn: ?BeginCatchUpFnType,
            finish_catch_up_fn: ?FinishCatchUpFnType,
            can_advance_to_target_fn: ?CanAdvanceToTargetFnType,
            resource_manager: ?*resource_manager_mod.ResourceManager,
        ) @This() {
            _ = alloc;
            _ = replay_source;
            _ = ctx;
            _ = apply_fn;
            _ = persist_fn;
            _ = truncate_fn;
            _ = begin_catch_up_fn;
            _ = finish_catch_up_fn;
            _ = can_advance_to_target_fn;
            _ = resource_manager;
            return .{};
        }

        pub fn deinit(self: *@This()) void {
            self.* = undefined;
        }

        pub fn hasWorkers(_: *@This()) bool {
            return false;
        }

        pub fn failIfUnhealthy(_: *@This()) !void {}

        pub fn addWorker(self: *@This(), name: []const u8, kind: index_manager_mod.ManagedIndexRef, applied_sequence: u64) !void {
            _ = self;
            _ = name;
            _ = kind;
            _ = applied_sequence;
            return error.UnsupportedPlatform;
        }

        pub fn removeWorker(self: *@This(), name: []const u8) void {
            _ = self;
            _ = name;
        }

        pub fn appliedSequence(self: *@This(), name: []const u8) ?u64 {
            _ = self;
            _ = name;
            return null;
        }

        pub fn notifySequence(self: *@This(), sequence: u64) void {
            _ = self;
            _ = sequence;
        }

        pub fn notifyIndexes(self: *@This(), sequence: u64, index_names: []const []const u8) void {
            _ = self;
            _ = sequence;
            _ = index_names;
        }

        pub fn notifyExceptKind(self: *@This(), sequence: u64, excluded_kind: types.IndexKind) void {
            _ = self;
            _ = sequence;
            _ = excluded_kind;
        }

        pub fn forceSequence(self: *@This(), sequence: u64) void {
            _ = self;
            _ = sequence;
        }

        pub fn trackBacklogBytes(self: *@This(), sequence: u64, bytes: u64) !void {
            _ = self;
            _ = sequence;
            _ = bytes;
        }

        pub fn shouldThrottleBacklog(_: *@This()) bool {
            return false;
        }

        pub fn releaseBacklogThrough(self: *@This(), sequence: u64) void {
            _ = self;
            _ = sequence;
        }

        pub fn waitForAll(self: *@This(), sequence: u64) !void {
            _ = self;
            _ = sequence;
            return error.UnsupportedPlatform;
        }

        pub fn waitForIndexes(self: *@This(), sequence: u64, index_names: []const []const u8) !void {
            _ = self;
            _ = sequence;
            _ = index_names;
            return error.UnsupportedPlatform;
        }
    };
} else @import("async_runtime.zig");
const derived_worker = @import("derived_worker.zig");
const io_threaded_runtime_mod = @import("io_threaded_runtime.zig");

pub const ApplyFn = async_runtime_mod.ApplyFn;
pub const PersistFn = async_runtime_mod.PersistFn;
pub const TruncateFn = async_runtime_mod.TruncateFn;
pub const BeginCatchUpFn = async_runtime_mod.BeginCatchUpFn;
pub const FinishCatchUpFn = async_runtime_mod.FinishCatchUpFn;
pub const CanAdvanceToTargetFn = async_runtime_mod.CanAdvanceToTargetFn;
pub const RuntimeError = async_runtime_mod.RuntimeError;

pub const Backend = runtime_backend.Backend;

pub const Config = struct {
    backend: Backend = runtime_backend.defaultExecutorBackend(),
};

pub const Executor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    const VTable = struct {
        deinit: *const fn (ptr: *anyopaque, alloc: Allocator) void,
        has_workers: *const fn (ptr: *anyopaque) bool,
        fail_if_unhealthy: *const fn (ptr: *anyopaque) anyerror!void,
        add_worker: *const fn (ptr: *anyopaque, name: []const u8, kind: index_manager_mod.ManagedIndexRef, applied_sequence: u64) anyerror!void,
        remove_worker: *const fn (ptr: *anyopaque, name: []const u8) void,
        applied_sequence: *const fn (ptr: *anyopaque, name: []const u8) ?u64,
        notify_sequence: *const fn (ptr: *anyopaque, sequence: u64) void,
        notify_indexes: *const fn (ptr: *anyopaque, sequence: u64, index_names: []const []const u8) void,
        notify_except_kind: *const fn (ptr: *anyopaque, sequence: u64, excluded_kind: types.IndexKind) void,
        force_sequence: *const fn (ptr: *anyopaque, sequence: u64) void,
        track_backlog_bytes: *const fn (ptr: *anyopaque, sequence: u64, bytes: u64) anyerror!void,
        should_throttle_backlog: *const fn (ptr: *anyopaque) bool,
        release_backlog_through: *const fn (ptr: *anyopaque, sequence: u64) void,
        wait_for_all: *const fn (ptr: *anyopaque, sequence: u64) anyerror!void,
        wait_for_indexes: *const fn (ptr: *anyopaque, sequence: u64, index_names: []const []const u8) anyerror!void,
    };

    pub fn deinit(self: *Executor, alloc: Allocator) void {
        self.vtable.deinit(self.ptr, alloc);
        self.* = undefined;
    }

    pub fn hasWorkers(self: *Executor) bool {
        return self.vtable.has_workers(self.ptr);
    }

    pub fn failIfUnhealthy(self: *Executor) !void {
        return try self.vtable.fail_if_unhealthy(self.ptr);
    }

    pub fn addWorker(self: *Executor, name: []const u8, kind: index_manager_mod.ManagedIndexRef, applied_sequence: u64) !void {
        return try self.vtable.add_worker(self.ptr, name, kind, applied_sequence);
    }

    pub fn removeWorker(self: *Executor, name: []const u8) void {
        self.vtable.remove_worker(self.ptr, name);
    }

    pub fn appliedSequence(self: *Executor, name: []const u8) ?u64 {
        return self.vtable.applied_sequence(self.ptr, name);
    }

    pub fn notifySequence(self: *Executor, sequence: u64) void {
        self.vtable.notify_sequence(self.ptr, sequence);
    }

    pub fn notifyIndexes(self: *Executor, sequence: u64, index_names: []const []const u8) void {
        self.vtable.notify_indexes(self.ptr, sequence, index_names);
    }

    pub fn notifyExceptKind(self: *Executor, sequence: u64, excluded_kind: types.IndexKind) void {
        self.vtable.notify_except_kind(self.ptr, sequence, excluded_kind);
    }

    pub fn forceSequence(self: *Executor, sequence: u64) void {
        self.vtable.force_sequence(self.ptr, sequence);
    }

    pub fn trackBacklogBytes(self: *Executor, sequence: u64, bytes: u64) !void {
        return try self.vtable.track_backlog_bytes(self.ptr, sequence, bytes);
    }

    pub fn shouldThrottleBacklog(self: *Executor) bool {
        return self.vtable.should_throttle_backlog(self.ptr);
    }

    pub fn releaseBacklogThrough(self: *Executor, sequence: u64) void {
        self.vtable.release_backlog_through(self.ptr, sequence);
    }

    pub fn waitForAll(self: *Executor, sequence: u64) !void {
        return try self.vtable.wait_for_all(self.ptr, sequence);
    }

    pub fn waitForIndexes(self: *Executor, sequence: u64, index_names: []const []const u8) !void {
        return try self.vtable.wait_for_indexes(self.ptr, sequence, index_names);
    }
};

pub fn init(
    alloc: Allocator,
    config: Config,
    replay_source: replay_source_mod.Source,
    ctx: *anyopaque,
    apply_fn: ApplyFn,
    persist_fn: PersistFn,
    truncate_fn: TruncateFn,
    begin_catch_up_fn: ?BeginCatchUpFn,
    finish_catch_up_fn: ?FinishCatchUpFn,
    can_advance_to_target_fn: ?CanAdvanceToTargetFn,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*background_runtime_mod.BackendRuntime,
) !Executor {
    try runtime_backend.ensureExecutorBackendAvailable(config.backend);
    return switch (config.backend) {
        .manual => try initManual(alloc, replay_source, ctx, apply_fn, persist_fn, truncate_fn, begin_catch_up_fn, finish_catch_up_fn, can_advance_to_target_fn, resource_manager),
        .io_threaded => try initIoThreaded(alloc, replay_source, ctx, apply_fn, persist_fn, truncate_fn, begin_catch_up_fn, finish_catch_up_fn, can_advance_to_target_fn, resource_manager, backend_runtime),
    };
}

const ManualWorker = struct {
    name: []u8,
    kind: index_manager_mod.ManagedIndexRef,
    applied_sequence: u64,
    target_sequence: u64,
};

const ManualRuntime = struct {
    alloc: Allocator,
    replay_source: replay_source_mod.Source,
    ctx: *anyopaque,
    apply_fn: ApplyFn,
    persist_fn: PersistFn,
    truncate_fn: TruncateFn,
    begin_catch_up_fn: ?BeginCatchUpFn,
    finish_catch_up_fn: ?FinishCatchUpFn,
    can_advance_to_target_fn: ?CanAdvanceToTargetFn,
    backlog: backlog_tracker_mod.Tracker,
    workers: std.ArrayListUnmanaged(ManualWorker) = .empty,

    fn deinit(self: *ManualRuntime) void {
        for (self.workers.items) |*worker| {
            self.alloc.free(worker.name);
        }
        self.backlog.deinit(self.alloc);
        self.workers.deinit(self.alloc);
        self.* = undefined;
    }

    fn hasWorkers(self: *ManualRuntime) bool {
        return self.workers.items.len > 0;
    }

    fn failIfUnhealthy(_: *ManualRuntime) !void {}

    fn addWorker(self: *ManualRuntime, name: []const u8, kind: index_manager_mod.ManagedIndexRef, applied_sequence: u64) !void {
        try self.workers.append(self.alloc, .{
            .name = try self.alloc.dupe(u8, name),
            .kind = .{
                .name = undefined,
                .kind = kind.kind,
            },
            .applied_sequence = applied_sequence,
            .target_sequence = applied_sequence,
        });
        self.workers.items[self.workers.items.len - 1].kind.name = self.workers.items[self.workers.items.len - 1].name;
    }

    fn removeWorker(self: *ManualRuntime, name: []const u8) void {
        const idx = for (self.workers.items, 0..) |worker, i| {
            if (std.mem.eql(u8, worker.name, name)) break i;
        } else return;
        var worker = self.workers.orderedRemove(idx);
        self.alloc.free(worker.name);
        worker = undefined;
    }

    fn appliedSequence(self: *ManualRuntime, name: []const u8) ?u64 {
        for (self.workers.items) |worker| {
            if (std.mem.eql(u8, worker.name, name)) return worker.applied_sequence;
        }
        return null;
    }

    fn notifySequence(self: *ManualRuntime, sequence: u64) void {
        for (self.workers.items) |*worker| {
            worker.target_sequence = @max(worker.target_sequence, sequence);
        }
    }

    fn notifyIndexes(self: *ManualRuntime, sequence: u64, index_names: []const []const u8) void {
        for (self.workers.items) |*worker| {
            if (!indexNameInList(worker.name, index_names)) continue;
            worker.target_sequence = @max(worker.target_sequence, sequence);
        }
    }

    fn notifyExceptKind(self: *ManualRuntime, sequence: u64, excluded_kind: types.IndexKind) void {
        for (self.workers.items) |*worker| {
            if (worker.kind.kind == excluded_kind) continue;
            worker.target_sequence = @max(worker.target_sequence, sequence);
        }
    }

    fn forceSequence(self: *ManualRuntime, sequence: u64) void {
        self.notifySequence(sequence);
    }

    fn trackBacklogBytes(self: *ManualRuntime, sequence: u64, bytes: u64) !void {
        return try self.backlog.track(self.alloc, sequence, bytes);
    }

    fn shouldThrottleBacklog(self: *ManualRuntime) bool {
        return self.backlog.shouldThrottleWrites();
    }

    fn releaseBacklogThrough(self: *ManualRuntime, sequence: u64) void {
        self.backlog.releaseThrough(sequence);
    }

    fn canAdvanceToTarget(self: *ManualRuntime, worker: *ManualWorker, from_sequence: u64, target_sequence: u64) !bool {
        if (self.can_advance_to_target_fn) |callback| {
            return try callback(self.ctx, worker.kind, from_sequence, target_sequence);
        }
        return true;
    }

    fn catchUpWorker(self: *ManualRuntime, worker: *ManualWorker) !derived_worker.CatchUpStats {
        return try derived_worker.catchUpIndexWithOptions(
            self.alloc,
            self.replay_source,
            worker.kind,
            worker.applied_sequence,
            self.ctx,
            self.apply_fn,
            .{
                .resource_manager = self.backlog.resource_manager,
                .catch_up_ctx = self.ctx,
                .begin_catch_up_fn = self.begin_catch_up_fn,
                .finish_catch_up_fn = self.finish_catch_up_fn,
            },
        );
    }

    fn waitForAll(self: *ManualRuntime, sequence: u64) !void {
        for (self.workers.items) |*worker| {
            worker.target_sequence = @max(worker.target_sequence, sequence);
            if (worker.target_sequence <= worker.applied_sequence) continue;

            const stats = try self.catchUpWorker(worker);
            const caught_up_sequence = if (stats.last_sequence > worker.applied_sequence)
                stats.last_sequence
            else if (stats.last_sequence == 0 and worker.target_sequence > worker.applied_sequence and
                try self.canAdvanceToTarget(worker, worker.applied_sequence, worker.target_sequence))
                worker.target_sequence
            else
                worker.applied_sequence;
            if (caught_up_sequence > worker.applied_sequence) {
                while (!try self.persist_fn(self.ctx, worker.name, caught_up_sequence, true)) {
                    std.atomic.spinLoopHint();
                }
                worker.applied_sequence = caught_up_sequence;
            }
        }

        const min_applied = computeMinApplied(self.workers.items);
        if (min_applied > 0) {
            try self.truncate_fn(self.ctx, min_applied);
            self.backlog.releaseThrough(min_applied);
        }
    }

    fn waitForIndexes(self: *ManualRuntime, sequence: u64, index_names: []const []const u8) !void {
        if (index_names.len == 0) return;
        for (self.workers.items) |*worker| {
            if (!indexNameInList(worker.name, index_names)) continue;
            worker.target_sequence = @max(worker.target_sequence, sequence);
            if (worker.target_sequence <= worker.applied_sequence) continue;

            const stats = try self.catchUpWorker(worker);
            const caught_up_sequence = if (stats.last_sequence > worker.applied_sequence)
                stats.last_sequence
            else if (stats.last_sequence == 0 and worker.target_sequence > worker.applied_sequence and
                try self.canAdvanceToTarget(worker, worker.applied_sequence, worker.target_sequence))
                worker.target_sequence
            else
                worker.applied_sequence;
            if (caught_up_sequence > worker.applied_sequence) {
                while (!try self.persist_fn(self.ctx, worker.name, caught_up_sequence, true)) {
                    std.atomic.spinLoopHint();
                }
                worker.applied_sequence = caught_up_sequence;
            }
        }

        const min_applied = computeMinApplied(self.workers.items);
        if (min_applied > 0) {
            try self.truncate_fn(self.ctx, min_applied);
            self.backlog.releaseThrough(min_applied);
        }
    }
};

fn indexNameInList(name: []const u8, index_names: []const []const u8) bool {
    for (index_names) |candidate| {
        if (std.mem.eql(u8, name, candidate)) return true;
    }
    return false;
}

fn computeMinApplied(workers: []const ManualWorker) u64 {
    if (workers.len == 0) return 0;
    var min_applied: u64 = std.math.maxInt(u64);
    for (workers) |worker| {
        min_applied = @min(min_applied, worker.applied_sequence);
    }
    return min_applied;
}

fn initManual(
    alloc: Allocator,
    replay_source: replay_source_mod.Source,
    ctx: *anyopaque,
    apply_fn: ApplyFn,
    persist_fn: PersistFn,
    truncate_fn: TruncateFn,
    begin_catch_up_fn: ?BeginCatchUpFn,
    finish_catch_up_fn: ?FinishCatchUpFn,
    can_advance_to_target_fn: ?CanAdvanceToTargetFn,
    resource_manager: ?*resource_manager_mod.ResourceManager,
) !Executor {
    const runtime = try alloc.create(ManualRuntime);
    errdefer alloc.destroy(runtime);
    runtime.* = .{
        .alloc = alloc,
        .replay_source = replay_source,
        .ctx = ctx,
        .apply_fn = apply_fn,
        .persist_fn = persist_fn,
        .truncate_fn = truncate_fn,
        .begin_catch_up_fn = begin_catch_up_fn,
        .finish_catch_up_fn = finish_catch_up_fn,
        .can_advance_to_target_fn = can_advance_to_target_fn,
        .backlog = backlog_tracker_mod.Tracker.init(resource_manager),
    };
    return .{
        .ptr = runtime,
        .vtable = &manual_vtable,
    };
}

const manual_vtable = Executor.VTable{
    .deinit = manualDeinit,
    .has_workers = manualHasWorkers,
    .fail_if_unhealthy = manualFailIfUnhealthy,
    .add_worker = manualAddWorker,
    .remove_worker = manualRemoveWorker,
    .applied_sequence = manualAppliedSequence,
    .notify_sequence = manualNotifySequence,
    .notify_indexes = manualNotifyIndexes,
    .notify_except_kind = manualNotifyExceptKind,
    .force_sequence = manualForceSequence,
    .track_backlog_bytes = manualTrackBacklogBytes,
    .should_throttle_backlog = manualShouldThrottleBacklog,
    .release_backlog_through = manualReleaseBacklogThrough,
    .wait_for_all = manualWaitForAll,
    .wait_for_indexes = manualWaitForIndexes,
};

fn manualDeinit(ptr: *anyopaque, alloc: Allocator) void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    runtime.deinit();
    alloc.destroy(runtime);
}

fn manualHasWorkers(ptr: *anyopaque) bool {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return runtime.hasWorkers();
}

fn manualFailIfUnhealthy(ptr: *anyopaque) !void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.failIfUnhealthy();
}

fn manualAddWorker(ptr: *anyopaque, name: []const u8, kind: index_manager_mod.ManagedIndexRef, applied_sequence: u64) !void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.addWorker(name, kind, applied_sequence);
}

fn manualRemoveWorker(ptr: *anyopaque, name: []const u8) void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    runtime.removeWorker(name);
}

fn manualAppliedSequence(ptr: *anyopaque, name: []const u8) ?u64 {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return runtime.appliedSequence(name);
}

fn manualNotifySequence(ptr: *anyopaque, sequence: u64) void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    runtime.notifySequence(sequence);
}

fn manualNotifyIndexes(ptr: *anyopaque, sequence: u64, index_names: []const []const u8) void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    runtime.notifyIndexes(sequence, index_names);
}

fn manualNotifyExceptKind(ptr: *anyopaque, sequence: u64, excluded_kind: types.IndexKind) void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    runtime.notifyExceptKind(sequence, excluded_kind);
}

fn manualForceSequence(ptr: *anyopaque, sequence: u64) void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    runtime.forceSequence(sequence);
}

fn manualTrackBacklogBytes(ptr: *anyopaque, sequence: u64, bytes: u64) !void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.trackBacklogBytes(sequence, bytes);
}

fn manualShouldThrottleBacklog(ptr: *anyopaque) bool {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return runtime.shouldThrottleBacklog();
}

fn manualReleaseBacklogThrough(ptr: *anyopaque, sequence: u64) void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    runtime.releaseBacklogThrough(sequence);
}

fn manualWaitForAll(ptr: *anyopaque, sequence: u64) !void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.waitForAll(sequence);
}

fn manualWaitForIndexes(ptr: *anyopaque, sequence: u64, index_names: []const []const u8) !void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.waitForIndexes(sequence, index_names);
}

fn initIoThreaded(
    alloc: Allocator,
    replay_source: replay_source_mod.Source,
    ctx: *anyopaque,
    apply_fn: ApplyFn,
    persist_fn: PersistFn,
    truncate_fn: TruncateFn,
    begin_catch_up_fn: ?BeginCatchUpFn,
    finish_catch_up_fn: ?FinishCatchUpFn,
    can_advance_to_target_fn: ?CanAdvanceToTargetFn,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*background_runtime_mod.BackendRuntime,
) !Executor {
    if (comptime builtin.os.tag == .freestanding) return error.UnsupportedPlatform;

    const runtime = try alloc.create(io_threaded_runtime_mod.DerivedRuntime);
    errdefer alloc.destroy(runtime);
    if (backend_runtime) |bg| {
        const io_impl = bg.io_impl orelse return error.MissingBackendRuntimeIo;
        runtime.* = io_threaded_runtime_mod.DerivedRuntime.initBorrowed(alloc, io_impl, replay_source, ctx, apply_fn, persist_fn, truncate_fn, begin_catch_up_fn, finish_catch_up_fn, can_advance_to_target_fn, resource_manager);
    } else {
        runtime.* = try io_threaded_runtime_mod.DerivedRuntime.init(alloc, replay_source, ctx, apply_fn, persist_fn, truncate_fn, begin_catch_up_fn, finish_catch_up_fn, can_advance_to_target_fn, resource_manager);
    }
    return .{
        .ptr = runtime,
        .vtable = &io_threaded_vtable,
    };
}

const io_threaded_vtable = Executor.VTable{
    .deinit = ioThreadedDeinit,
    .has_workers = ioThreadedHasWorkers,
    .fail_if_unhealthy = ioThreadedFailIfUnhealthy,
    .add_worker = ioThreadedAddWorker,
    .remove_worker = ioThreadedRemoveWorker,
    .applied_sequence = ioThreadedAppliedSequence,
    .notify_sequence = ioThreadedNotifySequence,
    .notify_indexes = ioThreadedNotifyIndexes,
    .notify_except_kind = ioThreadedNotifyExceptKind,
    .force_sequence = ioThreadedForceSequence,
    .track_backlog_bytes = ioThreadedTrackBacklogBytes,
    .should_throttle_backlog = ioThreadedShouldThrottleBacklog,
    .release_backlog_through = ioThreadedReleaseBacklogThrough,
    .wait_for_all = ioThreadedWaitForAll,
    .wait_for_indexes = ioThreadedWaitForIndexes,
};

fn ioThreadedDeinit(ptr: *anyopaque, alloc: Allocator) void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    runtime.deinit();
    alloc.destroy(runtime);
}

fn ioThreadedHasWorkers(ptr: *anyopaque) bool {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return runtime.hasWorkers();
}

fn ioThreadedFailIfUnhealthy(ptr: *anyopaque) !void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.failIfUnhealthy();
}

fn ioThreadedAddWorker(ptr: *anyopaque, name: []const u8, kind: index_manager_mod.ManagedIndexRef, applied_sequence: u64) !void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.addWorker(name, kind, applied_sequence);
}

fn ioThreadedRemoveWorker(ptr: *anyopaque, name: []const u8) void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    runtime.removeWorker(name);
}

fn ioThreadedAppliedSequence(ptr: *anyopaque, name: []const u8) ?u64 {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return runtime.appliedSequence(name);
}

fn ioThreadedNotifySequence(ptr: *anyopaque, sequence: u64) void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    runtime.notifySequence(sequence);
}

fn ioThreadedNotifyIndexes(ptr: *anyopaque, sequence: u64, index_names: []const []const u8) void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    runtime.notifyIndexes(sequence, index_names);
}

fn ioThreadedNotifyExceptKind(ptr: *anyopaque, sequence: u64, excluded_kind: types.IndexKind) void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    runtime.notifyExceptKind(sequence, excluded_kind);
}

fn ioThreadedForceSequence(ptr: *anyopaque, sequence: u64) void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    runtime.forceSequence(sequence);
}

fn ioThreadedTrackBacklogBytes(ptr: *anyopaque, sequence: u64, bytes: u64) !void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.trackBacklogBytes(sequence, bytes);
}

fn ioThreadedShouldThrottleBacklog(ptr: *anyopaque) bool {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return runtime.shouldThrottleBacklog();
}

fn ioThreadedReleaseBacklogThrough(ptr: *anyopaque, sequence: u64) void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    runtime.releaseBacklogThrough(sequence);
}

fn ioThreadedWaitForAll(ptr: *anyopaque, sequence: u64) !void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.waitForAll(sequence);
}

fn ioThreadedWaitForIndexes(ptr: *anyopaque, sequence: u64, index_names: []const []const u8) !void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.waitForIndexes(sequence, index_names);
}
