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
const Allocator = std.mem.Allocator;
const replay_source_mod = @import("replay_source.zig");
const change_journal_mod = @import("change_journal.zig");
const derived_types = @import("derived_types.zig");
const derived_worker = @import("derived_worker.zig");
const catch_up_policy = @import("catch_up_policy.zig");
const backlog_tracker_mod = @import("backlog_tracker.zig");
const resource_manager_mod = @import("../../resource_manager.zig");
const mem_backend_mod = @import("../../mem_backend.zig");
const docstore_mod = @import("../../docstore.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const types = @import("../types.zig");

pub const RuntimeError = error{AsyncWorkerFailed};

pub const ApplyFn = *const fn (ctx: *anyopaque, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) anyerror!bool;
pub const PersistFn = *const fn (ctx: *anyopaque, index_name: []const u8, sequence: u64, force: bool) anyerror!bool;
pub const TruncateFn = *const fn (ctx: *anyopaque, sequence: u64) anyerror!void;
pub const BeginCatchUpFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) anyerror!void;
pub const FinishCatchUpFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, success: bool) anyerror!void;
pub const CanAdvanceToTargetFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, from_sequence: u64, target_sequence: u64) anyerror!bool;

const Worker = struct {
    runtime: *DerivedRuntime,
    name: []u8,
    kind: index_manager_mod.ManagedIndexRef,
    applied_sequence: u64,
    persisted_sequence: u64,
    target_sequence: u64,
    stop: bool = false,
    thread: ?std.Thread = null,
    catch_up_open: bool = false,
    replay_cursor: ?replay_source_mod.MatchingCursor = null,
    replay_cursor_open_sequence: u64 = 0,
};

fn forcePersistAppliedSequence(worker: *const Worker) bool {
    return catch_up_policy.forIndex(worker.kind, worker.runtime.backlog.resource_manager).force_persist_applied_sequence;
}

fn canAdvanceToTarget(runtime: *DerivedRuntime, worker: *Worker, from_sequence: u64, target_sequence: u64) !bool {
    if (runtime.can_advance_to_target_fn) |callback| {
        return try callback(runtime.ctx, worker.kind, from_sequence, target_sequence);
    }
    return true;
}

fn indexNameInList(name: []const u8, index_names: []const []const u8) bool {
    for (index_names) |candidate| {
        if (std.mem.eql(u8, name, candidate)) return true;
    }
    return false;
}

pub const DerivedRuntime = struct {
    alloc: Allocator,
    replay_source: replay_source_mod.Source,
    ctx: *anyopaque,
    apply_fn: ApplyFn,
    persist_fn: PersistFn,
    truncate_fn: TruncateFn,
    begin_catch_up_fn: ?BeginCatchUpFn,
    finish_catch_up_fn: ?FinishCatchUpFn,
    can_advance_to_target_fn: ?CanAdvanceToTargetFn,
    mutex: std.atomic.Mutex = .unlocked,
    workers: std.ArrayListUnmanaged(*Worker) = .empty,
    shutdown: bool = false,
    last_error_name: ?[]const u8 = null,
    last_truncated_sequence: u64 = 0,
    force_catch_up_sequence: u64 = 0,
    last_notified_sequence: u64 = 0,
    truncates_in_flight: usize = 0,
    backlog: backlog_tracker_mod.Tracker,

    pub fn init(
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
    ) DerivedRuntime {
        return .{
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
    }

    pub fn deinit(self: *DerivedRuntime) void {
        lock(self);
        self.shutdown = true;
        for (self.workers.items) |worker| worker.stop = true;
        self.mutex.unlock();

        for (self.workers.items) |worker| {
            if (worker.thread) |thread| thread.join();
            closeWorkerCatchUpState(self, worker, true) catch |err| {
                std.log.warn("derived worker final catch-up close failed worker={s}: {s}", .{ worker.name, @errorName(err) });
            };
            if (worker.applied_sequence > worker.persisted_sequence) {
                _ = self.persist_fn(self.ctx, worker.name, worker.applied_sequence, true) catch |err| failed: {
                    std.log.warn("derived worker final applied-sequence persist failed worker={s}: {s}", .{ worker.name, @errorName(err) });
                    break :failed false;
                };
            }
            self.alloc.free(worker.name);
            self.alloc.destroy(worker);
        }
        self.backlog.deinit(self.alloc);
        self.workers.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn hasWorkers(self: *DerivedRuntime) bool {
        lock(self);
        defer self.mutex.unlock();
        return self.workers.items.len > 0;
    }

    pub fn failIfUnhealthy(self: *DerivedRuntime) !void {
        lock(self);
        defer self.mutex.unlock();
        if (self.last_error_name != null) return RuntimeError.AsyncWorkerFailed;
    }

    pub fn addWorker(self: *DerivedRuntime, name: []const u8, kind: index_manager_mod.ManagedIndexRef, applied_sequence: u64) !void {
        var worker = try self.alloc.create(Worker);
        errdefer self.alloc.destroy(worker);
        worker.* = .{
            .runtime = self,
            .name = try self.alloc.dupe(u8, name),
            .kind = .{
                .name = undefined,
                .kind = kind.kind,
            },
            .applied_sequence = applied_sequence,
            .persisted_sequence = applied_sequence,
            .target_sequence = applied_sequence,
        };
        worker.kind.name = worker.name;

        lock(self);
        worker.target_sequence = @max(worker.target_sequence, self.last_notified_sequence);
        try self.workers.append(self.alloc, worker);
        self.mutex.unlock();
        errdefer {
            lock(self);
            const idx = for (self.workers.items, 0..) |candidate, i| {
                if (candidate == worker) break i;
            } else unreachable;
            _ = self.workers.orderedRemove(idx);
            self.mutex.unlock();
            self.alloc.free(worker.name);
        }

        worker.thread = try std.Thread.spawn(.{}, workerMain, .{worker});
        errdefer if (worker.thread) |thread| thread.join();
    }

    pub fn removeWorker(self: *DerivedRuntime, name: []const u8) void {
        lock(self);
        const idx = for (self.workers.items, 0..) |worker, i| {
            if (std.mem.eql(u8, worker.name, name)) break i;
        } else {
            self.mutex.unlock();
            return;
        };
        const worker = self.workers.orderedRemove(idx);
        worker.stop = true;
        self.mutex.unlock();

        if (worker.thread) |thread| thread.join();
        closeWorkerCatchUpState(self, worker, true) catch |err| {
            std.log.warn("derived worker final catch-up close failed worker={s}: {s}", .{ worker.name, @errorName(err) });
        };
        self.alloc.free(worker.name);
        self.alloc.destroy(worker);
    }

    pub fn appliedSequence(self: *DerivedRuntime, name: []const u8) ?u64 {
        lock(self);
        defer self.mutex.unlock();
        for (self.workers.items) |worker| {
            if (std.mem.eql(u8, worker.name, name)) return worker.applied_sequence;
        }
        return null;
    }

    pub fn notifySequence(self: *DerivedRuntime, sequence: u64) void {
        lock(self);
        defer self.mutex.unlock();
        self.last_notified_sequence = @max(self.last_notified_sequence, sequence);
        for (self.workers.items) |worker| {
            worker.target_sequence = @max(worker.target_sequence, sequence);
        }
    }

    pub fn notifyIndexes(self: *DerivedRuntime, sequence: u64, index_names: []const []const u8) void {
        if (index_names.len == 0) return;
        lock(self);
        defer self.mutex.unlock();
        for (self.workers.items) |worker| {
            if (!indexNameInList(worker.name, index_names)) continue;
            worker.target_sequence = @max(worker.target_sequence, sequence);
        }
    }

    pub fn notifyExceptKind(self: *DerivedRuntime, sequence: u64, excluded_kind: types.IndexKind) void {
        lock(self);
        defer self.mutex.unlock();
        self.last_notified_sequence = @max(self.last_notified_sequence, sequence);
        for (self.workers.items) |worker| {
            if (worker.kind.kind == excluded_kind) continue;
            worker.target_sequence = @max(worker.target_sequence, sequence);
        }
    }

    pub fn forceSequence(self: *DerivedRuntime, sequence: u64) void {
        lock(self);
        defer self.mutex.unlock();
        self.last_notified_sequence = @max(self.last_notified_sequence, sequence);
        self.force_catch_up_sequence = @max(self.force_catch_up_sequence, sequence);
        for (self.workers.items) |worker| {
            worker.target_sequence = @max(worker.target_sequence, sequence);
        }
    }

    pub fn trackBacklogBytes(self: *DerivedRuntime, sequence: u64, bytes: u64) !void {
        lock(self);
        defer self.mutex.unlock();
        return try self.backlog.track(self.alloc, sequence, bytes);
    }

    pub fn shouldThrottleBacklog(self: *DerivedRuntime) bool {
        lock(self);
        defer self.mutex.unlock();
        return self.backlog.shouldThrottleWrites();
    }

    pub fn releaseBacklogThrough(self: *DerivedRuntime, sequence: u64) void {
        lock(self);
        defer self.mutex.unlock();
        self.backlog.releaseThrough(sequence);
    }

    pub fn waitForAll(self: *DerivedRuntime, sequence: u64) !void {
        while (true) {
            lock(self);
            self.force_catch_up_sequence = @max(self.force_catch_up_sequence, sequence);
            for (self.workers.items) |worker| {
                worker.target_sequence = @max(worker.target_sequence, sequence);
            }
            if (self.last_error_name != null) {
                self.mutex.unlock();
                return RuntimeError.AsyncWorkerFailed;
            }

            var all_applied = true;
            for (self.workers.items) |worker| {
                if (worker.applied_sequence < sequence) {
                    all_applied = false;
                    break;
                }
            }
            if (all_applied and self.truncates_in_flight == 0) {
                self.mutex.unlock();
                for (self.workers.items) |worker| {
                    try closeWorkerCatchUpState(self, worker, true);
                }
                lock(self);
                var all_persisted = true;
                for (self.workers.items) |worker| {
                    if (worker.applied_sequence == 0) continue;
                    if (try self.persist_fn(self.ctx, worker.name, worker.applied_sequence, true)) {
                        worker.persisted_sequence = @max(worker.persisted_sequence, worker.applied_sequence);
                    } else {
                        all_persisted = false;
                    }
                }
                if (!all_persisted) {
                    self.mutex.unlock();
                    std.Thread.yield() catch {};
                    continue;
                }
                const truncate_sequence = truncate: {
                    const min_persisted = self.computeMinPersistedLocked();
                    if (min_persisted > self.last_truncated_sequence) {
                        self.last_truncated_sequence = min_persisted;
                        break :truncate min_persisted;
                    }
                    break :truncate 0;
                };
                self.mutex.unlock();
                if (truncate_sequence > 0) {
                    try self.truncate_fn(self.ctx, truncate_sequence);
                    lock(self);
                    self.backlog.releaseThrough(truncate_sequence);
                    self.mutex.unlock();
                }
                return;
            }
            self.mutex.unlock();
            std.Thread.yield() catch {};
        }
    }

    pub fn waitForIndexes(self: *DerivedRuntime, sequence: u64, index_names: []const []const u8) !void {
        if (index_names.len == 0) return;
        while (true) {
            lock(self);
            for (self.workers.items) |worker| {
                if (!indexNameInList(worker.name, index_names)) continue;
                worker.target_sequence = @max(worker.target_sequence, sequence);
            }
            if (self.last_error_name != null) {
                self.mutex.unlock();
                return RuntimeError.AsyncWorkerFailed;
            }

            var all_applied = true;
            for (self.workers.items) |worker| {
                if (!indexNameInList(worker.name, index_names)) continue;
                if (worker.applied_sequence < sequence) {
                    all_applied = false;
                    break;
                }
            }
            if (all_applied and self.truncates_in_flight == 0) {
                self.mutex.unlock();
                for (self.workers.items) |worker| {
                    if (!indexNameInList(worker.name, index_names)) continue;
                    try closeWorkerCatchUpState(self, worker, true);
                }
                lock(self);
                var all_persisted = true;
                for (self.workers.items) |worker| {
                    if (!indexNameInList(worker.name, index_names)) continue;
                    if (worker.applied_sequence == 0) continue;
                    if (try self.persist_fn(self.ctx, worker.name, worker.applied_sequence, true)) {
                        worker.persisted_sequence = @max(worker.persisted_sequence, worker.applied_sequence);
                    } else {
                        all_persisted = false;
                    }
                }
                if (!all_persisted) {
                    self.mutex.unlock();
                    std.Thread.yield() catch {};
                    continue;
                }
                const truncate_sequence = truncate: {
                    const min_persisted = self.computeMinPersistedLocked();
                    if (min_persisted > self.last_truncated_sequence) {
                        self.last_truncated_sequence = min_persisted;
                        break :truncate min_persisted;
                    }
                    break :truncate 0;
                };
                self.mutex.unlock();
                if (truncate_sequence > 0) {
                    try self.truncate_fn(self.ctx, truncate_sequence);
                    lock(self);
                    self.backlog.releaseThrough(truncate_sequence);
                    self.mutex.unlock();
                }
                return;
            }
            self.mutex.unlock();
            std.Thread.yield() catch {};
        }
    }

    fn recordError(self: *DerivedRuntime, err: anyerror) void {
        std.log.err("derived worker failed: {s}", .{@errorName(err)});
        lock(self);
        defer self.mutex.unlock();
        if (self.last_error_name == null) self.last_error_name = @errorName(err);
    }

    fn computeMinPersistedLocked(self: *DerivedRuntime) u64 {
        if (self.workers.items.len == 0) return 0;
        var min_persisted: u64 = std.math.maxInt(u64);
        for (self.workers.items) |worker| {
            min_persisted = @min(min_persisted, worker.persisted_sequence);
        }
        return min_persisted;
    }
};

fn workerMain(worker: *Worker) void {
    const runtime = worker.runtime;
    var close_success = true;
    defer closeWorkerCatchUpState(runtime, worker, close_success) catch |err| runtime.recordError(err);

    while (true) {
        lock(runtime);
        if (runtime.shutdown or worker.stop or runtime.last_error_name != null) {
            runtime.mutex.unlock();
            return;
        }
        const from_sequence = worker.applied_sequence;
        const target_sequence = worker.target_sequence;
        runtime.mutex.unlock();
        if (target_sequence <= from_sequence) {
            if (waitForCatchUpSessionReuse(runtime, worker, from_sequence)) continue;
            closeWorkerCatchUpState(runtime, worker, true) catch |err| {
                close_success = false;
                runtime.recordError(err);
                return;
            };
            std.Thread.yield() catch {};
            continue;
        }

        ensureWorkerCatchUpState(runtime, worker, from_sequence) catch |err| {
            close_success = false;
            runtime.recordError(err);
            return;
        };
        waitForReplayWindow(runtime, worker, from_sequence);

        var stats = catchUpWorker(runtime, worker) catch |err| {
            if (isRecoverableCatchUpError(worker, err)) {
                closeWorkerCatchUpState(runtime, worker, false) catch |close_err| {
                    close_success = false;
                    runtime.recordError(close_err);
                    return;
                };
                std.Thread.yield() catch {};
                continue;
            }
            close_success = false;
            runtime.recordError(err);
            return;
        };
        if (stats.last_sequence == 0 and target_sequence > from_sequence) {
            if (worker.replay_cursor != null and worker.replay_cursor.?.canFollowTail()) {
                const target_visible = runtime.replay_source.isSequenceVisible(target_sequence) catch |err| {
                    close_success = false;
                    runtime.recordError(err);
                    return;
                };
                if (!target_visible) {
                    std.Thread.yield() catch {};
                    continue;
                }
            } else if (worker.replay_cursor != null) {
                closeWorkerReplayCursor(runtime, worker);
                ensureWorkerCatchUpState(runtime, worker, from_sequence) catch |err| {
                    close_success = false;
                    runtime.recordError(err);
                    return;
                };
                stats = catchUpWorker(runtime, worker) catch |err| {
                    if (isRecoverableCatchUpError(worker, err)) {
                        closeWorkerCatchUpState(runtime, worker, false) catch |close_err| {
                            close_success = false;
                            runtime.recordError(close_err);
                            return;
                        };
                        std.Thread.yield() catch {};
                        continue;
                    }
                    close_success = false;
                    runtime.recordError(err);
                    return;
                };
            }
        } else if (stats.last_sequence == 0 and !worker.replay_cursor.?.canFollowTail()) {
            closeWorkerReplayCursor(runtime, worker);
        }

        const target_advance_allowed = if (stats.last_sequence == 0 and target_sequence > from_sequence)
            canAdvanceToTarget(runtime, worker, from_sequence, target_sequence) catch |err| {
                close_success = false;
                runtime.recordError(err);
                return;
            }
        else
            false;
        const caught_up_sequence = if (stats.last_sequence > from_sequence)
            stats.last_sequence
        else if (target_advance_allowed)
            target_sequence
        else
            from_sequence;
        if (caught_up_sequence == from_sequence and stats.last_sequence == 0 and target_sequence > from_sequence) {
            closeWorkerCatchUpState(runtime, worker, false) catch |err| {
                close_success = false;
                runtime.recordError(err);
                return;
            };
            sleepNs(50 * std.time.ns_per_ms);
            continue;
        }

        if (caught_up_sequence > from_sequence) {
            closeWorkerCatchUpState(runtime, worker, true) catch |err| {
                if (isRecoverablePublishError(worker, err)) {
                    std.Thread.yield() catch {};
                    continue;
                }
                close_success = false;
                runtime.recordError(err);
                return;
            };
        }

        var persisted = false;
        if (caught_up_sequence > from_sequence) {
            persisted = runtime.persist_fn(runtime.ctx, worker.name, caught_up_sequence, forcePersistAppliedSequence(worker)) catch |err| {
                if (err == error.WriterLocked) {
                    std.Thread.yield() catch {};
                    continue;
                }
                runtime.recordError(err);
                return;
            };
        }

        var truncate_sequence: u64 = 0;
        lock(runtime);
        if (caught_up_sequence > worker.applied_sequence) {
            worker.applied_sequence = caught_up_sequence;
        }
        if (persisted and caught_up_sequence > worker.persisted_sequence) {
            worker.persisted_sequence = caught_up_sequence;
        }
        if (worker.persisted_sequence > runtime.last_truncated_sequence) {
            const min_persisted = runtime.computeMinPersistedLocked();
            if (min_persisted > runtime.last_truncated_sequence) {
                runtime.last_truncated_sequence = min_persisted;
                truncate_sequence = min_persisted;
            }
        }
        if (truncate_sequence > 0) {
            runtime.truncates_in_flight += 1;
        }
        runtime.mutex.unlock();

        if (shouldRefreshReplayCursor(worker, caught_up_sequence)) {
            closeWorkerReplayCursor(runtime, worker);
        }

        if (truncate_sequence > 0) {
            runtime.truncate_fn(runtime.ctx, truncate_sequence) catch |err| {
                if (err == error.WriterLocked) {
                    lock(runtime);
                    runtime.truncates_in_flight -= 1;
                    runtime.mutex.unlock();
                    std.Thread.yield() catch {};
                    continue;
                }
                lock(runtime);
                runtime.truncates_in_flight -= 1;
                runtime.mutex.unlock();
                runtime.recordError(err);
                return;
            };
            lock(runtime);
            runtime.backlog.releaseThrough(truncate_sequence);
            runtime.truncates_in_flight -= 1;
            runtime.mutex.unlock();
        }
    }
}

fn ensureWorkerCatchUpState(runtime: *DerivedRuntime, worker: *Worker, from_sequence: u64) !void {
    if (!worker.catch_up_open) {
        if (runtime.begin_catch_up_fn) |begin_catch_up| try begin_catch_up(runtime.ctx, worker.kind);
        worker.catch_up_open = true;
    }
    if (worker.replay_cursor == null) {
        worker.replay_cursor = try runtime.replay_source.openMatchingCursor(
            runtime.alloc,
            from_sequence,
            derived_worker.targetHintForManagedIndex(worker.kind),
        );
        worker.replay_cursor_open_sequence = from_sequence;
    }
}

fn closeWorkerReplayCursor(runtime: *DerivedRuntime, worker: *Worker) void {
    if (worker.replay_cursor) |*cursor| {
        cursor.deinit(runtime.alloc);
        worker.replay_cursor = null;
        worker.replay_cursor_open_sequence = 0;
    }
}

fn closeWorkerCatchUpState(runtime: *DerivedRuntime, worker: *Worker, success: bool) !void {
    closeWorkerReplayCursor(runtime, worker);
    if (!worker.catch_up_open) return;
    worker.catch_up_open = false;
    if (runtime.finish_catch_up_fn) |finish_catch_up| try finish_catch_up(runtime.ctx, worker.kind, success);
}

fn isRecoverablePublishError(worker: *const Worker, err: anyerror) bool {
    return switch (err) {
        error.NotFound => catch_up_policy.forIndex(worker.kind, worker.runtime.backlog.resource_manager).not_found_is_recoverable,
        error.ReplayDocumentNotVisible, error.WriterLocked => true,
        else => false,
    };
}

fn isRecoverableCatchUpError(worker: *const Worker, err: anyerror) bool {
    return switch (err) {
        error.WriterLocked,
        error.ReplayDocumentNotVisible,
        => true,
        error.NotFound => catch_up_policy.forIndex(worker.kind, worker.runtime.backlog.resource_manager).not_found_is_recoverable,
        else => false,
    };
}

fn waitForCatchUpSessionReuse(runtime: *DerivedRuntime, worker: *Worker, from_sequence: u64) bool {
    const policy = catch_up_policy.forIndex(worker.kind, runtime.backlog.resource_manager);
    if (!worker.catch_up_open or policy.session_idle_ns == 0) return false;
    var waited_ns: u64 = 0;
    const delay_ns = @max(@as(u64, std.time.ns_per_ms), policy.coalesce_delay_ns);
    while (waited_ns < policy.session_idle_ns) {
        lock(runtime);
        const shutdown = runtime.shutdown or worker.stop or runtime.last_error_name != null;
        const target = worker.target_sequence;
        const force_sequence = runtime.force_catch_up_sequence;
        runtime.mutex.unlock();
        if (shutdown) return false;
        if (target > from_sequence or force_sequence > from_sequence) return true;
        sleepNs(delay_ns);
        waited_ns +|= delay_ns;
    }
    return false;
}

fn waitForReplayWindow(runtime: *DerivedRuntime, worker: *Worker, from_sequence: u64) void {
    const policy = catch_up_policy.forIndex(worker.kind, runtime.backlog.resource_manager);
    const min_records = policy.coalesce_min_records;
    const delay_ns = policy.coalesce_delay_ns;
    if (min_records == 0 or delay_ns == 0) return;

    var waited_ns: u64 = 0;
    while (waited_ns < policy.coalesce_max_wait_ns) {
        lock(runtime);
        const shutdown = runtime.shutdown or worker.stop or runtime.last_error_name != null;
        const target = worker.target_sequence;
        const pending_records = target -| from_sequence;
        const force_sequence = runtime.force_catch_up_sequence;
        runtime.mutex.unlock();

        if (shutdown or pending_records == 0 or pending_records >= min_records or force_sequence > from_sequence) return;

        sleepNs(delay_ns);
        waited_ns +|= delay_ns;
    }
}

fn sleepNs(ns: u64) void {
    if (@TypeOf(std.c.nanosleep) == void) return;
    var req: std.c.timespec = .{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    _ = std.c.nanosleep(&req, null);
}

fn catchUpWorker(runtime: *DerivedRuntime, worker: *Worker) !derived_worker.CatchUpStats {
    const policy = catch_up_policy.forIndex(worker.kind, runtime.backlog.resource_manager);
    return try derived_worker.catchUpIndexFromMatchingCursor(
        runtime.alloc,
        &worker.replay_cursor.?,
        worker.kind,
        runtime.ctx,
        runtime.apply_fn,
        .{
            .resource_manager = runtime.backlog.resource_manager,
            .max_windows_per_call = policy.max_windows_per_publish,
            .max_items_per_window = policy.max_items_per_window,
            .max_chunk_bytes = policy.max_chunk_bytes,
            .estimated_dense_vector_bytes = policy.estimated_dense_vector_bytes,
        },
    );
}

fn shouldRefreshReplayCursor(worker: *const Worker, caught_up_sequence: u64) bool {
    const cursor = worker.replay_cursor orelse return false;
    if (cursor.canFollowTail()) return false;
    if (caught_up_sequence <= worker.replay_cursor_open_sequence) return false;
    // Primary-store replay cursors pin an LSM read snapshot. Refresh them
    // after each successful catch-up window so hot ingest does not hold a
    // cloned mutable memtable open across unrelated writes.
    return true;
}

fn lock(runtime: *DerivedRuntime) void {
    while (!runtime.mutex.tryLock()) {
        std.Thread.yield() catch {};
    }
}

const TestRuntimeCapture = struct {
    alloc: Allocator,
    apply_calls: std.atomic.Value(u64) = .init(0),
    begin_calls: std.atomic.Value(u64) = .init(0),
    finish_calls: std.atomic.Value(u64) = .init(0),
    publish_failures: std.atomic.Value(u64) = .init(0),
    apply_not_found_failures: std.atomic.Value(u64) = .init(0),
    persist_calls: std.atomic.Value(u64) = .init(0),
    persisted_sequence: std.atomic.Value(u64) = .init(0),
    last_applied_batch_sequence: std.atomic.Value(u64) = .init(0),
    fail_next_dense_apply_not_found: std.atomic.Value(bool) = .init(false),
    fail_next_publish: std.atomic.Value(bool) = .init(false),
};

fn testRuntimeApply(ctx: *anyopaque, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) !bool {
    const capture: *TestRuntimeCapture = @ptrCast(@alignCast(ctx));
    _ = capture.apply_calls.fetchAdd(1, .monotonic);
    capture.last_applied_batch_sequence.store(batch.sequence, .monotonic);
    if (index_ref.kind == .dense_vector and capture.fail_next_dense_apply_not_found.swap(false, .monotonic)) {
        _ = capture.apply_not_found_failures.fetchAdd(1, .monotonic);
        return error.NotFound;
    }
    return true;
}

fn testRuntimePersist(ctx: *anyopaque, index_name: []const u8, sequence: u64, force: bool) !bool {
    _ = index_name;
    _ = force;
    const capture: *TestRuntimeCapture = @ptrCast(@alignCast(ctx));
    _ = capture.persist_calls.fetchAdd(1, .monotonic);
    capture.persisted_sequence.store(sequence, .monotonic);
    return true;
}

fn testRuntimeTruncate(ctx: *anyopaque, sequence: u64) !void {
    _ = ctx;
    _ = sequence;
}

fn testRuntimeBeginCatchUp(ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) !void {
    _ = index_ref;
    const capture: *TestRuntimeCapture = @ptrCast(@alignCast(ctx));
    _ = capture.begin_calls.fetchAdd(1, .monotonic);
}

fn testRuntimeFinishCatchUp(ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, success: bool) !void {
    _ = index_ref;
    const capture: *TestRuntimeCapture = @ptrCast(@alignCast(ctx));
    _ = capture.finish_calls.fetchAdd(1, .monotonic);
    if (success and capture.fail_next_publish.swap(false, .monotonic)) {
        _ = capture.publish_failures.fetchAdd(1, .monotonic);
        return error.NotFound;
    }
}

fn appendTestChangeJournalRecord(log: *change_journal_mod.Journal, alloc: Allocator, record: change_journal_mod.Record) !void {
    const payload = try change_journal_mod.encodeRecord(alloc, record);
    defer alloc.free(payload);
    _ = try log.appendOpaque(payload);
}

fn testInMemoryJournalOpenOptions() change_journal_mod.OpenOptions {
    return .{
        .backend = .lsm_memory,
        .lsm_options = .{
            .flush_threshold = 512,
            .compact_threshold_runs = 256,
            .wal_enabled = false,
            .obsolete_retention_ns = 0,
        },
    };
}

test "async non-tail replay cursor refreshes before watermark advance" {
    const alloc = std.testing.allocator;

    var backend = mem_backend_mod.Backend.init(alloc, .{});
    defer backend.close();
    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try docstore_mod.DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const RefreshingPrimarySource = struct {
        alloc: Allocator,
        store: *docstore_mod.DocStore,
        open_calls: std.atomic.Value(u64) = .init(0),
        appended: std.atomic.Value(bool) = .init(false),

        fn iface(self: *@This()) replay_source_mod.Source {
            return .{
                .ptr = self,
                .vtable = &.{
                    .open_matching_cursor = openMatchingCursor,
                    .for_each_matching_record = forEachMatchingRecord,
                    .latest_matching_sequence = latestMatchingSequence,
                    .collect_enrichment_document_groups = collectEnrichmentDocumentGroups,
                    .is_sequence_visible = isSequenceVisible,
                },
            };
        }

        fn appendRecordOnce(self: *@This()) !void {
            if (self.appended.swap(true, .acq_rel)) return;
            const payload = try change_journal_mod.encodeRecord(self.alloc, .{
                .sequence = 1,
                .changed_doc_keys = &.{"doc:refreshed"},
                .target_hints = &.{.dense_vector},
            });
            defer self.alloc.free(payload);
            try self.store.appendReplayOpaque(self.alloc, 1, payload);
        }

        fn source(self: *@This()) replay_source_mod.Source {
            return replay_source_mod.Source.fromPrimaryStore(self.store, null, null);
        }

        fn openMatchingCursor(ptr: *anyopaque, cursor_alloc: Allocator, from_sequence: u64, hint: replay_source_mod.TargetHint) !replay_source_mod.MatchingCursor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const call = self.open_calls.fetchAdd(1, .acq_rel) + 1;
            if (call == 2) try self.appendRecordOnce();
            return try self.source().openMatchingCursor(cursor_alloc, from_sequence, hint);
        }

        fn forEachMatchingRecord(
            ptr: *anyopaque,
            record_alloc: Allocator,
            from_sequence: u64,
            hint: replay_source_mod.TargetHint,
            max_matched_entries: usize,
            ctx: *anyopaque,
            consume: *const fn (ctx: *anyopaque, sequence: u64, payload: []const u8) anyerror!void,
        ) !replay_source_mod.MatchingRecordStats {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.source().forEachMatchingRecord(record_alloc, from_sequence, hint, max_matched_entries, ctx, consume);
        }

        fn latestMatchingSequence(ptr: *anyopaque, record_alloc: Allocator, from_sequence: u64, hint: replay_source_mod.TargetHint) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.source().latestMatchingSequence(record_alloc, from_sequence, hint);
        }

        fn collectEnrichmentDocumentGroups(ptr: *anyopaque, group_alloc: Allocator, from_sequence: u64) ![]replay_source_mod.PendingDocumentGroup {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.source().collectEnrichmentDocumentGroups(group_alloc, from_sequence);
        }

        fn isSequenceVisible(ptr: *anyopaque, sequence: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.source().isSequenceVisible(sequence);
        }
    };

    var source = RefreshingPrimarySource{
        .alloc = alloc,
        .store = &store,
    };
    var capture = TestRuntimeCapture{ .alloc = alloc };
    var runtime = DerivedRuntime.init(
        alloc,
        source.iface(),
        &capture,
        testRuntimeApply,
        testRuntimePersist,
        testRuntimeTruncate,
        testRuntimeBeginCatchUp,
        testRuntimeFinishCatchUp,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("dense_idx", .{ .name = "dense_idx", .kind = .dense_vector }, 0);
    runtime.notifySequence(2);
    try runtime.waitForAll(2);
    try runtime.failIfUnhealthy();

    try std.testing.expect(source.open_calls.load(.monotonic) >= 2);
    try std.testing.expectEqual(@as(u64, 1), capture.apply_calls.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), capture.last_applied_batch_sequence.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 2), runtime.appliedSequence("dense_idx").?);
}

test "async dense publish NotFound retries without failing runtime" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/async-dense-publish-retry-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    try appendTestChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestRuntimeCapture{ .alloc = alloc };
    capture.fail_next_publish.store(true, .monotonic);

    var runtime = DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testRuntimeApply,
        testRuntimePersist,
        testRuntimeTruncate,
        testRuntimeBeginCatchUp,
        testRuntimeFinishCatchUp,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("dense_idx", .{ .name = "dense_idx", .kind = .dense_vector }, 0);
    runtime.notifySequence(1);
    try runtime.waitForAll(1);
    try runtime.failIfUnhealthy();

    try std.testing.expectEqual(@as(u64, 1), capture.publish_failures.load(.monotonic));
    try std.testing.expect(capture.begin_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.finish_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.apply_calls.load(.monotonic) >= 2);
    try std.testing.expectEqual(@as(u64, 1), runtime.appliedSequence("dense_idx").?);
    try std.testing.expectEqual(@as(u64, 1), capture.persisted_sequence.load(.monotonic));
}

test "async dense catch-up NotFound retries without failing runtime" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/async-dense-catch-up-retry-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    try appendTestChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestRuntimeCapture{ .alloc = alloc };
    capture.fail_next_dense_apply_not_found.store(true, .monotonic);

    var runtime = DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testRuntimeApply,
        testRuntimePersist,
        testRuntimeTruncate,
        testRuntimeBeginCatchUp,
        testRuntimeFinishCatchUp,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("dense_idx", .{ .name = "dense_idx", .kind = .dense_vector }, 0);
    runtime.notifySequence(1);
    try runtime.waitForAll(1);
    try runtime.failIfUnhealthy();

    try std.testing.expectEqual(@as(u64, 1), capture.apply_not_found_failures.load(.monotonic));
    try std.testing.expect(capture.apply_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.begin_calls.load(.monotonic) >= 1);
    try std.testing.expect(capture.finish_calls.load(.monotonic) >= 1);
    try std.testing.expectEqual(@as(u64, 1), runtime.appliedSequence("dense_idx").?);
    try std.testing.expectEqual(@as(u64, 1), capture.persisted_sequence.load(.monotonic));
}

test "async full-text catch-up uses generic publish lifecycle" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/async-full-text-generic-catch-up-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();
    try appendTestChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.full_text},
    });
    try appendTestChangeJournalRecord(&journal, alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:b"},
        .target_hints = &.{.full_text},
    });

    var capture = TestRuntimeCapture{ .alloc = alloc };
    var runtime = DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testRuntimeApply,
        testRuntimePersist,
        testRuntimeTruncate,
        testRuntimeBeginCatchUp,
        testRuntimeFinishCatchUp,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("text_idx", .{ .name = "text_idx", .kind = .full_text }, 0);
    runtime.notifySequence(2);
    try runtime.waitForAll(2);
    try runtime.failIfUnhealthy();

    try std.testing.expect(capture.begin_calls.load(.monotonic) >= 1);
    try std.testing.expect(capture.finish_calls.load(.monotonic) >= 1);
    try std.testing.expect(capture.apply_calls.load(.monotonic) >= 1);
    try std.testing.expectEqual(@as(u64, 2), runtime.appliedSequence("text_idx").?);
    try std.testing.expectEqual(@as(u64, 2), capture.persisted_sequence.load(.monotonic));
}

test "async dense publishes applied window before target tail is visible" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/async-dense-bounded-publish-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testInMemoryJournalOpenOptions());
    defer journal.close();

    try appendTestChangeJournalRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestRuntimeCapture{ .alloc = alloc };
    var runtime = DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testRuntimeApply,
        testRuntimePersist,
        testRuntimeTruncate,
        testRuntimeBeginCatchUp,
        testRuntimeFinishCatchUp,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("dense_idx", .{ .name = "dense_idx", .kind = .dense_vector }, 0);
    runtime.notifySequence(2);

    var waited: usize = 0;
    while (waited < 2_000) : (waited += 1) {
        if ((runtime.appliedSequence("dense_idx") orelse 0) >= 1) break;
        sleepNs(std.time.ns_per_ms);
    }

    try runtime.failIfUnhealthy();
    try std.testing.expectEqual(@as(u64, 1), runtime.appliedSequence("dense_idx").?);
    try std.testing.expect(capture.begin_calls.load(.monotonic) >= 1);
    try std.testing.expect(capture.finish_calls.load(.monotonic) >= 1);
    try std.testing.expectEqual(@as(u64, 1), capture.persisted_sequence.load(.monotonic));
}
