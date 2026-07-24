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
const replay_source_mod = @import("replay_source.zig");
const derived_worker = @import("derived_worker.zig");
const catch_up_policy = @import("catch_up_policy.zig");
const backlog_tracker_mod = @import("backlog_tracker.zig");
const resource_manager_mod = @import("../../resource_manager.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const types = @import("../types.zig");
const async_runtime_mod = @import("async_runtime.zig");
const change_journal_mod = @import("change_journal.zig");
const derived_types = @import("derived_types.zig");

pub const RuntimeError = async_runtime_mod.RuntimeError;
pub const ApplyFn = async_runtime_mod.ApplyFn;
pub const PersistFn = async_runtime_mod.PersistFn;
pub const TruncateFn = async_runtime_mod.TruncateFn;
pub const BeginCatchUpFn = async_runtime_mod.BeginCatchUpFn;
pub const FinishCatchUpFn = async_runtime_mod.FinishCatchUpFn;
pub const CanAdvanceToTargetFn = async_runtime_mod.CanAdvanceToTargetFn;
pub const AppliedSequenceAdvancedFn = async_runtime_mod.AppliedSequenceAdvancedFn;

const Worker = struct {
    runtime: *DerivedRuntime,
    name: []u8,
    kind: index_manager_mod.ManagedIndexRef,
    applied_sequence: u64,
    persisted_sequence: u64,
    target_sequence: u64,
    stop: bool = false,
    future: ?Io.Future(void) = null,
    catch_up_open: bool = false,
    replay_cursor: ?replay_source_mod.MatchingCursor = null,
    replay_cursor_open_sequence: u64 = 0,
    catch_up_active: bool = false,
    last_replay_tail_records: u64 = 0,
    recoverable_retry_backoff: catch_up_policy.RecoverableRetryBackoff = .{},
};

const PersistSnapshot = struct {
    name: []u8,
    sequence: u64,
};

fn freePersistSnapshots(alloc: Allocator, snapshots: []PersistSnapshot) void {
    for (snapshots) |snapshot| alloc.free(snapshot.name);
    if (snapshots.len > 0) alloc.free(snapshots);
}

fn appendPersistSnapshot(alloc: Allocator, snapshots: *std.ArrayListUnmanaged(PersistSnapshot), worker: *const Worker) !void {
    const name = try alloc.dupe(u8, worker.name);
    errdefer alloc.free(name);
    try snapshots.append(alloc, .{
        .name = name,
        .sequence = worker.applied_sequence,
    });
}

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

pub const DerivedRuntime = if (builtin.os.tag == .freestanding) struct {
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
        applied_sequence_advanced_fn: ?AppliedSequenceAdvancedFn,
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
        _ = applied_sequence_advanced_fn;
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

    pub fn snapshotStats(_: *@This()) types.DerivedWorkerStats {
        return .{};
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

    pub fn forceSequence(self: *@This(), sequence: u64) void {
        _ = self;
        _ = sequence;
    }

    pub fn trackBacklogBytes(self: *@This(), sequence: u64, bytes: u64) !void {
        _ = self;
        _ = sequence;
        _ = bytes;
    }

    pub fn backlogThrottleTargetSequence(_: *@This()) ?u64 {
        return null;
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
} else struct {
    const IoOwner = enum {
        owned,
        borrowed,
    };

    alloc: Allocator,
    threaded: *Io.Threaded,
    threaded_owner: IoOwner,
    replay_source: replay_source_mod.Source,
    ctx: *anyopaque,
    apply_fn: ApplyFn,
    persist_fn: PersistFn,
    truncate_fn: TruncateFn,
    begin_catch_up_fn: ?BeginCatchUpFn,
    finish_catch_up_fn: ?FinishCatchUpFn,
    can_advance_to_target_fn: ?CanAdvanceToTargetFn,
    applied_sequence_advanced_fn: ?AppliedSequenceAdvancedFn,
    mutex: Io.Mutex = .init,
    cond: Io.Condition = .init,
    workers: std.ArrayListUnmanaged(*Worker) = .empty,
    shutdown: bool = false,
    last_error_name: ?[]const u8 = null,
    last_truncated_sequence: u64 = 0,
    force_catch_up_sequence: u64 = 0,
    last_notified_sequence: u64 = 0,
    truncates_in_flight: usize = 0,
    backlog: backlog_tracker_mod.Tracker,
    recoverable_retry_counters: catch_up_policy.RecoverableRetryCounters = .{},

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
        applied_sequence_advanced_fn: ?AppliedSequenceAdvancedFn,
        resource_manager: ?*resource_manager_mod.ResourceManager,
    ) !DerivedRuntime {
        const threaded = try alloc.create(Io.Threaded);
        errdefer alloc.destroy(threaded);
        threaded.* = Io.Threaded.init(alloc, .{});
        return initWithIo(
            alloc,
            threaded,
            .owned,
            replay_source,
            ctx,
            apply_fn,
            persist_fn,
            truncate_fn,
            begin_catch_up_fn,
            finish_catch_up_fn,
            can_advance_to_target_fn,
            applied_sequence_advanced_fn,
            resource_manager,
        );
    }

    pub fn initBorrowed(
        alloc: Allocator,
        threaded: *Io.Threaded,
        replay_source: replay_source_mod.Source,
        ctx: *anyopaque,
        apply_fn: ApplyFn,
        persist_fn: PersistFn,
        truncate_fn: TruncateFn,
        begin_catch_up_fn: ?BeginCatchUpFn,
        finish_catch_up_fn: ?FinishCatchUpFn,
        can_advance_to_target_fn: ?CanAdvanceToTargetFn,
        applied_sequence_advanced_fn: ?AppliedSequenceAdvancedFn,
        resource_manager: ?*resource_manager_mod.ResourceManager,
    ) DerivedRuntime {
        return initWithIo(
            alloc,
            threaded,
            .borrowed,
            replay_source,
            ctx,
            apply_fn,
            persist_fn,
            truncate_fn,
            begin_catch_up_fn,
            finish_catch_up_fn,
            can_advance_to_target_fn,
            applied_sequence_advanced_fn,
            resource_manager,
        );
    }

    fn initWithIo(
        alloc: Allocator,
        threaded: *Io.Threaded,
        threaded_owner: IoOwner,
        replay_source: replay_source_mod.Source,
        ctx: *anyopaque,
        apply_fn: ApplyFn,
        persist_fn: PersistFn,
        truncate_fn: TruncateFn,
        begin_catch_up_fn: ?BeginCatchUpFn,
        finish_catch_up_fn: ?FinishCatchUpFn,
        can_advance_to_target_fn: ?CanAdvanceToTargetFn,
        applied_sequence_advanced_fn: ?AppliedSequenceAdvancedFn,
        resource_manager: ?*resource_manager_mod.ResourceManager,
    ) DerivedRuntime {
        return .{
            .alloc = alloc,
            .threaded = threaded,
            .threaded_owner = threaded_owner,
            .replay_source = replay_source,
            .ctx = ctx,
            .apply_fn = apply_fn,
            .persist_fn = persist_fn,
            .truncate_fn = truncate_fn,
            .begin_catch_up_fn = begin_catch_up_fn,
            .finish_catch_up_fn = finish_catch_up_fn,
            .can_advance_to_target_fn = can_advance_to_target_fn,
            .applied_sequence_advanced_fn = applied_sequence_advanced_fn,
            .backlog = backlog_tracker_mod.Tracker.init(resource_manager),
        };
    }

    fn ioContext(self: *DerivedRuntime) Io {
        return self.threaded.io();
    }

    pub fn deinit(self: *DerivedRuntime) void {
        const io = self.ioContext();

        self.mutex.lockUncancelable(io);
        self.shutdown = true;
        for (self.workers.items) |worker| worker.stop = true;
        self.cond.broadcast(io);
        self.mutex.unlock(io);

        for (self.workers.items) |worker| {
            if (worker.future) |*future| _ = future.await(io);
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
        self.workers.deinit(self.alloc);
        self.backlog.deinit(self.alloc);
        if (self.threaded_owner == .owned) {
            self.threaded.deinit();
            self.alloc.destroy(self.threaded);
        }
        self.* = undefined;
    }

    pub fn hasWorkers(self: *DerivedRuntime) bool {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.workers.items.len > 0;
    }

    pub fn failIfUnhealthy(self: *DerivedRuntime) !void {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.last_error_name != null) return RuntimeError.AsyncWorkerFailed;
    }

    pub fn addWorker(self: *DerivedRuntime, name: []const u8, kind: index_manager_mod.ManagedIndexRef, applied_sequence: u64) !void {
        const io = self.ioContext();

        const worker = try self.alloc.create(Worker);
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
        errdefer self.alloc.free(worker.name);
        worker.kind.name = worker.name;

        self.mutex.lockUncancelable(io);
        worker.target_sequence = @max(worker.target_sequence, self.last_notified_sequence);
        try self.workers.append(self.alloc, worker);
        self.mutex.unlock(io);
        errdefer {
            self.mutex.lockUncancelable(io);
            const idx = for (self.workers.items, 0..) |candidate, i| {
                if (candidate == worker) break i;
            } else unreachable;
            _ = self.workers.orderedRemove(idx);
            self.mutex.unlock(io);
        }

        worker.future = try io.concurrent(workerMain, .{worker});
        errdefer stopAndJoinWorker(self, worker, io);
    }

    pub fn removeWorker(self: *DerivedRuntime, name: []const u8) void {
        const io = self.ioContext();

        self.mutex.lockUncancelable(io);
        const idx = for (self.workers.items, 0..) |worker, i| {
            if (std.mem.eql(u8, worker.name, name)) break i;
        } else {
            self.mutex.unlock(io);
            return;
        };
        const worker = self.workers.orderedRemove(idx);
        worker.stop = true;
        self.cond.broadcast(io);
        self.mutex.unlock(io);

        if (worker.future) |*future| _ = future.await(io);
        closeWorkerCatchUpState(self, worker, true) catch |err| {
            std.log.warn("derived worker final catch-up close failed worker={s}: {s}", .{ worker.name, @errorName(err) });
        };
        self.alloc.free(worker.name);
        self.alloc.destroy(worker);
    }

    pub fn appliedSequence(self: *DerivedRuntime, name: []const u8) ?u64 {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        for (self.workers.items) |worker| {
            if (std.mem.eql(u8, worker.name, name)) return worker.applied_sequence;
        }
        return null;
    }

    pub fn snapshotStats(self: *DerivedRuntime) types.DerivedWorkerStats {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var stats = types.DerivedWorkerStats{
            .workers = @intCast(self.workers.items.len),
        };
        for (self.workers.items) |worker| {
            const lag = worker.target_sequence -| worker.applied_sequence;
            if (lag > 0) stats.workers_with_replay_debt += 1;
            stats.max_replay_lag_sequences = @max(stats.max_replay_lag_sequences, lag);
        }
        const retries = self.recoverable_retry_counters.snapshot();
        stats.recoverable_retries = retries.total;
        stats.writer_locked_retries = retries.writer_locked;
        stats.resource_budget_retries = retries.resource_budget;
        stats.replay_document_not_visible_retries = retries.replay_document_not_visible;
        stats.artifact_repair_required_retries = retries.artifact_repair_required;
        stats.not_found_retries = retries.not_found;
        return stats;
    }

    pub fn notifySequence(self: *DerivedRuntime, sequence: u64) void {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        self.last_notified_sequence = @max(self.last_notified_sequence, sequence);
        var changed = false;
        for (self.workers.items) |worker| {
            const next = @max(worker.target_sequence, sequence);
            changed = changed or next != worker.target_sequence;
            worker.target_sequence = next;
        }
        if (changed) self.cond.broadcast(io);
    }

    pub fn notifyIndexes(self: *DerivedRuntime, sequence: u64, index_names: []const []const u8) void {
        if (index_names.len == 0) return;
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        var changed = false;
        for (self.workers.items) |worker| {
            if (!indexNameInList(worker.name, index_names)) continue;
            const next = @max(worker.target_sequence, sequence);
            changed = changed or next != worker.target_sequence;
            worker.target_sequence = next;
        }
        if (changed) self.cond.broadcast(io);
    }

    pub fn notifyExceptKind(self: *DerivedRuntime, sequence: u64, excluded_kind: types.IndexKind) void {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        self.last_notified_sequence = @max(self.last_notified_sequence, sequence);
        var changed = false;
        for (self.workers.items) |worker| {
            if (worker.kind.kind == excluded_kind) continue;
            const next = @max(worker.target_sequence, sequence);
            changed = changed or next != worker.target_sequence;
            worker.target_sequence = next;
        }
        if (changed) self.cond.broadcast(io);
    }

    pub fn forceSequence(self: *DerivedRuntime, sequence: u64) void {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        self.last_notified_sequence = @max(self.last_notified_sequence, sequence);
        self.force_catch_up_sequence = @max(self.force_catch_up_sequence, sequence);
        var changed = false;
        for (self.workers.items) |worker| {
            const next = @max(worker.target_sequence, sequence);
            changed = changed or next != worker.target_sequence;
            worker.target_sequence = next;
        }
        if (changed) self.cond.broadcast(io);
    }

    pub fn trackBacklogBytes(self: *DerivedRuntime, sequence: u64, bytes: u64) !void {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return try self.backlog.track(self.alloc, sequence, bytes);
    }

    pub fn backlogThrottleTargetSequence(self: *DerivedRuntime) ?u64 {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.backlog.throttleTargetSequence();
    }

    pub fn releaseBacklogThrough(self: *DerivedRuntime, sequence: u64) void {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.backlog.releaseThrough(sequence);
    }

    pub fn waitForAll(self: *DerivedRuntime, sequence: u64) !void {
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.force_catch_up_sequence = @max(self.force_catch_up_sequence, sequence);
        for (self.workers.items) |worker| {
            worker.target_sequence = @max(worker.target_sequence, sequence);
        }
        self.cond.broadcast(io);

        while (true) {
            if (self.last_error_name != null) return RuntimeError.AsyncWorkerFailed;

            var all_applied = true;
            for (self.workers.items) |worker| {
                if (worker.applied_sequence < sequence or worker.catch_up_active) {
                    all_applied = false;
                    break;
                }
            }
            if (all_applied and self.truncates_in_flight == 0) {
                self.mutex.unlock(io);
                for (self.workers.items) |worker| {
                    try closeWorkerCatchUpState(self, worker, true);
                }
                self.mutex.lockUncancelable(io);
                var all_persisted = true;
                var snapshots = std.ArrayListUnmanaged(PersistSnapshot).empty;
                errdefer {
                    for (snapshots.items) |snapshot| self.alloc.free(snapshot.name);
                }
                defer snapshots.deinit(self.alloc);
                for (self.workers.items) |worker| {
                    if (worker.applied_sequence == 0) continue;
                    try appendPersistSnapshot(self.alloc, &snapshots, worker);
                }
                const persist_snapshots = try snapshots.toOwnedSlice(self.alloc);
                snapshots = .empty;
                defer freePersistSnapshots(self.alloc, persist_snapshots);
                self.mutex.unlock(io);
                for (persist_snapshots) |snapshot| {
                    const persisted = self.persist_fn(self.ctx, snapshot.name, snapshot.sequence, true) catch |err| {
                        self.mutex.lockUncancelable(io);
                        return err;
                    };
                    self.mutex.lockUncancelable(io);
                    if (persisted) {
                        for (self.workers.items) |worker| {
                            if (std.mem.eql(u8, worker.name, snapshot.name)) {
                                worker.persisted_sequence = @max(worker.persisted_sequence, snapshot.sequence);
                                break;
                            }
                        }
                    } else {
                        all_persisted = false;
                    }
                    self.mutex.unlock(io);
                }
                self.mutex.lockUncancelable(io);
                if (!all_persisted) {
                    self.mutex.unlock(io);
                    io.sleep(Io.Duration.zero, .awake) catch {};
                    self.mutex.lockUncancelable(io);
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
                if (truncate_sequence > 0) {
                    self.mutex.unlock(io);
                    self.truncate_fn(self.ctx, truncate_sequence) catch |err| {
                        self.mutex.lockUncancelable(io);
                        return err;
                    };
                    self.mutex.lockUncancelable(io);
                    self.backlog.releaseThrough(truncate_sequence);
                }
                return;
            }
            self.mutex.unlock(io);
            io.sleep(Io.Duration.fromNanoseconds(std.time.ns_per_ms), .awake) catch {};
            self.mutex.lockUncancelable(io);
        }
    }

    pub fn waitForIndexes(self: *DerivedRuntime, sequence: u64, index_names: []const []const u8) !void {
        if (index_names.len == 0) return;
        const io = self.ioContext();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        var changed = false;
        for (self.workers.items) |worker| {
            if (!indexNameInList(worker.name, index_names)) continue;
            const next = @max(worker.target_sequence, sequence);
            changed = changed or next != worker.target_sequence;
            worker.target_sequence = next;
        }
        if (changed) self.cond.broadcast(io);

        while (true) {
            if (self.last_error_name != null) return RuntimeError.AsyncWorkerFailed;

            var all_applied = true;
            for (self.workers.items) |worker| {
                if (!indexNameInList(worker.name, index_names)) continue;
                if (worker.applied_sequence < sequence or worker.catch_up_active) {
                    all_applied = false;
                    break;
                }
            }
            if (all_applied and self.truncates_in_flight == 0) {
                self.mutex.unlock(io);
                for (self.workers.items) |worker| {
                    if (!indexNameInList(worker.name, index_names)) continue;
                    try closeWorkerCatchUpState(self, worker, true);
                }
                self.mutex.lockUncancelable(io);
                var all_persisted = true;
                var snapshots = std.ArrayListUnmanaged(PersistSnapshot).empty;
                errdefer {
                    for (snapshots.items) |snapshot| self.alloc.free(snapshot.name);
                }
                defer snapshots.deinit(self.alloc);
                for (self.workers.items) |worker| {
                    if (!indexNameInList(worker.name, index_names)) continue;
                    if (worker.applied_sequence == 0) continue;
                    try appendPersistSnapshot(self.alloc, &snapshots, worker);
                }
                const persist_snapshots = try snapshots.toOwnedSlice(self.alloc);
                snapshots = .empty;
                defer freePersistSnapshots(self.alloc, persist_snapshots);
                self.mutex.unlock(io);
                for (persist_snapshots) |snapshot| {
                    const persisted = self.persist_fn(self.ctx, snapshot.name, snapshot.sequence, true) catch |err| {
                        self.mutex.lockUncancelable(io);
                        return err;
                    };
                    self.mutex.lockUncancelable(io);
                    if (persisted) {
                        for (self.workers.items) |worker| {
                            if (std.mem.eql(u8, worker.name, snapshot.name)) {
                                worker.persisted_sequence = @max(worker.persisted_sequence, snapshot.sequence);
                                break;
                            }
                        }
                    } else {
                        all_persisted = false;
                    }
                    self.mutex.unlock(io);
                }
                self.mutex.lockUncancelable(io);
                if (!all_persisted) {
                    self.mutex.unlock(io);
                    io.sleep(Io.Duration.zero, .awake) catch {};
                    self.mutex.lockUncancelable(io);
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
                if (truncate_sequence > 0) {
                    self.mutex.unlock(io);
                    self.truncate_fn(self.ctx, truncate_sequence) catch |err| {
                        self.mutex.lockUncancelable(io);
                        return err;
                    };
                    self.mutex.lockUncancelable(io);
                    self.backlog.releaseThrough(truncate_sequence);
                }
                return;
            }
            self.mutex.unlock(io);
            io.sleep(Io.Duration.fromNanoseconds(std.time.ns_per_ms), .awake) catch {};
            self.mutex.lockUncancelable(io);
        }
    }

    fn recordError(self: *DerivedRuntime, io: Io, worker_name: []const u8, stage: []const u8, err: anyerror) void {
        std.log.err("derived worker failed worker={s} stage={s}: {s}", .{ worker_name, stage, @errorName(err) });
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.last_error_name == null) self.last_error_name = @errorName(err);
        self.cond.broadcast(io);
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
    const io = runtime.ioContext();
    var close_success = true;
    defer closeWorkerCatchUpState(runtime, worker, close_success) catch |err| runtime.recordError(io, worker.name, "close_session", err);

    while (true) {
        runtime.mutex.lockUncancelable(io);
        while (!runtime.shutdown and !worker.stop and runtime.last_error_name == null and worker.target_sequence <= worker.applied_sequence) {
            if (worker.applied_sequence > worker.persisted_sequence) {
                const sequence = worker.applied_sequence;
                runtime.mutex.unlock(io);
                const persisted = persistIdleAppliedSequence(runtime, worker, sequence, io) catch |err| {
                    if (err == error.WriterLocked or err == error.ResourceBudgetExceeded) {
                        sleepAfterRecoverableCatchUpError(worker, err, io);
                        runtime.mutex.lockUncancelable(io);
                        continue;
                    }
                    runtime.recordError(io, worker.name, "idle_persist", err);
                    return;
                };
                worker.recoverable_retry_backoff.reset();
                if (!persisted) io.sleep(Io.Duration.fromNanoseconds(50 * std.time.ns_per_ms), .awake) catch {};
                runtime.mutex.lockUncancelable(io);
                continue;
            }
            if (!worker.catch_up_open) {
                runtime.cond.waitUncancelable(io, &runtime.mutex);
                continue;
            }
            runtime.mutex.unlock(io);
            if (waitForCatchUpSessionReuse(runtime, worker, io)) {
                runtime.mutex.lockUncancelable(io);
                continue;
            }
            closeWorkerCatchUpState(runtime, worker, true) catch |err| {
                close_success = false;
                runtime.recordError(io, worker.name, "idle_close", err);
                return;
            };
            runtime.mutex.lockUncancelable(io);
        }
        if (runtime.shutdown or worker.stop or runtime.last_error_name != null) {
            runtime.mutex.unlock(io);
            return;
        }
        const from_sequence = worker.applied_sequence;
        const target_sequence = worker.target_sequence;
        const replay_tail_records = target_sequence -| from_sequence;
        if (replay_tail_records > 0) worker.last_replay_tail_records = replay_tail_records;
        worker.catch_up_active = true;
        runtime.mutex.unlock(io);

        ensureWorkerCatchUpState(runtime, worker, from_sequence) catch |err| {
            runtime.mutex.lockUncancelable(io);
            worker.catch_up_active = false;
            runtime.cond.broadcast(io);
            runtime.mutex.unlock(io);
            if (isRecoverableCatchUpError(worker, err)) {
                closeWorkerCatchUpState(runtime, worker, false) catch |close_err| {
                    close_success = false;
                    runtime.recordError(io, worker.name, "recoverable_begin_catch_up_close", close_err);
                    return;
                };
                sleepAfterRecoverableCatchUpError(worker, err, io);
                continue;
            }
            close_success = false;
            runtime.recordError(io, worker.name, "begin_catch_up_session", err);
            return;
        };
        waitForReplayWindow(runtime, worker, from_sequence, io);

        var stats = catchUpWorker(runtime, worker) catch |err| {
            runtime.mutex.lockUncancelable(io);
            worker.catch_up_active = false;
            runtime.cond.broadcast(io);
            runtime.mutex.unlock(io);
            if (isRecoverableCatchUpError(worker, err)) {
                closeWorkerCatchUpState(runtime, worker, false) catch |close_err| {
                    close_success = false;
                    runtime.recordError(io, worker.name, "recoverable_catch_up_close", close_err);
                    return;
                };
                sleepAfterRecoverableCatchUpError(worker, err, io);
                continue;
            }
            close_success = false;
            runtime.recordError(io, worker.name, "catch_up", err);
            return;
        };
        if (stats.last_sequence == 0 and target_sequence > from_sequence) {
            if (worker.replay_cursor != null) {
                if (worker.replay_cursor.?.canFollowTail()) {
                    const target_visible = runtime.replay_source.isSequenceVisible(target_sequence) catch |err| {
                        runtime.mutex.lockUncancelable(io);
                        worker.catch_up_active = false;
                        runtime.cond.broadcast(io);
                        runtime.mutex.unlock(io);
                        close_success = false;
                        runtime.recordError(io, worker.name, "target_visibility", err);
                        return;
                    };
                    if (!target_visible) {
                        runtime.mutex.lockUncancelable(io);
                        worker.catch_up_active = false;
                        runtime.cond.broadcast(io);
                        runtime.mutex.unlock(io);
                        io.sleep(Io.Duration.zero, .awake) catch {};
                        continue;
                    }
                }
                closeWorkerReplayCursor(runtime, worker);
                ensureWorkerCatchUpState(runtime, worker, from_sequence) catch |err| {
                    runtime.mutex.lockUncancelable(io);
                    worker.catch_up_active = false;
                    runtime.cond.broadcast(io);
                    runtime.mutex.unlock(io);
                    if (isRecoverableCatchUpError(worker, err)) {
                        closeWorkerCatchUpState(runtime, worker, false) catch |close_err| {
                            close_success = false;
                            runtime.recordError(io, worker.name, "recoverable_refresh_replay_cursor_close", close_err);
                            return;
                        };
                        sleepAfterRecoverableCatchUpError(worker, err, io);
                        continue;
                    }
                    close_success = false;
                    runtime.recordError(io, worker.name, "refresh_replay_cursor", err);
                    return;
                };
                stats = catchUpWorker(runtime, worker) catch |err| {
                    runtime.mutex.lockUncancelable(io);
                    worker.catch_up_active = false;
                    runtime.cond.broadcast(io);
                    runtime.mutex.unlock(io);
                    if (isRecoverableCatchUpError(worker, err)) {
                        closeWorkerCatchUpState(runtime, worker, false) catch |close_err| {
                            close_success = false;
                            runtime.recordError(io, worker.name, "recoverable_refreshed_catch_up_close", close_err);
                            return;
                        };
                        sleepAfterRecoverableCatchUpError(worker, err, io);
                        continue;
                    }
                    close_success = false;
                    runtime.recordError(io, worker.name, "catch_up_refreshed", err);
                    return;
                };
            }
        }
        runtime.mutex.lockUncancelable(io);
        worker.catch_up_active = false;
        runtime.cond.broadcast(io);
        runtime.mutex.unlock(io);
        if (stats.last_sequence == 0 and worker.replay_cursor != null and !worker.replay_cursor.?.canFollowTail()) {
            closeWorkerReplayCursor(runtime, worker);
        }

        const target_advance_allowed = if (stats.shouldTryTargetAdvance(from_sequence, target_sequence))
            canAdvanceToTarget(runtime, worker, from_sequence, target_sequence) catch |err| {
                close_success = false;
                runtime.recordError(io, worker.name, "target_advance", err);
                return;
            }
        else
            false;
        const caught_up_sequence = if (stats.appliedSequenceAdvance(from_sequence)) |sequence|
            sequence
        else if (target_advance_allowed)
            target_sequence
        else
            from_sequence;
        if (caught_up_sequence == from_sequence and stats.shouldTryTargetAdvance(from_sequence, target_sequence)) {
            closeWorkerCatchUpState(runtime, worker, false) catch |err| {
                close_success = false;
                runtime.recordError(io, worker.name, "coverage_gap_close", err);
                return;
            };
            io.sleep(Io.Duration.fromNanoseconds(50 * std.time.ns_per_ms), .awake) catch {};
            continue;
        }

        if (caught_up_sequence > from_sequence) {
            closeWorkerCatchUpState(runtime, worker, true) catch |err| {
                if (isRecoverablePublishError(worker, err)) {
                    sleepAfterRecoverableCatchUpError(worker, err, io);
                    continue;
                }
                close_success = false;
                runtime.recordError(io, worker.name, "publish_catch_up", err);
                return;
            };
        }

        var persisted = false;
        if (caught_up_sequence > from_sequence) {
            persisted = runtime.persist_fn(runtime.ctx, worker.name, caught_up_sequence, forcePersistAppliedSequence(worker)) catch |err| {
                if (err == error.WriterLocked or err == error.ResourceBudgetExceeded) {
                    sleepAfterRecoverableCatchUpError(worker, err, io);
                    continue;
                }
                runtime.recordError(io, worker.name, "persist", err);
                return;
            };
        }

        var truncate_sequence: u64 = 0;
        runtime.mutex.lockUncancelable(io);
        const applied_sequence_advanced = caught_up_sequence > worker.applied_sequence;
        if (applied_sequence_advanced) {
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
        } else {
            runtime.cond.broadcast(io);
        }
        runtime.mutex.unlock(io);
        if (applied_sequence_advanced) if (runtime.applied_sequence_advanced_fn) |callback| {
            callback(runtime.ctx, worker.name, caught_up_sequence);
        };
        worker.recoverable_retry_backoff.reset();

        if (shouldRefreshReplayCursor(worker, caught_up_sequence)) {
            closeWorkerReplayCursor(runtime, worker);
        }

        if (truncate_sequence > 0) {
            runtime.truncate_fn(runtime.ctx, truncate_sequence) catch |err| {
                if (err == error.WriterLocked) {
                    runtime.mutex.lockUncancelable(io);
                    runtime.truncates_in_flight -= 1;
                    runtime.cond.broadcast(io);
                    runtime.mutex.unlock(io);
                    io.sleep(Io.Duration.zero, .awake) catch {};
                    continue;
                }
                runtime.mutex.lockUncancelable(io);
                runtime.truncates_in_flight -= 1;
                runtime.cond.broadcast(io);
                runtime.mutex.unlock(io);
                runtime.recordError(io, worker.name, "truncate", err);
                return;
            };
            runtime.mutex.lockUncancelable(io);
            runtime.backlog.releaseThrough(truncate_sequence);
            runtime.truncates_in_flight -= 1;
            runtime.cond.broadcast(io);
            runtime.mutex.unlock(io);
        }
    }
}

fn persistIdleAppliedSequence(runtime: *DerivedRuntime, worker: *Worker, sequence: u64, io: Io) !bool {
    const persisted = try runtime.persist_fn(runtime.ctx, worker.name, sequence, false);
    var truncate_sequence: u64 = 0;
    runtime.mutex.lockUncancelable(io);
    if (persisted and sequence > worker.persisted_sequence) {
        worker.persisted_sequence = sequence;
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
    } else {
        runtime.cond.broadcast(io);
    }
    runtime.mutex.unlock(io);

    if (truncate_sequence > 0) {
        runtime.truncate_fn(runtime.ctx, truncate_sequence) catch |err| {
            runtime.mutex.lockUncancelable(io);
            runtime.truncates_in_flight -= 1;
            runtime.cond.broadcast(io);
            runtime.mutex.unlock(io);
            return err;
        };
        runtime.mutex.lockUncancelable(io);
        runtime.backlog.releaseThrough(truncate_sequence);
        runtime.truncates_in_flight -= 1;
        runtime.cond.broadcast(io);
        runtime.mutex.unlock(io);
    }
    return persisted;
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
    const io = runtime.ioContext();
    runtime.mutex.lockUncancelable(io);
    var replay_cursor = worker.replay_cursor;
    worker.replay_cursor = null;
    worker.replay_cursor_open_sequence = 0;
    runtime.mutex.unlock(io);

    if (replay_cursor) |*cursor| cursor.deinit(runtime.alloc);
}

fn closeWorkerCatchUpState(runtime: *DerivedRuntime, worker: *Worker, success: bool) !void {
    const io = runtime.ioContext();
    runtime.mutex.lockUncancelable(io);
    var replay_cursor = worker.replay_cursor;
    const catch_up_open = worker.catch_up_open;
    worker.replay_cursor = null;
    worker.replay_cursor_open_sequence = 0;
    worker.catch_up_open = false;
    worker.last_replay_tail_records = 0;
    runtime.mutex.unlock(io);

    if (replay_cursor) |*cursor| cursor.deinit(runtime.alloc);
    if (!catch_up_open) return;
    if (runtime.finish_catch_up_fn) |finish_catch_up| try finish_catch_up(runtime.ctx, worker.kind, success);
}

fn isRecoverablePublishError(worker: *const Worker, err: anyerror) bool {
    return switch (err) {
        error.NotFound => catch_up_policy.forIndex(worker.kind, worker.runtime.backlog.resource_manager).not_found_is_recoverable,
        error.ReplayDocumentNotVisible, error.ArtifactRepairRequired, error.WriterLocked, error.ResourceBudgetExceeded => true,
        else => false,
    };
}

fn isRecoverableCatchUpError(worker: *const Worker, err: anyerror) bool {
    return switch (err) {
        error.WriterLocked,
        error.ResourceBudgetExceeded,
        error.ReplayDocumentNotVisible,
        error.ArtifactRepairRequired,
        => true,
        error.NotFound => catch_up_policy.forIndex(worker.kind, worker.runtime.backlog.resource_manager).not_found_is_recoverable,
        else => false,
    };
}

fn sleepAfterRecoverableCatchUpError(worker: *Worker, err: anyerror, io: Io) void {
    const delay_ns = catch_up_policy.recordRecoverableRetry(
        &worker.runtime.recoverable_retry_counters,
        &worker.recoverable_retry_backoff,
        err,
    );
    if (worker.recoverable_retry_backoff.shouldLog()) {
        std.log.warn(
            "derived worker retrying recoverable failure worker={s} error={s} failures={} retry_ms={}",
            .{ worker.name, @errorName(err), worker.recoverable_retry_backoff.failures, delay_ns / std.time.ns_per_ms },
        );
    }
    io.sleep(Io.Duration.fromNanoseconds(@intCast(delay_ns)), .awake) catch {};
}

fn waitForCatchUpSessionReuse(runtime: *DerivedRuntime, worker: *Worker, io: Io) bool {
    const policy = catch_up_policy.forIndex(worker.kind, runtime.backlog.resource_manager);
    const idle_wait_ns = catch_up_policy.sessionIdleMaxWaitNs(policy, worker.last_replay_tail_records);
    if (!worker.catch_up_open or idle_wait_ns == 0) return false;
    var waited_ns: u64 = 0;
    const from_sequence = worker.applied_sequence;
    const delay_ns = @max(@as(u64, std.time.ns_per_ms), policy.coalesce_delay_ns);
    while (waited_ns < idle_wait_ns) {
        runtime.mutex.lockUncancelable(io);
        const shutdown = runtime.shutdown or worker.stop or runtime.last_error_name != null;
        const target = worker.target_sequence;
        const force_sequence = runtime.force_catch_up_sequence;
        runtime.mutex.unlock(io);
        if (shutdown) return false;
        if (target > from_sequence or force_sequence > from_sequence) return true;
        const sleep_ns = @min(delay_ns, idle_wait_ns - waited_ns);
        io.sleep(Io.Duration.fromNanoseconds(@intCast(sleep_ns)), .awake) catch {};
        waited_ns +|= sleep_ns;
    }
    return false;
}

fn waitForReplayWindow(runtime: *DerivedRuntime, worker: *Worker, from_sequence: u64, io: Io) void {
    const policy = catch_up_policy.forIndex(worker.kind, runtime.backlog.resource_manager);
    const delay_ns = policy.coalesce_delay_ns;
    if (delay_ns == 0) return;

    var waited_ns: u64 = 0;
    while (true) {
        runtime.mutex.lockUncancelable(io);
        const shutdown = runtime.shutdown or worker.stop or runtime.last_error_name != null;
        const target = worker.target_sequence;
        const pending_records = target -| from_sequence;
        const force_sequence = runtime.force_catch_up_sequence;
        runtime.mutex.unlock(io);

        const max_wait_ns = catch_up_policy.replayWindowMaxWaitNs(policy, pending_records);
        if (shutdown or pending_records == 0 or max_wait_ns == 0 or force_sequence > from_sequence or waited_ns >= max_wait_ns) return;

        const sleep_ns = @min(delay_ns, max_wait_ns - waited_ns);
        io.sleep(Io.Duration.fromNanoseconds(@intCast(sleep_ns)), .awake) catch {};
        waited_ns +|= sleep_ns;
    }
}

fn catchUpWorker(runtime: *DerivedRuntime, worker: *Worker) !derived_worker.CatchUpStats {
    const policy = catch_up_policy.forIndex(worker.kind, runtime.backlog.resource_manager);
    if (worker.replay_cursor == null) {
        try ensureWorkerCatchUpState(runtime, worker, worker.applied_sequence);
    }
    const max_windows_per_call: usize = blk: {
        const io = runtime.ioContext();
        runtime.mutex.lockUncancelable(io);
        defer runtime.mutex.unlock(io);
        if (runtime.force_catch_up_sequence >= worker.target_sequence) break :blk 0;
        break :blk policy.max_windows_per_publish;
    };
    return try derived_worker.catchUpIndexFromMatchingCursor(
        runtime.alloc,
        &worker.replay_cursor.?,
        worker.kind,
        runtime.ctx,
        runtime.apply_fn,
        .{
            .resource_manager = runtime.backlog.resource_manager,
            .max_windows_per_call = max_windows_per_call,
            .max_items_per_window = policy.max_items_per_window,
            .max_chunk_bytes = policy.max_chunk_bytes,
            .estimated_dense_vector_bytes = policy.estimated_dense_vector_bytes,
            .target_sequence = worker.target_sequence,
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

fn stopAndJoinWorker(runtime: *DerivedRuntime, worker: *Worker, io: Io) void {
    runtime.mutex.lockUncancelable(io);
    worker.stop = true;
    runtime.cond.broadcast(io);
    runtime.mutex.unlock(io);
    if (worker.future) |*future| _ = future.await(io);
}

const TestThreadedRuntimeCapture = struct {
    runtime: ?*DerivedRuntime = null,
    apply_calls: std.atomic.Value(u64) = .init(0),
    begin_calls: std.atomic.Value(u64) = .init(0),
    finish_calls: std.atomic.Value(u64) = .init(0),
    publish_failures: std.atomic.Value(u64) = .init(0),
    apply_not_found_failures: std.atomic.Value(u64) = .init(0),
    resource_budget_failures: std.atomic.Value(u64) = .init(0),
    persisted_sequence: std.atomic.Value(u64) = .init(0),
    advanced_sequence: std.atomic.Value(u64) = .init(0),
    callback_observed_applied_sequence: std.atomic.Value(u64) = .init(0),
    fail_next_dense_apply_not_found: std.atomic.Value(bool) = .init(false),
    fail_next_apply_resource_budget: std.atomic.Value(bool) = .init(false),
    fail_next_publish: std.atomic.Value(bool) = .init(false),
};

fn testThreadedRuntimeAppliedSequenceAdvanced(ctx: *anyopaque, index_name: []const u8, sequence: u64) void {
    const capture: *TestThreadedRuntimeCapture = @ptrCast(@alignCast(ctx));
    capture.advanced_sequence.store(sequence, .release);
    const runtime = capture.runtime orelse return;
    capture.callback_observed_applied_sequence.store(runtime.appliedSequence(index_name) orelse 0, .release);
}

fn testThreadedRuntimeApply(ctx: *anyopaque, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) !bool {
    _ = batch;
    const capture: *TestThreadedRuntimeCapture = @ptrCast(@alignCast(ctx));
    _ = capture.apply_calls.fetchAdd(1, .monotonic);
    if (capture.fail_next_apply_resource_budget.swap(false, .monotonic)) {
        _ = capture.resource_budget_failures.fetchAdd(1, .monotonic);
        return error.ResourceBudgetExceeded;
    }
    if (index_ref.kind == .dense_vector and capture.fail_next_dense_apply_not_found.swap(false, .monotonic)) {
        _ = capture.apply_not_found_failures.fetchAdd(1, .monotonic);
        return error.NotFound;
    }
    return true;
}

fn testThreadedRuntimePersist(ctx: *anyopaque, index_name: []const u8, sequence: u64, force: bool) !bool {
    _ = index_name;
    _ = force;
    const capture: *TestThreadedRuntimeCapture = @ptrCast(@alignCast(ctx));
    capture.persisted_sequence.store(sequence, .monotonic);
    return true;
}

fn testThreadedRuntimeTruncate(ctx: *anyopaque, sequence: u64) !void {
    _ = ctx;
    _ = sequence;
}

fn testThreadedRuntimeBeginCatchUp(ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) !void {
    _ = index_ref;
    const capture: *TestThreadedRuntimeCapture = @ptrCast(@alignCast(ctx));
    _ = capture.begin_calls.fetchAdd(1, .monotonic);
}

fn testThreadedRuntimeFinishCatchUp(ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, success: bool) !void {
    _ = index_ref;
    const capture: *TestThreadedRuntimeCapture = @ptrCast(@alignCast(ctx));
    _ = capture.finish_calls.fetchAdd(1, .monotonic);
    if (success and capture.fail_next_publish.swap(false, .monotonic)) {
        _ = capture.publish_failures.fetchAdd(1, .monotonic);
        return error.NotFound;
    }
}

fn testThreadedRuntimeJournalOpenOptions() change_journal_mod.OpenOptions {
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

fn appendTestThreadedRuntimeRecord(log: *change_journal_mod.Journal, alloc: Allocator, record: change_journal_mod.Record) !void {
    const payload = try change_journal_mod.encodeRecord(alloc, record);
    defer alloc.free(payload);
    _ = try log.appendOpaque(payload);
}

test "io threaded applied callback observes published watermark outside runtime lock" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/io-threaded-applied-callback-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    try appendTestThreadedRuntimeRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.full_text},
    });

    var capture = TestThreadedRuntimeCapture{};
    var runtime = try DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        null,
        null,
        null,
        testThreadedRuntimeAppliedSequenceAdvanced,
        null,
    );
    capture.runtime = &runtime;
    defer runtime.deinit();

    try runtime.addWorker("text_idx", .{ .name = "text_idx", .kind = .full_text }, 0);
    runtime.notifySequence(1);
    try runtime.waitForAll(1);
    try runtime.failIfUnhealthy();

    try std.testing.expectEqual(@as(u64, 1), capture.advanced_sequence.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), capture.callback_observed_applied_sequence.load(.acquire));
}

test "io threaded dense catch-up NotFound closes session before retry" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/io-threaded-dense-catch-up-retry-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    try appendTestThreadedRuntimeRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestThreadedRuntimeCapture{};
    capture.fail_next_dense_apply_not_found.store(true, .monotonic);
    var runtime = try DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        testThreadedRuntimeBeginCatchUp,
        testThreadedRuntimeFinishCatchUp,
        null,
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
    try std.testing.expect(capture.begin_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.finish_calls.load(.monotonic) >= 2);
    try std.testing.expectEqual(@as(u64, 1), runtime.appliedSequence("dense_idx").?);
    try std.testing.expectEqual(@as(u64, 1), capture.persisted_sequence.load(.monotonic));
}

test "io threaded dense publish NotFound retries with a fresh session" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/io-threaded-dense-publish-retry-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    try appendTestThreadedRuntimeRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.dense_vector},
    });

    var capture = TestThreadedRuntimeCapture{};
    capture.fail_next_publish.store(true, .monotonic);
    var runtime = try DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        testThreadedRuntimeBeginCatchUp,
        testThreadedRuntimeFinishCatchUp,
        null,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("dense_idx", .{ .name = "dense_idx", .kind = .dense_vector }, 0);
    runtime.notifySequence(1);
    try runtime.waitForAll(1);
    try runtime.failIfUnhealthy();

    try std.testing.expectEqual(@as(u64, 1), capture.publish_failures.load(.monotonic));
    try std.testing.expect(capture.apply_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.begin_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.finish_calls.load(.monotonic) >= 2);
    try std.testing.expectEqual(@as(u64, 1), runtime.appliedSequence("dense_idx").?);
    try std.testing.expectEqual(@as(u64, 1), capture.persisted_sequence.load(.monotonic));
}

test "io threaded full-text resource pressure retries without poisoning runtime" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const journal_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/io-threaded-full-text-resource-retry-journal", .{tmp.sub_path});
    defer alloc.free(journal_path);
    const journal_path_z = try alloc.dupeZ(u8, journal_path);
    defer alloc.free(journal_path_z);

    var journal = try change_journal_mod.Journal.open(journal_path_z, testThreadedRuntimeJournalOpenOptions());
    defer journal.close();
    try appendTestThreadedRuntimeRecord(&journal, alloc, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.full_text},
    });

    var capture = TestThreadedRuntimeCapture{};
    capture.fail_next_apply_resource_budget.store(true, .monotonic);
    var runtime = try DerivedRuntime.init(
        alloc,
        replay_source_mod.Source.fromJournal(&journal),
        &capture,
        testThreadedRuntimeApply,
        testThreadedRuntimePersist,
        testThreadedRuntimeTruncate,
        testThreadedRuntimeBeginCatchUp,
        testThreadedRuntimeFinishCatchUp,
        null,
        null,
        null,
    );
    defer runtime.deinit();

    try runtime.addWorker("text_idx", .{ .name = "text_idx", .kind = .full_text }, 0);
    runtime.notifySequence(1);
    try runtime.waitForAll(1);
    try runtime.failIfUnhealthy();

    try std.testing.expectEqual(@as(u64, 1), capture.resource_budget_failures.load(.monotonic));
    try std.testing.expect(capture.apply_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.begin_calls.load(.monotonic) >= 2);
    try std.testing.expect(capture.finish_calls.load(.monotonic) >= 2);
    try std.testing.expectEqual(@as(u64, 1), runtime.appliedSequence("text_idx").?);
    try std.testing.expectEqual(@as(u64, 1), capture.persisted_sequence.load(.monotonic));
}
