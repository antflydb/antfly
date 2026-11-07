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

pub const VisibilityWait = runtime_types.VisibilityWait;

const runtime_types = @import("runtime_types.zig");
const derived_worker = @import("derived_worker.zig");
const io_threaded_runtime_mod = @import("io_threaded_runtime.zig");

pub const ApplyFn = runtime_types.ApplyFn;
pub const PersistFn = runtime_types.PersistFn;
pub const TruncateFn = runtime_types.TruncateFn;
pub const BeginCatchUpFn = runtime_types.BeginCatchUpFn;
pub const FinishCatchUpFn = runtime_types.FinishCatchUpFn;
pub const CanAdvanceToTargetFn = runtime_types.CanAdvanceToTargetFn;
pub const AppliedSequenceAdvancedFn = runtime_types.AppliedSequenceAdvancedFn;
pub const RuntimeError = runtime_types.RuntimeError;
pub const CatchUpSessionToken = runtime_types.CatchUpSessionToken;
pub const CatchUpFinishResult = runtime_types.CatchUpFinishResult;
pub const BacklogAdmission = backlog_tracker_mod.Tracker.Admission;

pub const Backend = runtime_backend.Backend;

pub const Config = struct {
    backend: Backend = runtime_backend.defaultExecutorBackend(),
};

pub const Executor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    const VTable = struct {
        begin_shutdown: *const fn (ptr: *anyopaque) void,
        deinit: *const fn (ptr: *anyopaque, alloc: Allocator) void,
        has_workers: *const fn (ptr: *anyopaque) bool,
        fail_if_unhealthy: *const fn (ptr: *anyopaque) anyerror!void,
        add_worker: *const fn (ptr: *anyopaque, name: []const u8, kind: index_manager_mod.ManagedIndexRef, applied_sequence: u64) anyerror!void,
        pause_worker: *const fn (ptr: *anyopaque, name: []const u8) anyerror!bool,
        resume_worker: *const fn (ptr: *anyopaque, name: []const u8) void,
        remove_worker: *const fn (ptr: *anyopaque, name: []const u8) void,
        applied_sequence: *const fn (ptr: *anyopaque, name: []const u8) ?u64,
        adopt_committed_sequence: ?*const fn (*anyopaque, []const u8, u64) void = null,
        snapshot_stats: *const fn (ptr: *anyopaque) types.DerivedWorkerStats,
        notify_sequence: *const fn (ptr: *anyopaque, sequence: u64) void,
        notify_indexes: *const fn (ptr: *anyopaque, sequence: u64, index_names: []const []const u8) void,
        notify_except_kind: *const fn (ptr: *anyopaque, sequence: u64, excluded_kind: types.IndexKind) void,
        force_sequence: *const fn (ptr: *anyopaque, sequence: u64) void,
        admit_backlog_bytes: *const fn (ptr: *anyopaque, bytes: u64) anyerror!BacklogAdmission,
        commit_backlog_admission: *const fn (ptr: *anyopaque, sequence: u64, admission: *BacklogAdmission) void,
        track_backlog_bytes: *const fn (ptr: *anyopaque, sequence: u64, bytes: u64) anyerror!void,
        backlog_throttle_target_sequence: *const fn (ptr: *anyopaque) ?u64,
        release_backlog_through: *const fn (ptr: *anyopaque, sequence: u64) void,
        wait_for_all: *const fn (ptr: *anyopaque, sequence: u64, wait: VisibilityWait) anyerror!void,
        wait_for_indexes: *const fn (ptr: *anyopaque, sequence: u64, index_names: []const []const u8, wait: VisibilityWait) anyerror!void,
    };

    /// Close worker admission and wake idle workers without joining them.
    /// Owners use this phase before shutting down dependencies that an active
    /// publisher may currently be waiting on.
    pub fn beginShutdown(self: *Executor) void {
        self.vtable.begin_shutdown(self.ptr);
    }

    pub fn deinit(self: *Executor, alloc: Allocator) void {
        self.vtable.deinit(self.ptr, alloc);
        self.* = undefined;
    }

    pub fn hasWorkers(self: *Executor) bool {
        return self.vtable.has_workers(self.ptr);
    }

    pub fn isManual(self: *const Executor) bool {
        return self.vtable == &manual_vtable;
    }

    pub fn failIfUnhealthy(self: *Executor) !void {
        return try self.vtable.fail_if_unhealthy(self.ptr);
    }

    /// Primary durability does not require a healthy secondary-index worker.
    /// Journal/backlog admission still bounds debt; index visibility requests
    /// fail closed before committing if their derived runtime is unhealthy.
    pub fn checkSyncLevelHealth(self: *Executor, level: types.SyncLevel) !void {
        switch (level) {
            .full_text, .full_index => try self.failIfUnhealthy(),
            .propose, .write, .enrichments => {},
        }
    }

    pub fn addWorker(self: *Executor, name: []const u8, kind: index_manager_mod.ManagedIndexRef, applied_sequence: u64) !void {
        return try self.vtable.add_worker(self.ptr, name, kind, applied_sequence);
    }

    /// Drain execution without releasing the registration or replay watermark.
    /// The structural owner must resume it or remove it after catalog commit.
    pub fn pauseWorker(self: *Executor, name: []const u8) !bool {
        return try self.vtable.pause_worker(self.ptr, name);
    }

    /// Rollback reuses the existing registration and never allocates.
    pub fn resumeWorker(self: *Executor, name: []const u8) void {
        self.vtable.resume_worker(self.ptr, name);
    }

    pub fn removeWorker(self: *Executor, name: []const u8) void {
        self.vtable.remove_worker(self.ptr, name);
    }

    pub fn appliedSequence(self: *Executor, name: []const u8) ?u64 {
        return self.vtable.applied_sequence(self.ptr, name);
    }

    /// Infallible activation of progress already durable in the same native
    /// transaction as the index. Caller holds this registration's catalog/apply
    /// fence. No persistence, reclamation, allocation or user callback runs here.
    pub fn adoptCommittedSequence(self: *Executor, name: []const u8, sequence: u64) void {
        if (self.vtable.adopt_committed_sequence) |adopt| adopt(self.ptr, name, sequence);
    }

    pub fn snapshotStats(self: *Executor) types.DerivedWorkerStats {
        return self.vtable.snapshot_stats(self.ptr);
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

    pub fn admitBacklogBytes(self: *Executor, bytes: u64) !BacklogAdmission {
        return try self.vtable.admit_backlog_bytes(self.ptr, bytes);
    }

    pub fn commitBacklogAdmission(self: *Executor, sequence: u64, admission: *BacklogAdmission) void {
        self.vtable.commit_backlog_admission(self.ptr, sequence, admission);
    }

    pub fn backlogThrottleTargetSequence(self: *Executor) ?u64 {
        return self.vtable.backlog_throttle_target_sequence(self.ptr);
    }

    pub fn releaseBacklogThrough(self: *Executor, sequence: u64) void {
        self.vtable.release_backlog_through(self.ptr, sequence);
    }

    pub fn waitForAll(self: *Executor, sequence: u64) !void {
        return try self.waitForAllWithVisibilityWait(sequence, .{});
    }

    pub fn waitForAllWithVisibilityWait(self: *Executor, sequence: u64, wait: VisibilityWait) !void {
        return try self.vtable.wait_for_all(self.ptr, sequence, wait);
    }

    pub fn waitForIndexes(self: *Executor, sequence: u64, index_names: []const []const u8) !void {
        return try self.waitForIndexesWithVisibilityWait(sequence, index_names, .{});
    }

    pub fn waitForIndexesWithVisibilityWait(self: *Executor, sequence: u64, index_names: []const []const u8, wait: VisibilityWait) !void {
        return try self.vtable.wait_for_indexes(self.ptr, sequence, index_names, wait);
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
    applied_sequence_advanced_fn: ?AppliedSequenceAdvancedFn,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*background_runtime_mod.BackendRuntime,
) !Executor {
    try runtime_backend.ensureExecutorBackendAvailable(config.backend);
    return switch (config.backend) {
        .manual => try initManual(alloc, replay_source, ctx, apply_fn, persist_fn, truncate_fn, begin_catch_up_fn, finish_catch_up_fn, can_advance_to_target_fn, applied_sequence_advanced_fn, resource_manager),
        .io_threaded => try initIoThreaded(alloc, replay_source, ctx, apply_fn, persist_fn, truncate_fn, begin_catch_up_fn, finish_catch_up_fn, can_advance_to_target_fn, applied_sequence_advanced_fn, resource_manager, backend_runtime),
    };
}

const ManualWorker = struct {
    name: []u8,
    kind: index_manager_mod.ManagedIndexRef,
    applied_sequence: u64,
    target_sequence: u64,
    paused: bool = false,
};

fn replayWorkElapsedNs(wait: VisibilityWait, kind: types.IndexKind, started: u64, waited_before: u64, preparation_before: u64) u64 {
    if (kind == .full_text) if (wait.full_text_preparation_ns) |counter|
        return counter.load(.monotonic) -| preparation_before;
    const elapsed = @import("antfly_platform").time.monotonicNs() -| started;
    const publication_wait = if (wait.publication_overhead_ns) |counter| counter.load(.monotonic) -| waited_before else 0;
    // Queueing, validation and the bounded prepared-publication hold are
    // separate from input preparation. Charging fixed sync/publication cost to
    // compute sizing creates smaller windows and still more publications.
    // Payload collection, tokenization and encoding remain charged.
    return elapsed -| publication_wait;
}

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
    applied_sequence_advanced_fn: ?AppliedSequenceAdvancedFn,
    backlog: backlog_tracker_mod.Tracker,
    workers: std.ArrayListUnmanaged(ManualWorker) = .empty,

    pub fn deinit(self: *ManualRuntime) void {
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

    fn pauseWorker(self: *ManualRuntime, name: []const u8) !bool {
        for (self.workers.items) |*worker| {
            if (!std.mem.eql(u8, worker.name, name)) continue;
            if (worker.paused) return error.DerivedWorkerAlreadyPaused;
            worker.paused = true;
            return true;
        }
        return false;
    }

    fn resumeWorker(self: *ManualRuntime, name: []const u8) void {
        for (self.workers.items) |*worker| {
            if (std.mem.eql(u8, worker.name, name)) worker.paused = false;
        }
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

    fn snapshotStats(self: *ManualRuntime) types.DerivedWorkerStats {
        var stats = types.DerivedWorkerStats{
            .workers = @intCast(self.workers.items.len),
        };
        for (self.workers.items) |worker| {
            const lag = worker.target_sequence -| worker.applied_sequence;
            if (lag > 0) stats.workers_with_replay_debt += 1;
            stats.max_replay_lag_sequences = @max(stats.max_replay_lag_sequences, lag);
        }
        return stats;
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

    fn admitBacklogBytes(self: *ManualRuntime, bytes: u64) !BacklogAdmission {
        return try self.backlog.admit(self.alloc, bytes);
    }

    fn commitBacklogAdmission(self: *ManualRuntime, sequence: u64, admission: *BacklogAdmission) void {
        self.backlog.commitAdmission(sequence, admission);
    }

    fn backlogThrottleTargetSequence(self: *ManualRuntime) ?u64 {
        return self.backlog.throttleTargetSequence();
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

    const CatchUpResult = struct {
        stats: derived_worker.CatchUpStats,
        token: CatchUpSessionToken,
        lane: usize,
        kind: types.IndexKind,
        budget: @import("replay_budget.zig").Budget,
        started: u64,
        waited_before: u64,
        preparation_before: u64,
        fragmented: bool,

        fn observe(self: @This(), wait: VisibilityWait, published: bool) void {
            const controller = wait.replay_budget orelse return;
            const preparation_ns = replayWorkElapsedNs(wait, self.kind, self.started, self.waited_before, self.preparation_before) +|
                (if (self.fragmented) self.stats.window_collect_ns else 0);
            if (!published) {
                controller.observeFailure(self.lane, self.budget, preparation_ns, null);
                return;
            }
            // Include finishing the capture and publishing the coarse progress
            // mirror. They are fixed publication costs too, even when the
            // physical index/fragment receipt already committed atomically.
            const fixed_ns = if (self.fragmented and wait.full_text_preparation_ns != null)
                (@import("antfly_platform").time.monotonicNs() -| self.started) -| preparation_ns
            else if (wait.publication_overhead_ns) |counter|
                counter.load(.monotonic) -| self.waited_before
            else
                0;
            controller.observePreparedWindow(self.lane, self.budget, preparation_ns, fixed_ns, self.stats.collection_budget_exhausted or self.stats.budgetWorkItems(self.fragmented) >= self.budget.records, false);
        }
    };

    fn catchUpWorker(self: *ManualRuntime, worker: *ManualWorker, wait: VisibilityWait) !CatchUpResult {
        const token = if (self.begin_catch_up_fn) |begin_catch_up|
            try begin_catch_up(self.ctx, worker.kind)
        else
            CatchUpSessionToken{};
        var catch_up_open = true;
        errdefer {
            if (catch_up_open) if (self.finish_catch_up_fn) |finish_catch_up| {
                _ = finish_catch_up(self.ctx, worker.kind, token, worker.applied_sequence, false) catch {};
            };
        }
        const ApplySession = struct {
            runtime: *ManualRuntime,
            token: CatchUpSessionToken,
            fragments: ?runtime_types.ReplayFragments = null,

            fn apply(ptr: *anyopaque, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) anyerror!bool {
                const session: *@This() = @ptrCast(@alignCast(ptr));
                return session.runtime.apply_fn(session.runtime.ctx, batch, index_ref, session.token);
            }
            fn applyFragment(ptr: *anyopaque, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef, position: derived_worker.replay_fragment.Position) !bool {
                const session: *@This() = @ptrCast(@alignCast(ptr));
                const port = session.fragments.?;
                return port.apply.?(port.ptr, batch, index_ref, position);
            }
        };
        var session = ApplySession{ .runtime = self, .token = token };
        const lane: usize = switch (worker.kind.kind) {
            .full_text => 0,
            .dense_vector => 1,
            .sparse_vector => 2,
            .graph => 3,
            .algebraic => 4,
        };
        var budget = if (wait.replay_budget) |controller| controller.budget(lane) else @import("replay_budget.zig").Budget{};
        budget.records = @min(budget.records, wait.replay_record_cap);
        const fragments = if (worker.kind.kind == .full_text) enabled: {
            const port = wait.fragments orelse break :enabled null;
            break :enabled if (try port.supports(port.ptr, worker.name)) port else null;
        } else null;
        session.fragments = fragments;
        const started = @import("antfly_platform").time.monotonicNs();
        const waited_before = if (wait.publication_overhead_ns) |counter| counter.load(.monotonic) else 0;
        const preparation_before = if (wait.full_text_preparation_ns) |counter| counter.load(.monotonic) else 0;
        var failure: ?anyerror = null;
        var completed = false;
        defer if (!completed) if (wait.replay_budget) |controller| controller.observeFailure(lane, budget, replayWorkElapsedNs(wait, worker.kind.kind, started, waited_before, preparation_before), failure);
        const stats = derived_worker.catchUpIndexWithOptions(
            self.alloc,
            self.replay_source,
            worker.kind,
            worker.applied_sequence,
            &session,
            ApplySession.apply,
            .{
                .resource_manager = self.backlog.resource_manager,
                .max_fragment_families = if (fragments != null) budget.records else 0,
                .fragment_ctx = if (fragments) |port| port.ptr else null,
                .load_fragment_fn = if (fragments) |port| port.load else null,
                .save_fragment_fn = if (fragments) |port| port.save else null,
                .apply_fragment_fn = if (fragments) |port| if (port.apply != null) ApplySession.applyFragment else null else null,
                .max_records_per_window = if (wait.replay_budget != null) budget.records else derived_worker.catch_up_max_records_per_window_default,
                .max_collected_items_per_window = if (wait.replay_budget != null) budget.records else 0,
                .max_chunk_bytes = if (wait.replay_budget != null) budget.bytes else derived_worker.catch_up_max_chunk_bytes_default,
                .max_windows_per_call = if (wait.replay_budget != null or fragments != null) 1 else 0,
                .release_read_snapshot_after_collect = wait.replay_budget != null or fragments != null,
                .max_call_ns = if (wait.replay_budget != null) @import("replay_budget.zig").Controller.quantum_ns else 0,
                .max_call_bytes = if (wait.replay_budget != null) budget.bytes else 0,
                .target_sequence = if (wait.replay_budget != null) @min(worker.target_sequence, wait.replay_target_cap) else 0,
            },
        ) catch |err| {
            failure = err;
            return err;
        };
        completed = true;
        catch_up_open = false;
        return .{ .stats = stats, .token = token, .lane = lane, .kind = worker.kind.kind, .budget = budget, .started = started, .waited_before = waited_before, .preparation_before = preparation_before, .fragmented = fragments != null };
    }

    fn waitForAll(self: *ManualRuntime, sequence: u64, wait: VisibilityWait) !void {
        return self.waitForSelected(sequence, null, wait);
    }

    fn waitForIndexes(self: *ManualRuntime, sequence: u64, index_names: []const []const u8, wait: VisibilityWait) !void {
        if (index_names.len == 0) return;
        return self.waitForSelected(sequence, index_names, wait);
    }

    fn waitForSelected(self: *ManualRuntime, sequence: u64, index_names: ?[]const []const u8, wait: VisibilityWait) !void {
        var capacity_deferred = false;
        workers: for (self.workers.items) |*worker| {
            if (index_names) |selected| if (!indexNameInList(worker.name, selected)) continue;
            worker.target_sequence = @max(worker.target_sequence, sequence);
            const target = @min(worker.target_sequence, wait.replay_target_cap);
            if (target <= worker.applied_sequence) continue;
            // Manual execution cannot wait for a structural owner on this same thread.
            if (worker.paused) return error.DerivedWorkerPaused;
            try wait.check();

            while (worker.applied_sequence < target) {
                const before_applied = worker.applied_sequence;
                const result = self.catchUpWorker(worker, wait) catch |err| {
                    if (wait.replay_budget != null and err == error.TextMergeProducerDeferred) {
                        // A capacity refusal leaves this consumer pending. Its
                        // catch-up session has unwound; other consumers still
                        // get their bounded slice before returning the refusal.
                        capacity_deferred = true;
                        continue :workers;
                    }
                    return err;
                };
                var published = false;
                defer result.observe(wait, published);
                var session_open = true;
                defer {
                    if (session_open) if (self.finish_catch_up_fn) |finish_catch_up| {
                        _ = finish_catch_up(self.ctx, worker.kind, result.token, worker.applied_sequence, false) catch {};
                    };
                }
                const stats = result.stats;
                try wait.check();
                const caught_up_sequence = if (stats.appliedSequenceAdvance(worker.applied_sequence)) |applied_sequence|
                    applied_sequence
                else if (stats.shouldTryTargetAdvance(worker.applied_sequence, target) and
                    try self.canAdvanceToTarget(worker, worker.applied_sequence, target))
                    target
                else
                    worker.applied_sequence;
                session_open = false;
                const finish_result = if (self.finish_catch_up_fn) |finish_catch_up|
                    try finish_catch_up(self.ctx, worker.kind, result.token, caught_up_sequence, true)
                else
                    CatchUpFinishResult{};
                if (caught_up_sequence > worker.applied_sequence) {
                    if (!finish_result.applied_sequence_persisted) {
                        while (!try self.persist_fn(self.ctx, worker.name, caught_up_sequence, true)) {
                            try wait.check();
                            if (wait.replay_budget != null) return error.EnrichmentWaitCanceled;
                            std.atomic.spinLoopHint();
                        }
                    }
                    worker.applied_sequence = caught_up_sequence;
                    if (self.applied_sequence_advanced_fn) |callback| callback(self.ctx, worker.name, caught_up_sequence);
                }
                published = true;
                if (wait.replay_budget != null or (stats.partial_sequence == 0 and worker.applied_sequence == before_applied)) break;
            }
        }

        const min_applied = computeMinApplied(self.workers.items);
        if (min_applied > 0) {
            try self.truncate_fn(self.ctx, min_applied);
            self.backlog.releaseThrough(min_applied);
        }
        if (capacity_deferred) return error.TextMergeProducerDeferred;
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
    applied_sequence_advanced_fn: ?AppliedSequenceAdvancedFn,
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
        .applied_sequence_advanced_fn = applied_sequence_advanced_fn,
        .backlog = backlog_tracker_mod.Tracker.init(resource_manager),
    };
    return .{
        .ptr = runtime,
        .vtable = &manual_vtable,
    };
}

const manual_vtable = Executor.VTable{
    .begin_shutdown = manualBeginShutdown,
    .deinit = manualDeinit,
    .has_workers = manualHasWorkers,
    .fail_if_unhealthy = manualFailIfUnhealthy,
    .add_worker = manualAddWorker,
    .pause_worker = manualPauseWorker,
    .resume_worker = manualResumeWorker,
    .remove_worker = manualRemoveWorker,
    .applied_sequence = manualAppliedSequence,
    .adopt_committed_sequence = manualAdoptCommittedSequence,
    .snapshot_stats = manualSnapshotStats,
    .notify_sequence = manualNotifySequence,
    .notify_indexes = manualNotifyIndexes,
    .notify_except_kind = manualNotifyExceptKind,
    .force_sequence = manualForceSequence,
    .admit_backlog_bytes = manualAdmitBacklogBytes,
    .commit_backlog_admission = manualCommitBacklogAdmission,
    .track_backlog_bytes = manualTrackBacklogBytes,
    .backlog_throttle_target_sequence = manualBacklogThrottleTargetSequence,
    .release_backlog_through = manualReleaseBacklogThrough,
    .wait_for_all = manualWaitForAll,
    .wait_for_indexes = manualWaitForIndexes,
};

fn manualAdoptCommittedSequence(ptr: *anyopaque, name: []const u8, sequence: u64) void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    for (runtime.workers.items) |*worker| {
        if (worker.kind.kind == .full_text and std.mem.eql(u8, worker.name, name)) {
            worker.applied_sequence = @max(worker.applied_sequence, sequence);
            worker.target_sequence = @max(worker.target_sequence, sequence);
            return;
        }
    }
}

fn manualBeginShutdown(_: *anyopaque) void {}

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

fn manualPauseWorker(ptr: *anyopaque, name: []const u8) !bool {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.pauseWorker(name);
}

fn manualResumeWorker(ptr: *anyopaque, name: []const u8) void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    runtime.resumeWorker(name);
}

fn manualRemoveWorker(ptr: *anyopaque, name: []const u8) void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    runtime.removeWorker(name);
}

fn manualAppliedSequence(ptr: *anyopaque, name: []const u8) ?u64 {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return runtime.appliedSequence(name);
}

fn manualSnapshotStats(ptr: *anyopaque) types.DerivedWorkerStats {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return runtime.snapshotStats();
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

fn manualAdmitBacklogBytes(ptr: *anyopaque, bytes: u64) !BacklogAdmission {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.admitBacklogBytes(bytes);
}

fn manualCommitBacklogAdmission(ptr: *anyopaque, sequence: u64, admission: *BacklogAdmission) void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    runtime.commitBacklogAdmission(sequence, admission);
}

fn manualBacklogThrottleTargetSequence(ptr: *anyopaque) ?u64 {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return runtime.backlogThrottleTargetSequence();
}

fn manualReleaseBacklogThrough(ptr: *anyopaque, sequence: u64) void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    runtime.releaseBacklogThrough(sequence);
}

fn manualWaitForAll(ptr: *anyopaque, sequence: u64, wait: VisibilityWait) !void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.waitForAll(sequence, wait);
}

fn manualWaitForIndexes(ptr: *anyopaque, sequence: u64, index_names: []const []const u8, wait: VisibilityWait) !void {
    const runtime: *ManualRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.waitForIndexes(sequence, index_names, wait);
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
    applied_sequence_advanced_fn: ?AppliedSequenceAdvancedFn,
    resource_manager: ?*resource_manager_mod.ResourceManager,
    backend_runtime: ?*background_runtime_mod.BackendRuntime,
) !Executor {
    if (comptime builtin.os.tag == .freestanding) return error.UnsupportedPlatform;

    const runtime = try alloc.create(io_threaded_runtime_mod.DerivedRuntime);
    errdefer alloc.destroy(runtime);
    if (backend_runtime) |bg| {
        const io_impl = bg.io_impl orelse return error.MissingBackendRuntimeIo;
        runtime.* = io_threaded_runtime_mod.DerivedRuntime.initBorrowed(alloc, io_impl, replay_source, ctx, apply_fn, persist_fn, truncate_fn, begin_catch_up_fn, finish_catch_up_fn, can_advance_to_target_fn, applied_sequence_advanced_fn, resource_manager);
        runtime.scheduler = try bg.maintenanceScheduler();
    } else {
        runtime.* = try io_threaded_runtime_mod.DerivedRuntime.init(alloc, replay_source, ctx, apply_fn, persist_fn, truncate_fn, begin_catch_up_fn, finish_catch_up_fn, can_advance_to_target_fn, applied_sequence_advanced_fn, resource_manager);
    }
    return .{
        .ptr = runtime,
        .vtable = &io_threaded_vtable,
    };
}

const io_threaded_vtable = Executor.VTable{
    .begin_shutdown = ioThreadedBeginShutdown,
    .deinit = ioThreadedDeinit,
    .has_workers = ioThreadedHasWorkers,
    .fail_if_unhealthy = ioThreadedFailIfUnhealthy,
    .add_worker = ioThreadedAddWorker,
    .pause_worker = ioThreadedPauseWorker,
    .resume_worker = ioThreadedResumeWorker,
    .remove_worker = ioThreadedRemoveWorker,
    .applied_sequence = ioThreadedAppliedSequence,
    .adopt_committed_sequence = ioThreadedAdoptCommittedSequence,
    .snapshot_stats = ioThreadedSnapshotStats,
    .notify_sequence = ioThreadedNotifySequence,
    .notify_indexes = ioThreadedNotifyIndexes,
    .notify_except_kind = ioThreadedNotifyExceptKind,
    .force_sequence = ioThreadedForceSequence,
    .admit_backlog_bytes = ioThreadedAdmitBacklogBytes,
    .commit_backlog_admission = ioThreadedCommitBacklogAdmission,
    .track_backlog_bytes = ioThreadedTrackBacklogBytes,
    .backlog_throttle_target_sequence = ioThreadedBacklogThrottleTargetSequence,
    .release_backlog_through = ioThreadedReleaseBacklogThrough,
    .wait_for_all = ioThreadedWaitForAll,
    .wait_for_indexes = ioThreadedWaitForIndexes,
};

fn ioThreadedAdoptCommittedSequence(ptr: *anyopaque, name: []const u8, sequence: u64) void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    runtime.adoptCommittedSequence(name, sequence);
}

fn ioThreadedBeginShutdown(ptr: *anyopaque) void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    runtime.beginShutdown();
}

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

fn ioThreadedPauseWorker(ptr: *anyopaque, name: []const u8) !bool {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.pauseWorker(name);
}

fn ioThreadedResumeWorker(ptr: *anyopaque, name: []const u8) void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    runtime.resumeWorker(name);
}

fn ioThreadedRemoveWorker(ptr: *anyopaque, name: []const u8) void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    runtime.removeWorker(name);
}

fn ioThreadedAppliedSequence(ptr: *anyopaque, name: []const u8) ?u64 {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return runtime.appliedSequence(name);
}

fn ioThreadedSnapshotStats(ptr: *anyopaque) types.DerivedWorkerStats {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return runtime.snapshotStats();
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

fn ioThreadedAdmitBacklogBytes(ptr: *anyopaque, bytes: u64) !BacklogAdmission {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.admitBacklogBytes(bytes);
}

fn ioThreadedCommitBacklogAdmission(ptr: *anyopaque, sequence: u64, admission: *BacklogAdmission) void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    runtime.commitBacklogAdmission(sequence, admission);
}

fn ioThreadedBacklogThrottleTargetSequence(ptr: *anyopaque) ?u64 {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return runtime.backlogThrottleTargetSequence();
}

fn ioThreadedReleaseBacklogThrough(ptr: *anyopaque, sequence: u64) void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    runtime.releaseBacklogThrough(sequence);
}

fn ioThreadedWaitForAll(ptr: *anyopaque, sequence: u64, wait: VisibilityWait) !void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.waitForAllWithVisibilityWait(sequence, wait);
}

fn ioThreadedWaitForIndexes(ptr: *anyopaque, sequence: u64, index_names: []const []const u8, wait: VisibilityWait) !void {
    const runtime: *io_threaded_runtime_mod.DerivedRuntime = @ptrCast(@alignCast(ptr));
    return try runtime.waitForIndexesWithVisibilityWait(sequence, index_names, wait);
}

test "manual replay background budget persists partial progress and leaves foreground waits complete" {
    const alloc = std.testing.allocator;
    var backend = @import("../../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var store = try @import("../../docstore.zig").DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    for (1..65) |sequence| {
        const payload = try @import("change_journal.zig").encodeRecord(alloc, .{
            .sequence = @intCast(sequence),
            .changed_doc_keys = &.{"doc"},
            .target_hints = &.{.full_text},
        });
        defer alloc.free(payload);
        try store.appendReplayOpaque(alloc, @intCast(sequence), payload);
    }
    const Probe = struct {
        persisted: u64 = 0,
        retired: u64 = 0,
        refuse: bool = false,
        fn apply(_: *anyopaque, _: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef, _: CatchUpSessionToken) !bool {
            return true;
        }
        fn persist(ptr: *anyopaque, _: []const u8, sequence: u64, _: bool) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.refuse) return false;
            self.persisted = sequence;
            return true;
        }
        fn truncate(ptr: *anyopaque, sequence: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.retired = sequence;
        }
    };
    var probe: Probe = .{};
    var executor = try initManual(alloc, replay_source_mod.Source.fromPrimaryStore(&store, null, null), &probe, Probe.apply, Probe.persist, Probe.truncate, null, null, null, null, null);
    defer executor.deinit(alloc);
    try executor.addWorker("ft", .{ .name = "ft", .kind = .full_text }, 0);
    var controller: @import("replay_budget.zig").Controller = .{};
    const wait: VisibilityWait = .{ .replay_budget = &controller, .replay_record_cap = 8 };
    probe.refuse = true;
    try std.testing.expectError(error.EnrichmentWaitCanceled, executor.waitForAllWithVisibilityWait(64, wait));
    try std.testing.expectEqual(@as(u64, 0), executor.appliedSequence("ft").?);
    probe.refuse = false;
    try executor.waitForAllWithVisibilityWait(64, wait);
    const partial = executor.appliedSequence("ft").?;
    try std.testing.expect(partial > 0 and partial < 64);
    try std.testing.expectEqual(partial, probe.persisted);
    try std.testing.expectEqual(partial, probe.retired);
    try std.testing.expect(controller.snapshot().observations >= 2);
    const produced_prefix = partial + 2;
    try executor.waitForAllWithVisibilityWait(64, .{ .replay_budget = &controller, .replay_target_cap = produced_prefix });
    try std.testing.expectEqual(produced_prefix, executor.appliedSequence("ft").?);
    try std.testing.expectEqual(@as(usize, 1), executor.snapshotStats().workers_with_replay_debt);
    try executor.waitForAll(64);
    try std.testing.expectEqual(@as(u64, 64), executor.appliedSequence("ft").?);
}

test "manual replay background budget bounds document fanout rather than only journal records" {
    const alloc = std.testing.allocator;
    var backend = @import("../../mem_backend.zig").Backend.init(alloc, .{});
    defer backend.close();
    var store = try @import("../../docstore.zig").DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    for (1..65) |sequence| {
        var keys: [64][]const u8 = undefined;
        for (&keys, 0..) |*key, item| key.* = try std.fmt.allocPrint(arena.allocator(), "doc-{d}-{d}", .{ sequence, item });
        const payload = try @import("change_journal.zig").encodeRecord(alloc, .{
            .sequence = @intCast(sequence),
            .changed_doc_keys = &keys,
            .target_hints = &.{.full_text},
        });
        defer alloc.free(payload);
        try store.appendReplayOpaque(alloc, @intCast(sequence), payload);
    }
    const Probe = struct {
        maximum_documents: usize = 0,
        fn apply(ptr: *anyopaque, batch: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef, _: CatchUpSessionToken) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.maximum_documents = @max(self.maximum_documents, batch.documents.len);
            return true;
        }
        fn persist(_: *anyopaque, _: []const u8, _: u64, _: bool) !bool {
            return true;
        }
        fn truncate(_: *anyopaque, _: u64) !void {}
    };
    var probe: Probe = .{};
    var executor = try initManual(alloc, replay_source_mod.Source.fromPrimaryStore(&store, null, null), &probe, Probe.apply, Probe.persist, Probe.truncate, null, null, null, null, null);
    defer executor.deinit(alloc);
    try executor.addWorker("ft", .{ .name = "ft", .kind = .full_text }, 0);
    var controller: @import("replay_budget.zig").Controller = .{};
    const initial = controller.budget(0);
    try executor.waitForAllWithVisibilityWait(64, .{ .replay_budget = &controller });
    try std.testing.expectEqual(initial.records, probe.maximum_documents);
    try std.testing.expect(executor.appliedSequence("ft").? < 64);
    try executor.waitForAll(64);
    try std.testing.expectEqual(@as(u64, 64), executor.appliedSequence("ft").?);
}

test "manual replay fragments one oversized record and resumes after replacement and checkpoint failure" {
    @import("antfly_test_error_logs").expectErrorLogs(1);
    const a = std.testing.allocator;
    const fragment = @import("replay_fragment.zig");
    var backend = @import("../../mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try @import("../../docstore.zig").DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var keys: [1024][]const u8 = undefined;
    for (&keys, 0..) |*key, i| key.* = try std.fmt.allocPrint(arena.allocator(), "doc-{d}", .{i});
    const payload = try @import("change_journal.zig").encodeRecord(a, .{
        .sequence = 1,
        .changed_doc_keys = &keys,
        .target_hints = &.{.full_text},
    });
    defer a.free(payload);
    try store.appendReplayOpaque(a, 1, payload);
    const Probe = struct {
        seen: [1024]bool = @splat(false),
        max_documents: usize = 0,
        encoded: ?[fragment.encoded_size]u8 = null,
        scalar: u64 = 0,
        retired: u64 = 0,
        fail_save: bool = false,
        delay_once: bool = true,
        preparation_ns: std.atomic.Value(u64) = .init(0),

        fn supports(_: *anyopaque, _: []const u8) !bool {
            return true;
        }
        fn apply(ptr: *anyopaque, batch: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef, _: CatchUpSessionToken) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.max_documents = @max(self.max_documents, batch.documents.len);
            _ = self.preparation_ns.fetchAdd(1, .monotonic);
            for (batch.documents) |doc| self.seen[try std.fmt.parseInt(usize, doc.key[4..], 10)] = true;
            return true;
        }
        fn load(ptr: *anyopaque, _: []const u8) !?fragment.Position {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return if (self.encoded) |raw| (try fragment.decode(&raw)).position else null;
        }
        fn save(ptr: *anyopaque, _: []const u8, position: fragment.Position) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.delay_once) {
                self.delay_once = false;
                try std.testing.io.sleep(.fromNanoseconds(2 * @import("replay_budget.zig").Controller.quantum_ns), .awake);
            }
            if (self.fail_save) {
                self.fail_save = false;
                return error.TestFragmentPersistFailure;
            }
            self.encoded = fragment.encode(.{ .position = position, .identity = .{ .generation = 1, .config_hash = 2, .incarnation = 3 } });
        }
        fn persist(ptr: *anyopaque, _: []const u8, sequence: u64, _: bool) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const position = (try fragment.decode(&self.encoded.?)).position;
            try std.testing.expectEqual(position.total_items, position.next_item);
            for (self.seen) |seen| try std.testing.expect(seen);
            self.scalar = sequence;
            return true;
        }
        fn truncate(ptr: *anyopaque, sequence: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.retired = sequence;
        }
    };
    var probe: Probe = .{};
    var budget: @import("replay_budget.zig").Controller = .{};
    const wait: VisibilityWait = .{
        .replay_budget = &budget,
        .replay_record_cap = 8,
        .full_text_preparation_ns = &probe.preparation_ns,
        .fragments = .{ .ptr = &probe, .supports = Probe.supports, .load = Probe.load, .save = Probe.save },
    };
    const source = replay_source_mod.Source.fromPrimaryStore(&store, null, null);
    {
        var executor = try initManual(a, source, &probe, Probe.apply, Probe.persist, Probe.truncate, null, null, null, null, null);
        defer executor.deinit(a);
        try executor.addWorker("ft", .{ .name = "ft", .kind = .full_text }, 0);
        try executor.waitForAllWithVisibilityWait(1, wait);
        // A slow continuation checkpoint must not shrink a cheap preparation
        // window. The fixed record cap still bounds this test to eight families.
        try std.testing.expectEqual(@as(usize, 16), budget.budget(0).records);
        try std.testing.expectEqual(@as(u64, 0), executor.appliedSequence("ft").?);
        try std.testing.expectEqual(@as(u64, 8), (try fragment.decode(&probe.encoded.?)).position.next_item);
        try std.testing.expectEqual(@as(u64, 0), probe.retired);
    }
    var replacement = try initManual(a, source, &probe, Probe.apply, Probe.persist, Probe.truncate, null, null, null, null, null);
    defer replacement.deinit(a);
    try replacement.addWorker("ft", .{ .name = "ft", .kind = .full_text }, probe.scalar);
    probe.fail_save = true;
    try std.testing.expectError(error.TestFragmentPersistFailure, replacement.waitForIndexesWithVisibilityWait(1, &.{"ft"}, wait));
    try std.testing.expectEqual(@as(u64, 0), replacement.appliedSequence("ft").?);
    try std.testing.expectEqual(@as(u64, 8), (try fragment.decode(&probe.encoded.?)).position.next_item);
    var turns: usize = 0;
    while (probe.scalar == 0 and turns < 128) : (turns += 1) try replacement.waitForIndexesWithVisibilityWait(1, &.{"ft"}, wait);
    try std.testing.expectEqual(@as(usize, 8), probe.max_documents);
    try std.testing.expectEqual(@as(u64, 1), probe.scalar);
    try std.testing.expectEqual(@as(u64, 1), probe.retired);
}

test "manual replay coalesces complete records atomically and recovers before the coarse mirror" {
    const a = std.testing.allocator;
    const fragment = @import("replay_fragment.zig");
    const journal = @import("change_journal.zig");
    var backend = @import("../../mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try @import("../../docstore.zig").DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    for (1..5) |sequence| {
        var buffer: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&buffer, "doc-{d}", .{sequence});
        const payload = try journal.encodeRecord(a, .{ .sequence = sequence, .changed_doc_keys = &.{key}, .deleted_doc_keys = &.{key}, .overwritten_doc_keys = &.{key}, .target_hints = &.{.full_text} });
        defer a.free(payload);
        try store.appendReplayOpaque(a, sequence, payload);
    }
    const Probe = struct {
        saved: ?fragment.Position = null,
        scalar: u64 = 0,
        seen: [4]usize = @splat(0),
        publications: usize = 0,
        fail_coarse: bool = true,
        fn supports(_: *anyopaque, _: []const u8) !bool {
            return true;
        }
        fn load(ptr: *anyopaque, _: []const u8) !?fragment.Position {
            return @as(*@This(), @ptrCast(@alignCast(ptr))).saved;
        }
        fn save(ptr: *anyopaque, _: []const u8, position: fragment.Position) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualDeep(self.saved.?, position);
        }
        fn ordinary(_: *anyopaque, _: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef, _: CatchUpSessionToken) !bool {
            return error.UnexpectedNonAtomicPublication;
        }
        fn publish(ptr: *anyopaque, batch: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef, proposed: fragment.Position) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 2), batch.documents.len);
            try std.testing.expectEqual(@as(usize, 2), batch.deleted_keys.len);
            try std.testing.expectEqual(@as(usize, 2), batch.overwritten_doc_keys.len);
            try std.testing.expectEqual(proposed.total_items, proposed.next_item);
            for (batch.documents) |doc| self.seen[(try std.fmt.parseInt(usize, doc.key[4..], 10)) - 1] += 1;
            self.publications += 1;
            var position = proposed;
            position.applied = true;
            self.saved = position;
            return true;
        }
        fn persist(ptr: *anyopaque, _: []const u8, sequence: u64, _: bool) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail_coarse) {
                self.fail_coarse = false;
                return error.TestCoarseMirrorFailure;
            }
            self.scalar = sequence;
            return true;
        }
        fn truncate(_: *anyopaque, _: u64) !void {}
    };
    var probe: Probe = .{};
    var budget: @import("replay_budget.zig").Controller = .{};
    const wait: VisibilityWait = .{ .replay_budget = &budget, .replay_record_cap = 8, .fragments = .{ .ptr = &probe, .supports = Probe.supports, .apply = Probe.publish, .load = Probe.load, .save = Probe.save } };
    const source = replay_source_mod.Source.fromPrimaryStore(&store, null, null);
    {
        var executor = try initManual(a, source, &probe, Probe.ordinary, Probe.persist, Probe.truncate, null, null, null, null, null);
        defer executor.deinit(a);
        try executor.addWorker("text", .{ .name = "text", .kind = .full_text }, 0);
        try std.testing.expectError(error.TestCoarseMirrorFailure, executor.waitForAllWithVisibilityWait(4, wait));
        try std.testing.expectEqual(@as(usize, 1), probe.publications);
        try std.testing.expectEqual(@as(u64, 2), probe.saved.?.sequence);
        try std.testing.expectEqual(@as(u64, 0), probe.scalar);
    }
    var replacement = try initManual(a, source, &probe, Probe.ordinary, Probe.persist, Probe.truncate, null, null, null, null, null);
    defer replacement.deinit(a);
    try replacement.addWorker("text", .{ .name = "text", .kind = .full_text }, 0);
    try replacement.waitForAllWithVisibilityWait(4, wait);
    try std.testing.expectEqual(@as(u64, 4), probe.scalar);
    try std.testing.expectEqual(@as(usize, 2), probe.publications);
    for (probe.seen) |count| try std.testing.expectEqual(@as(usize, 1), count);
}

test "manual replay canonical windows publish complete records and recover a lost mirror exactly once" {
    @import("antfly_test_error_logs").expectErrorLogs(1);
    const a = std.testing.allocator;
    const fragment = @import("replay_fragment.zig");
    const journal = @import("change_journal.zig");
    const chunks = @import("replay_chunks.zig");
    const references = @import("replay_reference.zig");
    const keys_mod = @import("../../internal_keys.zig");
    var backend = @import("../../mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try @import("../../docstore.zig").DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var keys: [384][]const u8 = undefined;
    for (&keys, 0..) |*key, i| key.* = try std.fmt.allocPrint(arena.allocator(), "doc-{d:0>4}", .{i});
    for (0..4) |ordinal| {
        const sequence = ordinal + 1;
        const record: journal.Record = .{ .sequence = sequence, .changed_doc_keys = keys[ordinal * 96 ..][0..96], .target_hints = &.{.full_text} };
        const payload = try journal.encodeRecord(a, record);
        defer a.free(payload);
        var prepared = (try chunks.prepare(a, record)).?;
        defer prepared.deinit();
        var write = try store.beginWriteBatch();
        errdefer write.abort();
        try write.setReplayOpaque(sequence, payload);
        const canonical = keys_mod.replayEntryKey(keys_mod.replay_all_kind, sequence);
        const hint = keys_mod.replayEntryKey(@intCast(@backingInt(@as(journal.TargetHint, .full_text))), sequence);
        try write.put(&canonical, &prepared.manifest.encode());
        try write.put(&hint, &references.encodeReference(sequence, .full_text));
        for (prepared.chunks, 0..) |raw, index| try write.put(&chunks.chunkKey(sequence, @intCast(index)), raw);
        try write.commit();
    }
    const Probe = struct {
        preparation_ns: std.atomic.Value(u64) = .init(0),
        physical: ?fragment.Position = null,
        scalar: u64 = 0,
        seen: [384]u8 = @splat(0),
        sizes: [2]usize = @splat(0),
        publications: usize = 0,
        fail_mirror: bool = true,
        fn supports(_: *anyopaque, _: []const u8) !bool {
            return true;
        }
        fn load(ptr: *anyopaque, _: []const u8) !?fragment.Position {
            return @as(*@This(), @ptrCast(@alignCast(ptr))).physical;
        }
        fn save(ptr: *anyopaque, _: []const u8, position: fragment.Position) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualDeep(self.physical.?, position);
            if (self.fail_mirror) {
                self.fail_mirror = false;
                return error.TestFragmentPersistFailure;
            }
        }
        fn ordinary(_: *anyopaque, _: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef, _: CatchUpSessionToken) !bool {
            return error.UnexpectedNonAtomicPublication;
        }
        fn publish(ptr: *anyopaque, batch: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef, proposed: fragment.Position) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.publications >= self.sizes.len) return error.UnexpectedExtraPublication;
            self.sizes[self.publications] = batch.documents.len;
            self.publications += 1;
            for (batch.documents) |doc| self.seen[try std.fmt.parseInt(usize, doc.key[4..], 10)] += 1;
            var position = proposed;
            position.applied = true;
            self.physical = position;
            return true;
        }
        fn persist(ptr: *anyopaque, _: []const u8, sequence: u64, _: bool) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.scalar = sequence;
            return true;
        }
        fn truncate(_: *anyopaque, _: u64) !void {}
    };
    var probe: Probe = .{};
    var budget: @import("replay_budget.zig").Controller = .{};
    budget.lanes[0].records = 192;
    const wait: VisibilityWait = .{ .replay_budget = &budget, .replay_record_cap = 192, .full_text_preparation_ns = &probe.preparation_ns, .fragments = .{ .ptr = &probe, .supports = Probe.supports, .apply = Probe.publish, .load = Probe.load, .save = Probe.save } };
    const source = replay_source_mod.Source.fromPrimaryStore(&store, null, null);
    {
        var executor = try initManual(a, source, &probe, Probe.ordinary, Probe.persist, Probe.truncate, null, null, null, null, null);
        defer executor.deinit(a);
        try executor.addWorker("text", .{ .name = "text", .kind = .full_text }, 0);
        try std.testing.expectError(error.TestFragmentPersistFailure, executor.waitForAllWithVisibilityWait(4, wait));
        try std.testing.expectEqual(@as(u64, 2), probe.physical.?.sequence);
        try std.testing.expectEqual(@as(u64, 96), probe.physical.?.next_item);
        try std.testing.expectEqual(@as(u64, 96), probe.physical.?.total_items);
        try std.testing.expectEqual(@as(u64, 0), probe.scalar);
    }
    var replacement = try initManual(a, source, &probe, Probe.ordinary, Probe.persist, Probe.truncate, null, null, null, null, null);
    defer replacement.deinit(a);
    try replacement.addWorker("text", .{ .name = "text", .kind = .full_text }, 0);
    for (0..4) |_| if (probe.scalar < 4) try replacement.waitForAllWithVisibilityWait(4, wait);
    try std.testing.expectEqual(@as(u64, 4), probe.scalar);
    try std.testing.expectEqual(@as(usize, 2), probe.publications);
    try std.testing.expectEqualDeep([_]usize{ 192, 192 }, probe.sizes);
    for (probe.seen) |seen| try std.testing.expectEqual(@as(u8, 1), seen);
}

test "manual replay feedback includes staging and coarse progress on both wait paths" {
    const a = std.testing.allocator;
    const fragment = @import("replay_fragment.zig");
    var backend = @import("../../mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try @import("../../docstore.zig").DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const payload = try @import("change_journal.zig").encodeRecord(a, .{ .sequence = 1, .changed_doc_keys = &.{"doc"}, .target_hints = &.{.full_text} });
    defer a.free(payload);
    try store.appendReplayOpaque(a, 1, payload);
    const Probe = struct {
        coarse_cost: bool,
        preparation: std.atomic.Value(u64) = .init(0),
        lease_time: std.atomic.Value(u64) = .init(0),
        saved: ?fragment.Position = null,
        fn supports(_: *anyopaque, _: []const u8) !bool {
            return true;
        }
        fn load(ptr: *anyopaque, _: []const u8) !?fragment.Position {
            return @as(*@This(), @ptrCast(@alignCast(ptr))).saved;
        }
        fn save(_: *anyopaque, _: []const u8, _: fragment.Position) !void {}
        fn ordinary(_: *anyopaque, _: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef, _: CatchUpSessionToken) !bool {
            return error.UnexpectedNonAtomicPublication;
        }
        fn publish(ptr: *anyopaque, _: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef, proposed: fragment.Position) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.preparation.fetchAdd(std.time.ns_per_ms, .monotonic);
            // Artifact staging/sync outside a writer lease still costs one
            // fixed interval per publication. Its lease counter stays zero.
            if (!self.coarse_cost) try std.testing.io.sleep(.fromMilliseconds(60), .awake);
            var position = proposed;
            position.applied = true;
            self.saved = position;
            return true;
        }
        fn persist(ptr: *anyopaque, _: []const u8, _: u64, _: bool) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.coarse_cost) try std.testing.io.sleep(.fromMilliseconds(60), .awake);
            return true;
        }
        fn truncate(_: *anyopaque, _: u64) !void {}
    };
    for ([_]bool{ false, true }) |coarse_cost| {
        for ([_]bool{ false, true }) |selected| {
            var probe: Probe = .{ .coarse_cost = coarse_cost };
            var budget: @import("replay_budget.zig").Controller = .{};
            var executor = try initManual(a, replay_source_mod.Source.fromPrimaryStore(&store, null, null), &probe, Probe.ordinary, Probe.persist, Probe.truncate, null, null, null, null, null);
            defer executor.deinit(a);
            try executor.addWorker("text", .{ .name = "text", .kind = .full_text }, 0);
            const wait: VisibilityWait = .{ .replay_budget = &budget, .full_text_preparation_ns = &probe.preparation, .publication_overhead_ns = &probe.lease_time, .fragments = .{ .ptr = &probe, .supports = Probe.supports, .apply = Probe.publish, .load = Probe.load, .save = Probe.save } };
            if (selected) try executor.waitForIndexesWithVisibilityWait(1, &.{"text"}, wait) else try executor.waitForAllWithVisibilityWait(1, wait);
            try std.testing.expectEqual(@as(u64, 0), probe.lease_time.load(.monotonic));
            try std.testing.expect(budget.snapshot().effective_quantum_ns[0] > @import("replay_budget.zig").Controller.quantum_ns);
        }
    }
}

test "manual replay adopts atomic native progress without replay persistence or early retirement" {
    const a = std.testing.allocator;
    var backend = @import("../../mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try @import("../../docstore.zig").DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    defer store.close();
    const payload = try @import("change_journal.zig").encodeRecord(a, .{ .sequence = 8, .changed_doc_keys = &.{"document"}, .target_hints = &.{.full_text} });
    defer a.free(payload);
    try store.appendReplayOpaque(a, 8, payload);
    const Probe = struct {
        retired: u64 = 0,
        fn apply(_: *anyopaque, _: derived_types.DerivedBatch, _: index_manager_mod.ManagedIndexRef, _: CatchUpSessionToken) !bool {
            return error.UnexpectedDuplicateReplay;
        }
        fn persist(_: *anyopaque, _: []const u8, _: u64, _: bool) !bool {
            return error.UnexpectedDuplicatePersistence;
        }
        fn truncate(raw: *anyopaque, sequence: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.retired = sequence;
        }
    };
    var probe: Probe = .{};
    var executor = try initManual(a, replay_source_mod.Source.fromPrimaryStore(&store, null, null), &probe, Probe.apply, Probe.persist, Probe.truncate, null, null, null, null, null);
    defer executor.deinit(a);
    try executor.addWorker("text", .{ .name = "text", .kind = .full_text }, 0);
    executor.adoptCommittedSequence("text", 8);
    executor.adoptCommittedSequence("text", 4);
    executor.adoptCommittedSequence("absent", 10);
    try std.testing.expectEqual(@as(?u64, 8), executor.appliedSequence("text"));
    try std.testing.expectEqual(@as(u64, 0), probe.retired);
    try executor.waitForIndexes(8, &.{"text"});
    try std.testing.expectEqual(@as(u64, 8), probe.retired);
}
