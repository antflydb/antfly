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
const metadata = @import("../metadata/mod.zig");
const shard_ops = @import("shard_ops.zig");
const transition_runtime = @import("transition_runtime.zig");
const data = @import("../data/mod.zig");
const platform_time = @import("antfly_platform").time;
const raft_state_machine = @import("state_machine/mod.zig");

const transition_retry_initial_ms: u64 = 100;
const transition_retry_max_ms: u64 = 5_000;
const transition_observation_refresh_ms: u64 = 250;
const max_completed_transition_observations: usize = 1_024;

const TransitionRetry = struct {
    failures: u8,
    retry_at_ms: u64,
};

const CachedSplitObservation = struct {
    transition_id: u64,
    attempt_epoch: u64,
    observed_at_ms: u64,
    observation: metadata.SplitObservation,
};

const CachedMergeObservation = struct {
    transition_id: u64,
    observed_at_ms: u64,
    observation: metadata.MergeObservation,
};

const CompletedSplitObservation = struct {
    transition_id: u64,
    attempt_epoch: u64,
    observation: metadata.SplitObservation,
};

pub const RetryClock = struct {
    ptr: ?*anyopaque = null,
    now_ms_fn: *const fn (?*anyopaque) u64,

    pub fn real() RetryClock {
        return .{ .now_ms_fn = realMonotonicMillis };
    }

    pub fn nowMs(self: RetryClock) u64 {
        return self.now_ms_fn(self.ptr);
    }
};

pub const TransitionServiceMetrics = struct {
    queued_split_transitions: usize = 0,
    queued_merge_transitions: usize = 0,
    stepped_split_transitions: usize = 0,
    stepped_merge_transitions: usize = 0,
    completed_split_transitions: usize = 0,
    completed_merge_transitions: usize = 0,
    awaiting_split_source_start: usize = 0,
    bootstrapping_split_destination: usize = 0,
    split_replay_blocked: usize = 0,
    split_ready_to_finalize: usize = 0,
    awaiting_merge_receiver_acceptance: usize = 0,
    bootstrapping_merge_receiver: usize = 0,
    merge_replay_blocked: usize = 0,
    merge_ready_to_finalize: usize = 0,
};

pub const TransitionStepResult = struct {
    stepped_split: usize = 0,
    stepped_merge: usize = 0,
    completed_split: usize = 0,
    completed_merge: usize = 0,
    awaiting_split_source_start: usize = 0,
    bootstrapping_split_destination: usize = 0,
    split_replay_blocked: usize = 0,
    split_ready_to_finalize: usize = 0,
    awaiting_merge_receiver_acceptance: usize = 0,
    bootstrapping_merge_receiver: usize = 0,
    merge_replay_blocked: usize = 0,
    merge_ready_to_finalize: usize = 0,
};

pub const TransitionService = struct {
    alloc: std.mem.Allocator,
    retry_clock: RetryClock,
    ops: union(enum) {
        runtime: transition_runtime.TransitionRuntime,
        adapter: shard_ops.OwnedShardOperationAdapter,
    },
    pending_split: std.ArrayListUnmanaged(metadata.SplitTransitionRecord) = .empty,
    pending_merge: std.ArrayListUnmanaged(metadata.MergeTransitionRecord) = .empty,
    cached_split_observations: std.AutoHashMapUnmanaged(u64, CachedSplitObservation) = .empty,
    cached_merge_observations: std.AutoHashMapUnmanaged(u64, CachedMergeObservation) = .empty,
    completed_split_observations: std.ArrayListUnmanaged(CompletedSplitObservation) = .empty,
    completed_merge_observations: std.ArrayListUnmanaged(metadata.MergeRuntimeObservation) = .empty,
    completed_split_evict_index: usize = 0,
    completed_merge_evict_index: usize = 0,
    split_retries: std.AutoHashMapUnmanaged(u64, TransitionRetry) = .empty,
    merge_retries: std.AutoHashMapUnmanaged(u64, TransitionRetry) = .empty,
    metrics: TransitionServiceMetrics = .{},

    pub fn init(alloc: std.mem.Allocator, ops: anytype) !TransitionService {
        return try initWithRetryClock(alloc, ops, RetryClock.real());
    }

    pub fn initWithRetryClock(
        alloc: std.mem.Allocator,
        ops: anytype,
        retry_clock: RetryClock,
    ) !TransitionService {
        const OpsType = @TypeOf(ops);
        return .{
            .alloc = alloc,
            .retry_clock = retry_clock,
            .ops = if (@hasField(OpsType, "ptr") and @hasField(OpsType, "vtable"))
                .{ .adapter = try shard_ops.OwnedShardOperationAdapter.init(alloc, .{
                    .ptr = ops.ptr,
                    .vtable = ops.vtable,
                }) }
            else
                .{ .runtime = .{
                    .split = if (@hasField(OpsType, "split")) ops.split else null,
                    .merge = if (@hasField(OpsType, "merge")) ops.merge else null,
                } },
        };
    }

    pub fn deinit(self: *TransitionService) void {
        switch (self.ops) {
            .runtime => {},
            .adapter => |*adapter| adapter.deinit(),
        }
        for (self.pending_split.items) |*record| deinitSplitRecord(self.alloc, record);
        self.pending_split.deinit(self.alloc);
        for (self.pending_merge.items) |*record| deinitMergeRecord(self.alloc, record);
        self.pending_merge.deinit(self.alloc);
        self.cached_split_observations.deinit(self.alloc);
        self.cached_merge_observations.deinit(self.alloc);
        self.completed_split_observations.deinit(self.alloc);
        self.completed_merge_observations.deinit(self.alloc);
        self.split_retries.deinit(self.alloc);
        self.merge_retries.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn submitSplit(self: *TransitionService, record: metadata.SplitTransitionRecord) !void {
        if (findCompletedSplitIndex(self.completed_split_observations.items, record.transition_id)) |index| {
            if (self.completed_split_observations.items[index].attempt_epoch == record.attempt_epoch) {
                // A completed attempt is idempotent. Delayed controller
                // records must not resurrect work the local executor already
                // drove to a terminal shard state.
                return;
            }
            _ = self.removeCompletedSplit(record.transition_id);
        }
        if (findSplitIndex(self.pending_split.items, record.transition_id)) |index| {
            const previous = self.pending_split.items[index];
            var effective_record = record;
            const observation_context_changed =
                previous.attempt_epoch != record.attempt_epoch or
                previous.source_group_id != record.source_group_id or
                previous.destination_group_id != record.destination_group_id;
            const rollback_requested =
                previous.rollback_reason == null and record.rollback_reason != null;
            if (!observation_context_changed) {
                if (previous.phase == .finalized or previous.phase == .rolled_back) {
                    // Terminal shard state is immutable for one attempt.
                    effective_record.phase = previous.phase;
                    effective_record.rollback_reason = previous.rollback_reason;
                } else {
                    // A rollback request is a monotonic safety latch. A later
                    // projection may lag or omit it, but must never revive the
                    // forward path for the same attempt.
                    if (previous.rollback_reason != null and
                        effective_record.rollback_reason == null)
                    {
                        effective_record.rollback_reason = previous.rollback_reason;
                    }
                    if (@intFromEnum(effective_record.phase) < @intFromEnum(previous.phase)) {
                        // Controller snapshots can lag the executor phase
                        // while carrying a newly committed rollback request.
                        // Preserve local progress while adopting that intent.
                        if (!rollback_requested) return;
                        effective_record.phase = previous.phase;
                    }
                }
            }
            const phase_changed = previous.phase != effective_record.phase;
            var replacement = try cloneSplitRecord(self.alloc, effective_record);
            errdefer deinitSplitRecord(self.alloc, &replacement);
            deinitSplitRecord(self.alloc, &self.pending_split.items[index]);
            self.pending_split.items[index] = replacement;
            // Observations describe shard reality and remain valid while the
            // replicated controller phase catches up. A new attempt or a
            // changed shard identity requires a fresh observation.
            if (observation_context_changed) _ = self.removeCachedSplit(record.transition_id);
            if (observation_context_changed or phase_changed or rollback_requested)
                _ = self.split_retries.remove(record.transition_id);
        } else {
            _ = self.removeCachedSplit(record.transition_id);
            var owned = try cloneSplitRecord(self.alloc, record);
            errdefer deinitSplitRecord(self.alloc, &owned);
            try self.pending_split.append(self.alloc, owned);
        }
        self.metrics.queued_split_transitions = self.pending_split.items.len;
    }

    pub fn submitMerge(self: *TransitionService, record: metadata.MergeTransitionRecord) !void {
        // Merge transition IDs are immutable operation identities. Once an ID
        // is terminal, replayed metadata for that ID is a no-op.
        if (findCompletedMergeIndex(self.completed_merge_observations.items, record.transition_id) != null)
            return;
        if (findMergeIndex(self.pending_merge.items, record.transition_id)) |index| {
            const previous = self.pending_merge.items[index];
            var effective_record = record;
            const observation_context_changed =
                previous.donor_group_id != record.donor_group_id or
                previous.receiver_group_id != record.receiver_group_id;
            const rollback_requested =
                previous.rollback_reason == null and record.rollback_reason != null;
            if (!observation_context_changed) {
                effective_record.allow_doc_identity_reassignment =
                    previous.allow_doc_identity_reassignment or
                    record.allow_doc_identity_reassignment;
                if (previous.phase == .finalized or previous.phase == .rolled_back) {
                    effective_record.phase = previous.phase;
                    effective_record.rollback_reason = previous.rollback_reason;
                } else {
                    if (previous.rollback_reason != null and
                        effective_record.rollback_reason == null)
                    {
                        effective_record.rollback_reason = previous.rollback_reason;
                    }
                    if (@intFromEnum(effective_record.phase) < @intFromEnum(previous.phase)) {
                        if (!rollback_requested) return;
                        effective_record.phase = previous.phase;
                    }
                }
            }
            const phase_changed = previous.phase != effective_record.phase;
            var replacement = try cloneMergeRecord(self.alloc, effective_record);
            errdefer deinitMergeRecord(self.alloc, &replacement);
            deinitMergeRecord(self.alloc, &self.pending_merge.items[index]);
            self.pending_merge.items[index] = replacement;
            if (observation_context_changed) {
                _ = self.removeCachedMerge(record.transition_id);
            }
            if (observation_context_changed or phase_changed or rollback_requested) {
                _ = self.merge_retries.remove(record.transition_id);
            }
        } else {
            _ = self.removeCachedMerge(record.transition_id);
            var owned = try cloneMergeRecord(self.alloc, record);
            errdefer deinitMergeRecord(self.alloc, &owned);
            try self.pending_merge.append(self.alloc, owned);
        }
        self.metrics.queued_merge_transitions = self.pending_merge.items.len;
    }

    pub fn removeSplit(self: *TransitionService, transition_id: u64) bool {
        _ = self.split_retries.remove(transition_id);
        const removed_cached = self.removeCachedSplit(transition_id);
        if (findSplitIndex(self.pending_split.items, transition_id)) |index| {
            var removed = self.pending_split.orderedRemove(index);
            deinitSplitRecord(self.alloc, &removed);
            self.metrics.queued_split_transitions = self.pending_split.items.len;
            return true;
        }
        // Keep the bounded terminal snapshot after metadata compacts the
        // transition record. A same-id resubmission explicitly invalidates it.
        return removed_cached;
    }

    pub fn removeMerge(self: *TransitionService, transition_id: u64) bool {
        _ = self.merge_retries.remove(transition_id);
        const removed_cached = self.removeCachedMerge(transition_id);
        if (findMergeIndex(self.pending_merge.items, transition_id)) |index| {
            var removed = self.pending_merge.orderedRemove(index);
            deinitMergeRecord(self.alloc, &removed);
            self.metrics.queued_merge_transitions = self.pending_merge.items.len;
            return true;
        }
        return removed_cached;
    }

    pub fn hasCompletedSplit(self: *const TransitionService, transition_id: u64) bool {
        return findCompletedSplitIndex(self.completed_split_observations.items, transition_id) != null;
    }

    pub fn hasCompletedMerge(self: *const TransitionService, transition_id: u64) bool {
        return findCompletedMergeIndex(self.completed_merge_observations.items, transition_id) != null;
    }

    /// Returns only the latest observation published by the transition
    /// driver. Status and planning paths must never execute shard I/O.
    pub fn observeSplit(self: *const TransitionService, transition_id: u64) ?metadata.SplitObservation {
        if (findSplitIndex(self.pending_split.items, transition_id)) |index| {
            const record = self.pending_split.items[index];
            const cached = self.cached_split_observations.get(transition_id) orelse return null;
            if (cached.attempt_epoch != record.attempt_epoch) return null;
            return cached.observation;
        }
        if (findCompletedSplitIndex(self.completed_split_observations.items, transition_id)) |index| {
            return self.completed_split_observations.items[index].observation;
        }
        return null;
    }

    pub fn splitRecord(self: *TransitionService, transition_id: u64) ?metadata.SplitTransitionRecord {
        const index = findSplitIndex(self.pending_split.items, transition_id) orelse return null;
        return self.pending_split.items[index];
    }

    pub fn describeSplit(self: *const TransitionService, transition_id: u64) ?metadata.SplitExecutionState {
        const index = findSplitIndex(self.pending_split.items, transition_id) orelse return null;
        const record = self.pending_split.items[index];
        const observation = self.observeSplit(transition_id) orelse return null;
        return metadata.TransitionController.describeSplit(record, observation);
    }

    /// Returns only the latest observation published by the transition
    /// driver. Status and planning paths must never execute shard I/O.
    pub fn observeMerge(self: *const TransitionService, transition_id: u64) ?metadata.MergeObservation {
        if (findMergeIndex(self.pending_merge.items, transition_id) != null) {
            const cached = self.cached_merge_observations.get(transition_id) orelse return null;
            return cached.observation;
        }
        if (findCompletedMergeIndex(self.completed_merge_observations.items, transition_id)) |index| {
            return self.completed_merge_observations.items[index].observation;
        }
        return null;
    }

    pub fn mergeRecord(self: *TransitionService, transition_id: u64) ?metadata.MergeTransitionRecord {
        const index = findMergeIndex(self.pending_merge.items, transition_id) orelse return null;
        return self.pending_merge.items[index];
    }

    pub fn describeMerge(self: *const TransitionService, transition_id: u64) ?metadata.MergeExecutionState {
        const index = findMergeIndex(self.pending_merge.items, transition_id) orelse return null;
        const record = self.pending_merge.items[index];
        const observation = self.observeMerge(transition_id) orelse return null;
        return metadata.TransitionController.describeMerge(record, observation);
    }

    pub fn shardOperationAdapter(self: *TransitionService) shard_ops.ShardOperationAdapter {
        return switch (self.ops) {
            .runtime => |*runtime| runtime.shardOperationAdapter(),
            .adapter => |*adapter| adapter.adapter(),
        };
    }

    pub fn operationRegistration(
        self: *TransitionService,
    ) ?shard_ops.OwnedShardOperationAdapter.Registration {
        return switch (self.ops) {
            .runtime => null,
            .adapter => |*adapter| adapter.registration(),
        };
    }

    /// Refreshes read-only transition observations outside status and planning
    /// request paths. Non-authority replicas call this on their normal control
    /// cadence so a newly elected authority already owns a bounded, recent
    /// snapshot and never needs to execute shard I/O from an HTTP read.
    pub fn refreshPendingObservations(self: *TransitionService) !void {
        const now_ms = self.retry_clock.nowMs();
        const runtime = self.metadataRuntime();

        for (self.pending_split.items) |record| {
            if (splitObservationFresh(&self.cached_split_observations, record, now_ms)) continue;
            const observation = runtime.observeSplit(record) catch |err| {
                std.log.warn("split transition background observation failed transition_id={d} err={s}", .{
                    record.transition_id,
                    @errorName(err),
                });
                continue;
            };
            try self.rememberCachedSplitObservation(
                record.transition_id,
                record.attempt_epoch,
                now_ms,
                observation,
            );
        }

        for (self.pending_merge.items) |record| {
            if (mergeObservationFresh(&self.cached_merge_observations, record.transition_id, now_ms)) continue;
            const observation = runtime.observeMerge(record) catch |err| {
                std.log.warn("merge transition background observation failed transition_id={d} err={s}", .{
                    record.transition_id,
                    @errorName(err),
                });
                continue;
            };
            try self.rememberCachedMergeObservation(record.transition_id, now_ms, observation);
        }
    }

    pub fn stepPending(self: *TransitionService) !TransitionStepResult {
        var result = TransitionStepResult{};
        const now_ms = self.retry_clock.nowMs();
        const runtime = self.metadataRuntime();

        for (self.pending_split.items) |*record| {
            if (record.phase == .finalized or record.phase == .rolled_back) continue;
            if (retryPending(&self.split_retries, record.transition_id, now_ms)) continue;
            const observation = runtime.observeSplit(record.*) catch |err| {
                try recordRetry(self.alloc, &self.split_retries, record.transition_id, now_ms);
                std.log.warn("split transition observation failed transition_id={d} err={s}", .{ record.transition_id, @errorName(err) });
                continue;
            };
            try self.rememberCachedSplitObservation(
                record.transition_id,
                record.attempt_epoch,
                now_ms,
                observation,
            );
            const state = metadata.TransitionController.describeSplit(record.*, observation);
            switch (state.tag) {
                .awaiting_source_start => result.awaiting_split_source_start += 1,
                .bootstrapping_destination => result.bootstrapping_split_destination += 1,
                .replay_blocked => result.split_replay_blocked += 1,
                .ready_to_finalize => result.split_ready_to_finalize += 1,
                else => {},
            }
            _ = metadata.TransitionDriver.stepSplitObserved(runtime, record, observation) catch |err| {
                try recordRetry(self.alloc, &self.split_retries, record.transition_id, now_ms);
                std.log.warn("split transition step failed transition_id={d} phase={s} err={s}", .{ record.transition_id, @tagName(record.phase), @errorName(err) });
                continue;
            };
            _ = self.split_retries.remove(record.transition_id);
            const updated_observation: ?metadata.SplitObservation = runtime.observeSplit(record.*) catch |err| blk: {
                std.log.warn("split transition post-step observation failed transition_id={d} err={s}", .{ record.transition_id, @errorName(err) });
                break :blk null;
            };
            if (updated_observation) |updated| {
                try self.rememberCachedSplitObservation(
                    record.transition_id,
                    record.attempt_epoch,
                    now_ms,
                    updated,
                );
                if (record.phase == .finalized or record.phase == .rolled_back) {
                    try self.rememberCompletedSplitObservation(
                        record.transition_id,
                        record.attempt_epoch,
                        updated,
                    );
                }
            }
            result.stepped_split += 1;
        }

        for (self.pending_merge.items) |*record| {
            if (record.phase == .finalized or record.phase == .rolled_back) continue;
            if (retryPending(&self.merge_retries, record.transition_id, now_ms)) continue;
            const observation = runtime.observeMerge(record.*) catch |err| {
                try recordRetry(self.alloc, &self.merge_retries, record.transition_id, now_ms);
                std.log.warn("merge transition observation failed transition_id={d} err={s}", .{ record.transition_id, @errorName(err) });
                continue;
            };
            try self.rememberCachedMergeObservation(record.transition_id, now_ms, observation);
            const state = metadata.TransitionController.describeMerge(record.*, observation);
            switch (state.tag) {
                .awaiting_receiver_acceptance => result.awaiting_merge_receiver_acceptance += 1,
                .bootstrapping_receiver => result.bootstrapping_merge_receiver += 1,
                .replay_blocked => result.merge_replay_blocked += 1,
                .ready_to_finalize => result.merge_ready_to_finalize += 1,
                else => {},
            }
            _ = metadata.TransitionDriver.stepMergeObserved(runtime, record, observation) catch |err| {
                try recordRetry(self.alloc, &self.merge_retries, record.transition_id, now_ms);
                std.log.warn("merge transition step failed transition_id={d} phase={s} err={s}", .{ record.transition_id, @tagName(record.phase), @errorName(err) });
                continue;
            };
            _ = self.merge_retries.remove(record.transition_id);
            const updated_observation: ?metadata.MergeObservation = runtime.observeMerge(record.*) catch |err| blk: {
                std.log.warn("merge transition post-step observation failed transition_id={d} err={s}", .{ record.transition_id, @errorName(err) });
                break :blk null;
            };
            if (updated_observation) |updated| {
                try self.rememberCachedMergeObservation(
                    record.transition_id,
                    now_ms,
                    updated,
                );
                if (record.phase == .finalized or record.phase == .rolled_back) {
                    try self.rememberCompletedMergeObservation(record.transition_id, updated);
                }
            }
            result.stepped_merge += 1;
        }

        // Terminal records can arrive through metadata replication without
        // having been executed by this replica. Refresh them once before
        // compaction and retain the observation independently of the pending
        // record so status remains truthful after the queue is drained.
        try self.refreshTerminalObservations(runtime, now_ms);
        try self.preserveTerminalObservations();
        result.completed_split = self.compactCompletedSplits();
        result.completed_merge = self.compactCompletedMerges();
        self.metrics.stepped_split_transitions += result.stepped_split;
        self.metrics.stepped_merge_transitions += result.stepped_merge;
        self.metrics.completed_split_transitions += result.completed_split;
        self.metrics.completed_merge_transitions += result.completed_merge;
        self.metrics.awaiting_split_source_start += result.awaiting_split_source_start;
        self.metrics.bootstrapping_split_destination += result.bootstrapping_split_destination;
        self.metrics.split_replay_blocked += result.split_replay_blocked;
        self.metrics.split_ready_to_finalize += result.split_ready_to_finalize;
        self.metrics.awaiting_merge_receiver_acceptance += result.awaiting_merge_receiver_acceptance;
        self.metrics.bootstrapping_merge_receiver += result.bootstrapping_merge_receiver;
        self.metrics.merge_replay_blocked += result.merge_replay_blocked;
        self.metrics.merge_ready_to_finalize += result.merge_ready_to_finalize;
        self.metrics.queued_split_transitions = self.pending_split.items.len;
        self.metrics.queued_merge_transitions = self.pending_merge.items.len;
        return result;
    }

    fn metadataRuntime(self: *TransitionService) metadata.MetadataTransitionRuntime {
        return .{
            .ptr = self,
            .vtable = &.{
                .observe_split = observeSplitMeta,
                .observe_merge = observeMergeMeta,
                .execute = executeMeta,
            },
        };
    }

    fn observeSplitMeta(ptr: *anyopaque, record: metadata.SplitTransitionRecord) !metadata.SplitObservation {
        const self: *TransitionService = @ptrCast(@alignCast(ptr));
        return switch (self.ops) {
            .runtime => |*runtime| try runtime.metadataRuntime().observeSplit(record),
            .adapter => |*adapter| try adapter.adapter().observeSplit(record),
        };
    }

    fn observeMergeMeta(ptr: *anyopaque, record: metadata.MergeTransitionRecord) !metadata.MergeObservation {
        const self: *TransitionService = @ptrCast(@alignCast(ptr));
        return switch (self.ops) {
            .runtime => |*runtime| try runtime.metadataRuntime().observeMerge(record),
            .adapter => |*adapter| try adapter.adapter().observeMerge(record),
        };
    }

    fn executeMeta(ptr: *anyopaque, action: metadata.TransitionAction) !void {
        const self: *TransitionService = @ptrCast(@alignCast(ptr));
        switch (self.ops) {
            .runtime => |*runtime| try runtime.metadataRuntime().execute(action),
            .adapter => |*adapter| try adapter.adapter().execute(action),
        }
    }

    fn refreshTerminalObservations(
        self: *TransitionService,
        runtime: metadata.MetadataTransitionRuntime,
        now_ms: u64,
    ) !void {
        for (self.pending_split.items) |record| {
            if (record.phase != .finalized and record.phase != .rolled_back) continue;
            if (self.cached_split_observations.get(record.transition_id)) |cached| {
                if (cached.attempt_epoch == record.attempt_epoch and
                    observationFresh(cached.observed_at_ms, now_ms) and
                    splitObservationMatchesTerminalRecord(record, cached.observation))
                {
                    continue;
                }
            }
            const observation = runtime.observeSplit(record) catch |err| {
                std.log.warn("terminal split transition observation failed transition_id={d} err={s}", .{
                    record.transition_id,
                    @errorName(err),
                });
                continue;
            };
            try self.rememberCachedSplitObservation(
                record.transition_id,
                record.attempt_epoch,
                now_ms,
                observation,
            );
        }

        for (self.pending_merge.items) |record| {
            if (record.phase != .finalized and record.phase != .rolled_back) continue;
            if (self.cached_merge_observations.get(record.transition_id)) |cached| {
                if (observationFresh(cached.observed_at_ms, now_ms) and
                    mergeObservationMatchesTerminalRecord(record, cached.observation))
                {
                    continue;
                }
            }
            const observation = runtime.observeMerge(record) catch |err| {
                std.log.warn("terminal merge transition observation failed transition_id={d} err={s}", .{
                    record.transition_id,
                    @errorName(err),
                });
                continue;
            };
            try self.rememberCachedMergeObservation(record.transition_id, now_ms, observation);
        }
    }

    fn preserveTerminalObservations(self: *TransitionService) !void {
        for (self.pending_split.items) |record| {
            if (record.phase != .finalized and record.phase != .rolled_back) continue;
            const cached = self.cached_split_observations.get(record.transition_id) orelse continue;
            if (cached.attempt_epoch != record.attempt_epoch) continue;
            if (!splitObservationMatchesTerminalRecord(record, cached.observation)) continue;
            try self.rememberCompletedSplitObservation(
                record.transition_id,
                record.attempt_epoch,
                cached.observation,
            );
        }

        for (self.pending_merge.items) |record| {
            if (record.phase != .finalized and record.phase != .rolled_back) continue;
            const cached = self.cached_merge_observations.get(record.transition_id) orelse continue;
            if (!mergeObservationMatchesTerminalRecord(record, cached.observation)) continue;
            try self.rememberCompletedMergeObservation(
                record.transition_id,
                cached.observation,
            );
        }
    }

    fn compactCompletedSplits(self: *TransitionService) usize {
        var write_index: usize = 0;
        var removed: usize = 0;
        for (self.pending_split.items, 0..) |record, read_index| {
            const completed = if (findCompletedSplitIndex(
                self.completed_split_observations.items,
                record.transition_id,
            )) |index| blk: {
                const observation = self.completed_split_observations.items[index];
                break :blk observation.attempt_epoch == record.attempt_epoch and
                    splitObservationMatchesTerminalRecord(record, observation.observation);
            } else false;
            if (completed) {
                _ = self.split_retries.remove(record.transition_id);
                _ = self.removeCachedSplit(record.transition_id);
                var doomed = record;
                deinitSplitRecord(self.alloc, &doomed);
                removed += 1;
                continue;
            }
            if (write_index != read_index) self.pending_split.items[write_index] = record;
            write_index += 1;
        }
        self.pending_split.items.len = write_index;
        return removed;
    }

    fn compactCompletedMerges(self: *TransitionService) usize {
        var write_index: usize = 0;
        var removed: usize = 0;
        for (self.pending_merge.items, 0..) |record, read_index| {
            const completed = if (findCompletedMergeIndex(
                self.completed_merge_observations.items,
                record.transition_id,
            )) |index|
                mergeObservationMatchesTerminalRecord(
                    record,
                    self.completed_merge_observations.items[index].observation,
                )
            else
                false;
            if (completed) {
                _ = self.merge_retries.remove(record.transition_id);
                _ = self.removeCachedMerge(record.transition_id);
                var doomed = record;
                deinitMergeRecord(self.alloc, &doomed);
                removed += 1;
                continue;
            }
            if (write_index != read_index) self.pending_merge.items[write_index] = record;
            write_index += 1;
        }
        self.pending_merge.items.len = write_index;
        return removed;
    }

    fn rememberCompletedSplitObservation(
        self: *TransitionService,
        transition_id: u64,
        attempt_epoch: u64,
        observation: metadata.SplitObservation,
    ) !void {
        if (findCompletedSplitIndex(self.completed_split_observations.items, transition_id)) |index| {
            self.completed_split_observations.items[index].attempt_epoch = attempt_epoch;
            self.completed_split_observations.items[index].observation = observation;
            return;
        }
        if (self.completed_split_observations.items.len == max_completed_transition_observations) {
            self.completed_split_observations.items[self.completed_split_evict_index] = .{
                .transition_id = transition_id,
                .attempt_epoch = attempt_epoch,
                .observation = observation,
            };
            self.completed_split_evict_index =
                (self.completed_split_evict_index + 1) % max_completed_transition_observations;
            return;
        }
        try self.completed_split_observations.append(self.alloc, .{
            .transition_id = transition_id,
            .attempt_epoch = attempt_epoch,
            .observation = observation,
        });
    }

    fn rememberCachedSplitObservation(
        self: *TransitionService,
        transition_id: u64,
        attempt_epoch: u64,
        observed_at_ms: u64,
        observation: metadata.SplitObservation,
    ) !void {
        try self.cached_split_observations.put(self.alloc, transition_id, .{
            .transition_id = transition_id,
            .attempt_epoch = attempt_epoch,
            .observed_at_ms = observed_at_ms,
            .observation = observation,
        });
    }

    fn rememberCachedMergeObservation(
        self: *TransitionService,
        transition_id: u64,
        observed_at_ms: u64,
        observation: metadata.MergeObservation,
    ) !void {
        try self.cached_merge_observations.put(self.alloc, transition_id, .{
            .transition_id = transition_id,
            .observed_at_ms = observed_at_ms,
            .observation = observation,
        });
    }

    fn rememberCompletedMergeObservation(
        self: *TransitionService,
        transition_id: u64,
        observation: metadata.MergeObservation,
    ) !void {
        if (findCompletedMergeIndex(self.completed_merge_observations.items, transition_id)) |index| {
            self.completed_merge_observations.items[index].observation = observation;
            return;
        }
        if (self.completed_merge_observations.items.len == max_completed_transition_observations) {
            self.completed_merge_observations.items[self.completed_merge_evict_index] = .{
                .transition_id = transition_id,
                .observation = observation,
            };
            self.completed_merge_evict_index =
                (self.completed_merge_evict_index + 1) % max_completed_transition_observations;
            return;
        }
        try self.completed_merge_observations.append(self.alloc, .{
            .transition_id = transition_id,
            .observation = observation,
        });
    }

    fn removeCompletedSplit(self: *TransitionService, transition_id: u64) bool {
        const index = findCompletedSplitIndex(self.completed_split_observations.items, transition_id) orelse return false;
        _ = self.completed_split_observations.orderedRemove(index);
        return true;
    }

    fn removeCompletedMerge(self: *TransitionService, transition_id: u64) bool {
        const index = findCompletedMergeIndex(self.completed_merge_observations.items, transition_id) orelse return false;
        _ = self.completed_merge_observations.orderedRemove(index);
        return true;
    }

    fn removeCachedSplit(self: *TransitionService, transition_id: u64) bool {
        return self.cached_split_observations.remove(transition_id);
    }

    fn removeCachedMerge(self: *TransitionService, transition_id: u64) bool {
        return self.cached_merge_observations.remove(transition_id);
    }
};

fn realMonotonicMillis(_: ?*anyopaque) u64 {
    return @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
}

fn retryPending(retries: *const std.AutoHashMapUnmanaged(u64, TransitionRetry), transition_id: u64, now_ms: u64) bool {
    const retry = retries.get(transition_id) orelse return false;
    return now_ms < retry.retry_at_ms;
}

fn recordRetry(
    alloc: std.mem.Allocator,
    retries: *std.AutoHashMapUnmanaged(u64, TransitionRetry),
    transition_id: u64,
    now_ms: u64,
) !void {
    const entry = try retries.getOrPut(alloc, transition_id);
    const failures = if (entry.found_existing) @min(entry.value_ptr.failures +| 1, 8) else 1;
    const shift: u6 = @intCast(@min(failures - 1, 6));
    const delay_ms = @min(transition_retry_initial_ms << shift, transition_retry_max_ms);
    entry.value_ptr.* = .{
        .failures = failures,
        .retry_at_ms = now_ms +| delay_ms,
    };
}

fn findSplitIndex(records: []const metadata.SplitTransitionRecord, transition_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.transition_id == transition_id) return i;
    }
    return null;
}

fn findMergeIndex(records: []const metadata.MergeTransitionRecord, transition_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.transition_id == transition_id) return i;
    }
    return null;
}

fn findCompletedSplitIndex(records: []const CompletedSplitObservation, transition_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.transition_id == transition_id) return i;
    }
    return null;
}

fn findCompletedMergeIndex(records: []const metadata.MergeRuntimeObservation, transition_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.transition_id == transition_id) return i;
    }
    return null;
}

fn observationFresh(observed_at_ms: u64, now_ms: u64) bool {
    if (now_ms < observed_at_ms) return false;
    return now_ms - observed_at_ms < transition_observation_refresh_ms;
}

fn splitObservationMatchesTerminalRecord(
    record: metadata.SplitTransitionRecord,
    observation: metadata.SplitObservation,
) bool {
    return switch (record.phase) {
        .finalized => observation.status.phase == .finalized,
        .rolled_back => observation.status.phase == .rolled_back,
        else => false,
    };
}

fn mergeObservationMatchesTerminalRecord(
    record: metadata.MergeTransitionRecord,
    observation: metadata.MergeObservation,
) bool {
    return switch (record.phase) {
        .finalized => observation.donor.phase == .finalized and observation.receiver.phase == .finalized,
        .rolled_back => observation.donor.phase == .rolled_back and observation.receiver.phase == .rolled_back,
        else => false,
    };
}

fn splitObservationFresh(
    records: *const std.AutoHashMapUnmanaged(u64, CachedSplitObservation),
    record: metadata.SplitTransitionRecord,
    now_ms: u64,
) bool {
    const cached = records.get(record.transition_id) orelse return false;
    return cached.attempt_epoch == record.attempt_epoch and
        observationFresh(cached.observed_at_ms, now_ms);
}

fn mergeObservationFresh(
    records: *const std.AutoHashMapUnmanaged(u64, CachedMergeObservation),
    transition_id: u64,
    now_ms: u64,
) bool {
    const cached = records.get(transition_id) orelse return false;
    return observationFresh(cached.observed_at_ms, now_ms);
}

fn cloneSplitRecord(alloc: std.mem.Allocator, record: metadata.SplitTransitionRecord) !metadata.SplitTransitionRecord {
    var owned: metadata.SplitTransitionRecord = .{
        .transition_id = record.transition_id,
        .attempt_epoch = record.attempt_epoch,
        .source_group_id = record.source_group_id,
        .destination_group_id = record.destination_group_id,
        .phase = record.phase,
        .split_key = null,
        .source_range_end = null,
        .rollback_reason = null,
    };
    errdefer deinitSplitRecord(alloc, &owned);
    if (record.split_key) |split_key| owned.split_key = try alloc.dupe(u8, split_key);
    if (record.source_range_end) |end| owned.source_range_end = try alloc.dupe(u8, end);
    if (record.rollback_reason) |reason| owned.rollback_reason = try alloc.dupe(u8, reason);
    return owned;
}

fn deinitSplitRecord(alloc: std.mem.Allocator, record: *metadata.SplitTransitionRecord) void {
    if (record.split_key) |split_key| alloc.free(split_key);
    if (record.source_range_end) |end| alloc.free(end);
    if (record.rollback_reason) |reason| alloc.free(reason);
    record.* = undefined;
}

fn cloneMergeRecord(alloc: std.mem.Allocator, record: metadata.MergeTransitionRecord) !metadata.MergeTransitionRecord {
    return .{
        .transition_id = record.transition_id,
        .donor_group_id = record.donor_group_id,
        .receiver_group_id = record.receiver_group_id,
        .phase = record.phase,
        .rollback_reason = if (record.rollback_reason) |reason| try alloc.dupe(u8, reason) else null,
        .allow_doc_identity_reassignment = record.allow_doc_identity_reassignment,
    };
}

fn deinitMergeRecord(alloc: std.mem.Allocator, record: *metadata.MergeTransitionRecord) void {
    if (record.rollback_reason) |reason| alloc.free(reason);
    record.* = undefined;
}

test "transition service steps split and merge queues through runtime" {
    const FakeSplit = struct {
        status: data.SplitTransitionStatus = .{
            .phase = .prepare,
            .source_split_phase = .prepare,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },

        fn iface(self: *@This()) transition_runtime.SplitRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = prepareSource,
                    .start_source = startSource,
                    .bootstrap_destination = bootstrapDestination,
                    .catch_up_destination = catchUpDestination,
                    .finalize_source = finalizeSource,
                    .rollback_source = rollbackSource,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64, _: u64, _: u64) !data.SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn prepareSource(ptr: *anyopaque, _: u64, _: u64, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .prepare;
            self.status.source_split_phase = .prepare;
            return true;
        }

        fn startSource(ptr: *anyopaque, _: u64, _: u64, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .bootstrap_peer;
            self.status.source_split_phase = .splitting;
            self.status.replay_required = true;
            return true;
        }

        fn bootstrapDestination(ptr: *anyopaque, _: u64, _: u64, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .replay_deltas;
            self.status.bootstrapped = true;
            self.status.source_delta_sequence = 1;
            self.status.dest_delta_sequence = 0;
            return true;
        }

        fn catchUpDestination(ptr: *anyopaque, _: u64, _: u64, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .cutover_ready;
            self.status.replay_caught_up = true;
            self.status.cutover_ready = true;
            self.status.destination_ready_for_reads = true;
            self.status.dest_delta_sequence = 1;
            return 1;
        }

        fn finalizeSource(ptr: *anyopaque, _: u64, _: u64, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .finalized;
            self.status.source_split_phase = .none;
            self.status.replay_required = false;
            return true;
        }

        fn rollbackSource(_: *anyopaque, _: u64, _: u64, _: u64, _: u64) !bool {
            return true;
        }
    };

    const FakeMerge = struct {
        status: data.MergeTransitionStatus = .{
            .phase = .prepare,
            .donor_group_id = 21,
            .receiver_group_id = 22,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },

        fn iface(self: *@This()) transition_runtime.MergeRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .record_doc_identity_reassignment = recordDocIdentityReassignment,
                    .accept_receiver = acceptReceiver,
                    .catch_up_receiver = catchUpReceiver,
                    .finalize_merge = finalizeMerge,
                    .rollback_merge = rollbackMerge,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data.MergeTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn recordDocIdentityReassignment(ptr: *anyopaque, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.allow_doc_identity_reassignment = true;
        }

        fn acceptReceiver(ptr: *anyopaque, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .bootstrap_peer;
            self.status.receiver_accepts_donor_range = true;
        }

        fn catchUpReceiver(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .cutover_ready;
            self.status.bootstrapped = true;
            self.status.replay_required = true;
            self.status.replay_caught_up = true;
            self.status.cutover_ready = true;
            self.status.receiver_ready_for_reads = true;
            self.status.receiver_delta_sequence = 1;
            self.status.donor_delta_sequence = 1;
            return 1;
        }

        fn finalizeMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .finalized;
            self.status.replay_required = false;
            return true;
        }

        fn rollbackMerge(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }
    };

    var split = FakeSplit{};
    var merge = FakeMerge{};
    var svc = try TransitionService.init(std.testing.allocator, .{
        .split = split.iface(),
        .merge = merge.iface(),
    });
    defer svc.deinit();

    try svc.submitSplit(.{
        .transition_id = 1,
        .attempt_epoch = 1,
        .source_group_id = 11,
        .destination_group_id = 12,
    });
    try svc.submitMerge(.{
        .transition_id = 2,
        .donor_group_id = 21,
        .receiver_group_id = 22,
        .allow_doc_identity_reassignment = true,
    });

    _ = try svc.stepPending();
    _ = try svc.stepPending();
    _ = try svc.stepPending();
    _ = try svc.stepPending();
    _ = try svc.stepPending();

    try std.testing.expectEqual(@as(usize, 0), svc.pending_split.items.len);
    try std.testing.expectEqual(@as(usize, 0), svc.pending_merge.items.len);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.completed_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.completed_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.awaiting_split_source_start);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.bootstrapping_split_destination);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.split_replay_blocked);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.split_ready_to_finalize);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.awaiting_merge_receiver_acceptance);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.bootstrapping_merge_receiver);
    try std.testing.expectEqual(@as(usize, 0), svc.metrics.merge_replay_blocked);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.merge_ready_to_finalize);
    try std.testing.expect(merge.status.allow_doc_identity_reassignment);
}

test "transition service retries split bootstrap after leader recovery" {
    const Clock = struct {
        now_ms: u64 = 1_000,

        fn source(self: *@This()) RetryClock {
            return .{
                .ptr = self,
                .now_ms_fn = nowMs,
            };
        }

        fn nowMs(ptr: ?*anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            return self.now_ms;
        }
    };

    const Split = struct {
        recovering_status: data.SplitTransitionStatus = .{
            .phase = .bootstrap_peer,
            .source_split_phase = .splitting,
            .bootstrapped = false,
            .replay_required = true,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 1,
            .dest_delta_sequence = 0,
        },
        healthy_status: data.SplitTransitionStatus = .{
            .phase = .cutover_ready,
            .source_split_phase = .splitting,
            .bootstrapped = true,
            .replay_required = true,
            .replay_caught_up = true,
            .cutover_ready = true,
            .destination_ready_for_reads = true,
            .source_delta_sequence = 1,
            .dest_delta_sequence = 1,
        },
        bootstrap_calls: usize = 0,
        healthy_finalize_calls: usize = 0,
        failures_remaining: usize = 2,

        fn iface(self: *@This()) transition_runtime.SplitRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = prepareSource,
                    .start_source = startSource,
                    .bootstrap_destination = bootstrapDestination,
                    .catch_up_destination = catchUpDestination,
                    .finalize_source = finalizeSource,
                    .rollback_source = rollbackSource,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, transition_id: u64, _: u64, _: u64, _: u64) !data.SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return switch (transition_id) {
                7 => self.recovering_status,
                8 => self.healthy_status,
                else => error.UnknownGroup,
            };
        }

        fn prepareSource(_: *anyopaque, _: u64, _: u64, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            return true;
        }

        fn startSource(_: *anyopaque, _: u64, _: u64, _: u64, _: u64) !bool {
            return true;
        }

        fn bootstrapDestination(ptr: *anyopaque, transition_id: u64, _: u64, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (transition_id != 7) return error.UnknownGroup;
            self.bootstrap_calls += 1;
            if (self.failures_remaining > 0) {
                self.failures_remaining -= 1;
                return error.GroupLeaderUnavailable;
            }
            self.recovering_status.phase = .replay_deltas;
            self.recovering_status.bootstrapped = true;
            return true;
        }

        fn catchUpDestination(ptr: *anyopaque, transition_id: u64, _: u64, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (transition_id != 7) return error.UnknownGroup;
            self.recovering_status.phase = .cutover_ready;
            self.recovering_status.replay_caught_up = true;
            self.recovering_status.cutover_ready = true;
            self.recovering_status.destination_ready_for_reads = true;
            self.recovering_status.dest_delta_sequence = self.recovering_status.source_delta_sequence;
            return 1;
        }

        fn finalizeSource(ptr: *anyopaque, transition_id: u64, _: u64, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const status = switch (transition_id) {
                7 => &self.recovering_status,
                8 => blk: {
                    self.healthy_finalize_calls += 1;
                    break :blk &self.healthy_status;
                },
                else => return error.UnknownGroup,
            };
            status.phase = .finalized;
            status.source_split_phase = .none;
            status.replay_required = false;
            return true;
        }

        fn rollbackSource(_: *anyopaque, _: u64, _: u64, _: u64, _: u64) !bool {
            return true;
        }
    };

    var clock = Clock{};
    var split = Split{};
    var svc = try TransitionService.initWithRetryClock(
        std.testing.allocator,
        .{ .split = split.iface() },
        clock.source(),
    );
    defer svc.deinit();
    try svc.submitSplit(.{
        .transition_id = 7,
        .attempt_epoch = 1,
        .source_group_id = 11,
        .destination_group_id = 12,
        .phase = .bootstrap_peer,
    });
    try svc.submitSplit(.{
        .transition_id = 8,
        .attempt_epoch = 1,
        .source_group_id = 21,
        .destination_group_id = 22,
        .phase = .cutover_pending,
    });

    _ = try svc.stepPending();
    try std.testing.expectEqual(@as(usize, 1), split.bootstrap_calls);
    try std.testing.expectEqual(@as(usize, 1), split.healthy_finalize_calls);
    try std.testing.expectEqual(@as(usize, 2), svc.pending_split.items.len);

    _ = try svc.stepPending();
    try std.testing.expectEqual(@as(usize, 1), split.bootstrap_calls);
    try std.testing.expectEqual(@as(usize, 1), svc.pending_split.items.len);
    try std.testing.expectEqual(metadata.TransitionPhase.bootstrap_peer, svc.pending_split.items[0].phase);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.completed_split_transitions);

    clock.now_ms += transition_retry_initial_ms - 1;
    _ = try svc.stepPending();
    try std.testing.expectEqual(@as(usize, 1), split.bootstrap_calls);

    clock.now_ms += 1;
    _ = try svc.stepPending();
    try std.testing.expectEqual(@as(usize, 2), split.bootstrap_calls);

    clock.now_ms += transition_retry_initial_ms * 2 - 1;
    _ = try svc.stepPending();
    try std.testing.expectEqual(@as(usize, 2), split.bootstrap_calls);

    clock.now_ms += 1;
    _ = try svc.stepPending();
    try std.testing.expectEqual(@as(usize, 3), split.bootstrap_calls);
    try std.testing.expect(split.recovering_status.bootstrapped);
    try std.testing.expectEqual(@as(usize, 1), svc.pending_split.items.len);
    try std.testing.expectEqual(metadata.TransitionPhase.bootstrap_peer, svc.pending_split.items[0].phase);
    try std.testing.expect(!svc.split_retries.contains(7));

    _ = try svc.stepPending();
    _ = try svc.stepPending();
    _ = try svc.stepPending();
    try std.testing.expectEqual(@as(usize, 0), svc.pending_split.items.len);
    try std.testing.expectEqual(@as(usize, 2), svc.metrics.completed_split_transitions);
}

test "transition service upserts and removes queued transitions by id" {
    var svc = try TransitionService.init(std.testing.allocator, .{});
    defer svc.deinit();

    try svc.submitSplit(.{
        .transition_id = 7,
        .attempt_epoch = 1,
        .source_group_id = 1,
        .destination_group_id = 2,
    });
    try svc.submitSplit(.{
        .transition_id = 7,
        .attempt_epoch = 1,
        .source_group_id = 1,
        .destination_group_id = 3,
        .phase = .replay_deltas,
    });
    try std.testing.expectEqual(@as(usize, 1), svc.pending_split.items.len);
    try std.testing.expectEqual(@as(u64, 3), svc.pending_split.items[0].destination_group_id);
    try std.testing.expectEqual(metadata.TransitionPhase.replay_deltas, svc.pending_split.items[0].phase);
    try svc.submitSplit(.{
        .transition_id = 7,
        .attempt_epoch = 1,
        .source_group_id = 1,
        .destination_group_id = 3,
        .phase = .replay_deltas,
        .rollback_reason = "operator abort",
    });
    try svc.submitSplit(.{
        .transition_id = 7,
        .attempt_epoch = 1,
        .source_group_id = 1,
        .destination_group_id = 3,
        .phase = .replay_deltas,
    });
    try std.testing.expectEqualStrings(
        "operator abort",
        svc.pending_split.items[0].rollback_reason.?,
    );
    try std.testing.expect(svc.removeSplit(7));
    try std.testing.expectEqual(@as(usize, 0), svc.pending_split.items.len);

    try svc.submitMerge(.{
        .transition_id = 8,
        .donor_group_id = 4,
        .receiver_group_id = 5,
    });
    try svc.submitMerge(.{
        .transition_id = 8,
        .donor_group_id = 4,
        .receiver_group_id = 6,
        .phase = .rolling_back,
        .rollback_reason = "operator abort",
        .allow_doc_identity_reassignment = true,
    });
    try svc.submitMerge(.{
        .transition_id = 8,
        .donor_group_id = 4,
        .receiver_group_id = 6,
        .phase = .rolling_back,
    });
    try std.testing.expectEqual(@as(usize, 1), svc.pending_merge.items.len);
    try std.testing.expectEqual(@as(u64, 6), svc.pending_merge.items[0].receiver_group_id);
    try std.testing.expectEqual(metadata.TransitionPhase.rolling_back, svc.pending_merge.items[0].phase);
    try std.testing.expectEqualStrings(
        "operator abort",
        svc.pending_merge.items[0].rollback_reason.?,
    );
    try std.testing.expect(
        svc.pending_merge.items[0].allow_doc_identity_reassignment,
    );
    try std.testing.expect(svc.removeMerge(8));
    try std.testing.expectEqual(@as(usize, 0), svc.pending_merge.items.len);
}

test "transition service queue replacement is allocation-failure safe" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var svc = try TransitionService.init(alloc, .{});
            defer svc.deinit();
            try svc.submitSplit(.{
                .transition_id = 7,
                .attempt_epoch = 1,
                .source_group_id = 11,
                .destination_group_id = 12,
                .split_key = "doc:m",
                .source_range_end = "doc:z",
                .rollback_reason = "first",
            });
            try svc.submitSplit(.{
                .transition_id = 7,
                .attempt_epoch = 2,
                .source_group_id = 11,
                .destination_group_id = 12,
                .phase = .replay_deltas,
                .split_key = "doc:n",
                .source_range_end = "doc:z",
                .rollback_reason = "replacement",
            });
            try svc.submitMerge(.{
                .transition_id = 8,
                .donor_group_id = 21,
                .receiver_group_id = 22,
                .rollback_reason = "first",
            });
            try svc.submitMerge(.{
                .transition_id = 8,
                .donor_group_id = 21,
                .receiver_group_id = 22,
                .phase = .finalizing,
                .rollback_reason = "replacement",
            });
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "transition service compaction preserves live records after terminal records" {
    var svc = try TransitionService.init(std.testing.allocator, .{});
    defer svc.deinit();

    try svc.submitSplit(.{
        .transition_id = 1,
        .attempt_epoch = 1,
        .source_group_id = 11,
        .destination_group_id = 12,
        .split_key = "doc:b",
    });
    try svc.submitSplit(.{
        .transition_id = 2,
        .attempt_epoch = 1,
        .source_group_id = 21,
        .destination_group_id = 22,
        .phase = .finalized,
        .split_key = "doc:m",
    });
    try svc.submitSplit(.{
        .transition_id = 3,
        .attempt_epoch = 1,
        .source_group_id = 31,
        .destination_group_id = 32,
        .phase = .replay_deltas,
        .split_key = "doc:t",
    });
    try svc.rememberCompletedSplitObservation(2, 1, .{ .status = .{
        .phase = .finalized,
        .source_split_phase = .none,
        .bootstrapped = true,
        .replay_required = false,
        .replay_caught_up = true,
        .cutover_ready = true,
        .destination_ready_for_reads = true,
        .source_delta_sequence = 0,
        .dest_delta_sequence = 0,
    } });

    try std.testing.expectEqual(@as(usize, 1), svc.compactCompletedSplits());
    try std.testing.expectEqual(@as(usize, 2), svc.pending_split.items.len);
    try std.testing.expectEqual(@as(u64, 1), svc.pending_split.items[0].transition_id);
    try std.testing.expectEqual(@as(u64, 3), svc.pending_split.items[1].transition_id);
    try std.testing.expectEqualStrings("doc:t", svc.pending_split.items[1].split_key.?);

    try svc.submitMerge(.{
        .transition_id = 11,
        .donor_group_id = 111,
        .receiver_group_id = 112,
        .rollback_reason = "keep-first",
    });
    try svc.submitMerge(.{
        .transition_id = 12,
        .donor_group_id = 121,
        .receiver_group_id = 122,
        .phase = .rolled_back,
        .rollback_reason = "remove",
    });
    try svc.submitMerge(.{
        .transition_id = 13,
        .donor_group_id = 131,
        .receiver_group_id = 132,
        .phase = .replay_deltas,
        .rollback_reason = "keep-last",
    });
    const rolled_back_merge_status = data.MergeTransitionStatus{
        .phase = .rolled_back,
        .donor_group_id = 121,
        .receiver_group_id = 122,
        .receiver_accepts_donor_range = false,
        .bootstrapped = false,
        .replay_required = false,
        .replay_caught_up = false,
        .cutover_ready = false,
        .receiver_ready_for_reads = false,
        .donor_delta_sequence = 0,
        .receiver_delta_sequence = 0,
    };
    try svc.rememberCompletedMergeObservation(12, .{
        .donor = rolled_back_merge_status,
        .receiver = rolled_back_merge_status,
    });

    try std.testing.expectEqual(@as(usize, 1), svc.compactCompletedMerges());
    try std.testing.expectEqual(@as(usize, 2), svc.pending_merge.items.len);
    try std.testing.expectEqual(@as(u64, 11), svc.pending_merge.items[0].transition_id);
    try std.testing.expectEqual(@as(u64, 13), svc.pending_merge.items[1].transition_id);
    try std.testing.expectEqualStrings("keep-last", svc.pending_merge.items[1].rollback_reason.?);
}

test "transition service invalidates completed observations only on resubmit" {
    var svc = try TransitionService.init(std.testing.allocator, .{});
    defer svc.deinit();

    try svc.rememberCompletedSplitObservation(17, 1, .{
        .status = .{
            .phase = .rolled_back,
            .source_split_phase = null,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },
    });
    try std.testing.expect(svc.hasCompletedSplit(17));
    try svc.submitSplit(.{
        .transition_id = 17,
        .attempt_epoch = 2,
        .source_group_id = 71,
        .destination_group_id = 72,
    });
    try std.testing.expect(!svc.hasCompletedSplit(17));
    try std.testing.expectEqual(@as(usize, 1), svc.pending_split.items.len);
    try std.testing.expect(svc.removeSplit(17));
    try std.testing.expectEqual(@as(usize, 0), svc.pending_split.items.len);
    try std.testing.expect(!svc.removeSplit(17));

    const merge_status = data.MergeTransitionStatus{
        .phase = .finalized,
        .donor_group_id = 81,
        .receiver_group_id = 82,
        .receiver_accepts_donor_range = true,
        .bootstrapped = true,
        .replay_required = false,
        .replay_caught_up = true,
        .cutover_ready = true,
        .receiver_ready_for_reads = true,
        .donor_delta_sequence = 3,
        .receiver_delta_sequence = 3,
    };
    try svc.rememberCompletedMergeObservation(18, .{
        .donor = merge_status,
        .receiver = merge_status,
    });
    try std.testing.expect(svc.hasCompletedMerge(18));
    try std.testing.expect(!svc.removeMerge(18));
    try std.testing.expect(svc.hasCompletedMerge(18));
    try std.testing.expectEqual(.finalized, svc.observeMerge(18).?.receiver.phase);
    try svc.submitMerge(.{
        .transition_id = 18,
        .donor_group_id = 81,
        .receiver_group_id = 82,
    });
    try std.testing.expect(svc.hasCompletedMerge(18));
    try std.testing.expectEqual(@as(usize, 0), svc.pending_merge.items.len);
}

test "transition service clones queued transition record strings" {
    const RecordingSplit = struct {
        status: data.SplitTransitionStatus = .{
            .phase = .prepare,
            .source_split_phase = null,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },
        last_split_key: ?[]u8 = null,
        last_source_range_end: ?[]u8 = null,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            if (self.last_split_key) |split_key| alloc.free(split_key);
            if (self.last_source_range_end) |end| alloc.free(end);
            self.* = undefined;
        }

        fn iface(self: *@This()) transition_runtime.SplitRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = prepareSource,
                    .start_source = unsupportedBool,
                    .bootstrap_destination = unsupportedBool,
                    .catch_up_destination = unsupportedUsize,
                    .finalize_source = unsupportedBool,
                    .rollback_source = unsupportedBool,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64, _: u64, _: u64) !data.SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn prepareSource(ptr: *anyopaque, _: u64, _: u64, _: u64, _: u64, split_key: []const u8, source_range_end: ?[]const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.last_split_key) |existing| std.testing.allocator.free(existing);
            if (self.last_source_range_end) |existing| std.testing.allocator.free(existing);
            self.last_split_key = try std.testing.allocator.dupe(u8, split_key);
            self.last_source_range_end = if (source_range_end) |end| try std.testing.allocator.dupe(u8, end) else null;
            self.status.phase = .prepare;
            self.status.source_split_phase = .prepare;
            return true;
        }

        fn unsupportedBool(_: *anyopaque, _: u64, _: u64, _: u64, _: u64) !bool {
            return error.TestUnexpectedResult;
        }

        fn unsupportedUsize(_: *anyopaque, _: u64, _: u64, _: u64, _: u64) !usize {
            return error.TestUnexpectedResult;
        }
    };

    const RecordingMerge = struct {
        status: data.MergeTransitionStatus = .{
            .phase = .prepare,
            .donor_group_id = 21,
            .receiver_group_id = 22,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },
        rollback_calls: usize = 0,

        fn iface(self: *@This()) transition_runtime.MergeRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .accept_receiver = unsupportedAccept,
                    .catch_up_receiver = unsupportedCatchUp,
                    .finalize_merge = unsupportedFinalize,
                    .rollback_merge = rollbackMerge,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data.MergeTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn rollbackMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.rollback_calls += 1;
            self.status.phase = .rolled_back;
            return true;
        }

        fn unsupportedAccept(_: *anyopaque, _: u64, _: u64) !void {
            return error.TestUnexpectedResult;
        }

        fn unsupportedCatchUp(_: *anyopaque, _: u64, _: u64) !usize {
            return error.TestUnexpectedResult;
        }

        fn unsupportedFinalize(_: *anyopaque, _: u64, _: u64) !bool {
            return error.TestUnexpectedResult;
        }
    };

    var split = RecordingSplit{};
    defer split.deinit(std.testing.allocator);
    var merge = RecordingMerge{};
    var svc = try TransitionService.init(std.testing.allocator, .{
        .split = split.iface(),
        .merge = merge.iface(),
    });
    defer svc.deinit();

    const split_key_a = try std.testing.allocator.dupe(u8, "doc:m");
    defer std.testing.allocator.free(split_key_a);
    const split_end_a = try std.testing.allocator.dupe(u8, "doc:z");
    defer std.testing.allocator.free(split_end_a);
    const rollback_a = try std.testing.allocator.dupe(u8, "rollback-a");
    defer std.testing.allocator.free(rollback_a);
    try svc.submitSplit(.{
        .transition_id = 17,
        .attempt_epoch = 1,
        .source_group_id = 71,
        .destination_group_id = 72,
        .split_key = split_key_a,
        .source_range_end = split_end_a,
        .rollback_reason = rollback_a,
    });

    const split_key_b = try std.testing.allocator.dupe(u8, "doc:n");
    defer std.testing.allocator.free(split_key_b);
    const split_end_b = try std.testing.allocator.dupe(u8, "doc:y");
    defer std.testing.allocator.free(split_end_b);
    try svc.submitSplit(.{
        .transition_id = 17,
        .attempt_epoch = 1,
        .source_group_id = 71,
        .destination_group_id = 73,
        .split_key = split_key_b,
        .source_range_end = split_end_b,
    });

    const merge_reason = try std.testing.allocator.dupe(u8, "operator abort");
    defer std.testing.allocator.free(merge_reason);
    try svc.submitMerge(.{
        .transition_id = 18,
        .donor_group_id = 81,
        .receiver_group_id = 82,
        .rollback_reason = merge_reason,
    });

    @memset(split_key_a, 'x');
    @memset(split_end_a, 'x');
    @memset(rollback_a, 'x');
    @memset(split_key_b, 'q');
    @memset(split_end_b, 'q');
    @memset(merge_reason, 'r');

    _ = try svc.stepPending();

    try std.testing.expectEqualStrings("doc:n", split.last_split_key.?);
    try std.testing.expectEqualStrings("doc:y", split.last_source_range_end.?);
    try std.testing.expectEqual(@as(usize, 1), merge.rollback_calls);
}

test "transition service refreshes bounded observations and preserves terminal status" {
    const Clock = struct {
        now_ms: u64 = 1_000,

        fn source(self: *@This()) RetryClock {
            return .{
                .ptr = self,
                .now_ms_fn = nowMs,
            };
        }

        fn nowMs(ptr: ?*anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            return self.now_ms;
        }
    };

    const FakeSplit = struct {
        observe_calls: usize = 0,
        status: data.SplitTransitionStatus = .{
            .phase = .replay_deltas,
            .source_split_phase = .splitting,
            .bootstrapped = true,
            .replay_required = true,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 3,
            .dest_delta_sequence = 2,
        },

        fn iface(self: *@This()) transition_runtime.SplitRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = unsupportedPrepare,
                    .start_source = unsupportedBool,
                    .bootstrap_destination = unsupportedBool,
                    .catch_up_destination = unsupportedUsize,
                    .finalize_source = unsupportedBool,
                    .rollback_source = unsupportedBool,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64, _: u64, _: u64) !data.SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.observe_calls += 1;
            return self.status;
        }

        fn unsupportedPrepare(_: *anyopaque, _: u64, _: u64, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            return error.TestUnexpectedResult;
        }

        fn unsupportedBool(_: *anyopaque, _: u64, _: u64, _: u64, _: u64) !bool {
            return error.TestUnexpectedResult;
        }

        fn unsupportedUsize(_: *anyopaque, _: u64, _: u64, _: u64, _: u64) !usize {
            return error.TestUnexpectedResult;
        }
    };

    const FakeMerge = struct {
        observe_calls: usize = 0,
        status: data.MergeTransitionStatus = .{
            .phase = .cutover_ready,
            .donor_group_id = 91,
            .receiver_group_id = 92,
            .receiver_accepts_donor_range = true,
            .bootstrapped = true,
            .replay_required = true,
            .replay_caught_up = true,
            .cutover_ready = true,
            .receiver_ready_for_reads = true,
            .donor_delta_sequence = 4,
            .receiver_delta_sequence = 4,
        },

        fn iface(self: *@This()) transition_runtime.MergeRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .accept_receiver = unsupportedVoid,
                    .catch_up_receiver = unsupportedUsize,
                    .finalize_merge = unsupportedBool,
                    .rollback_merge = unsupportedBool,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data.MergeTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.observe_calls += 1;
            return self.status;
        }

        fn unsupportedVoid(_: *anyopaque, _: u64, _: u64) !void {
            return error.TestUnexpectedResult;
        }

        fn unsupportedBool(_: *anyopaque, _: u64, _: u64) !bool {
            return error.TestUnexpectedResult;
        }

        fn unsupportedUsize(_: *anyopaque, _: u64, _: u64) !usize {
            return error.TestUnexpectedResult;
        }
    };

    var clock = Clock{};
    var split = FakeSplit{};
    var merge = FakeMerge{};
    var svc = try TransitionService.initWithRetryClock(
        std.testing.allocator,
        .{
            .split = split.iface(),
            .merge = merge.iface(),
        },
        clock.source(),
    );
    defer svc.deinit();

    try svc.submitSplit(.{
        .transition_id = 71,
        .attempt_epoch = 1,
        .source_group_id = 11,
        .destination_group_id = 12,
    });
    try svc.submitMerge(.{
        .transition_id = 72,
        .donor_group_id = 91,
        .receiver_group_id = 92,
    });
    try std.testing.expect(svc.observeSplit(71) == null);
    try std.testing.expect(svc.observeMerge(72) == null);
    try svc.refreshPendingObservations();
    try std.testing.expectEqual(@as(usize, 1), split.observe_calls);
    try std.testing.expectEqual(@as(usize, 1), merge.observe_calls);

    // A control-loop burst reuses the bounded snapshot instead of multiplying
    // shard RPCs. Once the interval elapses, every pending observation is
    // refreshed independently.
    try svc.refreshPendingObservations();
    clock.now_ms += transition_observation_refresh_ms - 1;
    try svc.refreshPendingObservations();
    try std.testing.expectEqual(@as(usize, 1), split.observe_calls);
    try std.testing.expectEqual(@as(usize, 1), merge.observe_calls);
    clock.now_ms += 1;
    try svc.refreshPendingObservations();
    try std.testing.expectEqual(@as(usize, 2), split.observe_calls);
    try std.testing.expectEqual(@as(usize, 2), merge.observe_calls);

    const split_observation = svc.observeSplit(71) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(data.SplitTransitionStatus, @TypeOf(split_observation.status));
    try std.testing.expectEqual(.replay_deltas, split_observation.status.phase);
    try std.testing.expectEqual(@as(u64, 3), split_observation.status.source_delta_sequence);
    const split_state = svc.describeSplit(71) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata.SplitExecutionStateTag.replay_blocked, split_state.tag);
    try std.testing.expect(split_state.actionable());
    try std.testing.expect(split_state.action == .catch_up_split_destination);

    const merge_observation = svc.observeMerge(72) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(.cutover_ready, merge_observation.receiver.phase);
    try std.testing.expectEqual(@as(u64, 4), merge_observation.receiver.receiver_delta_sequence);
    const merge_state = svc.describeMerge(72) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata.MergeExecutionStateTag.ready_to_finalize, merge_state.tag);
    try std.testing.expect(merge_state.actionable());
    try std.testing.expect(merge_state.action == .finalize_merge);

    try std.testing.expect(svc.observeSplit(999) == null);
    try std.testing.expect(svc.observeMerge(999) == null);
    try std.testing.expect(svc.describeSplit(999) == null);
    try std.testing.expect(svc.describeMerge(999) == null);

    split.status.phase = .finalized;
    split.status.source_split_phase = .none;
    merge.status.phase = .finalized;
    try svc.submitSplit(.{
        .transition_id = 71,
        .attempt_epoch = 1,
        .source_group_id = 11,
        .destination_group_id = 12,
        .phase = .finalized,
    });
    try svc.submitMerge(.{
        .transition_id = 72,
        .donor_group_id = 91,
        .receiver_group_id = 92,
        .phase = .finalized,
    });
    const completed = try svc.stepPending();
    try std.testing.expectEqual(@as(usize, 1), completed.completed_split);
    try std.testing.expectEqual(@as(usize, 1), completed.completed_merge);
    try std.testing.expect(svc.splitRecord(71) == null);
    try std.testing.expect(svc.mergeRecord(72) == null);
    try std.testing.expectEqual(.finalized, svc.observeSplit(71).?.status.phase);
    try std.testing.expectEqual(.finalized, svc.observeMerge(72).?.receiver.phase);
}

test "transition service retains terminal records until terminal observation succeeds" {
    const FakeSplit = struct {
        fail_observation: bool = false,
        phase: data.RangeTransitionPhase = .replay_deltas,

        fn iface(self: *@This()) transition_runtime.SplitRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = unsupportedPrepare,
                    .start_source = unsupportedBool,
                    .bootstrap_destination = unsupportedBool,
                    .catch_up_destination = unsupportedUsize,
                    .finalize_source = unsupportedBool,
                    .rollback_source = unsupportedBool,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64, _: u64, _: u64) !data.SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail_observation) return error.GroupLeaderUnavailable;
            return .{
                .phase = self.phase,
                .source_split_phase = if (self.phase == .finalized) .none else .splitting,
                .bootstrapped = true,
                .replay_required = true,
                .replay_caught_up = self.phase == .finalized,
                .cutover_ready = self.phase == .finalized,
                .destination_ready_for_reads = self.phase == .finalized,
                .source_delta_sequence = 4,
                .dest_delta_sequence = if (self.phase == .finalized) 4 else 3,
            };
        }

        fn unsupportedPrepare(_: *anyopaque, _: u64, _: u64, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            return error.TestUnexpectedResult;
        }

        fn unsupportedBool(_: *anyopaque, _: u64, _: u64, _: u64, _: u64) !bool {
            return error.TestUnexpectedResult;
        }

        fn unsupportedUsize(_: *anyopaque, _: u64, _: u64, _: u64, _: u64) !usize {
            return error.TestUnexpectedResult;
        }
    };

    var split = FakeSplit{};
    var svc = try TransitionService.init(std.testing.allocator, .{
        .split = split.iface(),
    });
    defer svc.deinit();

    try svc.submitSplit(.{
        .transition_id = 73,
        .attempt_epoch = 1,
        .source_group_id = 11,
        .destination_group_id = 12,
        .phase = .replay_deltas,
    });
    try svc.refreshPendingObservations();
    try std.testing.expectEqual(.replay_deltas, svc.observeSplit(73).?.status.phase);

    try svc.submitSplit(.{
        .transition_id = 73,
        .attempt_epoch = 1,
        .source_group_id = 11,
        .destination_group_id = 12,
        .phase = .finalized,
    });

    split.fail_observation = true;
    const unavailable = try svc.stepPending();
    try std.testing.expectEqual(@as(usize, 0), unavailable.completed_split);
    try std.testing.expect(svc.splitRecord(73) != null);
    try std.testing.expectEqual(.replay_deltas, svc.observeSplit(73).?.status.phase);
    try std.testing.expect(!svc.hasCompletedSplit(73));

    split.fail_observation = false;
    split.phase = .finalized;
    const observed = try svc.stepPending();
    try std.testing.expectEqual(@as(usize, 1), observed.completed_split);
    try std.testing.expect(svc.splitRecord(73) == null);
    try std.testing.expectEqual(.finalized, svc.observeSplit(73).?.status.phase);
}

test "transition service steps real split coordinator from prepared source state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-real-split-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-real-split-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    {
        var source = try data.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();

        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-0\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:991:1:2302:doc:m") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 2301,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    var split = try transition_runtime.SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .transition_id = 991,
        .attempt_epoch = 1,
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 2301,
        .dest_group_id = 2302,
    });
    defer split.deinit();

    var svc = try TransitionService.init(std.testing.allocator, .{
        .split = split.runtime(),
    });
    defer svc.deinit();

    try svc.submitSplit(.{
        .transition_id = 991,
        .attempt_epoch = 1,
        .source_group_id = 2301,
        .destination_group_id = 2302,
    });

    const step = try svc.stepPending();
    try std.testing.expectEqual(@as(usize, 1), step.stepped_split);
    try std.testing.expectEqual(@as(usize, 1), step.awaiting_split_source_start);

    const after = svc.describeSplit(991) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata.SplitExecutionStateTag.bootstrapping_destination, after.tag);
    try std.testing.expect(after.action == .bootstrap_split_destination);
}

test "transition service steps real merge coordinator from prepared donor state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-real-merge-donor", .{tmp.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-real-merge-receiver", .{tmp.sub_path});
    defer std.testing.allocator.free(receiver_root);

    {
        var donor = try data.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();

        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"donor\"}") },
        });
        defer std.testing.allocator.free(setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 2401,
            .commit_index = 2,
            .entries_bytes = setup,
        });
    }

    {
        var receiver = try data.SplitDestination.init(std.testing.allocator, .{ .root_dir = receiver_root });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    var merge = try transition_runtime.MergeCoordinatorRuntime.init(std.testing.allocator, .{
        .donor_root_dir = donor_root,
        .receiver_root_dir = receiver_root,
        .donor_group_id = 2401,
        .receiver_group_id = 2402,
    });
    defer merge.deinit();

    var svc = try TransitionService.init(std.testing.allocator, .{
        .merge = merge.runtime(),
    });
    defer svc.deinit();

    try svc.submitMerge(.{
        .transition_id = 992,
        .donor_group_id = 2401,
        .receiver_group_id = 2402,
    });

    var rounds: usize = 0;
    while (rounds < 8 and svc.metrics.completed_merge_transitions == 0) : (rounds += 1) {
        _ = try svc.stepPending();
    }

    try std.testing.expectEqual(@as(usize, 0), svc.metrics.queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.completed_merge_transitions);

    var receiver = try data.SplitDestination.initReadOnly(std.testing.allocator, receiver_root);
    defer receiver.deinit();
    const range = receiver.getRange();
    try std.testing.expectEqualStrings("doc:a", range.start);
    try std.testing.expectEqualStrings("doc:z", range.end);
    const donor_doc = (try receiver.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(donor_doc);
    try std.testing.expectEqualStrings("{\"v\":\"donor\"}", donor_doc);
}
