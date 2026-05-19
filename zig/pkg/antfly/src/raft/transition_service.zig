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
const raft_state_machine = @import("state_machine/mod.zig");

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
    ops: union(enum) {
        runtime: transition_runtime.TransitionRuntime,
        adapter: shard_ops.ShardOperationAdapter,
    },
    pending_split: std.ArrayListUnmanaged(metadata.SplitTransitionRecord) = .empty,
    pending_merge: std.ArrayListUnmanaged(metadata.MergeTransitionRecord) = .empty,
    completed_split_observations: std.ArrayListUnmanaged(metadata.SplitRuntimeObservation) = .empty,
    completed_merge_observations: std.ArrayListUnmanaged(metadata.MergeRuntimeObservation) = .empty,
    metrics: TransitionServiceMetrics = .{},

    pub fn init(alloc: std.mem.Allocator, ops: anytype) TransitionService {
        const OpsType = @TypeOf(ops);
        return .{
            .alloc = alloc,
            .ops = if (@hasField(OpsType, "ptr") and @hasField(OpsType, "vtable"))
                .{ .adapter = .{
                    .ptr = ops.ptr,
                    .vtable = ops.vtable,
                } }
            else
                .{ .runtime = .{
                    .split = if (@hasField(OpsType, "split")) ops.split else null,
                    .merge = if (@hasField(OpsType, "merge")) ops.merge else null,
                } },
        };
    }

    pub fn deinit(self: *TransitionService) void {
        for (self.pending_split.items) |*record| deinitSplitRecord(self.alloc, record);
        self.pending_split.deinit(self.alloc);
        for (self.pending_merge.items) |*record| deinitMergeRecord(self.alloc, record);
        self.pending_merge.deinit(self.alloc);
        self.completed_split_observations.deinit(self.alloc);
        self.completed_merge_observations.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn submitSplit(self: *TransitionService, record: metadata.SplitTransitionRecord) !void {
        _ = self.removeCompletedSplit(record.transition_id);
        if (findSplitIndex(self.pending_split.items, record.transition_id)) |index| {
            deinitSplitRecord(self.alloc, &self.pending_split.items[index]);
            self.pending_split.items[index] = try cloneSplitRecord(self.alloc, record);
        } else {
            try self.pending_split.append(self.alloc, try cloneSplitRecord(self.alloc, record));
        }
        self.metrics.queued_split_transitions = self.pending_split.items.len;
    }

    pub fn submitMerge(self: *TransitionService, record: metadata.MergeTransitionRecord) !void {
        _ = self.removeCompletedMerge(record.transition_id);
        if (findMergeIndex(self.pending_merge.items, record.transition_id)) |index| {
            deinitMergeRecord(self.alloc, &self.pending_merge.items[index]);
            self.pending_merge.items[index] = try cloneMergeRecord(self.alloc, record);
        } else {
            try self.pending_merge.append(self.alloc, try cloneMergeRecord(self.alloc, record));
        }
        self.metrics.queued_merge_transitions = self.pending_merge.items.len;
    }

    pub fn removeSplit(self: *TransitionService, transition_id: u64) bool {
        const removed_completed = self.removeCompletedSplit(transition_id);
        if (findSplitIndex(self.pending_split.items, transition_id)) |index| {
            var removed = self.pending_split.orderedRemove(index);
            deinitSplitRecord(self.alloc, &removed);
            self.metrics.queued_split_transitions = self.pending_split.items.len;
            return true;
        }
        return removed_completed;
    }

    pub fn removeMerge(self: *TransitionService, transition_id: u64) bool {
        const removed_completed = self.removeCompletedMerge(transition_id);
        if (findMergeIndex(self.pending_merge.items, transition_id)) |index| {
            var removed = self.pending_merge.orderedRemove(index);
            deinitMergeRecord(self.alloc, &removed);
            self.metrics.queued_merge_transitions = self.pending_merge.items.len;
            return true;
        }
        return removed_completed;
    }

    pub fn hasCompletedSplit(self: *const TransitionService, transition_id: u64) bool {
        return findCompletedSplitIndex(self.completed_split_observations.items, transition_id) != null;
    }

    pub fn hasCompletedMerge(self: *const TransitionService, transition_id: u64) bool {
        return findCompletedMergeIndex(self.completed_merge_observations.items, transition_id) != null;
    }

    pub fn observeSplit(self: *TransitionService, transition_id: u64) !?metadata.SplitObservation {
        if (findSplitIndex(self.pending_split.items, transition_id)) |index| {
            return try self.metadataRuntime().observeSplit(self.pending_split.items[index]);
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

    pub fn describeSplit(self: *TransitionService, transition_id: u64) !?metadata.SplitExecutionState {
        const index = findSplitIndex(self.pending_split.items, transition_id) orelse return null;
        const record = self.pending_split.items[index];
        const observation = try self.metadataRuntime().observeSplit(record);
        return metadata.TransitionController.describeSplit(record, observation);
    }

    pub fn observeMerge(self: *TransitionService, transition_id: u64) !?metadata.MergeObservation {
        if (findMergeIndex(self.pending_merge.items, transition_id)) |index| {
            return try self.metadataRuntime().observeMerge(self.pending_merge.items[index]);
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

    pub fn describeMerge(self: *TransitionService, transition_id: u64) !?metadata.MergeExecutionState {
        const index = findMergeIndex(self.pending_merge.items, transition_id) orelse return null;
        const record = self.pending_merge.items[index];
        const observation = try self.metadataRuntime().observeMerge(record);
        return metadata.TransitionController.describeMerge(record, observation);
    }

    pub fn shardOperationAdapter(self: *TransitionService) shard_ops.ShardOperationAdapter {
        return switch (self.ops) {
            .runtime => |*runtime| runtime.shardOperationAdapter(),
            .adapter => |adapter| adapter,
        };
    }

    pub fn stepPending(self: *TransitionService) !TransitionStepResult {
        var result = TransitionStepResult{};
        const runtime = self.metadataRuntime();

        for (self.pending_split.items) |*record| {
            if (record.phase == .finalized or record.phase == .rolled_back) continue;
            if (self.describeSplit(record.transition_id) catch |err| blk: {
                std.log.warn("split transition describe failed transition_id={d} err={s}", .{ record.transition_id, @errorName(err) });
                break :blk null;
            }) |state| {
                switch (state.tag) {
                    .awaiting_source_start => result.awaiting_split_source_start += 1,
                    .bootstrapping_destination => result.bootstrapping_split_destination += 1,
                    .replay_blocked => result.split_replay_blocked += 1,
                    .ready_to_finalize => result.split_ready_to_finalize += 1,
                    else => {},
                }
            }
            _ = metadata.TransitionDriver.stepSplit(runtime, record) catch |err| {
                std.log.warn("split transition step failed transition_id={d} phase={s} err={s}", .{ record.transition_id, @tagName(record.phase), @errorName(err) });
                continue;
            };
            if (record.phase == .finalized or record.phase == .rolled_back) {
                if (runtime.observeSplit(record.*)) |observation| {
                    try self.rememberCompletedSplitObservation(record.transition_id, observation);
                } else |err| {
                    std.log.warn("split transition completion observation failed transition_id={d} err={s}", .{ record.transition_id, @errorName(err) });
                }
            }
            result.stepped_split += 1;
        }

        for (self.pending_merge.items) |*record| {
            if (record.phase == .finalized or record.phase == .rolled_back) continue;
            if (self.describeMerge(record.transition_id) catch |err| blk: {
                std.log.warn("merge transition describe failed transition_id={d} err={s}", .{ record.transition_id, @errorName(err) });
                break :blk null;
            }) |state| {
                switch (state.tag) {
                    .awaiting_receiver_acceptance => result.awaiting_merge_receiver_acceptance += 1,
                    .bootstrapping_receiver => result.bootstrapping_merge_receiver += 1,
                    .replay_blocked => result.merge_replay_blocked += 1,
                    .ready_to_finalize => result.merge_ready_to_finalize += 1,
                    else => {},
                }
            }
            _ = metadata.TransitionDriver.stepMerge(runtime, record) catch |err| {
                std.log.warn("merge transition step failed transition_id={d} phase={s} err={s}", .{ record.transition_id, @tagName(record.phase), @errorName(err) });
                continue;
            };
            if (record.phase == .finalized or record.phase == .rolled_back) {
                if (runtime.observeMerge(record.*)) |observation| {
                    try self.rememberCompletedMergeObservation(record.transition_id, observation);
                } else |err| {
                    std.log.warn("merge transition completion observation failed transition_id={d} err={s}", .{ record.transition_id, @errorName(err) });
                }
            }
            result.stepped_merge += 1;
        }

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
            .adapter => |adapter| try adapter.observeSplit(record),
        };
    }

    fn observeMergeMeta(ptr: *anyopaque, record: metadata.MergeTransitionRecord) !metadata.MergeObservation {
        const self: *TransitionService = @ptrCast(@alignCast(ptr));
        return switch (self.ops) {
            .runtime => |*runtime| try runtime.metadataRuntime().observeMerge(record),
            .adapter => |adapter| try adapter.observeMerge(record),
        };
    }

    fn executeMeta(ptr: *anyopaque, action: metadata.TransitionAction) !void {
        const self: *TransitionService = @ptrCast(@alignCast(ptr));
        switch (self.ops) {
            .runtime => |*runtime| try runtime.metadataRuntime().execute(action),
            .adapter => |adapter| try adapter.execute(action),
        }
    }

    fn compactCompletedSplits(self: *TransitionService) usize {
        var write_index: usize = 0;
        var removed: usize = 0;
        for (self.pending_split.items) |record| {
            if (record.phase == .finalized or record.phase == .rolled_back) {
                var doomed = record;
                deinitSplitRecord(self.alloc, &doomed);
                removed += 1;
                continue;
            }
            if (write_index != removed) self.pending_split.items[write_index] = record;
            write_index += 1;
        }
        self.pending_split.items.len = write_index;
        return removed;
    }

    fn compactCompletedMerges(self: *TransitionService) usize {
        var write_index: usize = 0;
        var removed: usize = 0;
        for (self.pending_merge.items) |record| {
            if (record.phase == .finalized or record.phase == .rolled_back) {
                var doomed = record;
                deinitMergeRecord(self.alloc, &doomed);
                removed += 1;
                continue;
            }
            if (write_index != removed) self.pending_merge.items[write_index] = record;
            write_index += 1;
        }
        self.pending_merge.items.len = write_index;
        return removed;
    }

    fn rememberCompletedSplitObservation(
        self: *TransitionService,
        transition_id: u64,
        observation: metadata.SplitObservation,
    ) !void {
        if (findCompletedSplitIndex(self.completed_split_observations.items, transition_id)) |index| {
            self.completed_split_observations.items[index].observation = observation;
            return;
        }
        try self.completed_split_observations.append(self.alloc, .{
            .transition_id = transition_id,
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
};

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

fn findCompletedSplitIndex(records: []const metadata.SplitRuntimeObservation, transition_id: u64) ?usize {
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

fn cloneSplitRecord(alloc: std.mem.Allocator, record: metadata.SplitTransitionRecord) !metadata.SplitTransitionRecord {
    return .{
        .transition_id = record.transition_id,
        .source_group_id = record.source_group_id,
        .destination_group_id = record.destination_group_id,
        .phase = record.phase,
        .split_key = if (record.split_key) |split_key| try alloc.dupe(u8, split_key) else null,
        .source_range_end = if (record.source_range_end) |end| try alloc.dupe(u8, end) else null,
        .rollback_reason = if (record.rollback_reason) |reason| try alloc.dupe(u8, reason) else null,
    };
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

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data.SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn prepareSource(ptr: *anyopaque, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .prepare;
            self.status.source_split_phase = .prepare;
            return true;
        }

        fn startSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .bootstrap_peer;
            self.status.source_split_phase = .splitting;
            self.status.replay_required = true;
            return true;
        }

        fn bootstrapDestination(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .replay_deltas;
            self.status.bootstrapped = true;
            self.status.source_delta_sequence = 1;
            self.status.dest_delta_sequence = 0;
            return true;
        }

        fn catchUpDestination(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .cutover_ready;
            self.status.replay_caught_up = true;
            self.status.cutover_ready = true;
            self.status.destination_ready_for_reads = true;
            self.status.dest_delta_sequence = 1;
            return 1;
        }

        fn finalizeSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .finalized;
            self.status.source_split_phase = .none;
            self.status.replay_required = false;
            return true;
        }

        fn rollbackSource(_: *anyopaque, _: u64, _: u64) !bool {
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
    var svc = TransitionService.init(std.testing.allocator, .{
        .split = split.iface(),
        .merge = merge.iface(),
    });
    defer svc.deinit();

    try svc.submitSplit(.{
        .transition_id = 1,
        .source_group_id = 11,
        .destination_group_id = 12,
    });
    try svc.submitMerge(.{
        .transition_id = 2,
        .donor_group_id = 21,
        .receiver_group_id = 22,
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
}

test "transition service upserts and removes queued transitions by id" {
    var svc = TransitionService.init(std.testing.allocator, .{});
    defer svc.deinit();

    try svc.submitSplit(.{
        .transition_id = 7,
        .source_group_id = 1,
        .destination_group_id = 2,
    });
    try svc.submitSplit(.{
        .transition_id = 7,
        .source_group_id = 1,
        .destination_group_id = 3,
        .phase = .replay_deltas,
    });
    try std.testing.expectEqual(@as(usize, 1), svc.pending_split.items.len);
    try std.testing.expectEqual(@as(u64, 3), svc.pending_split.items[0].destination_group_id);
    try std.testing.expectEqual(metadata.TransitionPhase.replay_deltas, svc.pending_split.items[0].phase);
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
    });
    try std.testing.expectEqual(@as(usize, 1), svc.pending_merge.items.len);
    try std.testing.expectEqual(@as(u64, 6), svc.pending_merge.items[0].receiver_group_id);
    try std.testing.expectEqual(metadata.TransitionPhase.rolling_back, svc.pending_merge.items[0].phase);
    try std.testing.expect(svc.removeMerge(8));
    try std.testing.expectEqual(@as(usize, 0), svc.pending_merge.items.len);
}

test "transition service clears completed observations on resubmit and remove" {
    var svc = TransitionService.init(std.testing.allocator, .{});
    defer svc.deinit();

    try svc.rememberCompletedSplitObservation(17, .{
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
    try std.testing.expect(svc.removeMerge(18));
    try std.testing.expect(!svc.hasCompletedMerge(18));
    try std.testing.expect(!svc.removeMerge(18));
}

test "transition service clones queued transition record strings" {
    const RecordingSplit = struct {
        status: data.SplitTransitionStatus = .{
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

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data.SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn prepareSource(ptr: *anyopaque, _: u64, _: u64, split_key: []const u8, source_range_end: ?[]const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.last_split_key) |existing| std.testing.allocator.free(existing);
            if (self.last_source_range_end) |existing| std.testing.allocator.free(existing);
            self.last_split_key = try std.testing.allocator.dupe(u8, split_key);
            self.last_source_range_end = if (source_range_end) |end| try std.testing.allocator.dupe(u8, end) else null;
            self.status.phase = .prepare;
            self.status.source_split_phase = .prepare;
            return true;
        }

        fn unsupportedBool(_: *anyopaque, _: u64, _: u64) !bool {
            return error.TestUnexpectedResult;
        }

        fn unsupportedUsize(_: *anyopaque, _: u64, _: u64) !usize {
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
    var svc = TransitionService.init(std.testing.allocator, .{
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

test "transition service observes queued split and merge transitions through runtime" {
    const FakeSplit = struct {
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

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !data.SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn unsupportedPrepare(_: *anyopaque, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            return error.TestUnexpectedResult;
        }

        fn unsupportedBool(_: *anyopaque, _: u64, _: u64) !bool {
            return error.TestUnexpectedResult;
        }

        fn unsupportedUsize(_: *anyopaque, _: u64, _: u64) !usize {
            return error.TestUnexpectedResult;
        }
    };

    const FakeMerge = struct {
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

    var split = FakeSplit{};
    var merge = FakeMerge{};
    var svc = TransitionService.init(std.testing.allocator, .{
        .split = split.iface(),
        .merge = merge.iface(),
    });
    defer svc.deinit();

    try svc.submitSplit(.{
        .transition_id = 71,
        .source_group_id = 11,
        .destination_group_id = 12,
    });
    try svc.submitMerge(.{
        .transition_id = 72,
        .donor_group_id = 91,
        .receiver_group_id = 92,
    });

    const split_observation = (try svc.observeSplit(71)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(data.SplitTransitionStatus, @TypeOf(split_observation.status));
    try std.testing.expectEqual(.replay_deltas, split_observation.status.phase);
    try std.testing.expectEqual(@as(u64, 3), split_observation.status.source_delta_sequence);
    const split_state = (try svc.describeSplit(71)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata.SplitExecutionStateTag.replay_blocked, split_state.tag);
    try std.testing.expect(split_state.actionable());
    try std.testing.expect(split_state.action == .catch_up_split_destination);

    const merge_observation = (try svc.observeMerge(72)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(.cutover_ready, merge_observation.receiver.phase);
    try std.testing.expectEqual(@as(u64, 4), merge_observation.receiver.receiver_delta_sequence);
    const merge_state = (try svc.describeMerge(72)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata.MergeExecutionStateTag.ready_to_finalize, merge_state.tag);
    try std.testing.expect(merge_state.actionable());
    try std.testing.expect(merge_state.action == .finalize_merge);

    try std.testing.expect((try svc.observeSplit(999)) == null);
    try std.testing.expect((try svc.observeMerge(999)) == null);
    try std.testing.expect((try svc.describeSplit(999)) == null);
    try std.testing.expect((try svc.describeMerge(999)) == null);
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
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 2301,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    var split = try transition_runtime.SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 2301,
        .dest_group_id = 2302,
    });
    defer split.deinit();

    var svc = TransitionService.init(std.testing.allocator, .{
        .split = split.runtime(),
    });
    defer svc.deinit();

    try svc.submitSplit(.{
        .transition_id = 991,
        .source_group_id = 2301,
        .destination_group_id = 2302,
    });

    const before = (try svc.describeSplit(991)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata.SplitExecutionStateTag.awaiting_source_start, before.tag);
    try std.testing.expect(before.action == .start_split_source);

    const step = try svc.stepPending();
    try std.testing.expectEqual(@as(usize, 1), step.stepped_split);
    try std.testing.expectEqual(@as(usize, 1), step.awaiting_split_source_start);

    const after = (try svc.describeSplit(991)) orelse return error.TestExpectedEqual;
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

    var svc = TransitionService.init(std.testing.allocator, .{
        .merge = merge.runtime(),
    });
    defer svc.deinit();

    try svc.submitMerge(.{
        .transition_id = 992,
        .donor_group_id = 2401,
        .receiver_group_id = 2402,
    });

    const before = (try svc.describeMerge(992)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(metadata.MergeExecutionStateTag.awaiting_receiver_acceptance, before.tag);
    try std.testing.expect(before.action == .accept_merge_receiver);

    var rounds: usize = 0;
    while (rounds < 8 and svc.metrics.completed_merge_transitions == 0) : (rounds += 1) {
        _ = try svc.stepPending();
    }

    try std.testing.expectEqual(@as(usize, 0), svc.metrics.queued_merge_transitions);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.completed_merge_transitions);

    var receiver = try data.SplitDestination.init(std.testing.allocator, .{ .root_dir = receiver_root });
    defer receiver.deinit();
    const range = receiver.getRange();
    try std.testing.expectEqualStrings("doc:a", range.start);
    try std.testing.expectEqualStrings("doc:z", range.end);
    const donor_doc = (try receiver.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(donor_doc);
    try std.testing.expectEqualStrings("{\"v\":\"donor\"}", donor_doc);
}
