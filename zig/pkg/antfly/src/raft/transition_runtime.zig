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
const data = @import("../data/mod.zig");
const metadata = @import("../metadata/mod.zig");
const shard_ops = @import("shard_ops.zig");
const raft_state_machine = @import("state_machine/mod.zig");

const PrepareSplitSource = std.meta.fieldInfo(metadata.TransitionAction, .prepare_split_source).type;
const StartSplitSource = std.meta.fieldInfo(metadata.TransitionAction, .start_split_source).type;
const BootstrapSplitDestination = std.meta.fieldInfo(metadata.TransitionAction, .bootstrap_split_destination).type;
const CatchUpSplitDestination = std.meta.fieldInfo(metadata.TransitionAction, .catch_up_split_destination).type;
const FinalizeSplitSource = std.meta.fieldInfo(metadata.TransitionAction, .finalize_split_source).type;
const RollbackSplit = std.meta.fieldInfo(metadata.TransitionAction, .rollback_split).type;
const AcceptMergeReceiver = std.meta.fieldInfo(metadata.TransitionAction, .accept_merge_receiver).type;
const CatchUpMergeReceiver = std.meta.fieldInfo(metadata.TransitionAction, .catch_up_merge_receiver).type;
const FinalizeMerge = std.meta.fieldInfo(metadata.TransitionAction, .finalize_merge).type;
const RollbackMerge = std.meta.fieldInfo(metadata.TransitionAction, .rollback_merge).type;

pub const SplitCoordinatorRuntime = struct {
    coordinator: data.SplitSyncCoordinator,

    pub fn init(alloc: std.mem.Allocator, cfg: data.SplitSyncConfig) !SplitCoordinatorRuntime {
        return .{
            .coordinator = try data.SplitSyncCoordinator.init(alloc, cfg),
        };
    }

    pub fn deinit(self: *SplitCoordinatorRuntime) void {
        self.coordinator.deinit();
        self.* = undefined;
    }

    pub fn runtime(self: *SplitCoordinatorRuntime) SplitRuntime {
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
        const self: *SplitCoordinatorRuntime = @ptrCast(@alignCast(ptr));
        const status = try self.coordinator.status();
        return .{
            .phase = status.phase,
            .source_split_phase = status.source_split_phase,
            .bootstrapped = status.bootstrapped,
            .replay_required = status.replay_required,
            .replay_caught_up = status.replay_caught_up,
            .cutover_ready = status.cutover_ready,
            .destination_ready_for_reads = status.destination_ready_for_reads,
            .source_delta_sequence = status.source_delta_sequence,
            .dest_delta_sequence = status.dest_delta_sequence,
        };
    }

    fn prepareSource(ptr: *anyopaque, _: u64, _: u64, split_key: []const u8, source_range_end: ?[]const u8) !bool {
        const self: *SplitCoordinatorRuntime = @ptrCast(@alignCast(ptr));
        return try self.coordinator.prepareSourceSplit(split_key, source_range_end);
    }

    fn startSource(ptr: *anyopaque, _: u64, _: u64) !bool {
        const self: *SplitCoordinatorRuntime = @ptrCast(@alignCast(ptr));
        return try self.coordinator.startSourceSplit();
    }

    fn bootstrapDestination(ptr: *anyopaque, _: u64, _: u64) !bool {
        const self: *SplitCoordinatorRuntime = @ptrCast(@alignCast(ptr));
        return try self.coordinator.ensureBootstrapped();
    }

    fn catchUpDestination(ptr: *anyopaque, _: u64, _: u64) !usize {
        const self: *SplitCoordinatorRuntime = @ptrCast(@alignCast(ptr));
        return try self.coordinator.catchUp();
    }

    fn finalizeSource(ptr: *anyopaque, _: u64, _: u64) !bool {
        const self: *SplitCoordinatorRuntime = @ptrCast(@alignCast(ptr));
        return try self.coordinator.finalizeSource();
    }

    fn rollbackSource(ptr: *anyopaque, _: u64, _: u64) !bool {
        const self: *SplitCoordinatorRuntime = @ptrCast(@alignCast(ptr));
        return try self.coordinator.rollbackSource();
    }
};

pub const MergeCoordinatorRuntime = struct {
    coordinator: data.MergeCoordinator,

    pub fn init(alloc: std.mem.Allocator, cfg: data.MergeConfig) !MergeCoordinatorRuntime {
        return .{
            .coordinator = try data.MergeCoordinator.init(alloc, cfg),
        };
    }

    pub fn deinit(self: *MergeCoordinatorRuntime) void {
        self.coordinator.deinit();
        self.* = undefined;
    }

    pub fn runtime(self: *MergeCoordinatorRuntime) MergeRuntime {
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
        const self: *MergeCoordinatorRuntime = @ptrCast(@alignCast(ptr));
        return try self.coordinator.status();
    }

    fn acceptReceiver(ptr: *anyopaque, _: u64, _: u64) !void {
        const self: *MergeCoordinatorRuntime = @ptrCast(@alignCast(ptr));
        try self.coordinator.acceptDonorRange();
    }

    fn catchUpReceiver(ptr: *anyopaque, _: u64, _: u64) !usize {
        const self: *MergeCoordinatorRuntime = @ptrCast(@alignCast(ptr));
        _ = try self.coordinator.ensureReceiverBootstrapped();
        return try self.coordinator.catchUp();
    }

    fn finalizeMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
        const self: *MergeCoordinatorRuntime = @ptrCast(@alignCast(ptr));
        return try self.coordinator.finalizeMerge();
    }

    fn rollbackMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
        const self: *MergeCoordinatorRuntime = @ptrCast(@alignCast(ptr));
        return try self.coordinator.rollbackMerge();
    }
};

pub const MultiplexedTransitionRuntime = struct {
    alloc: std.mem.Allocator,
    split_entries: std.ArrayListUnmanaged(SplitEntry) = .empty,
    merge_entries: std.ArrayListUnmanaged(MergeEntry) = .empty,

    const SplitEntry = struct {
        source_group_id: u64,
        destination_group_id: u64,
        runtime: SplitRuntime,
    };

    const MergeEntry = struct {
        donor_group_id: u64,
        receiver_group_id: u64,
        runtime: MergeRuntime,
    };

    pub fn init(alloc: std.mem.Allocator) MultiplexedTransitionRuntime {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MultiplexedTransitionRuntime) void {
        self.split_entries.deinit(self.alloc);
        self.merge_entries.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn addSplit(self: *MultiplexedTransitionRuntime, source_group_id: u64, destination_group_id: u64, split_runtime: SplitRuntime) !void {
        if (self.findSplitIndex(source_group_id, destination_group_id)) |index| {
            self.split_entries.items[index].runtime = split_runtime;
            return;
        }
        try self.split_entries.append(self.alloc, .{
            .source_group_id = source_group_id,
            .destination_group_id = destination_group_id,
            .runtime = split_runtime,
        });
    }

    pub fn addMerge(self: *MultiplexedTransitionRuntime, donor_group_id: u64, receiver_group_id: u64, merge_runtime: MergeRuntime) !void {
        if (self.findMergeIndex(donor_group_id, receiver_group_id)) |index| {
            self.merge_entries.items[index].runtime = merge_runtime;
            return;
        }
        try self.merge_entries.append(self.alloc, .{
            .donor_group_id = donor_group_id,
            .receiver_group_id = receiver_group_id,
            .runtime = merge_runtime,
        });
    }

    pub fn runtime(self: *MultiplexedTransitionRuntime) TransitionRuntime {
        return .{
            .split = .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = multiplexObserveSplit,
                    .prepare_source = multiplexPrepareSplit,
                    .start_source = multiplexStartSplit,
                    .bootstrap_destination = multiplexBootstrapSplit,
                    .catch_up_destination = multiplexCatchUpSplit,
                    .finalize_source = multiplexFinalizeSplit,
                    .rollback_source = multiplexRollbackSplit,
                },
            },
            .merge = .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = multiplexObserveMerge,
                    .accept_receiver = multiplexAcceptMerge,
                    .catch_up_receiver = multiplexCatchUpMerge,
                    .finalize_merge = multiplexFinalizeMerge,
                    .rollback_merge = multiplexRollbackMerge,
                },
            },
        };
    }

    fn findSplitIndex(self: *const MultiplexedTransitionRuntime, source_group_id: u64, destination_group_id: u64) ?usize {
        for (self.split_entries.items, 0..) |entry, i| {
            if (entry.source_group_id == source_group_id and entry.destination_group_id == destination_group_id) return i;
        }
        return null;
    }

    fn findMergeIndex(self: *const MultiplexedTransitionRuntime, donor_group_id: u64, receiver_group_id: u64) ?usize {
        for (self.merge_entries.items, 0..) |entry, i| {
            if (entry.donor_group_id == donor_group_id and entry.receiver_group_id == receiver_group_id) return i;
        }
        return null;
    }

    fn requireSplit(self: *const MultiplexedTransitionRuntime, source_group_id: u64, destination_group_id: u64) !SplitRuntime {
        const index = self.findSplitIndex(source_group_id, destination_group_id) orelse return error.UnknownSplitRuntime;
        return self.split_entries.items[index].runtime;
    }

    fn requireMerge(self: *const MultiplexedTransitionRuntime, donor_group_id: u64, receiver_group_id: u64) !MergeRuntime {
        const index = self.findMergeIndex(donor_group_id, receiver_group_id) orelse return error.UnknownMergeRuntime;
        return self.merge_entries.items[index].runtime;
    }

    fn multiplexObserveSplit(ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) !data.SplitTransitionStatus {
        const self: *MultiplexedTransitionRuntime = @ptrCast(@alignCast(ptr));
        const split_runtime = try self.requireSplit(source_group_id, destination_group_id);
        return try split_runtime.observeStatus(source_group_id, destination_group_id);
    }

    fn multiplexPrepareSplit(ptr: *anyopaque, source_group_id: u64, destination_group_id: u64, split_key: []const u8, source_range_end: ?[]const u8) !bool {
        const self: *MultiplexedTransitionRuntime = @ptrCast(@alignCast(ptr));
        const split_runtime = try self.requireSplit(source_group_id, destination_group_id);
        return try split_runtime.prepareSource(source_group_id, destination_group_id, split_key, source_range_end);
    }

    fn multiplexStartSplit(ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) !bool {
        const self: *MultiplexedTransitionRuntime = @ptrCast(@alignCast(ptr));
        const split_runtime = try self.requireSplit(source_group_id, destination_group_id);
        return try split_runtime.startSource(source_group_id, destination_group_id);
    }

    fn multiplexBootstrapSplit(ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) !bool {
        const self: *MultiplexedTransitionRuntime = @ptrCast(@alignCast(ptr));
        const split_runtime = try self.requireSplit(source_group_id, destination_group_id);
        return try split_runtime.bootstrapDestination(source_group_id, destination_group_id);
    }

    fn multiplexCatchUpSplit(ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) !usize {
        const self: *MultiplexedTransitionRuntime = @ptrCast(@alignCast(ptr));
        const split_runtime = try self.requireSplit(source_group_id, destination_group_id);
        return try split_runtime.catchUpDestination(source_group_id, destination_group_id);
    }

    fn multiplexFinalizeSplit(ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) !bool {
        const self: *MultiplexedTransitionRuntime = @ptrCast(@alignCast(ptr));
        const split_runtime = try self.requireSplit(source_group_id, destination_group_id);
        return try split_runtime.finalizeSource(source_group_id, destination_group_id);
    }

    fn multiplexRollbackSplit(ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) !bool {
        const self: *MultiplexedTransitionRuntime = @ptrCast(@alignCast(ptr));
        const split_runtime = try self.requireSplit(source_group_id, destination_group_id);
        return try split_runtime.rollbackSource(source_group_id, destination_group_id);
    }

    fn multiplexObserveMerge(ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) !data.MergeTransitionStatus {
        const self: *MultiplexedTransitionRuntime = @ptrCast(@alignCast(ptr));
        const merge_runtime = try self.requireMerge(donor_group_id, receiver_group_id);
        return try merge_runtime.observeStatus(donor_group_id, receiver_group_id);
    }

    fn multiplexAcceptMerge(ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) !void {
        const self: *MultiplexedTransitionRuntime = @ptrCast(@alignCast(ptr));
        const merge_runtime = try self.requireMerge(donor_group_id, receiver_group_id);
        try merge_runtime.acceptReceiver(donor_group_id, receiver_group_id);
    }

    fn multiplexCatchUpMerge(ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) !usize {
        const self: *MultiplexedTransitionRuntime = @ptrCast(@alignCast(ptr));
        const merge_runtime = try self.requireMerge(donor_group_id, receiver_group_id);
        return try merge_runtime.catchUpReceiver(donor_group_id, receiver_group_id);
    }

    fn multiplexFinalizeMerge(ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) !bool {
        const self: *MultiplexedTransitionRuntime = @ptrCast(@alignCast(ptr));
        const merge_runtime = try self.requireMerge(donor_group_id, receiver_group_id);
        return try merge_runtime.finalizeMerge(donor_group_id, receiver_group_id);
    }

    fn multiplexRollbackMerge(ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) !bool {
        const self: *MultiplexedTransitionRuntime = @ptrCast(@alignCast(ptr));
        const merge_runtime = try self.requireMerge(donor_group_id, receiver_group_id);
        return try merge_runtime.rollbackMerge(donor_group_id, receiver_group_id);
    }
};

pub const SplitRuntime = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        observe_status: *const fn (ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) anyerror!data.SplitTransitionStatus,
        prepare_source: *const fn (ptr: *anyopaque, source_group_id: u64, destination_group_id: u64, split_key: []const u8, source_range_end: ?[]const u8) anyerror!bool,
        start_source: *const fn (ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) anyerror!bool,
        bootstrap_destination: *const fn (ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) anyerror!bool,
        catch_up_destination: *const fn (ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) anyerror!usize,
        finalize_source: *const fn (ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) anyerror!bool,
        rollback_source: *const fn (ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) anyerror!bool,
    };

    pub fn observeStatus(self: SplitRuntime, source_group_id: u64, destination_group_id: u64) !data.SplitTransitionStatus {
        return try self.vtable.observe_status(self.ptr, source_group_id, destination_group_id);
    }

    pub fn prepareSource(self: SplitRuntime, source_group_id: u64, destination_group_id: u64, split_key: []const u8, source_range_end: ?[]const u8) !bool {
        return try self.vtable.prepare_source(self.ptr, source_group_id, destination_group_id, split_key, source_range_end);
    }

    pub fn startSource(self: SplitRuntime, source_group_id: u64, destination_group_id: u64) !bool {
        return try self.vtable.start_source(self.ptr, source_group_id, destination_group_id);
    }

    pub fn bootstrapDestination(self: SplitRuntime, source_group_id: u64, destination_group_id: u64) !bool {
        return try self.vtable.bootstrap_destination(self.ptr, source_group_id, destination_group_id);
    }

    pub fn catchUpDestination(self: SplitRuntime, source_group_id: u64, destination_group_id: u64) !usize {
        return try self.vtable.catch_up_destination(self.ptr, source_group_id, destination_group_id);
    }

    pub fn finalizeSource(self: SplitRuntime, source_group_id: u64, destination_group_id: u64) !bool {
        return try self.vtable.finalize_source(self.ptr, source_group_id, destination_group_id);
    }

    pub fn rollbackSource(self: SplitRuntime, source_group_id: u64, destination_group_id: u64) !bool {
        return try self.vtable.rollback_source(self.ptr, source_group_id, destination_group_id);
    }
};

pub const MergeRuntime = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        observe_status: *const fn (ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) anyerror!data.MergeTransitionStatus,
        accept_receiver: *const fn (ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) anyerror!void,
        catch_up_receiver: *const fn (ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) anyerror!usize,
        finalize_merge: *const fn (ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) anyerror!bool,
        rollback_merge: *const fn (ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) anyerror!bool,
    };

    pub fn observeStatus(self: MergeRuntime, donor_group_id: u64, receiver_group_id: u64) !data.MergeTransitionStatus {
        return try self.vtable.observe_status(self.ptr, donor_group_id, receiver_group_id);
    }

    pub fn acceptReceiver(self: MergeRuntime, donor_group_id: u64, receiver_group_id: u64) !void {
        try self.vtable.accept_receiver(self.ptr, donor_group_id, receiver_group_id);
    }

    pub fn catchUpReceiver(self: MergeRuntime, donor_group_id: u64, receiver_group_id: u64) !usize {
        return try self.vtable.catch_up_receiver(self.ptr, donor_group_id, receiver_group_id);
    }

    pub fn finalizeMerge(self: MergeRuntime, donor_group_id: u64, receiver_group_id: u64) !bool {
        return try self.vtable.finalize_merge(self.ptr, donor_group_id, receiver_group_id);
    }

    pub fn rollbackMerge(self: MergeRuntime, donor_group_id: u64, receiver_group_id: u64) !bool {
        return try self.vtable.rollback_merge(self.ptr, donor_group_id, receiver_group_id);
    }
};

pub const TransitionRuntime = struct {
    split: ?SplitRuntime = null,
    merge: ?MergeRuntime = null,

    pub fn shardOperationAdapter(self: *const TransitionRuntime) shard_ops.ShardOperationAdapter {
        return .{
            .ptr = @constCast(self),
            .vtable = &.{
                .observe_split = observeSplitAdapter,
                .observe_merge = observeMergeAdapter,
                .prepare_split_source = prepareSplitSourceAdapter,
                .start_split_source = startSplitSourceAdapter,
                .bootstrap_split_destination = bootstrapSplitDestinationAdapter,
                .catch_up_split_destination = catchUpSplitDestinationAdapter,
                .finalize_split_source = finalizeSplitSourceAdapter,
                .rollback_split = rollbackSplitAdapter,
                .accept_merge_receiver = acceptMergeReceiverAdapter,
                .catch_up_merge_receiver = catchUpMergeReceiverAdapter,
                .finalize_merge = finalizeMergeAdapter,
                .rollback_merge = rollbackMergeAdapter,
            },
        };
    }

    pub fn metadataRuntime(self: *TransitionRuntime) metadata.MetadataTransitionRuntime {
        return .{
            .ptr = self,
            .vtable = &.{
                .observe_split = observeSplitMeta,
                .observe_merge = observeMergeMeta,
                .execute = executeMeta,
            },
        };
    }

    pub fn observeSplit(self: TransitionRuntime, record: metadata.SplitTransitionRecord) !data.SplitTransitionStatus {
        const split = self.split orelse return error.MissingSplitRuntime;
        return try split.observeStatus(record.source_group_id, record.destination_group_id);
    }

    pub fn observeMerge(self: TransitionRuntime, record: metadata.MergeTransitionRecord) !metadata.MergeObservation {
        const merge = self.merge orelse return error.MissingMergeRuntime;
        const status = try merge.observeStatus(record.donor_group_id, record.receiver_group_id);
        return .{
            .donor = status,
            .receiver = status,
        };
    }

    pub fn execute(self: TransitionRuntime, action: metadata.TransitionAction) !void {
        switch (action) {
            .none => {},
            .prepare_split_source => |op| {
                const split = self.split orelse return error.MissingSplitRuntime;
                _ = try split.prepareSource(op.source_group_id, op.destination_group_id, op.split_key, op.source_range_end);
            },
            .start_split_source => |op| {
                const split = self.split orelse return error.MissingSplitRuntime;
                _ = try split.startSource(op.source_group_id, op.destination_group_id);
            },
            .bootstrap_split_destination => |op| {
                const split = self.split orelse return error.MissingSplitRuntime;
                _ = try split.bootstrapDestination(op.source_group_id, op.destination_group_id);
            },
            .catch_up_split_destination => |op| {
                const split = self.split orelse return error.MissingSplitRuntime;
                _ = try split.catchUpDestination(op.source_group_id, op.destination_group_id);
            },
            .finalize_split_source => |op| {
                const split = self.split orelse return error.MissingSplitRuntime;
                _ = try split.finalizeSource(op.source_group_id, op.destination_group_id);
            },
            .rollback_split => |op| {
                const split = self.split orelse return error.MissingSplitRuntime;
                _ = try split.rollbackSource(op.source_group_id, op.destination_group_id);
            },
            .accept_merge_receiver => |op| {
                const merge = self.merge orelse return error.MissingMergeRuntime;
                try merge.acceptReceiver(op.donor_group_id, op.receiver_group_id);
            },
            .catch_up_merge_receiver => |op| {
                const merge = self.merge orelse return error.MissingMergeRuntime;
                _ = try merge.catchUpReceiver(op.donor_group_id, op.receiver_group_id);
            },
            .finalize_merge => |op| {
                const merge = self.merge orelse return error.MissingMergeRuntime;
                _ = try merge.finalizeMerge(op.donor_group_id, op.receiver_group_id);
            },
            .rollback_merge => |op| {
                const merge = self.merge orelse return error.MissingMergeRuntime;
                _ = try merge.rollbackMerge(op.donor_group_id, op.receiver_group_id);
            },
        }
    }

    fn observeSplitMeta(ptr: *anyopaque, record: metadata.SplitTransitionRecord) !metadata.SplitObservation {
        const self: *TransitionRuntime = @ptrCast(@alignCast(ptr));
        return .{
            .status = try self.observeSplit(record),
        };
    }

    fn observeMergeMeta(ptr: *anyopaque, record: metadata.MergeTransitionRecord) !metadata.MergeObservation {
        const self: *TransitionRuntime = @ptrCast(@alignCast(ptr));
        return try self.observeMerge(record);
    }

    fn executeMeta(ptr: *anyopaque, action: metadata.TransitionAction) !void {
        const self: *TransitionRuntime = @ptrCast(@alignCast(ptr));
        try self.execute(action);
    }

    fn observeSplitAdapter(ptr: *anyopaque, record: metadata.SplitTransitionRecord) !metadata.SplitObservation {
        const self: *const TransitionRuntime = @ptrCast(@alignCast(ptr));
        return .{ .status = try self.observeSplit(record) };
    }

    fn observeMergeAdapter(ptr: *anyopaque, record: metadata.MergeTransitionRecord) !metadata.MergeObservation {
        const self: *const TransitionRuntime = @ptrCast(@alignCast(ptr));
        return try self.observeMerge(record);
    }

    fn prepareSplitSourceAdapter(ptr: *anyopaque, op: PrepareSplitSource) !void {
        const self: *TransitionRuntime = @ptrCast(@alignCast(ptr));
        try self.execute(.{ .prepare_split_source = op });
    }

    fn startSplitSourceAdapter(ptr: *anyopaque, op: StartSplitSource) !void {
        const self: *TransitionRuntime = @ptrCast(@alignCast(ptr));
        try self.execute(.{ .start_split_source = op });
    }

    fn bootstrapSplitDestinationAdapter(ptr: *anyopaque, op: BootstrapSplitDestination) !void {
        const self: *TransitionRuntime = @ptrCast(@alignCast(ptr));
        try self.execute(.{ .bootstrap_split_destination = op });
    }

    fn catchUpSplitDestinationAdapter(ptr: *anyopaque, op: CatchUpSplitDestination) !void {
        const self: *TransitionRuntime = @ptrCast(@alignCast(ptr));
        try self.execute(.{ .catch_up_split_destination = op });
    }

    fn finalizeSplitSourceAdapter(ptr: *anyopaque, op: FinalizeSplitSource) !void {
        const self: *TransitionRuntime = @ptrCast(@alignCast(ptr));
        try self.execute(.{ .finalize_split_source = op });
    }

    fn rollbackSplitAdapter(ptr: *anyopaque, op: RollbackSplit) !void {
        const self: *TransitionRuntime = @ptrCast(@alignCast(ptr));
        try self.execute(.{ .rollback_split = op });
    }

    fn acceptMergeReceiverAdapter(ptr: *anyopaque, op: AcceptMergeReceiver) !void {
        const self: *TransitionRuntime = @ptrCast(@alignCast(ptr));
        try self.execute(.{ .accept_merge_receiver = op });
    }

    fn catchUpMergeReceiverAdapter(ptr: *anyopaque, op: CatchUpMergeReceiver) !void {
        const self: *TransitionRuntime = @ptrCast(@alignCast(ptr));
        try self.execute(.{ .catch_up_merge_receiver = op });
    }

    fn finalizeMergeAdapter(ptr: *anyopaque, op: FinalizeMerge) !void {
        const self: *TransitionRuntime = @ptrCast(@alignCast(ptr));
        try self.execute(.{ .finalize_merge = op });
    }

    fn rollbackMergeAdapter(ptr: *anyopaque, op: RollbackMerge) !void {
        const self: *TransitionRuntime = @ptrCast(@alignCast(ptr));
        try self.execute(.{ .rollback_merge = op });
    }
};

test "transition runtime executes split and merge actions through local seams" {
    const FakeSplit = struct {
        calls: std.ArrayListUnmanaged([]const u8) = .empty,
        status: data.SplitTransitionStatus,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) SplitRuntime {
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
            try self.calls.append(std.testing.allocator, "prepare");
            return true;
        }

        fn startSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "start");
            return true;
        }

        fn bootstrapDestination(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "bootstrap");
            return true;
        }

        fn catchUpDestination(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "catchup");
            return 1;
        }

        fn finalizeSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "finalize");
            return true;
        }

        fn rollbackSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "rollback");
            return true;
        }
    };

    const FakeMerge = struct {
        calls: std.ArrayListUnmanaged([]const u8) = .empty,
        status: data.MergeTransitionStatus,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) MergeRuntime {
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
            try self.calls.append(std.testing.allocator, "accept");
        }

        fn catchUpReceiver(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "catchup");
            return 2;
        }

        fn finalizeMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "finalize");
            return true;
        }

        fn rollbackMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "rollback");
            return true;
        }
    };

    var split = FakeSplit{
        .status = .{
            .phase = .cutover_ready,
            .source_split_phase = .finalizing,
            .bootstrapped = true,
            .replay_required = true,
            .replay_caught_up = true,
            .cutover_ready = true,
            .destination_ready_for_reads = true,
            .source_delta_sequence = 1,
            .dest_delta_sequence = 1,
        },
    };
    defer split.deinit(std.testing.allocator);

    var merge = FakeMerge{
        .status = .{
            .phase = .cutover_ready,
            .donor_group_id = 61,
            .receiver_group_id = 62,
            .receiver_accepts_donor_range = true,
            .bootstrapped = true,
            .replay_required = true,
            .replay_caught_up = true,
            .cutover_ready = true,
            .receiver_ready_for_reads = true,
            .donor_delta_sequence = 2,
            .receiver_delta_sequence = 2,
        },
    };
    defer merge.deinit(std.testing.allocator);

    const runtime = TransitionRuntime{
        .split = split.iface(),
        .merge = merge.iface(),
    };

    const split_status = try runtime.observeSplit(.{
        .transition_id = 1,
        .source_group_id = 41,
        .destination_group_id = 42,
    });
    try std.testing.expectEqual(data.RangeTransitionPhase.cutover_ready, split_status.phase);

    const merge_observation = try runtime.observeMerge(.{
        .transition_id = 2,
        .donor_group_id = 61,
        .receiver_group_id = 62,
    });
    try std.testing.expectEqual(data.RangeTransitionPhase.cutover_ready, merge_observation.receiver.phase);

    try runtime.execute(.{ .start_split_source = .{ .transition_id = 1, .source_group_id = 41, .destination_group_id = 42 } });
    try runtime.execute(.{ .bootstrap_split_destination = .{ .transition_id = 1, .source_group_id = 41, .destination_group_id = 42 } });
    try runtime.execute(.{ .finalize_split_source = .{ .transition_id = 1, .source_group_id = 41, .destination_group_id = 42 } });
    try runtime.execute(.{ .accept_merge_receiver = .{ .transition_id = 2, .donor_group_id = 61, .receiver_group_id = 62 } });
    try runtime.execute(.{ .finalize_merge = .{ .transition_id = 2, .donor_group_id = 61, .receiver_group_id = 62 } });

    try std.testing.expectEqual(@as(usize, 3), split.calls.items.len);
    try std.testing.expectEqualStrings("start", split.calls.items[0]);
    try std.testing.expectEqualStrings("bootstrap", split.calls.items[1]);
    try std.testing.expectEqualStrings("finalize", split.calls.items[2]);
    try std.testing.expectEqual(@as(usize, 2), merge.calls.items.len);
    try std.testing.expectEqualStrings("accept", merge.calls.items[0]);
    try std.testing.expectEqualStrings("finalize", merge.calls.items[1]);
}

test "multiplexed transition runtime dispatches by group ids" {
    const FakeSplit = struct {
        calls: std.ArrayListUnmanaged([]const u8) = .empty,
        status: data.SplitTransitionStatus,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) SplitRuntime {
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
            try self.calls.append(std.testing.allocator, "prepare");
            return true;
        }

        fn startSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "start");
            return true;
        }

        fn bootstrapDestination(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "bootstrap");
            return true;
        }

        fn catchUpDestination(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "catchup");
            return 1;
        }

        fn finalizeSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "finalize");
            return true;
        }

        fn rollbackSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "rollback");
            return true;
        }
    };

    const FakeMerge = struct {
        calls: std.ArrayListUnmanaged([]const u8) = .empty,
        status: data.MergeTransitionStatus,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) MergeRuntime {
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
            try self.calls.append(std.testing.allocator, "accept");
        }

        fn catchUpReceiver(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "catchup");
            return 1;
        }

        fn finalizeMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "finalize");
            return true;
        }

        fn rollbackMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "rollback");
            return true;
        }
    };

    var split_a = FakeSplit{ .status = .{
        .phase = .prepare,
        .source_split_phase = .prepare,
        .bootstrapped = false,
        .replay_required = false,
        .replay_caught_up = false,
        .cutover_ready = false,
        .destination_ready_for_reads = false,
        .source_delta_sequence = 0,
        .dest_delta_sequence = 0,
    } };
    defer split_a.deinit(std.testing.allocator);
    var split_b = FakeSplit{ .status = .{
        .phase = .cutover_ready,
        .source_split_phase = .finalizing,
        .bootstrapped = true,
        .replay_required = true,
        .replay_caught_up = true,
        .cutover_ready = true,
        .destination_ready_for_reads = true,
        .source_delta_sequence = 3,
        .dest_delta_sequence = 3,
    } };
    defer split_b.deinit(std.testing.allocator);
    var merge_a = FakeMerge{ .status = .{
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
    } };
    defer merge_a.deinit(std.testing.allocator);
    var merge_b = FakeMerge{ .status = .{
        .phase = .cutover_ready,
        .donor_group_id = 31,
        .receiver_group_id = 32,
        .receiver_accepts_donor_range = true,
        .bootstrapped = true,
        .replay_required = true,
        .replay_caught_up = true,
        .cutover_ready = true,
        .receiver_ready_for_reads = true,
        .donor_delta_sequence = 4,
        .receiver_delta_sequence = 4,
    } };
    defer merge_b.deinit(std.testing.allocator);

    var multiplex = MultiplexedTransitionRuntime.init(std.testing.allocator);
    defer multiplex.deinit();
    try multiplex.addSplit(1, 2, split_a.iface());
    try multiplex.addSplit(3, 4, split_b.iface());
    try multiplex.addMerge(21, 22, merge_a.iface());
    try multiplex.addMerge(31, 32, merge_b.iface());

    const runtime = multiplex.runtime();
    const split_status = try runtime.observeSplit(.{ .transition_id = 1, .source_group_id = 3, .destination_group_id = 4 });
    try std.testing.expectEqual(data.RangeTransitionPhase.cutover_ready, split_status.phase);
    const merge_observation = try runtime.observeMerge(.{ .transition_id = 2, .donor_group_id = 21, .receiver_group_id = 22 });
    try std.testing.expectEqual(data.RangeTransitionPhase.prepare, merge_observation.receiver.phase);

    try runtime.execute(.{ .start_split_source = .{ .transition_id = 10, .source_group_id = 1, .destination_group_id = 2 } });
    try runtime.execute(.{ .finalize_split_source = .{ .transition_id = 11, .source_group_id = 3, .destination_group_id = 4 } });
    try runtime.execute(.{ .accept_merge_receiver = .{ .transition_id = 12, .donor_group_id = 21, .receiver_group_id = 22 } });
    try runtime.execute(.{ .rollback_merge = .{ .transition_id = 13, .donor_group_id = 31, .receiver_group_id = 32 } });

    try std.testing.expectEqual(@as(usize, 1), split_a.calls.items.len);
    try std.testing.expectEqualStrings("start", split_a.calls.items[0]);
    try std.testing.expectEqual(@as(usize, 1), split_b.calls.items.len);
    try std.testing.expectEqualStrings("finalize", split_b.calls.items[0]);
    try std.testing.expectEqual(@as(usize, 1), merge_a.calls.items.len);
    try std.testing.expectEqualStrings("accept", merge_a.calls.items[0]);
    try std.testing.expectEqual(@as(usize, 1), merge_b.calls.items.len);
    try std.testing.expectEqualStrings("rollback", merge_b.calls.items[0]);
}

test "metadata transition controller drives split runtime deterministically" {
    const StatefulSplit = struct {
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
        calls: std.ArrayListUnmanaged([]const u8) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) SplitRuntime {
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
            try self.calls.append(std.testing.allocator, "prepare");
            self.status = .{
                .phase = .prepare,
                .source_split_phase = .prepare,
                .bootstrapped = false,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = false,
                .destination_ready_for_reads = false,
                .source_delta_sequence = 0,
                .dest_delta_sequence = 0,
            };
            return true;
        }

        fn startSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "start");
            self.status = .{
                .phase = .bootstrap_peer,
                .source_split_phase = .splitting,
                .bootstrapped = false,
                .replay_required = true,
                .replay_caught_up = false,
                .cutover_ready = false,
                .destination_ready_for_reads = false,
                .source_delta_sequence = 1,
                .dest_delta_sequence = 0,
            };
            return true;
        }

        fn bootstrapDestination(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "bootstrap");
            self.status = .{
                .phase = .replay_deltas,
                .source_split_phase = .splitting,
                .bootstrapped = true,
                .replay_required = true,
                .replay_caught_up = false,
                .cutover_ready = false,
                .destination_ready_for_reads = false,
                .source_delta_sequence = 2,
                .dest_delta_sequence = 1,
            };
            return true;
        }

        fn catchUpDestination(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "catchup");
            self.status = .{
                .phase = .cutover_ready,
                .source_split_phase = .finalizing,
                .bootstrapped = true,
                .replay_required = true,
                .replay_caught_up = true,
                .cutover_ready = true,
                .destination_ready_for_reads = true,
                .source_delta_sequence = 2,
                .dest_delta_sequence = 2,
            };
            return 1;
        }

        fn finalizeSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "finalize");
            self.status = .{
                .phase = .finalized,
                .source_split_phase = .none,
                .bootstrapped = true,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = true,
                .destination_ready_for_reads = true,
                .source_delta_sequence = 2,
                .dest_delta_sequence = 2,
            };
            return true;
        }

        fn rollbackSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "rollback");
            self.status = .{
                .phase = .rolled_back,
                .source_split_phase = .none,
                .bootstrapped = false,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = false,
                .destination_ready_for_reads = false,
                .source_delta_sequence = 0,
                .dest_delta_sequence = 0,
            };
            return true;
        }
    };

    var split = StatefulSplit{};
    defer split.deinit(std.testing.allocator);

    const runtime = TransitionRuntime{ .split = split.iface() };
    var record = metadata.SplitTransitionRecord{
        .transition_id = 20,
        .source_group_id = 71,
        .destination_group_id = 72,
    };

    var rounds: usize = 0;
    while (rounds < 5 and record.phase != .finalized) : (rounds += 1) {
        const observation = metadata.SplitObservation{
            .status = try runtime.observeSplit(record),
        };
        const decision = metadata.TransitionController.planSplit(record, observation);
        record.phase = decision.next_phase;
        try runtime.execute(decision.action);
    }

    try std.testing.expectEqual(metadata.TransitionPhase.finalized, record.phase);
    try std.testing.expectEqual(@as(usize, 4), split.calls.items.len);
    try std.testing.expectEqualStrings("start", split.calls.items[0]);
    try std.testing.expectEqualStrings("bootstrap", split.calls.items[1]);
    try std.testing.expectEqualStrings("catchup", split.calls.items[2]);
    try std.testing.expectEqualStrings("finalize", split.calls.items[3]);
}

test "metadata transition controller drives merge runtime deterministically" {
    const StatefulMerge = struct {
        status: data.MergeTransitionStatus = .{
            .phase = .prepare,
            .donor_group_id = 81,
            .receiver_group_id = 82,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },
        calls: std.ArrayListUnmanaged([]const u8) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) MergeRuntime {
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
            try self.calls.append(std.testing.allocator, "accept");
            self.status = .{
                .phase = .bootstrap_peer,
                .donor_group_id = 81,
                .receiver_group_id = 82,
                .receiver_accepts_donor_range = true,
                .bootstrapped = false,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = false,
                .receiver_ready_for_reads = false,
                .donor_delta_sequence = 0,
                .receiver_delta_sequence = 0,
            };
        }

        fn catchUpReceiver(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "catchup");
            self.status = .{
                .phase = .cutover_ready,
                .donor_group_id = 81,
                .receiver_group_id = 82,
                .receiver_accepts_donor_range = true,
                .bootstrapped = true,
                .replay_required = true,
                .replay_caught_up = true,
                .cutover_ready = true,
                .receiver_ready_for_reads = true,
                .donor_delta_sequence = 4,
                .receiver_delta_sequence = 4,
            };
            return 1;
        }

        fn finalizeMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "finalize");
            self.status = .{
                .phase = .finalized,
                .donor_group_id = 81,
                .receiver_group_id = 82,
                .receiver_accepts_donor_range = true,
                .bootstrapped = true,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = true,
                .receiver_ready_for_reads = true,
                .donor_delta_sequence = 4,
                .receiver_delta_sequence = 4,
            };
            return true;
        }

        fn rollbackMerge(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.calls.append(std.testing.allocator, "rollback");
            self.status = .{
                .phase = .rolled_back,
                .donor_group_id = 81,
                .receiver_group_id = 82,
                .receiver_accepts_donor_range = false,
                .bootstrapped = false,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = false,
                .receiver_ready_for_reads = false,
                .donor_delta_sequence = 0,
                .receiver_delta_sequence = 0,
            };
            return true;
        }
    };

    var merge = StatefulMerge{};
    defer merge.deinit(std.testing.allocator);

    const runtime = TransitionRuntime{ .merge = merge.iface() };
    var record = metadata.MergeTransitionRecord{
        .transition_id = 21,
        .donor_group_id = 81,
        .receiver_group_id = 82,
    };

    var rounds: usize = 0;
    while (rounds < 4 and record.phase != .finalized) : (rounds += 1) {
        const observation = try runtime.observeMerge(record);
        const decision = metadata.TransitionController.planMerge(record, observation);
        record.phase = decision.next_phase;
        try runtime.execute(decision.action);
    }

    try std.testing.expectEqual(metadata.TransitionPhase.finalized, record.phase);
    try std.testing.expectEqual(@as(usize, 3), merge.calls.items.len);
    try std.testing.expectEqualStrings("accept", merge.calls.items[0]);
    try std.testing.expectEqualStrings("catchup", merge.calls.items[1]);
    try std.testing.expectEqualStrings("finalize", merge.calls.items[2]);
}

test "coordinator-backed transition runtime types compile" {
    _ = SplitCoordinatorRuntime;
    _ = MergeCoordinatorRuntime;
    _ = raft_state_machine;
}

test "real split coordinator runtime observes prepared source state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/runtime-real-split-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/runtime-real-split-dst", .{tmp.sub_path});
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
            .group_id = 1701,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    {
        var reopened = try data.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer reopened.deinit();
        const split_state = (try reopened.currentSplitState(std.testing.allocator, 1701)) orelse return error.MissingSplitState;
        defer data.storage.shard_state_store.freeSplitState(std.testing.allocator, split_state);
        try std.testing.expectEqual(data.storage.shard_state_store.SplitPhase.prepare, split_state.phase);
        try std.testing.expectEqualStrings("doc:m", split_state.split_key);
    }

    var split = try SplitCoordinatorRuntime.init(std.testing.allocator, .{
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 1701,
        .dest_group_id = 1702,
    });
    defer split.deinit();

    const runtime = TransitionRuntime{ .split = split.runtime() };
    const observation = try runtime.observeSplit(.{
        .transition_id = 1,
        .source_group_id = 1701,
        .destination_group_id = 1702,
    });

    try std.testing.expectEqual(data.storage.range_transition.TransitionPhase.prepare, observation.phase);
    try std.testing.expectEqual(data.storage.shard_state_store.SplitPhase.prepare, observation.source_split_phase.?);
    try std.testing.expect(!observation.bootstrapped);
}
