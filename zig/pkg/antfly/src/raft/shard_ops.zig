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
const metadata_actions = @import("../metadata/transition_actions.zig");
const metadata_driver = @import("../metadata/transition_driver.zig");
const metadata_state = @import("../metadata/transition_state.zig");

const PrepareSplitSource = std.meta.fieldInfo(metadata_actions.TransitionAction, .prepare_split_source).type;
const StartSplitSource = std.meta.fieldInfo(metadata_actions.TransitionAction, .start_split_source).type;
const BootstrapSplitDestination = std.meta.fieldInfo(metadata_actions.TransitionAction, .bootstrap_split_destination).type;
const CatchUpSplitDestination = std.meta.fieldInfo(metadata_actions.TransitionAction, .catch_up_split_destination).type;
const FinalizeSplitSource = std.meta.fieldInfo(metadata_actions.TransitionAction, .finalize_split_source).type;
const RollbackSplit = std.meta.fieldInfo(metadata_actions.TransitionAction, .rollback_split).type;
const AcceptMergeReceiver = std.meta.fieldInfo(metadata_actions.TransitionAction, .accept_merge_receiver).type;
const CatchUpMergeReceiver = std.meta.fieldInfo(metadata_actions.TransitionAction, .catch_up_merge_receiver).type;
const FinalizeMerge = std.meta.fieldInfo(metadata_actions.TransitionAction, .finalize_merge).type;
const RollbackMerge = std.meta.fieldInfo(metadata_actions.TransitionAction, .rollback_merge).type;

pub const ShardOperationAdapter = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        observe_split: *const fn (ptr: *anyopaque, record: metadata_state.SplitTransitionRecord) anyerror!metadata_state.SplitObservation,
        observe_merge: *const fn (ptr: *anyopaque, record: metadata_state.MergeTransitionRecord) anyerror!metadata_state.MergeObservation,
        prepare_split_source: *const fn (ptr: *anyopaque, op: PrepareSplitSource) anyerror!void,
        start_split_source: *const fn (ptr: *anyopaque, op: StartSplitSource) anyerror!void,
        bootstrap_split_destination: *const fn (ptr: *anyopaque, op: BootstrapSplitDestination) anyerror!void,
        catch_up_split_destination: *const fn (ptr: *anyopaque, op: CatchUpSplitDestination) anyerror!void,
        finalize_split_source: *const fn (ptr: *anyopaque, op: FinalizeSplitSource) anyerror!void,
        rollback_split: *const fn (ptr: *anyopaque, op: RollbackSplit) anyerror!void,
        accept_merge_receiver: *const fn (ptr: *anyopaque, op: AcceptMergeReceiver) anyerror!void,
        catch_up_merge_receiver: *const fn (ptr: *anyopaque, op: CatchUpMergeReceiver) anyerror!void,
        finalize_merge: *const fn (ptr: *anyopaque, op: FinalizeMerge) anyerror!void,
        rollback_merge: *const fn (ptr: *anyopaque, op: RollbackMerge) anyerror!void,
    };

    pub fn observeSplit(self: ShardOperationAdapter, record: metadata_state.SplitTransitionRecord) !metadata_state.SplitObservation {
        return try self.vtable.observe_split(self.ptr, record);
    }

    pub fn observeMerge(self: ShardOperationAdapter, record: metadata_state.MergeTransitionRecord) !metadata_state.MergeObservation {
        return try self.vtable.observe_merge(self.ptr, record);
    }

    pub fn execute(self: ShardOperationAdapter, action: metadata_actions.TransitionAction) !void {
        switch (action) {
            .none => {},
            .prepare_split_source => |op| try self.vtable.prepare_split_source(self.ptr, op),
            .start_split_source => |op| try self.vtable.start_split_source(self.ptr, op),
            .bootstrap_split_destination => |op| try self.vtable.bootstrap_split_destination(self.ptr, op),
            .catch_up_split_destination => |op| try self.vtable.catch_up_split_destination(self.ptr, op),
            .finalize_split_source => |op| try self.vtable.finalize_split_source(self.ptr, op),
            .rollback_split => |op| try self.vtable.rollback_split(self.ptr, op),
            .accept_merge_receiver => |op| try self.vtable.accept_merge_receiver(self.ptr, op),
            .catch_up_merge_receiver => |op| try self.vtable.catch_up_merge_receiver(self.ptr, op),
            .finalize_merge => |op| try self.vtable.finalize_merge(self.ptr, op),
            .rollback_merge => |op| try self.vtable.rollback_merge(self.ptr, op),
        }
    }

    pub fn metadataRuntime(self: *const ShardOperationAdapter) metadata_driver.TransitionRuntime {
        return .{
            .ptr = @constCast(self),
            .vtable = &.{
                .observe_split = observeSplitMeta,
                .observe_merge = observeMergeMeta,
                .execute = executeMeta,
            },
        };
    }

    fn observeSplitMeta(ptr: *anyopaque, record: metadata_state.SplitTransitionRecord) !metadata_state.SplitObservation {
        const self: *const ShardOperationAdapter = @ptrCast(@alignCast(ptr));
        return try self.observeSplit(record);
    }

    fn observeMergeMeta(ptr: *anyopaque, record: metadata_state.MergeTransitionRecord) !metadata_state.MergeObservation {
        const self: *const ShardOperationAdapter = @ptrCast(@alignCast(ptr));
        return try self.observeMerge(record);
    }

    fn executeMeta(ptr: *anyopaque, action: metadata_actions.TransitionAction) !void {
        const self: *const ShardOperationAdapter = @ptrCast(@alignCast(ptr));
        try self.execute(action);
    }
};

test "shard operation adapter metadata runtime dispatches actions" {
    const Fake = struct {
        split_prepared: bool = false,

        fn adapter(self: *@This()) ShardOperationAdapter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_split = observeSplit,
                    .observe_merge = observeMerge,
                    .prepare_split_source = prepareSplitSource,
                    .start_split_source = noopStartSplitSource,
                    .bootstrap_split_destination = noopBootstrapSplitDestination,
                    .catch_up_split_destination = noopCatchUpSplitDestination,
                    .finalize_split_source = noopFinalizeSplitSource,
                    .rollback_split = noopRollbackSplit,
                    .accept_merge_receiver = noopAcceptMergeReceiver,
                    .catch_up_merge_receiver = noopCatchUpMergeReceiver,
                    .finalize_merge = noopFinalizeMerge,
                    .rollback_merge = noopRollbackMerge,
                },
            };
        }

        fn observeSplit(_: *anyopaque, _: metadata_state.SplitTransitionRecord) !metadata_state.SplitObservation {
            return .{
                .status = .{
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
            };
        }

        fn observeMerge(_: *anyopaque, record: metadata_state.MergeTransitionRecord) !metadata_state.MergeObservation {
            const status = @import("../data/mod.zig").MergeTransitionStatus{
                .phase = .prepare,
                .donor_group_id = record.donor_group_id,
                .receiver_group_id = record.receiver_group_id,
                .receiver_accepts_donor_range = false,
                .bootstrapped = false,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = false,
                .receiver_ready_for_reads = false,
                .donor_delta_sequence = 0,
                .receiver_delta_sequence = 0,
            };
            return .{ .donor = status, .receiver = status };
        }

        fn prepareSplitSource(ptr: *anyopaque, _: PrepareSplitSource) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.split_prepared = true;
        }

        fn noopStartSplitSource(_: *anyopaque, _: StartSplitSource) !void {}
        fn noopBootstrapSplitDestination(_: *anyopaque, _: BootstrapSplitDestination) !void {}
        fn noopCatchUpSplitDestination(_: *anyopaque, _: CatchUpSplitDestination) !void {}
        fn noopFinalizeSplitSource(_: *anyopaque, _: FinalizeSplitSource) !void {}
        fn noopRollbackSplit(_: *anyopaque, _: RollbackSplit) !void {}
        fn noopAcceptMergeReceiver(_: *anyopaque, _: AcceptMergeReceiver) !void {}
        fn noopCatchUpMergeReceiver(_: *anyopaque, _: CatchUpMergeReceiver) !void {}
        fn noopFinalizeMerge(_: *anyopaque, _: FinalizeMerge) !void {}
        fn noopRollbackMerge(_: *anyopaque, _: RollbackMerge) !void {}
    };

    var fake = Fake{};
    var adapter = fake.adapter();
    const runtime = adapter.metadataRuntime();

    _ = try runtime.observeSplit(.{
        .transition_id = 1,
        .source_group_id = 10,
        .destination_group_id = 11,
    });
    try runtime.execute(.{
        .prepare_split_source = .{
            .transition_id = 1,
            .source_group_id = 10,
            .destination_group_id = 11,
            .split_key = "doc:m",
        },
    });
    try std.testing.expect(fake.split_prepared);
}
