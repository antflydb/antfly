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
const transition_actions = @import("transition_actions.zig");
const transition_controller = @import("transition_controller.zig");
const transition_state = @import("transition_state.zig");

pub const TransitionAction = transition_actions.TransitionAction;
pub const TransitionDecision = transition_actions.TransitionDecision;

pub const TransitionRuntime = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        observe_split: *const fn (ptr: *anyopaque, record: transition_state.SplitTransitionRecord) anyerror!transition_state.SplitObservation,
        observe_merge: *const fn (ptr: *anyopaque, record: transition_state.MergeTransitionRecord) anyerror!transition_state.MergeObservation,
        execute: *const fn (ptr: *anyopaque, action: TransitionAction) anyerror!void,
    };

    pub fn observeSplit(self: TransitionRuntime, record: transition_state.SplitTransitionRecord) !transition_state.SplitObservation {
        return try self.vtable.observe_split(self.ptr, record);
    }

    pub fn observeMerge(self: TransitionRuntime, record: transition_state.MergeTransitionRecord) !transition_state.MergeObservation {
        return try self.vtable.observe_merge(self.ptr, record);
    }

    pub fn execute(self: TransitionRuntime, action: TransitionAction) !void {
        try self.vtable.execute(self.ptr, action);
    }
};

pub const StepResult = union(transition_state.TransitionKind) {
    split: struct {
        next_phase: transition_state.TransitionPhase,
        action: TransitionAction,
    },
    merge: struct {
        next_phase: transition_state.TransitionPhase,
        action: TransitionAction,
    },
};

pub const TransitionDriver = struct {
    pub fn stepSplit(
        runtime: TransitionRuntime,
        record: *transition_state.SplitTransitionRecord,
    ) !StepResult {
        const observation = try runtime.observeSplit(record.*);
        const decision = transition_controller.TransitionController.planSplit(record.*, observation);
        try runtime.execute(decision.action);
        record.phase = decision.next_phase;
        return .{
            .split = .{
                .next_phase = decision.next_phase,
                .action = decision.action,
            },
        };
    }

    pub fn stepMerge(
        runtime: TransitionRuntime,
        record: *transition_state.MergeTransitionRecord,
    ) !StepResult {
        const observation = try runtime.observeMerge(record.*);
        const decision = transition_controller.TransitionController.planMerge(record.*, observation);
        try runtime.execute(decision.action);
        record.phase = decision.next_phase;
        return .{
            .merge = .{
                .next_phase = decision.next_phase,
                .action = decision.action,
            },
        };
    }
};

test "metadata transition driver steps split and merge through runtime interface" {
    const FakeRuntime = struct {
        split_phase: transition_state.TransitionPhase = .prepare,
        merge_phase: transition_state.TransitionPhase = .prepare,
        calls: std.ArrayListUnmanaged([]const u8) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.calls.deinit(alloc);
            self.* = undefined;
        }

        fn iface(self: *@This()) TransitionRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_split = observeSplit,
                    .observe_merge = observeMerge,
                    .execute = execute,
                },
            };
        }

        fn observeSplit(ptr: *anyopaque, _: transition_state.SplitTransitionRecord) !transition_state.SplitObservation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{
                    .phase = switch (self.split_phase) {
                        .prepare => .prepare,
                        .bootstrap_peer => .bootstrap_peer,
                        .replay_deltas => .replay_deltas,
                        .cutover_pending, .finalizing => .cutover_ready,
                        .finalized => .finalized,
                        .rolling_back => .rolling_back,
                        .rolled_back => .rolled_back,
                    },
                    .source_split_phase = switch (self.split_phase) {
                        .prepare => .prepare,
                        .bootstrap_peer, .replay_deltas => .splitting,
                        .cutover_pending, .finalizing => .finalizing,
                        .finalized, .rolling_back, .rolled_back => .none,
                    },
                    .bootstrapped = self.split_phase != .prepare,
                    .replay_required = self.split_phase == .bootstrap_peer or self.split_phase == .replay_deltas or self.split_phase == .cutover_pending or self.split_phase == .finalizing,
                    .replay_caught_up = self.split_phase == .cutover_pending or self.split_phase == .finalizing or self.split_phase == .finalized,
                    .cutover_ready = self.split_phase == .cutover_pending or self.split_phase == .finalizing or self.split_phase == .finalized,
                    .destination_ready_for_reads = self.split_phase == .cutover_pending or self.split_phase == .finalizing or self.split_phase == .finalized,
                    .source_delta_sequence = 1,
                    .dest_delta_sequence = if (self.split_phase == .prepare) 0 else 1,
                },
            };
        }

        fn observeMerge(ptr: *anyopaque, record: transition_state.MergeTransitionRecord) !transition_state.MergeObservation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const status: @import("../data/mod.zig").MergeTransitionStatus = .{
                .phase = switch (self.merge_phase) {
                    .prepare => .prepare,
                    .bootstrap_peer => .bootstrap_peer,
                    .replay_deltas => .replay_deltas,
                    .cutover_pending, .finalizing => .cutover_ready,
                    .finalized => .finalized,
                    .rolling_back => .rolling_back,
                    .rolled_back => .rolled_back,
                },
                .donor_group_id = record.donor_group_id,
                .receiver_group_id = record.receiver_group_id,
                .receiver_accepts_donor_range = self.merge_phase != .prepare and self.merge_phase != .rolled_back,
                .bootstrapped = self.merge_phase != .prepare,
                .replay_required = self.merge_phase == .bootstrap_peer or self.merge_phase == .replay_deltas or self.merge_phase == .cutover_pending or self.merge_phase == .finalizing,
                .replay_caught_up = self.merge_phase == .cutover_pending or self.merge_phase == .finalizing or self.merge_phase == .finalized,
                .cutover_ready = self.merge_phase == .cutover_pending or self.merge_phase == .finalizing or self.merge_phase == .finalized,
                .receiver_ready_for_reads = self.merge_phase == .cutover_pending or self.merge_phase == .finalizing or self.merge_phase == .finalized,
                .donor_delta_sequence = 1,
                .receiver_delta_sequence = if (self.merge_phase == .prepare) 0 else 1,
            };
            return .{ .donor = status, .receiver = status };
        }

        fn execute(ptr: *anyopaque, action: TransitionAction) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            switch (action) {
                .none => try self.calls.append(std.testing.allocator, "none"),
                .prepare_split_source => {
                    try self.calls.append(std.testing.allocator, "prepare_split_source");
                    self.split_phase = .prepare;
                },
                .start_split_source => {
                    try self.calls.append(std.testing.allocator, "start_split_source");
                    self.split_phase = .bootstrap_peer;
                },
                .bootstrap_split_destination => {
                    try self.calls.append(std.testing.allocator, "bootstrap_split_destination");
                    self.split_phase = .replay_deltas;
                },
                .catch_up_split_destination => {
                    try self.calls.append(std.testing.allocator, "catch_up_split_destination");
                    self.split_phase = .cutover_pending;
                },
                .finalize_split_source => {
                    try self.calls.append(std.testing.allocator, "finalize_split_source");
                    self.split_phase = .finalized;
                },
                .rollback_split => {
                    try self.calls.append(std.testing.allocator, "rollback_split");
                    self.split_phase = .rolled_back;
                },
                .accept_merge_receiver => {
                    try self.calls.append(std.testing.allocator, "accept_merge_receiver");
                    self.merge_phase = .bootstrap_peer;
                },
                .catch_up_merge_receiver => {
                    try self.calls.append(std.testing.allocator, "catch_up_merge_receiver");
                    self.merge_phase = .cutover_pending;
                },
                .finalize_merge => {
                    try self.calls.append(std.testing.allocator, "finalize_merge");
                    self.merge_phase = .finalized;
                },
                .rollback_merge => {
                    try self.calls.append(std.testing.allocator, "rollback_merge");
                    self.merge_phase = .rolled_back;
                },
            }
        }
    };

    var runtime = FakeRuntime{};
    defer runtime.deinit(std.testing.allocator);

    var split = transition_state.SplitTransitionRecord{
        .transition_id = 1,
        .source_group_id = 10,
        .destination_group_id = 11,
        .split_key = "doc:m",
    };
    _ = try TransitionDriver.stepSplit(runtime.iface(), &split);
    _ = try TransitionDriver.stepSplit(runtime.iface(), &split);
    _ = try TransitionDriver.stepSplit(runtime.iface(), &split);
    _ = try TransitionDriver.stepSplit(runtime.iface(), &split);
    _ = try TransitionDriver.stepSplit(runtime.iface(), &split);
    try std.testing.expectEqual(transition_state.TransitionPhase.finalized, split.phase);

    var merge = transition_state.MergeTransitionRecord{
        .transition_id = 2,
        .donor_group_id = 20,
        .receiver_group_id = 21,
    };
    _ = try TransitionDriver.stepMerge(runtime.iface(), &merge);
    _ = try TransitionDriver.stepMerge(runtime.iface(), &merge);
    _ = try TransitionDriver.stepMerge(runtime.iface(), &merge);
    _ = try TransitionDriver.stepMerge(runtime.iface(), &merge);
    try std.testing.expectEqual(transition_state.TransitionPhase.finalized, merge.phase);
}
