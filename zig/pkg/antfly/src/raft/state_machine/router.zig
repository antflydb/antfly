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
const raft_engine = @import("raft_engine");
const read_state_observer_mod = @import("antfly_read_state_observer");

pub const RoutedStateMachine = struct {
    metadata_group_id: ?raft_engine.core.types.GroupId = null,
    metadata_state_machine: raft_engine.runtime.storage_iface.StateMachine,
    data_state_machine: raft_engine.runtime.storage_iface.StateMachine,
    read_state_observer: ?read_state_observer_mod.ReadStateObserver = null,

    pub fn stateMachine(self: *RoutedStateMachine) raft_engine.runtime.storage_iface.StateMachine {
        return .{
            .ptr = self,
            .vtable = &.{
                .prepare_snapshot = prepareSnapshot,
                .build_snapshot = buildSnapshot,
                .apply_ready = applyReady,
                .is_apply_retryable = isApplyRetryable,
                .retire_group = retireGroup,
            },
        };
    }

    fn prepareSnapshot(
        ptr: *anyopaque,
        group_id: raft_engine.core.types.GroupId,
        applied_index: raft_engine.core.types.Index,
    ) !?raft_engine.runtime.storage_iface.SnapshotSource {
        const self: *RoutedStateMachine = @ptrCast(@alignCast(ptr));
        const target = if (self.metadata_group_id != null and group_id == self.metadata_group_id.?)
            self.metadata_state_machine
        else
            self.data_state_machine;
        return try target.prepareSnapshot(group_id, applied_index);
    }

    fn buildSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: raft_engine.core.types.GroupId) !?[]u8 {
        const self: *RoutedStateMachine = @ptrCast(@alignCast(ptr));
        const target = if (self.metadata_group_id != null and group_id == self.metadata_group_id.?)
            self.metadata_state_machine
        else
            self.data_state_machine;
        return try target.buildSnapshot(alloc, group_id);
    }

    fn applyReady(
        ptr: *anyopaque,
        group_id: raft_engine.core.types.GroupId,
        snapshot: ?raft_engine.core.types.Snapshot,
        committed_entries: []const raft_engine.core.Entry,
        read_states: []const raft_engine.core.ReadState,
    ) !void {
        const self: *RoutedStateMachine = @ptrCast(@alignCast(ptr));
        const target = if (self.metadata_group_id != null and group_id == self.metadata_group_id.?)
            self.metadata_state_machine
        else
            self.data_state_machine;
        try target.applyReady(group_id, snapshot, committed_entries, read_states);
        if (self.read_state_observer) |observer| {
            if (read_states.len > 0) {
                try observer.onReadStates(group_id, read_states);
            }
        }
    }

    fn isApplyRetryable(ptr: *anyopaque, group_id: u64, err: anyerror) bool {
        const self: *RoutedStateMachine = @ptrCast(@alignCast(ptr));
        const target = if (self.metadata_group_id != null and group_id == self.metadata_group_id.?)
            self.metadata_state_machine
        else
            self.data_state_machine;
        return target.isApplyRetryable(group_id, err);
    }

    fn retireGroup(ptr: *anyopaque, group_id: raft_engine.core.types.GroupId) void {
        const self: *RoutedStateMachine = @ptrCast(@alignCast(ptr));
        const target = if (self.metadata_group_id != null and group_id == self.metadata_group_id.?)
            self.metadata_state_machine
        else
            self.data_state_machine;
        target.retireGroup(group_id);
    }
};
