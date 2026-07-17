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

const raft_engine = @import("raft_engine");
const read_state_observer_mod = @import("read_state_observer.zig");

pub const RoutedStateMachine = struct {
    metadata_group_id: ?raft_engine.core.types.GroupId = null,
    metadata_state_machine: raft_engine.runtime.storage_iface.StateMachine,
    data_state_machine: raft_engine.runtime.storage_iface.StateMachine,
    read_state_observer: ?read_state_observer_mod.ReadStateObserver = null,

    pub fn stateMachine(self: *RoutedStateMachine) raft_engine.runtime.storage_iface.StateMachine {
        return .{
            .ptr = self,
            .vtable = &.{
                .apply_ready = applyReady,
            },
        };
    }

    fn applyReady(
        ptr: *anyopaque,
        group_id: raft_engine.core.types.GroupId,
        committed_entries: []const raft_engine.core.Entry,
        read_states: []const raft_engine.core.ReadState,
    ) !void {
        const self: *RoutedStateMachine = @ptrCast(@alignCast(ptr));
        const target = if (self.metadata_group_id != null and group_id == self.metadata_group_id.?)
            self.metadata_state_machine
        else
            self.data_state_machine;
        try target.applyReady(group_id, committed_entries, read_states);
        if (self.read_state_observer) |observer| {
            if (read_states.len > 0) {
                try observer.onReadStates(group_id, read_states);
            }
        }
    }
};
