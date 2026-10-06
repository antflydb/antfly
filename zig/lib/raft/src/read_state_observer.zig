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

pub const ReadStateObserver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        on_read_states: *const fn (
            ptr: *anyopaque,
            group_id: raft_engine.core.types.GroupId,
            read_states: []const raft_engine.core.ReadState,
        ) anyerror!void,
    };

    pub fn onReadStates(
        self: ReadStateObserver,
        group_id: raft_engine.core.types.GroupId,
        read_states: []const raft_engine.core.ReadState,
    ) !void {
        try self.vtable.on_read_states(self.ptr, group_id, read_states);
    }
};
