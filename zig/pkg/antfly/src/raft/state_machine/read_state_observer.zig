// Copyright 2026 Antfly, Inc.
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
