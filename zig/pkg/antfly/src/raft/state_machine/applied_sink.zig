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

pub const AppliedIndexSink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        set_applied_index: *const fn (ptr: *anyopaque, group_id: raft_engine.core.types.GroupId, index: raft_engine.core.types.Index) anyerror!void,
    };

    pub fn setAppliedIndex(self: AppliedIndexSink, group_id: raft_engine.core.types.GroupId, index: raft_engine.core.types.Index) !void {
        return try self.vtable.set_applied_index(self.ptr, group_id, index);
    }
};

pub fn noopAppliedIndexSink() AppliedIndexSink {
    return .{
        .ptr = undefined,
        .vtable = &.{
            .set_applied_index = setAppliedIndexNoop,
        },
    };
}

fn setAppliedIndexNoop(_: *anyopaque, _: raft_engine.core.types.GroupId, _: raft_engine.core.types.Index) !void {}
