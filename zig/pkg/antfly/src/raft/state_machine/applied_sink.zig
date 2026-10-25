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
