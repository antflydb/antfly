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

const core = @import("../core/mod.zig");

// SnapshotThrottle lets the host cap concurrent snapshot work across groups.
pub const SnapshotThrottle = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        begin_snapshot: *const fn (ptr: *anyopaque, group_id: core.types.GroupId) bool,
        end_snapshot: *const fn (ptr: *anyopaque, group_id: core.types.GroupId) void,
    };

    pub fn beginSnapshot(self: SnapshotThrottle, group_id: core.types.GroupId) bool {
        return self.vtable.begin_snapshot(self.ptr, group_id);
    }

    pub fn endSnapshot(self: SnapshotThrottle, group_id: core.types.GroupId) void {
        self.vtable.end_snapshot(self.ptr, group_id);
    }
};
