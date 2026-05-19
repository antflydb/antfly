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

pub const ReadyPressure = struct {
    group_id: core.types.GroupId,
    message_count: usize = 0,
    message_bytes: usize = 0,
    committed_entries: usize = 0,
    committed_entry_bytes: usize = 0,
    unstable_entries: usize = 0,
    unstable_entry_bytes: usize = 0,
    has_snapshot: bool = false,
    snapshot_bytes: usize = 0,
};

pub const Backpressure = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        allow_ready: *const fn (ptr: *anyopaque, pressure: ReadyPressure) bool,
    };

    pub fn allowReady(self: Backpressure, pressure: ReadyPressure) bool {
        return self.vtable.allow_ready(self.ptr, pressure);
    }
};

test "backpressure iface compiles" {
    _ = ReadyPressure;
    _ = Backpressure;
}
