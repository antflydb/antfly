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

//! Volatile scheduling ownership for restartable group work. Durable effects
//! still require their own authority/receipt checks after a leadership change.
pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        is_local_leader: *const fn (ptr: *anyopaque, group_id: u64) bool,
    };

    pub fn isLocalLeader(self: Source, group_id: u64) bool {
        return self.vtable.is_local_leader(self.ptr, group_id);
    }
};
