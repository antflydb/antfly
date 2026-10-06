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

//! Physical backup-pin control shared by the C ABI and routed write adapters.
//! Keep distributed table orchestration outside the storage compilation owner.
const std = @import("std");

pub fn execute(alloc: std.mem.Allocator, db: *@import("mod.zig").DB, group_id: u64, request: @import("native_backup_seal_contract.zig").Request, control: @import("../../api/backup_contract.zig").BackupOperationControl) ![]u8 {
    try control.ensureActive();
    const fence = switch (request) {
        .seal => |value| value.fence,
        .release => |value| value.fence,
        .cancel => |value| value,
    };
    if (fence.owner_group_id != group_id or fence.role != .backup_snapshot) return error.InvalidBackupFence;
    switch (request) {
        .seal => |value| {
            const handle = try db.sealBackupCohort(value.id, value.fence, control.token());
            return try std.json.Stringify.valueAlloc(alloc, handle, .{});
        },
        .release => |handle| {
            try db.releaseBackupCohort(handle);
            return try alloc.dupe(u8, "{}");
        },
        .cancel => |value| {
            try db.cancelBackupCohort(value);
            return try alloc.dupe(u8, "{}");
        },
    }
}
