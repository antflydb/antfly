// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

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
