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

const std = @import("std");
const bridge = @import("restore_staging_bridge.zig");
const restore_staging = @import("../storage/lite/restore_staging.zig");

const State = struct {
    allocator: std.mem.Allocator,
    staged: restore_staging.StagedRestore,
};

pub fn create(context: *const bridge.CreateContext) callconv(.c) c_int {
    const allocator_ptr: *const std.mem.Allocator = @ptrCast(@alignCast(context.allocator));
    const allocator = allocator_ptr.*;
    const state = allocator.create(State) catch return bridge.Status.out_of_memory;
    errdefer allocator.destroy(state);

    const input_path = context.input_path_ptr[0..context.input_path_len];
    const table_name = context.table_name_ptr[0..context.table_name_len];
    const backup_id = context.backup_id_ptr[0..context.backup_id_len];
    const location = context.location_ptr[0..context.location_len];
    state.* = .{
        .allocator = allocator,
        .staged = restore_staging.stageInputRestoreBackup(allocator, input_path, table_name, backup_id, location) catch |err| switch (err) {
            error.OutOfMemory => return bridge.Status.out_of_memory,
            error.InvalidArguments => return bridge.Status.invalid_arguments,
            else => return bridge.Status.failed,
        },
    };

    context.result.* = .{
        .handle = state,
        .backup_id_ptr = state.staged.backup_id.ptr,
        .backup_id_len = state.staged.backup_id.len,
        .location_ptr = state.staged.location.ptr,
        .location_len = state.staged.location.len,
        .snapshot_path_ptr = state.staged.snapshot_path.ptr,
        .snapshot_path_len = state.staged.snapshot_path.len,
        .table_name_ptr = state.staged.table_name.ptr,
        .table_name_len = state.staged.table_name.len,
    };
    return bridge.Status.ok;
}

pub fn destroy(handle: *anyopaque) callconv(.c) void {
    const state: *State = @ptrCast(@alignCast(handle));
    const allocator = state.allocator;
    state.staged.deinit(allocator);
    allocator.destroy(state);
}
