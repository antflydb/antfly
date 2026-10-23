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

const std = @import("std");
const bridge = @import("restore_staging_bridge.zig");
const restore_staging = @import("antfly_local_sources").storage_lite_restore_staging;

const State = struct {
    allocator: std.mem.Allocator,
    staged: restore_staging.StagedRestore,
};

pub fn create(context: *const bridge.CreateContext) callconv(.c) c_int {
    context.result.* = .{};
    const allocator = std.heap.c_allocator;
    const state = allocator.create(State) catch return bridge.Status.out_of_memory;
    var success = false;
    defer if (!success) allocator.destroy(state);

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
    success = true;
    return bridge.Status.ok;
}

pub fn destroy(handle: *anyopaque) callconv(.c) void {
    const state: *State = @ptrCast(@alignCast(handle));
    const allocator = state.allocator;
    state.staged.deinit(allocator);
    allocator.destroy(state);
}

pub fn inspect(context: *const bridge.InspectContext) callconv(.c) c_int {
    context.result_ptr.* = null;
    context.result_len.* = 0;
    const destination = if (context.destination_ptr) |ptr| ptr[0..context.destination_len] else null;
    const json = @import("backup_inspection.zig").inspectInputJsonAlloc(std.heap.c_allocator, context.input_ptr[0..context.input_len], destination, .{ .max_input_bytes = context.max_input_bytes, .max_expanded_bytes = context.max_expanded_bytes }) catch |err| return if (err == error.OutOfMemory) bridge.Status.out_of_memory else bridge.Status.invalid_arguments;
    context.result_ptr.* = json.ptr;
    context.result_len.* = json.len;
    return bridge.Status.ok;
}
pub fn freeInspection(ptr: [*]u8, len: usize) callconv(.c) void {
    std.heap.c_allocator.free(ptr[0..len]);
}
