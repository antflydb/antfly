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

//! Opaque internal ABI for staging `.aflite` and `.afb` inputs. The heavy Lite
//! storage implementation lives in the standalone codegen unit; the remote CLI
//! sees only this handle and borrowed string results.

const builtin = @import("builtin");
const std = @import("std");

const direct_impl = if (builtin.is_test)
    @import("../storage/lite/restore_staging.zig")
else
    struct {};

pub const max_afb_file_bytes: usize = 16 * 1024 * 1024 * 1024;

pub const Status = struct {
    pub const ok: c_int = 0;
    pub const out_of_memory: c_int = 1;
    pub const invalid_arguments: c_int = 2;
    pub const failed: c_int = 3;
};

pub const Result = extern struct {
    handle: ?*anyopaque = null,
    backup_id_ptr: [*]const u8 = undefined,
    backup_id_len: usize = 0,
    location_ptr: [*]const u8 = undefined,
    location_len: usize = 0,
    snapshot_path_ptr: [*]const u8 = undefined,
    snapshot_path_len: usize = 0,
    table_name_ptr: [*]const u8 = undefined,
    table_name_len: usize = 0,
};

pub const CreateContext = extern struct {
    allocator: *const anyopaque,
    input_path_ptr: [*]const u8,
    input_path_len: usize,
    table_name_ptr: [*]const u8,
    table_name_len: usize,
    backup_id_ptr: [*]const u8,
    backup_id_len: usize,
    location_ptr: [*]const u8,
    location_len: usize,
    result: *Result,
};

extern fn antfly_restore_staging_create(context: *const CreateContext) callconv(.c) c_int;
extern fn antfly_restore_staging_destroy(handle: *anyopaque) callconv(.c) void;

pub const StagedRestore = struct {
    handle: *anyopaque,
    backup_id: []const u8,
    location: []const u8,
    snapshot_path: []const u8,
    table_name: []const u8,

    pub fn deinit(self: *StagedRestore, allocator: std.mem.Allocator) void {
        if (comptime builtin.is_test) {
            const state: *TestState = @ptrCast(@alignCast(self.handle));
            state.staged.deinit(allocator);
            allocator.destroy(state);
        } else {
            antfly_restore_staging_destroy(self.handle);
        }
        self.* = undefined;
    }
};

const TestState = if (builtin.is_test)
    struct { staged: direct_impl.StagedRestore }
else
    opaque {};

pub fn defaultBackupIdAlloc(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    const base = std.fs.path.basename(path);
    const stem = if (std.mem.endsWith(u8, base, ".aflite"))
        base[0 .. base.len - ".aflite".len]
    else if (std.mem.endsWith(u8, base, ".afb"))
        base[0 .. base.len - ".afb".len]
    else
        base;
    var out = try std.ArrayListUnmanaged(u8).initCapacity(allocator, "lite-".len + stem.len);
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "lite-");
    for (stem) |byte| {
        try out.append(allocator, if (std.ascii.isAlphanumeric(byte) or byte == '-' or byte == '_') byte else '-');
    }
    return try out.toOwnedSlice(allocator);
}

pub fn stageInputRestoreBackup(
    allocator: std.mem.Allocator,
    input_path: []const u8,
    table_name: []const u8,
    backup_id: []const u8,
    location: []const u8,
) !StagedRestore {
    if (comptime builtin.is_test) {
        const state = try allocator.create(TestState);
        errdefer allocator.destroy(state);
        state.* = .{ .staged = try direct_impl.stageInputRestoreBackup(allocator, input_path, table_name, backup_id, location) };
        return .{
            .handle = state,
            .backup_id = state.staged.backup_id,
            .location = state.staged.location,
            .snapshot_path = state.staged.snapshot_path,
            .table_name = state.staged.table_name,
        };
    }

    var allocator_copy = allocator;
    var result = Result{};
    const status = antfly_restore_staging_create(&.{
        .allocator = &allocator_copy,
        .input_path_ptr = input_path.ptr,
        .input_path_len = input_path.len,
        .table_name_ptr = table_name.ptr,
        .table_name_len = table_name.len,
        .backup_id_ptr = backup_id.ptr,
        .backup_id_len = backup_id.len,
        .location_ptr = location.ptr,
        .location_len = location.len,
        .result = &result,
    });
    switch (status) {
        Status.ok => {},
        Status.out_of_memory => return error.OutOfMemory,
        Status.invalid_arguments => return error.InvalidArguments,
        else => return error.RestoreStagingFailed,
    }
    return .{
        .handle = result.handle orelse return error.RestoreStagingFailed,
        .backup_id = result.backup_id_ptr[0..result.backup_id_len],
        .location = result.location_ptr[0..result.location_len],
        .snapshot_path = result.snapshot_path_ptr[0..result.snapshot_path_len],
        .table_name = result.table_name_ptr[0..result.table_name_len],
    };
}
