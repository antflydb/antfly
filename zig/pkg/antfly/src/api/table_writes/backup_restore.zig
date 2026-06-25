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
const fs_paths = @import("../../common/fs_paths.zig");
const metadata_mod = @import("../../metadata/mod.zig");
const platform_time = @import("../../platform/time.zig");

const dropped_table_trash_dir_name = ".antfly-drop-trash";

pub fn droppedTableTrashDirPath(alloc: std.mem.Allocator, replica_root_dir: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ replica_root_dir, dropped_table_trash_dir_name });
}

pub fn droppedTableTrashPath(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    table_name: []const u8,
    group_id: u64,
) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}/table-{s}-group-{d}-{d}", .{
        replica_root_dir,
        dropped_table_trash_dir_name,
        table_name,
        group_id,
        platform_time.monotonicNs(),
    });
}

pub fn moveDroppedGroupPathToTrash(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    table_name: []const u8,
    group_id: u64,
) !?[]u8 {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);

    const trash_dir_path = try droppedTableTrashDirPath(alloc, replica_root_dir);
    defer alloc.free(trash_dir_path);
    const trash_path = try droppedTableTrashPath(alloc, replica_root_dir, table_name, group_id);
    errdefer alloc.free(trash_path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), trash_dir_path);
    std.Io.Dir.rename(std.Io.Dir.cwd(), path, std.Io.Dir.cwd(), trash_path, io_impl.io()) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    return trash_path;
}

pub fn deleteGroupPathIfPresent(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
) !void {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().access(io_impl.io(), path, .{}) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), path);
}
