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
const backups_api = @import("../backups.zig");
const metadata_mod = @import("../../metadata/mod.zig");
const platform_time = @import("../../platform/time.zig");
const db_mod = @import("../../storage/db/mod.zig");
const portable_backup = @import("../../storage/portable_backup.zig");

const dropped_table_trash_dir_name = ".antfly-drop-trash";

pub fn prepareLocalTablePathForRestore(alloc: std.mem.Allocator, path: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    try fs_paths.createDirPathPortable(io, path);

    const indexes_path = try std.fmt.allocPrint(alloc, "{s}/indexes", .{path});
    defer alloc.free(indexes_path);
    std.Io.Dir.cwd().deleteTree(io, indexes_path) catch {};

    const snapshots_path = try std.fmt.allocPrint(alloc, "{s}.snapshots", .{path});
    defer alloc.free(snapshots_path);
    std.Io.Dir.cwd().deleteTree(io, snapshots_path) catch {};
}

pub fn portableBackupShardRelPath(alloc: std.mem.Allocator, backup_id: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}.afb", .{backup_id});
}

pub fn exportPortableBackupShard(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    backup_root: []const u8,
    backup_id: []const u8,
    group_id: u64,
) ![]backups_api.ShardSnapshot {
    const rel_path = try portableBackupShardRelPath(alloc, backup_id);
    errdefer alloc.free(rel_path);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try portable_backup.exportPortable(alloc, db.core.store, &out);

    const dest_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ backup_root, rel_path });
    defer alloc.free(dest_path);
    try writeBackupFile(dest_path, out.items);

    const byte_range = db.getRange();
    const shards = try alloc.alloc(backups_api.ShardSnapshot, 1);
    shards[0] = .{
        .group_id = group_id,
        .start_key = try alloc.dupe(u8, byte_range.start),
        .end_key = if (byte_range.end.len > 0) try alloc.dupe(u8, byte_range.end) else null,
        .snapshot_path = rel_path,
    };
    return shards;
}

pub fn writeBackupFile(path: []const u8, body: []const u8) !void {
    if (std.fs.path.dirname(path)) |parent| {
        var io_parent = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_parent.deinit();
        try fs_paths.createDirPathPortable(io_parent.io(), parent);
    }

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var file = try fs_paths.createFilePortable(io, path, .{ .truncate = true });
    defer file.close(io);
    var buf: [8192]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.writeAll(body);
    try writer.end();
}

pub fn readBackupFileAlloc(alloc: std.mem.Allocator, path: []const u8) ![]u8 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(
        io_impl.io(),
        path,
        alloc,
        .limited(backups_api.max_portable_backup_file_bytes),
    );
}

pub fn freeBackupShards(alloc: std.mem.Allocator, shards: []const backups_api.ShardSnapshot) void {
    for (shards) |shard| shard.deinit(alloc);
    alloc.free(@constCast(shards));
}

pub fn cloneShardSnapshots(
    alloc: std.mem.Allocator,
    shards: []const backups_api.ShardSnapshot,
) ![]backups_api.ShardSnapshot {
    const out = try alloc.alloc(backups_api.ShardSnapshot, shards.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |shard| shard.deinit(alloc);
        alloc.free(out);
    }
    for (shards, 0..) |shard, i| {
        out[i] = .{
            .group_id = shard.group_id,
            .start_key = try alloc.dupe(u8, shard.start_key),
            .end_key = if (shard.end_key) |value| try alloc.dupe(u8, value) else null,
            .snapshot_path = try alloc.dupe(u8, shard.snapshot_path),
        };
        initialized += 1;
    }
    return out;
}

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
