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
const doc_identity = @import("../../storage/db/doc_identity.zig");
const portable_backup = @import("../../storage/portable_backup.zig");

const dropped_table_trash_dir_name = ".antfly-drop-trash";

pub const DroppedTableDeleteHook = struct {
    ptr: *anyopaque,
    run: *const fn (ptr: *anyopaque) void,

    pub fn runHook(self: DroppedTableDeleteHook) void {
        self.run(self.ptr);
    }
};

pub const DroppedTableDeleteWork = struct {
    path: []u8,
    before_delete: ?DroppedTableDeleteHook = null,

    pub fn deletePath(path: []const u8, log_failure: bool, before_delete: ?DroppedTableDeleteHook) !void {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        if (before_delete) |hook| hook.runHook();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch |err| {
            if (!log_failure) return err;
            std.log.warn("background dropped-table delete failed path={s} err={s}", .{
                path,
                @errorName(err),
            });
        };
    }

    pub fn run(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try deletePath(self.path, true, self.before_delete);
    }

    pub fn deinit(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        std.heap.page_allocator.free(self.path);
        std.heap.page_allocator.destroy(self);
    }
};

pub fn scheduleDroppedGroupDelete(
    runtime: *db_mod.background_runtime.BackendRuntime,
    owner_id: u64,
    path: []const u8,
    before_delete: ?DroppedTableDeleteHook,
) !void {
    const work = try std.heap.page_allocator.create(DroppedTableDeleteWork);
    errdefer std.heap.page_allocator.destroy(work);
    const owned_path = try std.heap.page_allocator.dupe(u8, path);
    errdefer std.heap.page_allocator.free(owned_path);
    work.* = .{
        .path = owned_path,
        .before_delete = before_delete,
    };
    try runtime.durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .cleanup,
        .ptr = work,
        .run = DroppedTableDeleteWork.run,
        .deinit = DroppedTableDeleteWork.deinit,
    });
}

pub fn deleteDroppedGroupPath(
    alloc: std.mem.Allocator,
    path: []u8,
    runtime: ?*db_mod.background_runtime.BackendRuntime,
    owner_id: ?u64,
    before_delete: ?DroppedTableDeleteHook,
) !void {
    defer alloc.free(path);
    if (runtime) |job_runtime| {
        if (owner_id) |id| {
            scheduleDroppedGroupDelete(job_runtime, id, path, before_delete) catch {
                try DroppedTableDeleteWork.deletePath(path, false, before_delete);
                return;
            };
            return;
        }
    }
    try DroppedTableDeleteWork.deletePath(path, false, before_delete);
}

fn sleepNs(duration_ns: u64) void {
    var req = std.posix.timespec{
        .sec = @intCast(duration_ns / std.time.ns_per_s),
        .nsec = @intCast(duration_ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

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

pub fn backupOpenDbShard(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    backup_root: []const u8,
    backup_id: []const u8,
    group_id: u64,
    snapshot_token: []const u8,
    format: backups_api.BackupFormat,
) ![]backups_api.ShardSnapshot {
    if (format == .portable) {
        return try exportPortableBackupShard(alloc, db, backup_root, backup_id, group_id);
    }

    _ = try db.snapshot(snapshot_token);

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/{s}", .{ db.core.path, snapshot_token });
    defer alloc.free(snapshot_root);
    const dest_root = try backups_api.shardSnapshotPath(alloc, backup_root, backup_id, group_id);
    defer alloc.free(dest_root);
    try backups_api.copyDirectoryRecursive(alloc, snapshot_root, dest_root);

    const rel_path = try backups_api.shardSnapshotRelPath(alloc, backup_id, group_id);
    errdefer alloc.free(rel_path);
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

pub fn backupLocalTable(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    plan: backups_api.TableBackupPlan,
) ![]backups_api.ShardSnapshot {
    const snapshot_token = try std.fmt.allocPrint(alloc, "{s}-local", .{plan.backup_id});
    defer alloc.free(snapshot_token);
    return try backupOpenDbShard(alloc, db, plan.backup_root, plan.backup_id, 0, snapshot_token, plan.format);
}

pub fn restoreLocalTable(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    plan: backups_api.TableRestorePlan,
) !void {
    if (plan.manifest.shards.len != 1) return error.UnsupportedBackupFormat;

    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ plan.backup_root, plan.manifest.shards[0].snapshot_path });
    defer alloc.free(snapshot_root);
    if (std.mem.endsWith(u8, plan.manifest.shards[0].snapshot_path, ".afb")) {
        const body = try readBackupFileAlloc(alloc, snapshot_root);
        defer alloc.free(body);
        try portable_backup.importPortable(alloc, db.core.store, body);
        return;
    }

    const db_path = try alloc.dupe(u8, db.core.path);
    defer alloc.free(db_path);
    const primary_backend = db.primary_backend;
    var owned_backend_runtime = db.owned_backend_runtime;
    db.owned_backend_runtime = null;
    errdefer if (owned_backend_runtime) |*runtime| runtime.deinit();
    const backend_runtime = if (owned_backend_runtime) |*runtime|
        runtime.runtime
    else
        db.backend_runtime;
    const identity_namespace = db.core.identity_namespace;

    db.close();
    try db_mod.DB.restoreSnapshotTo(alloc, snapshot_root, db_path, .{
        .primary_backend = primary_backend,
        .identity_namespace = identity_namespace,
    });
    db.* = try db_mod.DB.open(alloc, db_path, .{
        .primary_backend = primary_backend,
        .backend_runtime = backend_runtime,
    });
    db.owned_backend_runtime = owned_backend_runtime;
    owned_backend_runtime = null;
}

pub fn restoreProvisionedTableGroupDeferredRuntimeRepair(
    alloc: std.mem.Allocator,
    path: []const u8,
    table_name: []const u8,
    group_id: u64,
    identity_namespace: ?doc_identity.Namespace,
    plan: backups_api.TableRestorePlan,
    before_restore_work: ?*const fn () void,
) !void {
    const snapshot_path = plan.manifest.shards[0].snapshot_path;
    const snapshot_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ plan.backup_root, snapshot_path });
    defer alloc.free(snapshot_root);

    var restore_io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer restore_io_impl.deinit();

    const ready_deadline_ns = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    while (true) {
        if (std.Io.Dir.cwd().statFile(restore_io_impl.io(), path, .{})) |_| break else |_| {}
        if (platform_time.monotonicNs() >= ready_deadline_ns) break;
        sleepNs(50 * std.time.ns_per_ms);
    }

    if (before_restore_work) |run| run();
    try prepareLocalTablePathForRestore(alloc, path);
    db_mod.DB.restoreSnapshotToDeferredRuntimeRepair(alloc, snapshot_root, path, .{
        .identity_namespace = identity_namespace,
    }, .{
        .backup_id = plan.manifest.backup_id,
        .location = plan.backup_root,
        .snapshot_path = snapshot_path,
        .group_id = group_id,
    }) catch |err| {
        if (err == error.IdentityNamespaceMismatch) {
            std.log.warn("provisioned restoreTable failed table={s} group_id={d} path={s} snapshot_root={s} err={}", .{
                table_name,
                group_id,
                path,
                snapshot_root,
                err,
            });
        } else {
            std.log.err("provisioned restoreTable failed table={s} group_id={d} path={s} snapshot_root={s} err={}", .{
                table_name,
                group_id,
                path,
                snapshot_root,
                err,
            });
        }
        return err;
    };
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
