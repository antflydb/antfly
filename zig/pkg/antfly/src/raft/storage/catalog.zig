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
const builtin = @import("builtin");
const fs_paths = @import("../../common/fs_paths.zig");
const platform_sync = @import("antfly_platform").sync;
const raft_engine = @import("raft_engine");

var catalog_tmp_nonce: std.atomic.Value(u64) = .init(0);

const TestPersistFailureBoundary = enum {
    before_publish,
    after_publish,
};

pub const ReplicaBootstrapMode = enum {
    empty,
    persisted,
    fetch_snapshot,
};

pub const SnapshotBootstrapRecord = struct {
    from_node_id: u64,
    term: u64 = 0,
    snapshot_id: []const u8,
    uri: []const u8 = "",

    pub fn clone(self: SnapshotBootstrapRecord, alloc: std.mem.Allocator) !SnapshotBootstrapRecord {
        return .{
            .from_node_id = self.from_node_id,
            .term = self.term,
            .snapshot_id = try alloc.dupe(u8, self.snapshot_id),
            .uri = try alloc.dupe(u8, self.uri),
        };
    }

    pub fn deinit(self: *SnapshotBootstrapRecord, alloc: std.mem.Allocator) void {
        alloc.free(self.snapshot_id);
        alloc.free(self.uri);
        self.* = undefined;
    }

    pub fn toRuntime(self: SnapshotBootstrapRecord, alloc: std.mem.Allocator) !raft_engine.runtime.replica.SnapshotBootstrap {
        return .{
            .from = self.from_node_id,
            .term = self.term,
            .locator = .{
                .snapshot_id = try alloc.dupe(u8, self.snapshot_id),
                .uri = try alloc.dupe(u8, self.uri),
            },
            .fetch_immediately = true,
        };
    }
};

pub const BackupRestoreBootstrapRecord = struct {
    backup_id: []const u8,
    location: []const u8,
    snapshot_path: []const u8,

    pub fn clone(self: BackupRestoreBootstrapRecord, alloc: std.mem.Allocator) !BackupRestoreBootstrapRecord {
        return .{
            .backup_id = try alloc.dupe(u8, self.backup_id),
            .location = try alloc.dupe(u8, self.location),
            .snapshot_path = try alloc.dupe(u8, self.snapshot_path),
        };
    }

    pub fn deinit(self: *BackupRestoreBootstrapRecord, alloc: std.mem.Allocator) void {
        alloc.free(self.backup_id);
        alloc.free(self.location);
        alloc.free(self.snapshot_path);
        self.* = undefined;
    }
};

pub const ReplicaBootstrapSource = union(enum) {
    empty,
    persisted,
    raft_snapshot_fetch: SnapshotBootstrapRecord,
    backup_db_snapshot_restore: BackupRestoreBootstrapRecord,
};

pub const ReplicaRecord = struct {
    group_id: u64,
    replica_id: u64,
    local_node_id: u64,
    bootstrap_mode: ReplicaBootstrapMode = .persisted,
    metadata_version: u64 = 0,
    snapshot_bootstrap: ?SnapshotBootstrapRecord = null,
    backup_restore_bootstrap: ?BackupRestoreBootstrapRecord = null,

    pub fn clone(self: ReplicaRecord, alloc: std.mem.Allocator) !ReplicaRecord {
        var cloned = self;
        cloned.snapshot_bootstrap = if (self.snapshot_bootstrap) |record|
            try record.clone(alloc)
        else
            null;
        cloned.backup_restore_bootstrap = if (self.backup_restore_bootstrap) |record|
            try record.clone(alloc)
        else
            null;
        return cloned;
    }

    pub fn deinit(self: *ReplicaRecord, alloc: std.mem.Allocator) void {
        if (self.snapshot_bootstrap) |*record| record.deinit(alloc);
        if (self.backup_restore_bootstrap) |*record| record.deinit(alloc);
        self.* = undefined;
    }

    pub fn bootstrapSource(self: ReplicaRecord) ReplicaBootstrapSource {
        if (self.backup_restore_bootstrap) |record| return .{ .backup_db_snapshot_restore = record };
        if (self.snapshot_bootstrap) |record| return .{ .raft_snapshot_fetch = record };
        return switch (self.bootstrap_mode) {
            .empty => .empty,
            .persisted => .persisted,
            .fetch_snapshot => .persisted,
        };
    }
};

pub fn eqlReplicaRecord(left: ReplicaRecord, right: ReplicaRecord) bool {
    if (left.group_id != right.group_id) return false;
    if (left.replica_id != right.replica_id) return false;
    if (left.local_node_id != right.local_node_id) return false;
    if (left.bootstrap_mode != right.bootstrap_mode) return false;
    if (left.metadata_version != right.metadata_version) return false;
    if ((left.snapshot_bootstrap == null) != (right.snapshot_bootstrap == null)) return false;
    if ((left.backup_restore_bootstrap == null) != (right.backup_restore_bootstrap == null)) return false;
    if (left.snapshot_bootstrap) |snapshot| {
        const other = right.snapshot_bootstrap.?;
        if (snapshot.from_node_id != other.from_node_id) return false;
        if (snapshot.term != other.term) return false;
        if (!std.mem.eql(u8, snapshot.snapshot_id, other.snapshot_id)) return false;
        if (!std.mem.eql(u8, snapshot.uri, other.uri)) return false;
    }
    if (left.backup_restore_bootstrap) |backup| {
        const other = right.backup_restore_bootstrap.?;
        if (!std.mem.eql(u8, backup.backup_id, other.backup_id)) return false;
        if (!std.mem.eql(u8, backup.location, other.location)) return false;
        if (!std.mem.eql(u8, backup.snapshot_path, other.snapshot_path)) return false;
    }
    return true;
}

pub fn freeReplicaRecords(alloc: std.mem.Allocator, records: []ReplicaRecord) void {
    for (records) |*record| record.deinit(alloc);
    alloc.free(records);
}

pub fn freeRuntimeBootstrap(alloc: std.mem.Allocator, bootstrap: *raft_engine.runtime.ReplicaBootstrap) void {
    switch (bootstrap.*) {
        .fetch_snapshot => |*snapshot| {
            alloc.free(snapshot.locator.snapshot_id);
            alloc.free(snapshot.locator.uri);
        },
        else => {},
    }
    bootstrap.* = undefined;
}

pub fn runtimeBootstrapFromRecord(
    alloc: std.mem.Allocator,
    record: ReplicaRecord,
) !raft_engine.runtime.ReplicaBootstrap {
    return switch (record.bootstrapSource()) {
        .empty => .empty,
        .persisted => .persisted,
        .raft_snapshot_fetch => |snapshot| .{ .fetch_snapshot = try snapshot.toRuntime(alloc) },
        .backup_db_snapshot_restore => .persisted,
    };
}

pub const ReplicaCatalog = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        upsert_replica: *const fn (ptr: *anyopaque, record: ReplicaRecord) anyerror!void,
        remove_replica: *const fn (ptr: *anyopaque, group_id: u64) anyerror!bool,
        list_replicas: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator) anyerror![]ReplicaRecord,
    };

    pub fn upsertReplica(self: ReplicaCatalog, record: ReplicaRecord) !void {
        return try self.vtable.upsert_replica(self.ptr, record);
    }

    pub fn removeReplica(self: ReplicaCatalog, group_id: u64) !bool {
        return try self.vtable.remove_replica(self.ptr, group_id);
    }

    pub fn listReplicas(self: ReplicaCatalog, alloc: std.mem.Allocator) ![]ReplicaRecord {
        return try self.vtable.list_replicas(self.ptr, alloc);
    }
};

pub const MemoryReplicaCatalog = struct {
    alloc: std.mem.Allocator,
    records: std.AutoHashMapUnmanaged(u64, ReplicaRecord) = .empty,

    pub fn init(alloc: std.mem.Allocator) MemoryReplicaCatalog {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MemoryReplicaCatalog) void {
        var it = self.records.valueIterator();
        while (it.next()) |record| record.deinit(self.alloc);
        self.records.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn catalog(self: *MemoryReplicaCatalog) ReplicaCatalog {
        return .{
            .ptr = self,
            .vtable = &.{
                .upsert_replica = upsertReplica,
                .remove_replica = removeReplica,
                .list_replicas = listReplicas,
            },
        };
    }

    fn upsertReplica(ptr: *anyopaque, record: ReplicaRecord) !void {
        const self: *MemoryReplicaCatalog = @ptrCast(@alignCast(ptr));
        if (self.records.getPtr(record.group_id)) |existing| {
            if (eqlReplicaRecord(existing.*, record)) return;
        }
        const owned = try record.clone(self.alloc);
        errdefer {
            var cleanup = owned;
            cleanup.deinit(self.alloc);
        }
        if (self.records.getPtr(record.group_id)) |existing| {
            existing.deinit(self.alloc);
            existing.* = owned;
            return;
        }
        try self.records.put(self.alloc, record.group_id, owned);
    }

    fn removeReplica(ptr: *anyopaque, group_id: u64) !bool {
        const self: *MemoryReplicaCatalog = @ptrCast(@alignCast(ptr));
        return self.records.remove(group_id);
    }

    fn listReplicas(ptr: *anyopaque, alloc: std.mem.Allocator) ![]ReplicaRecord {
        const self: *MemoryReplicaCatalog = @ptrCast(@alignCast(ptr));
        var out = try alloc.alloc(ReplicaRecord, self.records.count());
        var i: usize = 0;
        errdefer {
            for (out[0..i]) |*record| record.deinit(alloc);
            alloc.free(out);
        }
        var it = self.records.valueIterator();
        while (it.next()) |record| : (i += 1) out[i] = try record.clone(alloc);
        return out;
    }
};

pub const FileReplicaCatalog = struct {
    alloc: std.mem.Allocator,
    io_impl: std.Io.Threaded,
    path: []const u8,
    records: std.AutoHashMapUnmanaged(u64, ReplicaRecord) = .empty,
    mutex: std.atomic.Mutex = .unlocked,
    test_persist_failure_boundary: if (builtin.is_test) ?TestPersistFailureBoundary else void = if (builtin.is_test) null else {},

    pub fn init(alloc: std.mem.Allocator, path: []const u8) !FileReplicaCatalog {
        var self = FileReplicaCatalog{
            .alloc = alloc,
            .io_impl = std.Io.Threaded.init(alloc, .{}),
            .path = try alloc.dupe(u8, path),
        };
        errdefer {
            alloc.free(self.path);
            self.io_impl.deinit();
        }
        try self.load();
        return self;
    }

    pub fn deinit(self: *FileReplicaCatalog) void {
        var it = self.records.valueIterator();
        while (it.next()) |record| record.deinit(self.alloc);
        self.records.deinit(self.alloc);
        self.alloc.free(self.path);
        self.io_impl.deinit();
        self.* = undefined;
    }

    pub fn catalog(self: *FileReplicaCatalog) ReplicaCatalog {
        return .{
            .ptr = self,
            .vtable = &.{
                .upsert_replica = upsertReplica,
                .remove_replica = removeReplica,
                .list_replicas = listReplicas,
            },
        };
    }

    fn upsertReplica(ptr: *anyopaque, record: ReplicaRecord) !void {
        const self: *FileReplicaCatalog = @ptrCast(@alignCast(ptr));
        platform_sync.lockYieldingIo(&self.mutex, self.io());
        defer self.mutex.unlock();

        if (self.records.getPtr(record.group_id)) |existing| {
            if (eqlReplicaRecord(existing.*, record)) return;
        }
        var owned = try record.clone(self.alloc);
        if (self.records.getPtr(record.group_id)) |existing| {
            var previous = existing.*;
            existing.* = owned;
            var published = false;
            self.persist(&published) catch |err| {
                // Even if rename published the new bytes, a directory-sync
                // failure is not a durable acknowledgement. Restore the live
                // map so an identical retry persists and syncs again.
                owned = existing.*;
                existing.* = previous;
                owned.deinit(self.alloc);
                return err;
            };
            previous.deinit(self.alloc);
            return;
        }

        self.records.put(self.alloc, record.group_id, owned) catch |err| {
            owned.deinit(self.alloc);
            return err;
        };
        var published = false;
        self.persist(&published) catch |err| {
            var rejected = self.records.fetchRemove(record.group_id).?.value;
            rejected.deinit(self.alloc);
            return err;
        };
    }

    fn removeReplica(ptr: *anyopaque, group_id: u64) !bool {
        const self: *FileReplicaCatalog = @ptrCast(@alignCast(ptr));
        platform_sync.lockYieldingIo(&self.mutex, self.io());
        defer self.mutex.unlock();

        const removed = self.records.fetchRemove(group_id) orelse return false;
        var published = false;
        self.persist(&published) catch |err| {
            self.records.putAssumeCapacity(removed.key, removed.value);
            return err;
        };
        var committed_removed = removed.value;
        committed_removed.deinit(self.alloc);
        return true;
    }

    fn listReplicas(ptr: *anyopaque, alloc: std.mem.Allocator) ![]ReplicaRecord {
        const self: *FileReplicaCatalog = @ptrCast(@alignCast(ptr));
        platform_sync.lockYieldingIo(&self.mutex, self.io());
        defer self.mutex.unlock();

        var out = try alloc.alloc(ReplicaRecord, self.records.count());
        var i: usize = 0;
        errdefer freeReplicaRecords(alloc, out[0..i]);
        var it = self.records.valueIterator();
        while (it.next()) |record| : (i += 1) out[i] = try record.clone(alloc);
        return out;
    }

    fn load(self: *FileReplicaCatalog) !void {
        const bytes = std.Io.Dir.cwd().readFileAlloc(self.io(), self.path, self.alloc, .limited(1 << 20)) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer self.alloc.free(bytes);
        if (bytes.len == 0) return;

        var lines = std.mem.tokenizeScalar(u8, bytes, '\n');
        while (lines.next()) |line| {
            if (line.len == 0) continue;
            var fields = std.mem.tokenizeScalar(u8, line, ' ');
            const group_id = std.fmt.parseInt(u64, fields.next() orelse return error.InvalidReplicaCatalog, 10) catch return error.InvalidReplicaCatalog;
            const replica_id = std.fmt.parseInt(u64, fields.next() orelse return error.InvalidReplicaCatalog, 10) catch return error.InvalidReplicaCatalog;
            const local_node_id = std.fmt.parseInt(u64, fields.next() orelse return error.InvalidReplicaCatalog, 10) catch return error.InvalidReplicaCatalog;
            const bootstrap_raw = fields.next() orelse return error.InvalidReplicaCatalog;
            const metadata_version = std.fmt.parseInt(u64, fields.next() orelse return error.InvalidReplicaCatalog, 10) catch return error.InvalidReplicaCatalog;
            const bootstrap_mode: ReplicaBootstrapMode = std.meta.stringToEnum(ReplicaBootstrapMode, bootstrap_raw) orelse return error.InvalidReplicaCatalog;
            var snapshot_bootstrap: ?SnapshotBootstrapRecord = null;
            var backup_restore_bootstrap: ?BackupRestoreBootstrapRecord = null;
            if (fields.next()) |source_tag| {
                if (std.mem.eql(u8, source_tag, "raft")) {
                    const from_raw = fields.next() orelse return error.InvalidReplicaCatalog;
                    const term_raw = fields.next() orelse return error.InvalidReplicaCatalog;
                    const snapshot_id = fields.next() orelse return error.InvalidReplicaCatalog;
                    const uri = fields.next() orelse "";
                    snapshot_bootstrap = .{
                        .from_node_id = std.fmt.parseInt(u64, from_raw, 10) catch return error.InvalidReplicaCatalog,
                        .term = std.fmt.parseInt(u64, term_raw, 10) catch return error.InvalidReplicaCatalog,
                        .snapshot_id = try self.alloc.dupe(u8, snapshot_id),
                        .uri = try self.alloc.dupe(u8, uri),
                    };
                } else if (std.mem.eql(u8, source_tag, "backup")) {
                    const backup_id = fields.next() orelse return error.InvalidReplicaCatalog;
                    const location = fields.next() orelse return error.InvalidReplicaCatalog;
                    const snapshot_path = fields.next() orelse return error.InvalidReplicaCatalog;
                    backup_restore_bootstrap = .{
                        .backup_id = try self.alloc.dupe(u8, backup_id),
                        .location = try self.alloc.dupe(u8, location),
                        .snapshot_path = try self.alloc.dupe(u8, snapshot_path),
                    };
                } else if (bootstrap_mode == .fetch_snapshot) {
                    const from_raw = source_tag;
                    const term_raw = fields.next() orelse return error.InvalidReplicaCatalog;
                    const snapshot_id = fields.next() orelse return error.InvalidReplicaCatalog;
                    const uri = fields.next() orelse "";
                    snapshot_bootstrap = .{
                        .from_node_id = std.fmt.parseInt(u64, from_raw, 10) catch return error.InvalidReplicaCatalog,
                        .term = std.fmt.parseInt(u64, term_raw, 10) catch return error.InvalidReplicaCatalog,
                        .snapshot_id = try self.alloc.dupe(u8, snapshot_id),
                        .uri = try self.alloc.dupe(u8, uri),
                    };
                } else {
                    return error.InvalidReplicaCatalog;
                }
            }
            var record: ReplicaRecord = .{
                .group_id = group_id,
                .replica_id = replica_id,
                .local_node_id = local_node_id,
                .bootstrap_mode = bootstrap_mode,
                .metadata_version = metadata_version,
                .snapshot_bootstrap = snapshot_bootstrap,
                .backup_restore_bootstrap = backup_restore_bootstrap,
            };
            errdefer record.deinit(self.alloc);
            try self.records.put(self.alloc, group_id, record);
        }
    }

    fn persist(self: *FileReplicaCatalog, published: *bool) !void {
        published.* = false;
        const parent_dir = std.fs.path.dirname(self.path);
        if (parent_dir) |dir| try fs_paths.createDirPathPortable(self.io(), dir);

        const records = try self.listOwned(self.alloc);
        defer freeReplicaRecords(self.alloc, records);
        std.mem.sort(ReplicaRecord, records, {}, struct {
            fn lessThan(_: void, left: ReplicaRecord, right: ReplicaRecord) bool {
                return left.group_id < right.group_id;
            }
        }.lessThan);
        var encoded = std.ArrayListUnmanaged(u8).empty;
        defer encoded.deinit(self.alloc);
        for (records) |record| {
            const line = if (record.snapshot_bootstrap) |snapshot|
                try std.fmt.allocPrint(self.alloc, "{d} {d} {d} {s} {d} raft {d} {d} {s} {s}\n", .{
                    record.group_id,
                    record.replica_id,
                    record.local_node_id,
                    @tagName(record.bootstrap_mode),
                    record.metadata_version,
                    snapshot.from_node_id,
                    snapshot.term,
                    snapshot.snapshot_id,
                    snapshot.uri,
                })
            else if (record.backup_restore_bootstrap) |backup|
                try std.fmt.allocPrint(self.alloc, "{d} {d} {d} {s} {d} backup {s} {s} {s}\n", .{
                    record.group_id,
                    record.replica_id,
                    record.local_node_id,
                    @tagName(record.bootstrap_mode),
                    record.metadata_version,
                    backup.backup_id,
                    backup.location,
                    backup.snapshot_path,
                })
            else
                try std.fmt.allocPrint(self.alloc, "{d} {d} {d} {s} {d}\n", .{
                    record.group_id,
                    record.replica_id,
                    record.local_node_id,
                    @tagName(record.bootstrap_mode),
                    record.metadata_version,
                });
            defer self.alloc.free(line);
            try encoded.appendSlice(self.alloc, line);
        }

        const tmp_path = try std.fmt.allocPrint(self.alloc, "{s}.tmp-{x}-{d}", .{
            self.path,
            @intFromPtr(self),
            catalog_tmp_nonce.fetchAdd(1, .monotonic),
        });
        defer self.alloc.free(tmp_path);
        var temp_exists = true;
        defer if (temp_exists) deleteFilePortable(self.io(), tmp_path);

        {
            var file = try fs_paths.createFilePortable(self.io(), tmp_path, .{ .truncate = true });
            defer file.close(self.io());
            var writer_buffer: [4096]u8 = undefined;
            var writer = file.writer(self.io(), &writer_buffer);
            try writer.interface.writeAll(encoded.items);
            try writer.end();
            try file.sync(self.io());
        }

        // This deterministic boundary models a crash after the replacement is
        // durable but before it is published. The old catalog must remain the
        // only visible snapshot, and the caller must roll back its live map.
        if (builtin.is_test and self.test_persist_failure_boundary == .before_publish)
            return error.TestCatalogPersistBeforePublish;

        if (std.fs.path.isAbsolute(self.path))
            try std.Io.Dir.renameAbsolute(tmp_path, self.path, self.io())
        else
            try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), self.path, self.io());
        temp_exists = false;
        published.* = true;

        if (builtin.is_test and self.test_persist_failure_boundary == .after_publish)
            return error.TestCatalogPersistAfterPublish;

        // Once rename succeeds, the new snapshot is the committed in-process
        // state. Syncing the containing directory makes that name replacement
        // crash-durable on platforms that expose directory fsync.
        const sync_parent = parent_dir orelse if (std.fs.path.isAbsolute(self.path)) "/" else ".";
        try fs_paths.syncDirPortable(self.io(), sync_parent);
    }

    fn io(self: *FileReplicaCatalog) std.Io {
        return self.io_impl.io();
    }

    fn listOwned(self: *FileReplicaCatalog, alloc: std.mem.Allocator) ![]ReplicaRecord {
        var out = try alloc.alloc(ReplicaRecord, self.records.count());
        var i: usize = 0;
        errdefer {
            for (out[0..i]) |*record| record.deinit(alloc);
            alloc.free(out);
        }
        var it = self.records.valueIterator();
        while (it.next()) |record| : (i += 1) out[i] = try record.clone(alloc);
        return out;
    }
};

fn deleteFilePortable(io: std.Io, path: []const u8) void {
    if (std.fs.path.isAbsolute(path))
        std.Io.Dir.deleteFileAbsolute(io, path) catch {}
    else
        std.Io.Dir.cwd().deleteFile(io, path) catch {};
}

test "raft replica catalog storage module compiles" {
    _ = ReplicaBootstrapMode;
    _ = BackupRestoreBootstrapRecord;
    _ = ReplicaBootstrapSource;
    _ = SnapshotBootstrapRecord;
    _ = ReplicaRecord;
    _ = ReplicaCatalog;
    _ = MemoryReplicaCatalog;
    _ = FileReplicaCatalog;
    _ = freeReplicaRecords;
    _ = freeRuntimeBootstrap;
    _ = runtimeBootstrapFromRecord;
}

test "memory replica catalog stores and lists records" {
    var replica_catalog = MemoryReplicaCatalog.init(std.testing.allocator);
    defer replica_catalog.deinit();

    try replica_catalog.catalog().upsertReplica(.{
        .group_id = 11,
        .replica_id = 2,
        .local_node_id = 3,
    });
    const records = try replica_catalog.catalog().listReplicas(std.testing.allocator);
    defer freeReplicaRecords(std.testing.allocator, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqual(@as(u64, 11), records[0].group_id);
}

test "file replica catalog persists records across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/replica-catalog.json", .{tmp.sub_path});
    defer std.testing.allocator.free(path);

    {
        var replica_catalog = try FileReplicaCatalog.init(std.testing.allocator, path);
        defer replica_catalog.deinit();
        try replica_catalog.catalog().upsertReplica(.{
            .group_id = 21,
            .replica_id = 2,
            .local_node_id = 5,
            .bootstrap_mode = .fetch_snapshot,
            .metadata_version = 9,
            .snapshot_bootstrap = .{
                .from_node_id = 4,
                .term = 7,
                .snapshot_id = "snap-21",
                .uri = "http://127.0.0.1:7777/raft/v1/snapshot/fetch/snap-21",
            },
        });
    }

    {
        var reopened = try FileReplicaCatalog.init(std.testing.allocator, path);
        defer reopened.deinit();
        const records = try reopened.catalog().listReplicas(std.testing.allocator);
        defer freeReplicaRecords(std.testing.allocator, records);
        try std.testing.expectEqual(@as(usize, 1), records.len);
        try std.testing.expectEqual(@as(u64, 21), records[0].group_id);
        try std.testing.expectEqual(ReplicaBootstrapMode.fetch_snapshot, records[0].bootstrap_mode);
        try std.testing.expectEqual(@as(u64, 9), records[0].metadata_version);
        try std.testing.expect(records[0].snapshot_bootstrap != null);
        try std.testing.expectEqual(@as(u64, 4), records[0].snapshot_bootstrap.?.from_node_id);
        try std.testing.expectEqual(@as(u64, 7), records[0].snapshot_bootstrap.?.term);
        try std.testing.expectEqualStrings("snap-21", records[0].snapshot_bootstrap.?.snapshot_id);
    }
}

test "raft.storage file replica catalog crash before atomic publish preserves prior snapshot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/replica-catalog-atomic.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(path);

    {
        var replica_catalog = try FileReplicaCatalog.init(std.testing.allocator, path);
        defer replica_catalog.deinit();
        try replica_catalog.catalog().upsertReplica(.{
            .group_id = 21,
            .replica_id = 2,
            .local_node_id = 5,
            .metadata_version = 1,
        });

        replica_catalog.test_persist_failure_boundary = .before_publish;
        try std.testing.expectError(
            error.TestCatalogPersistBeforePublish,
            replica_catalog.catalog().upsertReplica(.{
                .group_id = 22,
                .replica_id = 3,
                .local_node_id = 5,
                .metadata_version = 2,
            }),
        );

        const live_records = try replica_catalog.catalog().listReplicas(std.testing.allocator);
        defer freeReplicaRecords(std.testing.allocator, live_records);
        try std.testing.expectEqual(@as(usize, 1), live_records.len);
        try std.testing.expectEqual(@as(u64, 21), live_records[0].group_id);
        try std.testing.expectEqual(@as(u64, 1), live_records[0].metadata_version);
    }

    var reopened = try FileReplicaCatalog.init(std.testing.allocator, path);
    defer reopened.deinit();
    const records = try reopened.catalog().listReplicas(std.testing.allocator);
    defer freeReplicaRecords(std.testing.allocator, records);

    // Atomic temp+fsync+rename persistence keeps the group-21 catalog visible
    // and never exposes group 22 after this crash boundary.
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqual(@as(u64, 21), records[0].group_id);
    try std.testing.expectEqual(@as(u64, 1), records[0].metadata_version);
}

test "raft.storage file replica catalog rolls back failed replacement and removal" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/replica-catalog-rollback.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(path);

    var replica_catalog = try FileReplicaCatalog.init(std.testing.allocator, path);
    defer replica_catalog.deinit();
    try replica_catalog.catalog().upsertReplica(.{
        .group_id = 31,
        .replica_id = 4,
        .local_node_id = 6,
        .metadata_version = 1,
    });

    replica_catalog.test_persist_failure_boundary = .before_publish;
    try std.testing.expectError(
        error.TestCatalogPersistBeforePublish,
        replica_catalog.catalog().upsertReplica(.{
            .group_id = 31,
            .replica_id = 4,
            .local_node_id = 6,
            .metadata_version = 2,
        }),
    );
    try std.testing.expectError(
        error.TestCatalogPersistBeforePublish,
        replica_catalog.catalog().removeReplica(31),
    );

    const live_records = try replica_catalog.catalog().listReplicas(std.testing.allocator);
    defer freeReplicaRecords(std.testing.allocator, live_records);
    try std.testing.expectEqual(@as(usize, 1), live_records.len);
    try std.testing.expectEqual(@as(u64, 31), live_records[0].group_id);
    try std.testing.expectEqual(@as(u64, 1), live_records[0].metadata_version);

    replica_catalog.test_persist_failure_boundary = null;
    try std.testing.expect(try replica_catalog.catalog().removeReplica(31));
}

test "raft.storage file replica catalog retries after publish before directory sync" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/replica-catalog-after-publish.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(path);

    var replica_catalog = try FileReplicaCatalog.init(std.testing.allocator, path);
    defer replica_catalog.deinit();
    replica_catalog.test_persist_failure_boundary = .after_publish;
    const record = ReplicaRecord{ .group_id = 41, .replica_id = 5, .local_node_id = 7, .metadata_version = 1 };
    try std.testing.expectError(error.TestCatalogPersistAfterPublish, replica_catalog.catalog().upsertReplica(record));
    const failed_records = try replica_catalog.catalog().listReplicas(std.testing.allocator);
    defer freeReplicaRecords(std.testing.allocator, failed_records);
    try std.testing.expectEqual(@as(usize, 0), failed_records.len);

    replica_catalog.test_persist_failure_boundary = null;
    try replica_catalog.catalog().upsertReplica(record);
    const retried_records = try replica_catalog.catalog().listReplicas(std.testing.allocator);
    defer freeReplicaRecords(std.testing.allocator, retried_records);
    try std.testing.expectEqual(@as(usize, 1), retried_records.len);
}

test "file replica catalog persists backup restore bootstrap records across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/replica-catalog-restore.json", .{tmp.sub_path});
    defer std.testing.allocator.free(path);

    {
        var replica_catalog = try FileReplicaCatalog.init(std.testing.allocator, path);
        defer replica_catalog.deinit();
        try replica_catalog.catalog().upsertReplica(.{
            .group_id = 22,
            .replica_id = 3,
            .local_node_id = 6,
            .bootstrap_mode = .fetch_snapshot,
            .metadata_version = 10,
            .backup_restore_bootstrap = .{
                .backup_id = "snap-22",
                .location = "file:///tmp/backups",
                .snapshot_path = "snap-22/groups/22",
            },
        });
    }

    {
        var reopened = try FileReplicaCatalog.init(std.testing.allocator, path);
        defer reopened.deinit();
        const records = try reopened.catalog().listReplicas(std.testing.allocator);
        defer freeReplicaRecords(std.testing.allocator, records);
        try std.testing.expectEqual(@as(usize, 1), records.len);
        try std.testing.expect(records[0].backup_restore_bootstrap != null);
        try std.testing.expectEqualStrings("snap-22", records[0].backup_restore_bootstrap.?.backup_id);
        try std.testing.expectEqualStrings("file:///tmp/backups", records[0].backup_restore_bootstrap.?.location);
        try std.testing.expectEqualStrings("snap-22/groups/22", records[0].backup_restore_bootstrap.?.snapshot_path);
    }
}
