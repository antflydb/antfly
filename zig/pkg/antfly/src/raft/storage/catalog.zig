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
const raft_engine = @import("raft_engine");

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
        } else {
            try self.records.put(self.alloc, record.group_id, owned);
        }
        try self.persist();
    }

    fn removeReplica(ptr: *anyopaque, group_id: u64) !bool {
        const self: *FileReplicaCatalog = @ptrCast(@alignCast(ptr));
        const removed = self.records.remove(group_id);
        if (removed) try self.persist();
        return removed;
    }

    fn listReplicas(ptr: *anyopaque, alloc: std.mem.Allocator) ![]ReplicaRecord {
        const self: *FileReplicaCatalog = @ptrCast(@alignCast(ptr));
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

    fn persist(self: *FileReplicaCatalog) !void {
        const parent_dir = std.fs.path.dirname(self.path);
        if (parent_dir) |dir| try fs_paths.createDirPathPortable(self.io(), dir);

        const records = try self.listOwned(self.alloc);
        defer freeReplicaRecords(self.alloc, records);
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

        try std.Io.Dir.cwd().writeFile(self.io(), .{
            .sub_path = self.path,
            .data = encoded.items,
        });
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
