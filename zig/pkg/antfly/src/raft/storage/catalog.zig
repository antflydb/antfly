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
const platform_sync = @import("antfly_platform").sync;

const replica_catalog_header = "ANTFLY_REPLICA_CATALOG 1";
const max_replica_catalog_record_bytes = 64 * 1024;

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

fn nextRevision(current: u64) !u64 {
    if (current == std.math.maxInt(u64)) return error.ReplicaCatalogRevisionExhausted;
    return current + 1;
}

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
        var cloned = SnapshotBootstrapRecord{
            .from_node_id = self.from_node_id,
            .term = self.term,
            .snapshot_id = try alloc.dupe(u8, self.snapshot_id),
            .uri = "",
        };
        errdefer alloc.free(cloned.snapshot_id);
        cloned.uri = try alloc.dupe(u8, self.uri);
        return cloned;
    }

    pub fn deinit(self: *SnapshotBootstrapRecord, alloc: std.mem.Allocator) void {
        alloc.free(self.snapshot_id);
        alloc.free(self.uri);
        self.* = undefined;
    }

    pub fn toRuntime(self: SnapshotBootstrapRecord, alloc: std.mem.Allocator) !raft_engine.runtime.replica.SnapshotBootstrap {
        var runtime = raft_engine.runtime.replica.SnapshotBootstrap{
            .from = self.from_node_id,
            .term = self.term,
            .locator = .{
                .snapshot_id = try alloc.dupe(u8, self.snapshot_id),
                .uri = "",
            },
            .fetch_immediately = true,
        };
        errdefer alloc.free(runtime.locator.snapshot_id);
        runtime.locator.uri = try alloc.dupe(u8, self.uri);
        return runtime;
    }
};

pub const BackupRestoreBootstrapRecord = struct {
    backup_id: []const u8,
    location: []const u8,
    snapshot_path: []const u8,
    connection: []const u8 = "",
    artifact_size_bytes: u64 = 0,
    artifact_sha256: []const u8 = "",

    pub fn clone(self: BackupRestoreBootstrapRecord, alloc: std.mem.Allocator) !BackupRestoreBootstrapRecord {
        var cloned = BackupRestoreBootstrapRecord{
            .backup_id = "",
            .location = "",
            .snapshot_path = "",
            .connection = "",
            .artifact_size_bytes = self.artifact_size_bytes,
            .artifact_sha256 = "",
        };
        cloned.backup_id = try alloc.dupe(u8, self.backup_id);
        errdefer alloc.free(cloned.backup_id);
        cloned.location = try alloc.dupe(u8, self.location);
        errdefer alloc.free(cloned.location);
        cloned.snapshot_path = try alloc.dupe(u8, self.snapshot_path);
        errdefer alloc.free(cloned.snapshot_path);
        cloned.connection = try alloc.dupe(u8, self.connection);
        errdefer alloc.free(cloned.connection);
        cloned.artifact_sha256 = try alloc.dupe(u8, self.artifact_sha256);
        return cloned;
    }

    pub fn deinit(self: *BackupRestoreBootstrapRecord, alloc: std.mem.Allocator) void {
        alloc.free(self.backup_id);
        alloc.free(self.location);
        alloc.free(self.snapshot_path);
        alloc.free(self.connection);
        alloc.free(self.artifact_sha256);
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
        cloned.snapshot_bootstrap = null;
        cloned.backup_restore_bootstrap = null;
        if (self.snapshot_bootstrap) |record| {
            cloned.snapshot_bootstrap = try record.clone(alloc);
        }
        errdefer if (cloned.snapshot_bootstrap) |*record| record.deinit(alloc);
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
        if (!std.mem.eql(u8, backup.connection, other.connection)) return false;
        if (backup.artifact_size_bytes != other.artifact_size_bytes) return false;
        if (!std.mem.eql(u8, backup.artifact_sha256, other.artifact_sha256)) return false;
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
        revision: *const fn (ptr: *anyopaque) u64,
        apply_batch: *const fn (
            ptr: *anyopaque,
            expected_revision: u64,
            upserts: []const ReplicaRecord,
            removals: []const u64,
        ) anyerror!void,
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

    pub fn revision(self: ReplicaCatalog) u64 {
        return self.vtable.revision(self.ptr);
    }

    pub fn applyBatch(
        self: ReplicaCatalog,
        expected_revision: u64,
        upserts: []const ReplicaRecord,
        removals: []const u64,
    ) !void {
        return try self.vtable.apply_batch(self.ptr, expected_revision, upserts, removals);
    }
};

pub const MemoryReplicaCatalog = struct {
    alloc: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    current_revision: u64 = 1,
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
                .revision = revision,
                .apply_batch = applyBatch,
            },
        };
    }

    fn upsertReplica(ptr: *anyopaque, record: ReplicaRecord) !void {
        const self: *MemoryReplicaCatalog = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.records.getPtr(record.group_id)) |existing| {
            if (eqlReplicaRecord(existing.*, record)) return;
        }
        const next_revision = try nextRevision(self.current_revision);
        const owned = try record.clone(self.alloc);
        errdefer {
            var cleanup = owned;
            cleanup.deinit(self.alloc);
        }
        if (self.records.getPtr(record.group_id)) |existing| {
            existing.deinit(self.alloc);
            existing.* = owned;
            self.current_revision = next_revision;
            return;
        }
        try self.records.put(self.alloc, record.group_id, owned);
        self.current_revision = next_revision;
    }

    fn removeReplica(ptr: *anyopaque, group_id: u64) !bool {
        const self: *MemoryReplicaCatalog = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (!self.records.contains(group_id)) return false;
        const next_revision = try nextRevision(self.current_revision);
        const removed = self.records.fetchRemove(group_id) orelse unreachable;
        var record = removed.value;
        record.deinit(self.alloc);
        self.current_revision = next_revision;
        return true;
    }

    fn listReplicas(ptr: *anyopaque, alloc: std.mem.Allocator) ![]ReplicaRecord {
        const self: *MemoryReplicaCatalog = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return try cloneReplicaRecordsFromMap(alloc, &self.records);
    }

    fn revision(ptr: *anyopaque) u64 {
        const self: *MemoryReplicaCatalog = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.current_revision;
    }

    fn applyBatch(
        ptr: *anyopaque,
        expected_revision: u64,
        upserts: []const ReplicaRecord,
        removals: []const u64,
    ) !void {
        const self: *MemoryReplicaCatalog = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.current_revision != expected_revision) return error.ReplicaCatalogRevisionChanged;
        if (upserts.len == 0 and removals.len == 0) return;
        const next_revision = try nextRevision(self.current_revision);

        var next = try cloneReplicaMapFromMap(self.alloc, &self.records);
        errdefer deinitReplicaMap(self.alloc, &next);
        try applyReplicaBatchToMap(self.alloc, &next, upserts, removals);
        deinitReplicaMap(self.alloc, &self.records);
        self.records = next;
        self.current_revision = next_revision;
    }
};

pub const FileReplicaCatalog = struct {
    alloc: std.mem.Allocator,
    io_impl: std.Io.Threaded,
    path: []const u8,
    mutex: std.atomic.Mutex = .unlocked,
    current_revision: u64 = 1,
    records: std.AutoHashMapUnmanaged(u64, ReplicaRecord) = .empty,

    pub fn init(alloc: std.mem.Allocator, path: []const u8) !FileReplicaCatalog {
        var self = FileReplicaCatalog{
            .alloc = alloc,
            .io_impl = std.Io.Threaded.init(alloc, .{}),
            .path = try alloc.dupe(u8, path),
        };
        errdefer {
            deinitReplicaMap(alloc, &self.records);
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
                .revision = revision,
                .apply_batch = applyBatch,
            },
        };
    }

    fn upsertReplica(ptr: *anyopaque, record: ReplicaRecord) !void {
        const self: *FileReplicaCatalog = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.records.getPtr(record.group_id)) |existing| {
            if (eqlReplicaRecord(existing.*, record)) return;
        }
        const next_revision = try nextRevision(self.current_revision);
        var owned = try record.clone(self.alloc);
        var map_owns_record = false;
        defer if (!map_owns_record) owned.deinit(self.alloc);

        const entry = try self.records.getOrPut(self.alloc, record.group_id);
        if (entry.found_existing) {
            var previous = entry.value_ptr.*;
            entry.value_ptr.* = owned;
            map_owns_record = true;
            self.persist() catch |err| {
                entry.value_ptr.* = previous;
                map_owns_record = false;
                return err;
            };
            previous.deinit(self.alloc);
            self.current_revision = next_revision;
        } else {
            entry.value_ptr.* = owned;
            map_owns_record = true;
            self.persist() catch |err| {
                _ = self.records.fetchRemove(record.group_id) orelse unreachable;
                map_owns_record = false;
                return err;
            };
            self.current_revision = next_revision;
        }
    }

    fn removeReplica(ptr: *anyopaque, group_id: u64) !bool {
        const self: *FileReplicaCatalog = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (!self.records.contains(group_id)) return false;
        const next_revision = try nextRevision(self.current_revision);
        const removed = self.records.fetchRemove(group_id) orelse unreachable;
        self.persist() catch |err| {
            self.records.putAssumeCapacity(group_id, removed.value);
            return err;
        };
        var record = removed.value;
        record.deinit(self.alloc);
        self.current_revision = next_revision;
        return true;
    }

    fn listReplicas(ptr: *anyopaque, alloc: std.mem.Allocator) ![]ReplicaRecord {
        const self: *FileReplicaCatalog = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return try cloneReplicaRecordsFromMap(alloc, &self.records);
    }

    fn revision(ptr: *anyopaque) u64 {
        const self: *FileReplicaCatalog = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.current_revision;
    }

    fn applyBatch(
        ptr: *anyopaque,
        expected_revision: u64,
        upserts: []const ReplicaRecord,
        removals: []const u64,
    ) !void {
        const self: *FileReplicaCatalog = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.current_revision != expected_revision) return error.ReplicaCatalogRevisionChanged;
        if (upserts.len == 0 and removals.len == 0) return;
        const next_revision = try nextRevision(self.current_revision);

        var next = try cloneReplicaMapFromMap(self.alloc, &self.records);
        errdefer deinitReplicaMap(self.alloc, &next);
        try applyReplicaBatchToMap(self.alloc, &next, upserts, removals);

        var previous = self.records;
        self.records = next;
        self.persist() catch |err| {
            self.records = previous;
            return err;
        };
        deinitReplicaMap(self.alloc, &previous);
        self.current_revision = next_revision;
    }

    fn load(self: *FileReplicaCatalog) !void {
        var file = (if (std.fs.path.isAbsolute(self.path))
            std.Io.Dir.openFileAbsolute(self.io(), self.path, .{})
        else
            std.Io.Dir.cwd().openFile(self.io(), self.path, .{})) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer file.close(self.io());

        // Total catalog size is unbounded by design, but each independently
        // parsed record has the same limit enforced by persistence.
        var read_buffer: [max_replica_catalog_record_bytes + 1]u8 = undefined;
        var reader = file.reader(self.io(), &read_buffer);
        const header = (reader.interface.takeDelimiter('\n') catch |err| switch (err) {
            error.StreamTooLong => return error.InvalidReplicaCatalog,
            else => return err,
        }) orelse return error.InvalidReplicaCatalog;
        if (!std.mem.eql(u8, header, replica_catalog_header))
            return error.InvalidReplicaCatalog;

        while ((reader.interface.takeDelimiter('\n') catch |err| switch (err) {
            error.StreamTooLong => return error.ReplicaCatalogRecordTooLarge,
            else => return err,
        })) |line| {
            if (line.len == 0) continue;
            if (line.len > max_replica_catalog_record_bytes)
                return error.ReplicaCatalogRecordTooLarge;
            var parsed = std.json.parseFromSlice(ReplicaRecord, self.alloc, line, .{
                .allocate = .alloc_always,
            }) catch return error.InvalidReplicaCatalog;
            defer parsed.deinit();
            try validateReplicaRecord(parsed.value);
            var record = try parsed.value.clone(self.alloc);
            errdefer record.deinit(self.alloc);
            if (self.records.contains(record.group_id))
                return error.InvalidReplicaCatalog;
            try self.records.put(self.alloc, record.group_id, record);
        }
    }

    fn persist(self: *FileReplicaCatalog) !void {
        const parent_dir = std.fs.path.dirname(self.path);
        if (parent_dir) |dir| try fs_paths.createDirPathPortable(self.io(), dir);

        const records = try self.alloc.alloc(*const ReplicaRecord, self.records.count());
        defer self.alloc.free(records);
        var values = self.records.valueIterator();
        var count: usize = 0;
        while (values.next()) |record| : (count += 1) records[count] = record;
        std.debug.assert(count == records.len);
        std.mem.sort(*const ReplicaRecord, records, {}, struct {
            fn lessThan(_: void, lhs: *const ReplicaRecord, rhs: *const ReplicaRecord) bool {
                return lhs.group_id < rhs.group_id;
            }
        }.lessThan);
        try writeCatalogAtomicallyDurable(self.alloc, self.io(), self.path, records);
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

fn cloneReplicaRecordsFromMap(
    alloc: std.mem.Allocator,
    records: *const std.AutoHashMapUnmanaged(u64, ReplicaRecord),
) ![]ReplicaRecord {
    var out = try alloc.alloc(ReplicaRecord, records.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*record| record.deinit(alloc);
        alloc.free(out);
    }
    var it = records.valueIterator();
    while (it.next()) |record| : (initialized += 1) out[initialized] = try record.clone(alloc);
    return out;
}

fn cloneReplicaMapFromMap(
    alloc: std.mem.Allocator,
    records: *const std.AutoHashMapUnmanaged(u64, ReplicaRecord),
) !std.AutoHashMapUnmanaged(u64, ReplicaRecord) {
    var out = std.AutoHashMapUnmanaged(u64, ReplicaRecord).empty;
    errdefer deinitReplicaMap(alloc, &out);
    try out.ensureTotalCapacity(alloc, @intCast(records.count()));
    var it = records.valueIterator();
    while (it.next()) |record| {
        const owned = try record.clone(alloc);
        const entry = out.getOrPutAssumeCapacity(record.group_id);
        std.debug.assert(!entry.found_existing);
        entry.value_ptr.* = owned;
    }
    return out;
}

fn applyReplicaBatchToMap(
    alloc: std.mem.Allocator,
    records: *std.AutoHashMapUnmanaged(u64, ReplicaRecord),
    upserts: []const ReplicaRecord,
    removals: []const u64,
) !void {
    for (removals) |group_id| {
        const removed = records.fetchRemove(group_id) orelse continue;
        var record = removed.value;
        record.deinit(alloc);
    }
    try records.ensureUnusedCapacity(alloc, @intCast(upserts.len));
    for (upserts) |record| {
        var owned = try record.clone(alloc);
        const entry = records.getOrPutAssumeCapacity(record.group_id);
        if (entry.found_existing) entry.value_ptr.deinit(alloc);
        entry.value_ptr.* = owned;
        owned = undefined;
    }
}

fn deinitReplicaMap(
    alloc: std.mem.Allocator,
    records: *std.AutoHashMapUnmanaged(u64, ReplicaRecord),
) void {
    var it = records.valueIterator();
    while (it.next()) |record| record.deinit(alloc);
    records.deinit(alloc);
    records.* = .empty;
}

fn validateReplicaRecord(record: ReplicaRecord) !void {
    if (record.snapshot_bootstrap != null and record.backup_restore_bootstrap != null)
        return error.InvalidReplicaCatalog;
}

fn writeCatalogAtomicallyDurable(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    records: []const *const ReplicaRecord,
) !void {
    // A process-local counter can collide with a temp file left by a crash
    // after restart. A 128-bit random suffix keeps stale files harmless while
    // exclusive creation still protects against an unexpected collision.
    var entropy: [16]u8 = undefined;
    io.random(&entropy);
    const suffix = std.fmt.bytesToHex(entropy, .lower);

    if (std.fs.path.dirname(path)) |parent| try fs_paths.createDirPathPortable(io, parent);
    for (0..8) |attempt| {
        const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{s}-{d}", .{ path, &suffix, attempt });
        defer alloc.free(tmp_path);

        var file = fs_paths.createFilePortable(io, tmp_path, .{
            .truncate = true,
            .exclusive = true,
        }) catch |err| switch (err) {
            error.PathAlreadyExists => continue,
            else => return err,
        };
        var tmp_exists = true;
        defer if (tmp_exists) {
            if (std.fs.path.isAbsolute(tmp_path)) {
                std.Io.Dir.deleteFileAbsolute(io, tmp_path) catch {};
            } else {
                std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
            }
        };

        {
            defer file.close(io);
            var buf: [4096]u8 = undefined;
            var writer = file.writer(io, &buf);
            try writer.interface.writeAll(replica_catalog_header);
            try writer.interface.writeByte('\n');
            var record_buffer: [max_replica_catalog_record_bytes]u8 = undefined;
            for (records) |record| {
                try validateReplicaRecord(record.*);
                var record_writer = std.Io.Writer.fixed(&record_buffer);
                std.json.Stringify.value(record.*, .{}, &record_writer) catch |err| switch (err) {
                    error.WriteFailed => return error.ReplicaCatalogRecordTooLarge,
                };
                try writer.interface.writeAll(record_writer.buffered());
                try writer.interface.writeByte('\n');
            }
            try writer.end();
            try file.sync(io);
        }

        if (std.fs.path.isAbsolute(path)) {
            try std.Io.Dir.renameAbsolute(tmp_path, path, io);
        } else {
            try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
        }
        tmp_exists = false;
        try fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse ".");
        return;
    }
    return error.ReplicaCatalogTemporaryPathCollision;
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

test "memory replica catalog batch is revision fenced and publishes atomically" {
    var replica_catalog = MemoryReplicaCatalog.init(std.testing.allocator);
    defer replica_catalog.deinit();
    const iface = replica_catalog.catalog();

    try iface.upsertReplica(.{ .group_id = 11, .replica_id = 1, .local_node_id = 3 });
    const revision = iface.revision();
    try iface.applyBatch(revision, &.{
        .{ .group_id = 12, .replica_id = 2, .local_node_id = 3 },
        .{ .group_id = 13, .replica_id = 3, .local_node_id = 3 },
    }, &.{11});
    try std.testing.expectEqual(revision + 1, iface.revision());

    try std.testing.expectError(
        error.ReplicaCatalogRevisionChanged,
        iface.applyBatch(revision, &.{.{ .group_id = 14, .replica_id = 4, .local_node_id = 3 }}, &.{}),
    );
    const records = try iface.listReplicas(std.testing.allocator);
    defer freeReplicaRecords(std.testing.allocator, records);
    try std.testing.expectEqual(@as(usize, 2), records.len);
    for (records) |record| {
        try std.testing.expect(record.group_id == 12 or record.group_id == 13);
    }
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

test "file replica catalog reopens catalogs larger than one MiB" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/replica-catalog-large", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const uri = try std.testing.allocator.alloc(u8, 1100);
    defer std.testing.allocator.free(uri);
    @memset(uri, 'x');
    const upserts = try std.testing.allocator.alloc(ReplicaRecord, 1000);
    defer std.testing.allocator.free(upserts);
    for (upserts, 0..) |*record, i| {
        record.* = .{
            .group_id = @intCast(i + 1),
            .replica_id = @intCast(i + 1001),
            .local_node_id = 5,
            .bootstrap_mode = .fetch_snapshot,
            .snapshot_bootstrap = .{
                .from_node_id = 4,
                .term = 7,
                .snapshot_id = "snapshot",
                .uri = uri,
            },
        };
    }

    {
        var replica_catalog = try FileReplicaCatalog.init(std.testing.allocator, path);
        defer replica_catalog.deinit();
        const iface = replica_catalog.catalog();
        try iface.applyBatch(iface.revision(), upserts, &.{});
    }

    var reopened = try FileReplicaCatalog.init(std.testing.allocator, path);
    defer reopened.deinit();
    const records = try reopened.catalog().listReplicas(std.testing.allocator);
    defer freeReplicaRecords(std.testing.allocator, records);
    try std.testing.expectEqual(upserts.len, records.len);
    for (records) |record| try std.testing.expectEqual(uri.len, record.snapshot_bootstrap.?.uri.len);
}

test "file replica catalog round trips escaped bootstrap fields" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/replica-catalog-escaped", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    {
        var replica_catalog = try FileReplicaCatalog.init(std.testing.allocator, path);
        defer replica_catalog.deinit();
        try replica_catalog.catalog().upsertReplica(.{
            .group_id = 23,
            .replica_id = 4,
            .local_node_id = 6,
            .bootstrap_mode = .fetch_snapshot,
            .snapshot_bootstrap = .{
                .from_node_id = 8,
                .term = 9,
                .snapshot_id = "snapshot with spaces\nand a newline",
                .uri = "file:///tmp/snapshot path?q=hello world",
            },
        });
    }

    var reopened = try FileReplicaCatalog.init(std.testing.allocator, path);
    defer reopened.deinit();
    const records = try reopened.catalog().listReplicas(std.testing.allocator);
    defer freeReplicaRecords(std.testing.allocator, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqualStrings(
        "snapshot with spaces\nand a newline",
        records[0].snapshot_bootstrap.?.snapshot_id,
    );
    try std.testing.expectEqualStrings(
        "file:///tmp/snapshot path?q=hello world",
        records[0].snapshot_bootstrap.?.uri,
    );
}

test "file replica catalog rejects records its loader cannot reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/replica-catalog-oversized", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const oversized_uri = try std.testing.allocator.alloc(u8, max_replica_catalog_record_bytes);
    defer std.testing.allocator.free(oversized_uri);
    @memset(oversized_uri, 'x');

    {
        var replica_catalog = try FileReplicaCatalog.init(std.testing.allocator, path);
        defer replica_catalog.deinit();
        const iface = replica_catalog.catalog();
        const revision_before = iface.revision();
        try std.testing.expectError(error.ReplicaCatalogRecordTooLarge, iface.upsertReplica(.{
            .group_id = 24,
            .replica_id = 5,
            .local_node_id = 7,
            .bootstrap_mode = .fetch_snapshot,
            .snapshot_bootstrap = .{
                .from_node_id = 9,
                .snapshot_id = "snapshot",
                .uri = oversized_uri,
            },
        }));
        try std.testing.expectEqual(revision_before, iface.revision());
        const records = try iface.listReplicas(std.testing.allocator);
        defer freeReplicaRecords(std.testing.allocator, records);
        try std.testing.expectEqual(@as(usize, 0), records.len);
    }

    var reopened = try FileReplicaCatalog.init(std.testing.allocator, path);
    defer reopened.deinit();
    const records = try reopened.catalog().listReplicas(std.testing.allocator);
    defer freeReplicaRecords(std.testing.allocator, records);
    try std.testing.expectEqual(@as(usize, 0), records.len);
}

test "file replica catalog rejects duplicate groups without leaking loaded records" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/replica-catalog-duplicate", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{
        .sub_path = path,
        .data =
        \\ANTFLY_REPLICA_CATALOG 1
        \\{"group_id":21,"replica_id":2,"local_node_id":5,"bootstrap_mode":"persisted","metadata_version":9,"snapshot_bootstrap":null,"backup_restore_bootstrap":null}
        \\{"group_id":21,"replica_id":3,"local_node_id":5,"bootstrap_mode":"persisted","metadata_version":10,"snapshot_bootstrap":null,"backup_restore_bootstrap":null}
        \\
        ,
    });

    try std.testing.expectError(
        error.InvalidReplicaCatalog,
        FileReplicaCatalog.init(std.testing.allocator, path),
    );
}

test "file replica catalog rejects an existing truncated empty file" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/replica-catalog-empty", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{
        .sub_path = path,
        .data = "",
    });

    try std.testing.expectError(
        error.InvalidReplicaCatalog,
        FileReplicaCatalog.init(std.testing.allocator, path),
    );
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

test "file replica catalog rolls back failed durable upserts" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/replica-catalog.json", .{tmp.sub_path});
    defer std.testing.allocator.free(catalog_path);
    const unwritable_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer std.testing.allocator.free(unwritable_path);

    var replica_catalog = try FileReplicaCatalog.init(std.testing.allocator, catalog_path);
    defer replica_catalog.deinit();
    try replica_catalog.catalog().upsertReplica(.{
        .group_id = 23,
        .replica_id = 1,
        .local_node_id = 2,
        .metadata_version = 3,
    });

    std.testing.allocator.free(replica_catalog.path);
    replica_catalog.path = try std.testing.allocator.dupe(u8, unwritable_path);
    try std.testing.expectError(error.IsDir, replica_catalog.catalog().upsertReplica(.{
        .group_id = 23,
        .replica_id = 4,
        .local_node_id = 5,
        .metadata_version = 6,
    }));
    replica_catalog.catalog().upsertReplica(.{
        .group_id = 24,
        .replica_id = 7,
        .local_node_id = 8,
    }) catch |err| try std.testing.expect(err == error.IsDir);

    {
        const records = try replica_catalog.catalog().listReplicas(std.testing.allocator);
        defer freeReplicaRecords(std.testing.allocator, records);
        try std.testing.expectEqual(@as(usize, 1), records.len);
        try std.testing.expectEqual(@as(u64, 23), records[0].group_id);
        try std.testing.expectEqual(@as(u64, 1), records[0].replica_id);
        try std.testing.expectEqual(@as(u64, 3), records[0].metadata_version);
    }

    std.testing.allocator.free(replica_catalog.path);
    replica_catalog.path = try std.testing.allocator.dupe(u8, catalog_path);
    try replica_catalog.catalog().upsertReplica(.{
        .group_id = 23,
        .replica_id = 9,
        .local_node_id = 2,
        .metadata_version = 10,
    });
}
